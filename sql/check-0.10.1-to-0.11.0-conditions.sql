--
-- E-Maj: migration from 0.10.1 to 0.11.0
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script checks that the conditions required to migrate from 0.10.1 to 0.11.0 are met
--

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO NOTICE;

\echo 'Checking that conditions for an E-maj upgrade from version 0.10.1 to version 0.11.0 are met...'

-- Creation of a specific function to check the migration conditions are met.
CREATE or REPLACE FUNCTION emaj.tmp() 
RETURNS TEXT LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_emajVersion        TEXT;
    v_nbWarning          BIGINT := 0;
    v_nbNotice           BIGINT := 0;
    v_logTableName       TEXT;
    v_prev_mark_datetime TIMESTAMPTZ;
    v_prev_mark_name     TEXT;
    v_prev_mark_group    TEXT;
    v_prev_emaj_id       BIGINT;
    v_prev_emaj_verb     TEXT;
    v_prev_emaj_changed  TIMESTAMPTZ;
    v_prev_emaj_txid     BIGINT;
    v_emaj_id            BIGINT;
    v_emaj_changed       TIMESTAMPTZ;
    r_mark               RECORD;
    r_table              RECORD;
    r_log                RECORD;
    r_seq                RECORD;
  BEGIN
-- the emaj version registered in emaj_param must be '0.10.1'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '0.10.1' THEN
      RAISE EXCEPTION 'The current E-Maj version (%) is not 0.10.1',v_emajVersion;
    END IF;
-- Check that timestamps in emaj_mark and in all log tables are usable to order rows in order to assign global id.
-- Once sorted by mark_id, all marks must have increasing mark_datetime
    v_prev_mark_datetime = '-infinity';
    FOR r_mark IN
      SELECT mark_group, mark_name, mark_id, mark_datetime  FROM emaj.emaj_mark ORDER BY mark_id
      LOOP
      IF r_mark.mark_datetime < v_prev_mark_datetime THEN
        RAISE WARNING 'Time change detected between marks % for group % (%) and % for group % (%).',
           v_prev_mark_name, v_prev_mark_group, v_prev_mark_datetime, 
           r_mark.mark_name, r_mark.mark_group, r_mark.mark_datetime;
        v_nbWarning = v_nbWarning + 1;
      END IF;
      v_prev_mark_datetime = r_mark.mark_datetime;
      v_prev_mark_name = r_mark.mark_name;
      v_prev_mark_group = r_mark.mark_group;
    END LOOP;
-- Once sorted by emaj_id, all rows from log tables must have increasing emaj_changed
-- i.e. one can rely on emaj_changed to properly sort rows from all log tables and compute the new emaj_gid values
-- for all application tables belonging to created groups,
    FOR r_table IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_group FROM emaj.emaj_relation WHERE rel_kind = 'r'
        ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
      v_logTableName := r_table.rel_schema || '_' || r_table.rel_tblseq || '_log';
-- scan log table in emaj_id order and check emaj_changed is also increasing
-- (discarding the UPD NEW rows that can be inserted after another rows from another txid)
      v_prev_emaj_changed = '-infinity';
      FOR r_log IN
        EXECUTE 'SELECT emaj_id, emaj_changed, emaj_verb, emaj_txid FROM emaj.' || quote_ident(v_logTableName) || ' WHERE emaj_id % 2 = 1 ORDER BY emaj_id'
        LOOP
        IF r_log.emaj_changed < v_prev_emaj_changed THEN
-- time of the log row is less than the time the previous log row
          IF v_prev_emaj_verb <> 'UPD' OR v_prev_emaj_txid = r_log.emaj_txid 
              OR v_prev_emaj_changed - r_log.emaj_changed > '1 millisecond'::interval THEN
            RAISE WARNING 'In log table % (group %), negative time shift detected between emaj_id % (% at % txid %) and % (% at % txid %).',
              quote_literal(v_logTableName), r_table.rel_group, 
              v_prev_emaj_id, v_prev_emaj_verb, v_prev_emaj_changed, r_log.emaj_txid, 
              r_log.emaj_id, r_log.emaj_verb, r_log.emaj_changed, v_prev_emaj_txid;
            v_nbWarning = v_nbWarning + 1;
          ELSE
-- are considered as normal cases when an update and another statement for another tx id are involved with a short time interval 
            RAISE NOTICE 'In log table % (group %), negative time shift detected between emaj_id % (% at % txid %) and % (% at % txid %).',
              quote_literal(v_logTableName), r_table.rel_group, 
              v_prev_emaj_id, v_prev_emaj_verb, v_prev_emaj_changed, r_log.emaj_txid, 
              r_log.emaj_id, r_log.emaj_verb, r_log.emaj_changed, v_prev_emaj_txid;
            v_nbNotice = v_nbNotice + 1;
          END IF;
        END IF;
        v_prev_emaj_id = r_log.emaj_id;
        v_prev_emaj_verb = r_log.emaj_verb;
        v_prev_emaj_changed = r_log.emaj_changed;
        v_prev_emaj_txid = r_log.emaj_txid;
      END LOOP;
-- check for rare cases when time change occurred between a mark set and the next recorded log row.
-- get log sequence values at each mark
      FOR r_seq IN
        SELECT sequ_datetime, sequ_mark, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence
          WHERE sequ_schema = 'emaj' AND 
                sequ_name = r_table.rel_schema || '_' || r_table.rel_tblseq || '_log_emaj_id_seq'
          ORDER BY sequ_id
        LOOP
-- get the next log row to check if its timestamp is greater than the mark's timestamp
        EXECUTE 'SELECT emaj_id, emaj_changed FROM emaj.' || quote_ident(v_logTableName) || 
                ' WHERE (NOT ' || CASE WHEN r_seq.sequ_is_called THEN 'true' ELSE 'false' END || 
                             ' AND emaj_id = 1) OR ' ||
                ' ( ' || CASE WHEN r_seq.sequ_is_called THEN 'true' ELSE 'false' END || 
                ' AND emaj_id > ' || r_seq.sequ_last_val || ' + 1) LIMIT 1' 
                INTO v_emaj_id, v_emaj_changed;
        IF v_emaj_changed IS NOT NULL AND v_emaj_changed < r_seq.sequ_datetime THEN
          RAISE WARNING 'In log table % (group %), negative time shift detected between emaj_id % (%) and mark % (%).', 
            quote_literal(v_logTableName), r_table.rel_group, 
            v_emaj_id, v_emaj_changed, 
            r_seq.sequ_mark, r_seq.sequ_datetime;
          v_nbWarning = v_nbWarning + 1;
        END IF;
      END LOOP;
    END LOOP;
    IF v_nbWarning = 0 THEN
      RETURN ('This E-Maj environment can be migrated into 0.11.0.');
    ELSE
      RETURN ('This E-Maj environment can NOT be migrated into 0.11.0.');
    END IF;
  END;
$tmp$;

SELECT emaj.tmp();

DROP FUNCTION emaj.tmp();

