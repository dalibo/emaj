--
-- E-Maj: migration from 1.0.0 to <NEXT_VERSION>
--
-- This software is distributed under the GNU General Public License.
--
-- This script migrates an existing installation of E-Maj extension.
-- If version 1.0.0 has not been yet installed, simply use emaj.sql script. 
--

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO WARNING;
--SET client_min_messages TO NOTICE;
\echo 'E-maj upgrade from version 1.0.0 to version 1.0.1'
\echo 'Checking...'
------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Creation of a specific function to check the migration conditions are met.
-- The function generates an exception if at least one condition is not met.
CREATE or REPLACE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_emajVersion        TEXT;
  BEGIN
-- the emaj version registered in emaj_param must be '1.0.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '1.0.0' THEN
      RAISE EXCEPTION 'The current E-Maj version (%) is not 1.0.0',v_emajVersion;
    END IF;
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj installation: the current user (%) is not a superuser.', current_user;
    END IF;
--
    RETURN;
  END;
$tmp$;
SELECT emaj.tmp();
DROP FUNCTION emaj.tmp();

-- OK, upgrade...
\echo '... OK, Migration start...'

BEGIN TRANSACTION;

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

\echo 'Updating E-Maj internal objects ...'

------------------------------------
--                                --
-- emaj functions                 --
--                                --
------------------------------------

CREATE OR REPLACE FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT, v_unloggedRlbk BOOLEAN)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_groups_step6$
-- This is the sixth step of a rollback group processing. It recreates the previously deleted foreign keys and 'set immediate' the others.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_ts_start          TIMESTAMP;
    v_ts_end            TIMESTAMP;
    v_fullTableName     TEXT;
    v_logTriggerName    TEXT;
    v_rows              BIGINT;
    r_fk                RECORD;
    r_tbl               RECORD;
  BEGIN
-- set recorded foreign keys as IMMEDIATE
    FOR r_fk IN
-- get all recorded fk
      SELECT fk_schema, fk_table, fk_name
        FROM emaj.emaj_fk
        WHERE fk_action = 'set_fk_immediate' AND fk_groups = v_groupNames AND fk_session = v_session
        ORDER BY fk_schema, fk_table, fk_name
      LOOP
-- record the time at the alter table start
        SELECT clock_timestamp() INTO v_ts_start;
-- set the fkey constraint as immediate
        EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_name) || ' IMMEDIATE';
-- record the time after the alter table and insert FK creation duration into the emaj_rlbk_stat table
        SELECT clock_timestamp() INTO v_ts_end;
-- compute the total number of fk that has been checked.
-- (this is in fact overestimated because inserts in the referecing table and deletes in the referenced table should not be taken into account. But the required log table scan would be too costly).
        SELECT coalesce((
--   get the number of rollbacked rows in the referencing table (or 0 if not covered by emaj)
        SELECT rel_rows
          FROM emaj.emaj_relation
          WHERE rel_schema = r_fk.fk_schema AND rel_tblseq = r_fk.fk_table
               ), 0) + coalesce((
--   get the number of rollbacked rows in the referenced table (or 0 if not covered by emaj)
        SELECT rel_rows
          FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_namespace rn,
               pg_catalog.pg_class rt, emaj.emaj_relation r
          WHERE c.conname = r_fk.fk_name                                   -- constraint id (name + schema)
            AND c.connamespace = n.oid AND n.nspname = r_fk.fk_schema
            AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace
            AND rn.nspname = r.rel_schema AND rt.relname = r.rel_tblseq    -- join on groups table
               ), 0) INTO v_rows;
-- record the set_fk_immediate duration into the rollbacks statistics table
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration)
           VALUES ('set_fk_immediate', r_fk.fk_schema, r_fk.fk_name, v_ts_start, v_rows, v_ts_end - v_ts_start);
    END LOOP;
-- process foreign key recreation
    FOR r_fk IN
-- get all recorded fk to recreate, plus the number of rows of the related table as estimated by postgres (pg_class.reltuples)
      SELECT fk_schema, fk_table, fk_name, fk_def, pg_class.reltuples
        FROM emaj.emaj_fk, pg_catalog.pg_namespace, pg_catalog.pg_class
        WHERE fk_action = 'add_fk' AND
              fk_groups = v_groupNames AND fk_session = v_session AND                         -- restrictions
              pg_namespace.oid = relnamespace AND relname = fk_table AND nspname = fk_schema  -- joins
        ORDER BY fk_schema, fk_table, fk_name
      LOOP
-- record the time at the alter table start
        SELECT clock_timestamp() INTO v_ts_start;
-- ... recreate the foreign key
        EXECUTE 'ALTER TABLE ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_table) || ' ADD CONSTRAINT ' || quote_ident(r_fk.fk_name) || ' ' || r_fk.fk_def;
-- record the time after the alter table and insert FK creation duration into the emaj_rlbk_stat table
        SELECT clock_timestamp() INTO v_ts_end;
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration)
           VALUES ('add_fk', r_fk.fk_schema, r_fk.fk_name, v_ts_start, r_fk.reltuples, v_ts_end - v_ts_start);
    END LOOP;
-- if unlogged rollback., enable log triggers that had been previously disabled
    IF v_unloggedRlbk THEN
      FOR r_tbl IN
        SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_session = v_session AND rel_kind = 'r' AND rel_rows > 0
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
        v_fullTableName  := quote_ident(r_tbl.rel_schema) || '.' || quote_ident(r_tbl.rel_tblseq);
        v_logTriggerName := quote_ident(r_tbl.rel_schema || '_' || r_tbl.rel_tblseq || '_emaj_log_trg');
        EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
      END LOOP;
    END IF;
    RETURN;
  END;
$_rlbk_groups_step6$;

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------
-- revoke grants on all functions from PUBLIC
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT, v_unloggedRlbk BOOLEAN) FROM PUBLIC; 

-- and give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT, v_unloggedRlbk BOOLEAN) TO emaj_adm; 

-- and give appropriate rights on functions to emaj_viewer role
--GRANT EXECUTE ON FUNCTION emaj._build_log_seq_name(TEXT, TEXT) TO emaj_viewer;

------------------------------------
--                                --
-- commit migration               --
--                                --
------------------------------------

UPDATE emaj.emaj_param SET param_value_text = '1.0.1' WHERE param_key = 'emaj_version';

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 1.0.1', 'Migration from 1.0.0 completed');

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-Maj successfully migrated to 1.0.1'

