--
-- E-Maj: migration from 4.5.0 to <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- Complain if this script is executed in psql, rather than via an ALTER EXTENSION statement.
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

------------------------------------
--                                --
-- Checks                         --
--                                --
------------------------------------
-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_txid                   TEXT;
    v_nbNoError              INT;
    v_nbWarning              INT;
  BEGIN
-- The current emaj version must be '4.5.0'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.5.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.5.0',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 11.
    IF current_setting('server_version_num')::int < 110000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 11.', current_setting('server_version');
    END IF;
-- Check the E-Maj environment state, if not yet done by a previous upgrade in the same transaction.
    SELECT current_setting('emaj.upgrade_verify_txid', TRUE) INTO v_txid;
    IF v_txid IS NULL OR v_txid <> txid_current()::TEXT THEN
      BEGIN
        SELECT count(msg) FILTER (WHERE msg = 'No error detected'),
               count(msg) FILTER (WHERE msg LIKE 'Warning:%')
          INTO v_nbNoError, v_nbWarning
          FROM emaj.emaj_verify_all() AS t(msg);
      EXCEPTION
-- Errors during the emaj_verify_all() execution are trapped. The emaj_verify_all() code may be incompatible with the current PG version.
        WHEN OTHERS THEN -- do nothing
      END;
      IF v_nbNoError = 0 THEN
        RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. '
                        'You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details. '
                        'An "ALTER EXTENSION emaj UPDATE TO ''%'';" statement may be required before.', v_emajVersion;
      END IF;
      IF v_nbWarning > 0 THEN
        RAISE WARNING 'E-Maj upgrade: the E-Maj environment health check reports warning. '
                      'You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details.';
      END IF;
      IF v_nbWarning IS NOT NULL THEN
        PERFORM set_config('emaj.upgrade_verify_txid', txid_current()::TEXT, TRUE);
      END IF;
    END IF;

  END;
$do$;

-- OK, the upgrade operation can start...

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.5.0 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------


--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

------------------------------------
--                                --
-- emaj functions                 --
--                                --
------------------------------------
-- Recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script.


--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._delete_log_tbl(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT, p_lastGlobalSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_delete_log_tbl$
-- This function deletes the part of a log table corresponding to updates that have been rolled back.
-- The function is only called by emaj._rlbk_session_exec(), for unlogged rollbacks.
-- It deletes sequences records corresponding to marks that are not visible anymore after the rollback.
-- It also registers the hole in sequence numbers generated by the deleted log rows.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        begin and end time stamp ids to define the time range identifying the hole to create in the log sequence
--        global sequence value limit for rollback
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- Delete obsolete log rows
    EXECUTE format('DELETE FROM %I.%I WHERE emaj_gid > %s',
                   r_rel.rel_log_schema, r_rel.rel_log_table, p_lastGlobalSeq);
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- Record the sequence holes generated by the delete operation.
-- This is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group() function
--   (and indirectly by emaj_estimate_rollback_group() and emaj_estimate_rollback_groups()).
-- First delete, if exist, sequence holes that have disappeared with the rollback.
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema
        AND sqhl_table = r_rel.rel_tblseq
        AND sqhl_begin_time_id >= p_beginTimeId
        AND sqhl_begin_time_id < p_endTimeId;
-- Then insert the new log sequence hole.
    INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)
      SELECT r_rel.rel_schema, r_rel.rel_tblseq, p_beginTimeId, p_endTimeId,
             emaj._get_log_sequence_last_value(r_rel.rel_log_schema, r_rel.rel_log_sequence) - tbl_log_seq_last_val
        FROM emaj.emaj_table
        WHERE tbl_schema = r_rel.rel_schema AND tbl_name = r_rel.rel_tblseq AND tbl_time_id = p_beginTimeId;
--
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_log_stat_tbl$
-- This function returns the number of log rows for a single table between 2 time stamps or between a time stamp and the current state.
-- It is called by various functions, when building log statistics, but also when setting or deleting a mark, rollbacking a group
--   or dumping changes.
-- These statistics are computed using the log sequence associated to each application table and holes in sequences recorded into
--   emaj_seq_hole.
-- Input: emaj_relation row corresponding to the appplication table to proccess, the time stamp ids defining the time range to examine
--        (a end time stamp id set to NULL indicates the current state)
-- Output: number of log rows between both marks for the table
  BEGIN
    IF p_endTimeId IS NULL THEN
-- Compute log rows between a mark and the current state.
      RETURN
           -- the current last value of the log sequence
          (SELECT emaj._get_log_sequence_last_value(r_rel.rel_log_schema, r_rel.rel_log_sequence))
           -- the log sequence last value at begin time id
        - (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_beginTimeId)
           -- sum of hole from the begin time to now
        - (SELECT coalesce(sum(sqhl_hole_size),0)
             FROM emaj.emaj_seq_hole
             WHERE sqhl_schema = r_rel.rel_schema
               AND sqhl_table = r_rel.rel_tblseq
               AND sqhl_begin_time_id >= p_beginTimeId);
    ELSE
-- Compute log rows between 2 marks.
      RETURN
           -- the log sequence last value at end time id
          (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_endTimeId)
           -- the log sequence last value at begin time id
        - (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_beginTimeId)
           -- sum of hole between begin time and end time
        - (SELECT coalesce(sum(sqhl_hole_size),0)
             FROM emaj.emaj_seq_hole
             WHERE sqhl_schema = r_rel.rel_schema
               AND sqhl_table = r_rel.rel_tblseq
               AND sqhl_begin_time_id >= p_beginTimeId
               AND sqhl_end_time_id <= p_endTimeId);
    END IF;
  END;
$_log_stat_tbl$;

CREATE OR REPLACE FUNCTION emaj._get_current_sequence_state(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql AS
$_get_current_sequence_state$
-- The function returns the current state of a single sequence.
-- Input: schema and sequence name,
--        time_id to set the sequ_time_id
-- Output: an emaj_sequence record
  DECLARE
    r_sequ                   emaj.emaj_sequence%ROWTYPE;
  BEGIN
    EXECUTE format('SELECT nspname, relname, %s, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                   '  FROM %I.%I sq,'
                   '       pg_catalog.pg_sequence s'
                   '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                   '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                   '  WHERE nspname = %L AND relname = %L',
                   coalesce(p_timeId, 0), p_schema, p_sequence, p_schema, p_sequence)
      INTO STRICT r_sequ;
    RETURN r_sequ;
  END;
$_get_current_sequence_state$;

CREATE OR REPLACE FUNCTION emaj._get_log_sequence_last_value(p_schema TEXT, p_sequence TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_get_log_sequence_last_value$
-- The function returns the last value state of a single log sequence.
-- If the sequence has not been called, it returns the previous value, the increment being always 1.
-- It first calls the undocumented but very efficient pg_sequence_last_value(oid) function.
-- If this pg_sequence_last_value() function returns NULL, meaning the is_called attribute is FALSE which is not a frequent case,
--   select the sequence itself.
-- Input: log schema and log sequence name,
-- Output: last_value
  DECLARE
    v_lastValue                BIGINT;
  BEGIN
    SELECT pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
    IF v_lastValue IS NULL THEN
-- The is_called attribute seems to be false, so reach the sequence itself.
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END as last_value FROM %I.%I',
                     p_schema, p_sequence)
        INTO STRICT v_lastValue;
    END IF;
    RETURN v_lastValue;
  END;
$_get_log_sequence_last_value$;

CREATE OR REPLACE FUNCTION emaj._get_app_sequence_last_value(p_schema TEXT, p_sequence TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_get_app_sequence_last_value$
-- The function returns the last value state of a single application sequence.
-- If the sequence has not been called, it returns the previous value defined as (last_value - increment).
-- It first calls the undocumented but very efficient pg_sequence_last_value(oid) function.
-- If this pg_sequence_last_value() function returns NULL, meaning the is_called attribute is FALSE which is not a frequent case,
--   select the sequence itself.
-- Input: schema and sequence name
-- Output: last_value
  DECLARE
    v_lastValue                BIGINT;
  BEGIN
    SELECT pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
    IF v_lastValue IS NULL THEN
-- The is_called attribute seems to be false, so reach the sequence itself.
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value -'
                     '         (SELECT seqincrement'
                     '            FROM pg_catalog.pg_sequence s'
                     '                 JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '                 JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '            WHERE nspname = %L AND relname = %L'
                     '         ) END as last_value FROM %I.%I',
                     p_schema, p_sequence, p_schema, p_sequence)
        INTO STRICT v_lastValue;
    END IF;
    RETURN v_lastValue;
  END;
$_get_app_sequence_last_value$;

CREATE OR REPLACE FUNCTION emaj._get_sequences_last_value(p_groupsIncludeFilter TEXT, p_groupsExcludeFilter TEXT,
                                                          p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                          p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                          OUT p_key TEXT, OUT p_value TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_get_sequences_last_value$
-- The function is used by the emajStat client and Emaj_web to monitor the recorded tables and/or sequences changes.
-- It returns in textual format:
--    - the last_value of selected log and application sequences,
--    - the last value of 2 emaj technical sequences to detect tables groups changes or marks set,
--    - the current timestamp as EPOCH
-- The function traps the execution error that may happen when table groups structure are changing.
-- In this case, it just returns a key set to 'error' with the SQLSTATE (typically XX000) as value.
-- This error trapping is the main reason for this function exists. Otherwise, the client could just execute the main query.
-- Input: include and exclude regexps to filter tables groups, tables and sequences.
-- Output: a set of (key, value) records.
  DECLARE
    v_stmt                   TEXT;
  BEGIN
-- Set the default value for NULL filters (to include all and exclude nothing).
    p_groupsIncludeFilter = coalesce(p_groupsIncludeFilter, '.*');
    p_groupsExcludeFilter = coalesce(p_groupsExcludeFilter, '');
    p_tablesIncludeFilter = coalesce(p_tablesIncludeFilter, '.*');
    p_tablesExcludeFilter = coalesce(p_tablesExcludeFilter, '');
    p_sequencesIncludeFilter = coalesce(p_sequencesIncludeFilter, '.*');
    p_sequencesExcludeFilter = coalesce(p_sequencesExcludeFilter, '');
-- Build the statement to execute.
    v_stmt = $$
      WITH filtered_group AS (
        SELECT group_name
          FROM emaj.emaj_group
          WHERE group_name ~ $1
            AND ($2 = '' OR group_name !~ $2)
        )
        SELECT 'current_epoch', extract('EPOCH' FROM statement_timestamp())::TEXT
      UNION ALL
        SELECT 'emaj.emaj_time_stamp_time_id_seq', last_value::TEXT FROM emaj.emaj_time_stamp_time_id_seq
      UNION ALL
        SELECT 'emaj.emaj_global_seq', last_value::TEXT FROM emaj.emaj_global_seq
    $$;
    IF p_tablesExcludeFilter != '.*' THEN
      v_stmt = v_stmt || $$
      UNION ALL
        SELECT rel_schema || '.' || rel_tblseq, emaj._get_log_sequence_last_value(rel_log_schema, rel_log_sequence)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'r'
           AND (rel_schema || '.' || rel_tblseq) ~ $3
           AND ($4 = '' OR (rel_schema || '.' || rel_tblseq) !~ $4)
      $$;
    END IF;
    IF p_sequencesExcludeFilter != '.*' THEN
      v_stmt = v_stmt || $$
      UNION ALL
        SELECT rel_schema || '.' || rel_tblseq, emaj._get_app_sequence_last_value(rel_schema, rel_tblseq)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'S'
           AND (rel_schema || '.' || rel_tblseq) ~ $5
           AND ($6 = '' OR (rel_schema || '.' || rel_tblseq) !~ $6)
      $$;
    END IF;
    BEGIN
      RETURN QUERY EXECUTE v_stmt
        USING p_groupsIncludeFilter, p_groupsExcludeFilter,
              p_tablesIncludeFilter, p_tablesExcludeFilter,
              p_sequencesIncludeFilter, p_sequencesExcludeFilter;
    EXCEPTION WHEN OTHERS THEN
-- If an error occurs, just return an error key with the SQLSTATE.
      RETURN QUERY SELECT 'error', SQLSTATE;
    END;
--
    RETURN;
  END;
$_get_sequences_last_value$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(p_groupName TEXT, p_dir TEXT, p_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_snap_group$
-- This function creates a file for each table and sequence belonging to the group.
-- For tables, these files contain all rows sorted on primary key.
-- For sequences, they contain a single row describing the sequence.
-- To do its job, the function performs COPY TO statement, with all default parameters.
-- For table without primary key, rows are sorted on all columns.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability:
--   - to create the directory (with proper permissions allowing the cluster to write into) before the emaj_snap_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name,
--        the absolute pathname of the directory where the files are to be created and the options to used in the COPY TO statements
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_colList                TEXT;
    v_pathName               TEXT;
    v_stmt                   TEXT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'BEGIN', p_groupName, p_dir);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the supplied directory is not null.
    IF p_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_group: The directory parameter cannot be NULL.';
    END IF;
-- Check the copy options parameter doesn't contain unquoted ; that could be used for sql injection.
    IF regexp_replace(p_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: The COPY options parameter format is invalid.';
    END IF;
-- For each table/sequence of the emaj_relation table.
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = p_groupName
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_pathName = emaj._build_path_name(p_dir, r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap');
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- It is a table.
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
--   Build the order by column list.
          IF EXISTS
               (SELECT 0
                  FROM pg_catalog.pg_class
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                       JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                  WHERE contype = 'p'
                    AND nspname = r_tblsq.rel_schema
                    AND relname = r_tblsq.rel_tblseq
               ) THEN
--   The table has a pkey.
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList
              FROM
                (SELECT attname
                   FROM pg_catalog.pg_attribute
                        JOIN pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
                   WHERE attnum = ANY (indkey)
                     AND indrelid = v_fullTableName::regclass
                     AND indisprimary
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          ELSE
--   The table has no pkey.
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList
              FROM
                (SELECT attname
                   FROM pg_catalog.pg_attribute
                   WHERE attrelid = v_fullTableName::regclass
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          END IF;
--   Dump the table
          v_stmt = format('(SELECT * FROM %s ORDER BY %s)', v_fullTableName, v_colList);
          PERFORM emaj._copy_to_file(v_stmt, v_pathName, p_copyOptions);
        WHEN 'S' THEN
-- It is a sequence.
          v_stmt = format('(SELECT relname, rel.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, rel.is_called'
                          '  FROM %I.%I rel,'
                          '       pg_catalog.pg_sequence s'
                          '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                          '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                          '  WHERE nspname = %L AND relname = %L)',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_schema, r_tblsq.rel_tblseq);
--    Dump the sequence properties.
          PERFORM emaj._copy_to_file(v_stmt, v_pathName, p_copyOptions);
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || transaction_timestamp()) || ')';
    PERFORM emaj._copy_to_file(v_stmt, p_dir || '/_INFO', NULL);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

--<end_functions>                                pattern used by the tool that extracts and insert the functions definition
------------------------------------------
--                                      --
-- event triggers and related functions --
--                                      --
------------------------------------------

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA emaj FROM PUBLIC;

GRANT ALL ON ALL TABLES IN SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL SEQUENCES IN SCHEMA emaj TO emaj_adm;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA emaj TO emaj_adm;

GRANT SELECT ON ALL TABLES IN SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA emaj TO emaj_viewer;
REVOKE SELECT ON TABLE emaj.emaj_param FROM emaj_viewer;

GRANT EXECUTE ON FUNCTION emaj._get_app_sequence_last_value(p_schema TEXT, p_sequence TEXT) TO emaj_viewer;

------------------------------------
--                                --
-- Complete the upgrade           --
--                                --
------------------------------------

-- Enable the event triggers.
DO
$tmp$
  DECLARE
    v_event_trigger_array    TEXT[];
  BEGIN
-- Build the event trigger names array from the pg_event_trigger table.
    SELECT coalesce(array_agg(evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- Call the _enable_event_triggers() function.
    PERFORM emaj._enable_event_triggers(v_event_trigger_array);
  END;
$tmp$;

-- Set comments for all internal functions, by directly inserting a row in the pg_description table for all emaj functions
-- that do not have yet a recorded comment.
INSERT INTO pg_catalog.pg_description (objoid, classoid, objsubid, description)
  SELECT pg_proc.oid, pg_class.oid, 0 , 'E-Maj internal function'
    FROM pg_catalog.pg_proc, pg_catalog.pg_class
    WHERE pg_class.relname = 'pg_proc'
      AND pg_proc.oid IN               -- list all emaj functions that do not have yet a comment in pg_description
       (SELECT pg_proc.oid
          FROM pg_catalog.pg_proc
               JOIN pg_catalog.pg_namespace ON (pronamespace=pg_namespace.oid)
               LEFT OUTER JOIN pg_catalog.pg_description ON (pg_description.objoid = pg_proc.oid
                                     AND classoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_proc')
                                     AND objsubid = 0)
          WHERE nspname = 'emaj' AND (proname LIKE E'emaj\\_%' OR proname LIKE E'\\_%')
            AND pg_description.description IS NULL
       );

-- Update the previous versions from emaj_version_hist and insert the new version into this same table.
-- The upgrade start time (as recorded in emaj_hist) is used as upper time bound of the previous version.
WITH start_time_data AS (
  SELECT hist_datetime AS start_time, clock_timestamp() - hist_datetime AS duration
    FROM emaj.emaj_hist
    ORDER BY hist_id DESC
    LIMIT 1
  ), updated_versions AS (
  UPDATE emaj.emaj_version_hist
    SET verh_time_range = TSTZRANGE(lower(verh_time_range), start_time, '[]')
    FROM start_time_data
    WHERE upper_inf(verh_time_range)
  )
  INSERT INTO emaj.emaj_version_hist (verh_version, verh_time_range, verh_install_duration)
    SELECT '<devel>', TSTZRANGE(clock_timestamp(), null, '[]'), duration
      FROM start_time_data;

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.5.0 completed');

-- Post installation checks.
DO
$tmp$
  DECLARE
  BEGIN
-- Check the max_prepared_transactions GUC value.
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback '
                    'is possible.', current_setting('max_prepared_transactions');
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
