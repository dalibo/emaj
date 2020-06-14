--
-- E-Maj: migration from 3.3.0 to <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- complain if this script is executed in psql, rather than via an ALTER EXTENSION statement
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_groupList              TEXT;
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current user (%) is not a superuser.', current_user;
    END IF;
-- the emaj version registered in emaj_param must be '3.3.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '3.3.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 3.3.0',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.5
    IF current_setting('server_version_num')::int < 90500 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.5.', current_setting('server_version');
    END IF;
-- the E-Maj environment is not damaged
    PERFORM * FROM (SELECT * FROM emaj.emaj_verify_all()) AS t(msg) WHERE msg <> 'No error detected';
    IF FOUND THEN
      RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details.';
    END IF;
-- no existing group must have been created with a postgres version prior 8.4
    SELECT string_agg(group_name, ', ') INTO v_groupList FROM emaj.emaj_group
      WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                 to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804;
    IF v_groupList IS NOT NULL THEN
      RAISE EXCEPTION 'E-Maj upgrade: groups "%" have been created with a too old postgres version (< 8.4). Drop these groups before upgrading. ',v_groupList;
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- insert the upgrade begin record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 3.3.0 started');

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------


--
-- add created or recreated tables and sequences to the list of content to save by pg_dump
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
-- recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script


--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------
DROP FUNCTION IF EXISTS emaj._purge_hist();

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._create_tbl(v_schema TEXT, v_tbl TEXT, v_groupName TEXT, v_priority INT, v_logDatTsp TEXT,
                                            v_logIdxTsp TEXT, v_timeId BIGINT, v_groupIsRollbackable BOOLEAN, v_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table.
-- Input: the application table to process, the group to add it into, the priority and tablespaces attributes, the time id of the
--        operation, 2 booleans indicating whether the group is rollbackable and whether the group is currently in logging state.
-- The objects created in the log schema:
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_schemaPrefix           TEXT = 'emaj_';
    v_emajNamesPrefix        TEXT;
    v_baseLogTableName       TEXT;
    v_baseLogIdxName         TEXT;
    v_baseLogFnctName        TEXT;
    v_baseSequenceName       TEXT;
    v_logSchema              TEXT;
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_logIdxName             TEXT;
    v_logFnctName            TEXT;
    v_sequenceName           TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_rlbkColList            TEXT;
    v_rlbkPkColList          TEXT;
    v_rlbkPkConditions       TEXT;
    v_genColList             TEXT;
    v_genValList             TEXT;
    v_genSetList             TEXT;
    v_genPkConditions        TEXT;
    v_nbGenAlwaysIdentCol    INTEGER;
    v_attnum                 SMALLINT;
    v_alter_log_table_param  TEXT;
    v_stmt                   TEXT;
    v_triggerList            TEXT;
  BEGIN
-- the checks on the table properties are performed by the calling functions
-- build the prefix of all emaj object to create
    IF length(v_tbl) <= 50 THEN
-- for not too long table name, the prefix is the table name itself
      v_emajNamesPrefix = v_tbl;
    ELSE
-- for long table names (over 50 char long), compute the suffix to add to the first 50 characters (#1, #2, ...), by looking at the
-- existing names
      SELECT substr(v_tbl, 1, 50) || '#' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_emajNamesPrefix
        FROM
          (SELECT unnest(regexp_matches(substr(rel_log_table, 51),'#(\d+)'))::INT AS suffix
             FROM emaj.emaj_relation
             WHERE substr(rel_log_table, 1, 50) = substr(v_tbl, 1, 50)
          ) AS t;
    END IF;
-- build the name of emaj components associated to the application table (non schema qualified and not quoted)
    v_baseLogTableName     = v_emajNamesPrefix || '_log';
    v_baseLogIdxName       = v_emajNamesPrefix || '_log_idx';
    v_baseLogFnctName      = v_emajNamesPrefix || '_log_fnct';
    v_baseSequenceName     = v_emajNamesPrefix || '_log_seq';
-- build the different name for table, trigger, functions,...
    v_logSchema        = v_schemaPrefix || v_schema;
    v_fullTableName    = quote_ident(v_schema) || '.' || quote_ident(v_tbl);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogTableName);
    v_logIdxName       = quote_ident(v_baseLogIdxName);
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogFnctName);
    v_sequenceName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseSequenceName);
-- prepare TABLESPACE clauses for data and index
    v_dataTblSpace = coalesce('TABLESPACE ' || quote_ident(v_logDatTsp),'');
    v_idxTblSpace = coalesce('TABLESPACE ' || quote_ident(v_logIdxTsp),'');
-- create the log table: it looks like the application table, with some additional technical columns
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('CREATE TABLE %s (LIKE %s,'
                   '    emaj_verb      VARCHAR(3),'
                   '    emaj_tuple     VARCHAR(3),'
                   '    emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
                   '    emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
                   '    emaj_txid      BIGINT      DEFAULT txid_current(),'
                   '    emaj_user      VARCHAR(32) DEFAULT session_user'
                   '    ) %s',
                    v_logTableName, v_fullTableName, v_dataTblSpace);
-- get the attnum of the emaj_verb column
    SELECT attnum INTO STRICT v_attnum
      FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
        AND nspname = v_logSchema
        AND relname = v_baseLogTableName
        AND attname = 'emaj_verb';
-- adjust the log table structure with the alter_log_table parameter, if set
    SELECT param_value_text INTO v_alter_log_table_param FROM emaj.emaj_param WHERE param_key = ('alter_log_table');
    IF v_alter_log_table_param IS NOT NULL AND v_alter_log_table_param <> '' THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_alter_log_table_param);
    END IF;
-- create the index on the log table
    EXECUTE format('CREATE UNIQUE INDEX %s ON %s(emaj_gid, emaj_tuple)',
                    v_logIdxName, v_logTableName, v_idxTblSpace);
-- set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE format('ALTER TABLE ONLY %s CLUSTER ON %s',
                   v_logTableName, v_logIdxName);
-- remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables
    SELECT string_agg(action, ',') INTO v_stmt FROM (
      SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
        FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
          AND nspname = v_logSchema AND relname = v_baseLogTableName
          AND attnum > 0 AND attnum < v_attnum AND attisdropped = FALSE AND attnotnull) AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_stmt);
    END IF;
-- create the sequence associated to the log table
    EXECUTE format('CREATE SEQUENCE %s',
                   v_sequenceName);
-- create the log function and the log trigger
    PERFORM emaj._create_log_trigger_tbl(v_fullTableName, v_logTableName, v_sequenceName, v_logFnctName);
-- If the group is idle, deactivate the log trigger (it will be enabled at emaj_start_group time)
    IF NOT v_groupIsLogging THEN
      EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_log_trg',
                     v_fullTableName);
    END IF;
-- creation of the trigger that manage any TRUNCATE on the application table
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                   v_fullTableName);
    IF v_groupIsRollbackable THEN
-- For rollbackable groups, use the common _forbid_truncate_fnct() function that blocks the operation
      EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                     '  BEFORE TRUNCATE ON %s'
                     '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()',
                     v_fullTableName);
    ELSE
-- For audit_only groups, use the common _log_truncate_fnct() function that records the operation into the log table
      EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                     '  BEFORE TRUNCATE ON %s'
                     '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._log_truncate_fnct()',
                     v_fullTableName);
    END IF;
    IF NOT v_groupIsLogging THEN
      EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_trunc_trg',
                     v_fullTableName);
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                   v_logTableName);
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE %s TO emaj_adm',
                   v_logTableName);
    EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                   v_sequenceName);
    EXECUTE format('GRANT ALL PRIVILEGES ON SEQUENCE %s TO emaj_adm',
                   v_sequenceName);
-- Build some pieces of SQL statements that will be needed at table rollback and gen_sql times
--   left NULL if the table hos no pkey
    SELECT * FROM emaj._build_sql_tbl(v_fullTableName)
      INTO v_rlbkColList, v_rlbkPkColList, v_rlbkPkConditions, v_genColList,
           v_genValList, v_genSetList, v_genPkConditions, v_nbGenAlwaysIdentCol;
-- register the table into emaj_relation
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function, rel_emaj_verb_attnum, rel_has_always_ident_col,
                rel_sql_rlbk_columns, rel_sql_rlbk_pk_columns, rel_sql_rlbk_pk_conditions,
                rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions)
        VALUES (v_schema, v_tbl, int8range(v_timeId, NULL, '[)'), v_groupName, v_priority,
                v_logSchema, v_logDatTsp, v_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName, v_attnum, v_nbGenAlwaysIdentCol > 0,
                v_rlbkColList, v_rlbkPkColList, v_rlbkPkConditions,
                v_genColList, v_genValList, v_genSetList, v_genPkConditions);
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    SELECT string_agg(tgname, ', ' ORDER BY tgname) INTO v_triggerList FROM (
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'emaj\\_%\\_trg') AS t;
-- if yes, issue a warning
--   (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers (%). They will be automatically disabled during E-Maj rollback operations,'
                    ' unless they have been recorded into the list of triggers that may be kept enabled, with the'
                    ' emaj_ignore_app_trigger() function.', v_fullTableName, v_triggerList;
    END IF;
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark.
-- It also delete oldest rows in emaj_hist table.
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function,
--        boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and
-- sequences.
  DECLARE
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_eventTriggers          TEXT[];
    r_tblsq                  RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','),
              CASE WHEN v_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := 'IDLE')
      INTO v_groupNames;
    IF v_groupNames IS NOT NULL THEN
-- if there is at least 1 group to process, go on
-- check that no group is damaged
      PERFORM 0 FROM emaj._verify_groups(v_groupNames, TRUE);
-- check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(v_groupNames);
-- purge the history tables, if needed
      PERFORM emaj._purge_histories();
-- if requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs
      IF v_resetLog THEN
        PERFORM emaj._reset_groups(v_groupNames);
--    drop the log schemas that would have been emptied by the _reset_groups() call
        SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
        PERFORM emaj._drop_log_schemas(CASE WHEN v_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, FALSE);
        PERFORM emaj._enable_event_triggers(v_eventTriggers);
      END IF;
-- check the supplied mark name (the check must be performed after the _reset_groups() call to allow to reuse an old mark name that is
-- being deleted
      IF v_mark IS NULL OR v_mark = '' THEN
        v_mark = 'START_%';
      END IF;
      SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
-- OK, lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
--   risk of deadlock.
      PERFORM emaj._lock_groups(v_groupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
-- enable all log triggers for the groups
-- for each relation currently belonging to the group,
      FOR r_tblsq IN
         SELECT rel_kind, quote_ident(rel_schema) || '.' || quote_ident(rel_tblseq) AS full_relation_name FROM emaj.emaj_relation
           WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
          EXECUTE format('ALTER TABLE %s ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg',
                           r_tblsq.full_relation_name);
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
-- update the state of the group row from the emaj_group table
      UPDATE emaj.emaj_group SET group_is_logging = TRUE WHERE group_name = ANY (v_groupNames);
-- Set the first mark for each group
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, TRUE);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'END', array_to_string(v_groupNames,','),
              v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_delete_before_mark_group$
-- This function deletes all logs and marks set before a given mark.
-- The function is called by the emaj_delete_before_mark_group(), emaj_delete_mark_group() functions.
-- It deletes rows corresponding to the marks to delete from emaj_mark and emaj_sequence.
-- It deletes rows from emaj_relation corresponding to old versions that become unreacheable.
-- It deletes rows from all concerned log tables.
-- To complete, the function deletes oldest rows from emaj_hist.
-- Input: group name, name of the new first mark.
-- Output: number of deleted marks, number of tables effectively processed (for which at least one log row has been deleted)
  DECLARE
    v_eventTriggers          TEXT[];
    v_markGlobalSeq          BIGINT;
    v_markTimeId             BIGINT;
    v_nbMark                 INT;
    r_rel                    RECORD;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- retrieve the timestamp and the emaj_gid value and the time stamp id of the target new first mark
    SELECT time_last_emaj_gid, mark_time_id INTO v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_mark;
--
-- first process all obsolete time ranges for the group
--
-- drop obsolete old log tables
    FOR r_rel IN
          -- log tables for the group, whose end time stamp is older than the new first mark time stamp
          SELECT DISTINCT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r' AND rel_group = v_groupName AND upper(rel_time_range) <= v_markTimeId
        EXCEPT
          -- unless they are also used for more recent time range, or are also linked to other groups
          SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r'
              AND (upper(rel_time_range) > v_markTimeId OR upper_inf(rel_time_range) OR rel_group <> v_groupName)
          ORDER BY 1,2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- delete emaj_sequence rows corresponding to obsolete relation time range that will be deleted just later
-- (the related emaj_seq_hole rows will be deleted just later ; they are not directly linked to an emaj_relation row)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation r1
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper(rel_time_range) <= v_markTimeId
        AND (sequ_time_id < v_markTimeId                  -- all sequences prior the mark time
          OR (sequ_time_id = v_markTimeId                 -- and the sequence of the mark time
              AND NOT EXISTS (                            --   if it is not the lower bound of an adjacent time range
                SELECT 1 FROM emaj.emaj_relation r2
                  WHERE r2.rel_schema = r1.rel_log_schema AND r2.rel_tblseq = r1.rel_log_sequence
                    AND lower(r2.rel_time_range) = v_marktimeid)));
-- keep a trace of the relation group ownership history
--   and finaly delete from the emaj_relation table the relation that ended before the new first mark
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND upper(rel_time_range) <= v_markTimeId
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
        FROM deleted;
-- drop the E-Maj log schemas that are now useless (i.e. not used by any created group)
    PERFORM emaj._drop_log_schemas('DELETE_BEFORE_MARK_GROUP', FALSE);
--
-- then process the current relation time range for the group
--
-- delete rows from all log tables
    FOR r_rel IN
        SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r'
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
          ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
-- delete log rows prior to the new first mark
      EXECUTE format('DELETE FROM %s WHERE emaj_gid <= $1',
                     r_rel.log_table_name)
        USING v_markGlobalSeq;
    END LOOP;
-- process emaj_seq_hole content
-- delete all existing holes, if any, before the mark
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks)
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id < v_markTimeId;
-- now the sequences related to the mark to delete can be suppressed
--   delete first application sequences related data for the group
--   the sequence state at time range bounds are kept (if the mark comes from a logging group alter operation)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   delete then emaj sequences related data for the group
--   the sequence state at time range bounds are kept
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--    and that may have one of the deleted marks as target mark from a previous logged rollback operation
    UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = v_groupName AND mark_time_id >= v_markTimeId
        AND mark_logged_rlbk_target_mark IN (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_time_id < v_markTimeId
            );
-- delete oldest marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_time_id < v_markTimeId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- purge the history tables, if needed (even if no mark as been really dropped)
    PERFORM emaj._purge_histories();
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_colList                TEXT;
    v_fileName               TEXT;
    v_stmt                   TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'BEGIN', v_groupName, v_dir);
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := FALSE, v_checkList := '');
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_group: The directory parameter cannot be NULL.';
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: The COPY options parameter format is invalid.';
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range) AND rel_group = v_groupName
          ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_fileName = v_dir || '/' || translate(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap', E' /\\$<>*', '_______');
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- if it is a table,
--   first build the order by column list
          PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
                  contype = 'p' AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
          IF FOUND THEN
--   the table has a pkey,
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList FROM (
              SELECT attname FROM pg_catalog.pg_attribute, pg_catalog.pg_index
                WHERE pg_attribute.attrelid = pg_index.indrelid
                  AND attnum = ANY (indkey)
                  AND indrelid = v_fullTableName::regclass AND indisprimary
                  AND attnum > 0 AND attisdropped = FALSE) AS t;
          ELSE
--   the table has no pkey
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList FROM (
              SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = FALSE) AS t;
          END IF;
--   prepare the COPY statement
          v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO ' ||
                  quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
        WHEN 'S' THEN
-- if it is a sequence, the statement has no order by
          IF emaj._pg_version_num() >= 100000 THEN
            v_stmt = 'COPY (SELECT sequencename, rel.last_value, start_value, increment_by, max_value, '
                  || 'min_value, cache_size, cycle, rel.is_called '
                  || 'FROM ' || v_fullTableName || ' rel, pg_catalog.pg_sequences '
                  || 'WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = '
                  || quote_literal(r_tblsq.rel_tblseq) ||') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
          ELSE
            v_stmt = 'COPY (SELECT sequence_name, last_value, start_value, increment_by, max_value, '
                  || 'min_value, cache_value, is_cycled, is_called FROM ' || v_fullTableName
                  || ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
          END IF;
      END CASE;
-- and finaly perform the COPY
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj snap of tables group ' || v_groupName ||
            ' at ' || transaction_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_purge_histories(v_retentionDelay INTERVAL)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_purge_histories$
-- This function purges the emaj histories
-- The function is called by an emaj_adm user, typically an external scheduler.
-- It calls the _purge_histories() function.
-- Input: retention delay
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('PURGE_HISTORIES', 'BEGIN', 'Retention delay ' || v_retentionDelay);
-- effectively perform the purge
    PERFORM emaj._purge_histories(v_retentionDelay);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES ('PURGE_HISTORIES', 'END');
    RETURN;
  END;
$emaj_purge_histories$;
COMMENT ON FUNCTION emaj.emaj_purge_histories(INTERVAL) IS
$$Purges obsolete data from emaj history tables.$$;

CREATE OR REPLACE FUNCTION emaj._purge_histories(v_retentionDelay INTERVAL DEFAULT NULL)
RETURNS VOID LANGUAGE plpgsql AS
$_purge_histories$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter, but
--   without deleting event traces neither after the oldest active mark or after the oldest not committed or aborted rollback operation.
-- It also purges oldest rows from the emaj_exec_plan, emaj_rlbk_session and emaj_rlbk_plan tables, using the same rules.
-- The function is called at start group time and when oldest marks are deleted.
-- It is also called by the emaj_purge_histories() function.
-- A retention delay >= 100 years means infinite.
-- Input: retention delay ; if supplied, it overloads the history_retention parameter from the emaj_param table.
  DECLARE
    v_delay                  INTERVAL;
    v_datetimeLimit          TIMESTAMPTZ;
    v_maxTimeId              BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbPurgedHist           BIGINT;
    v_nbPurgedRelHist        BIGINT;
    v_nbPurgedRlbk           BIGINT;
    v_nbPurgedAlter          BIGINT;
    v_wording                TEXT = '';
  BEGIN
-- compute the retention delay to use
    SELECT coalesce(v_retentionDelay, (SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'),'1 YEAR')
      INTO v_delay;
-- immediately exit if the delay is infinity
    IF v_delay >= INTERVAL '100 years' THEN
      RETURN;
    END IF;
-- compute the timestamp limit
    SELECT least(
                                         -- compute the timestamp limit from the retention delay value
        (SELECT current_timestamp - v_delay),
                                         -- get the transaction timestamp of the oldest non deleted mark for all groups
        (SELECT min(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_mark
           WHERE time_id = mark_time_id AND NOT mark_is_deleted),
                                         -- get the transaction timestamp of the oldest non committed or aborted rollback
        (SELECT min(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_rlbk
           WHERE time_id = rlbk_time_id AND rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED')))
     INTO v_datetimeLimit;
-- get the greatest timestamp identifier corresponding to the timeframe to purge, if any
    SELECT max(time_id) INTO v_maxTimeId FROM emaj.emaj_time_stamp
      WHERE time_tx_timestamp < v_datetimeLimit;
-- delete oldest rows from emaj_hist
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < v_datetimeLimit;
    GET DIAGNOSTICS v_nbPurgedHist = ROW_COUNT;
    IF v_nbPurgedHist > 0 THEN
      v_wording = v_nbPurgedHist || ' emaj_hist rows deleted';
    END IF;
-- delete oldest rows from emaj_rel_hist
    DELETE FROM emaj.emaj_rel_hist WHERE upper(relh_time_range) < v_maxTimeId;
    GET DIAGNOSTICS v_nbPurgedRelHist = ROW_COUNT;
    IF v_nbPurgedRelHist > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedRelHist || ' relation history rows deleted';
    END IF;
-- purge the emaj_alter_plan table
    WITH deleted_alter AS (
      DELETE FROM emaj.emaj_alter_plan
        WHERE altr_time_id <= v_maxTimeId
        RETURNING altr_time_id
      )
      SELECT COUNT (DISTINCT altr_time_id) INTO v_nbPurgedAlter FROM deleted_alter;
    IF v_nbPurgedAlter > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedAlter || ' alter groups events deleted';
    END IF;
-- get the greatest rollback identifier to purge
    SELECT max(rlbk_id) INTO v_maxRlbkId FROM emaj.emaj_rlbk
      WHERE rlbk_time_id <= v_maxTimeId;
-- and purge the emaj_rlbk_plan and emaj_rlbk_session tables
    IF v_maxRlbkId IS NOT NULL THEN
      DELETE FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id <= v_maxRlbkId;
      WITH deleted_rlbk AS (
        DELETE FROM emaj.emaj_rlbk_session
          WHERE rlbs_rlbk_id <= v_maxRlbkId
          RETURNING rlbs_rlbk_id
        )
        SELECT COUNT (DISTINCT rlbs_rlbk_id) INTO v_nbPurgedRlbk FROM deleted_rlbk;
      v_wording = v_wording || ' ; ' || v_nbPurgedRlbk || ' rollback events deleted';
    END IF;
-- record the purge into the history if there are significant data
    IF v_wording <> '' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_wording)
        VALUES ('PURGE_HISTORIES', v_wording);
    END IF;
    RETURN;
  END;
$_purge_histories$;

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


------------------------------------
--                                --
-- complete the upgrade           --
--                                --
------------------------------------

-- enable the event triggers
DO
$tmp$
  DECLARE
    v_event_trigger_array    TEXT[];
  BEGIN
-- build the event trigger names array from the pg_event_trigger table
    SELECT coalesce(array_agg(evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- call the _enable_event_triggers() function
    PERFORM emaj._enable_event_triggers(v_event_trigger_array);
  END;
$tmp$;

-- Set comments for all internal functions,
-- by directly inserting a row in the pg_description table for all emaj functions that do not have yet a recorded comment
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

-- update the version id in the emaj_param table
ALTER TABLE emaj.emaj_param DISABLE TRIGGER emaj_param_change_trg;
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';
ALTER TABLE emaj.emaj_param ENABLE TRIGGER emaj_param_change_trg;

-- insert the upgrade end record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 3.3.0 completed');

-- post installation checks
DO
$tmp$
  DECLARE
  BEGIN
-- check the max_prepared_transactions GUC value
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback is possible.', current_setting('max_prepared_transactions');
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
