--
-- E-Maj: migration from 2.2.3 to <devel>
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
-- the emaj version registered in emaj_param must be '2.2.3'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '2.2.3' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 2.2.3',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.2
    IF current_setting('server_version_num')::int < 90200 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.2.', current_setting('server_version');
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
-- process the emaj_relation table
--
-- a new column is just added to the table
ALTER TABLE emaj.emaj_relation ADD COLUMN rel_log_seq_last_value BIGINT;
-- set a value only for tables that have been previously removed from their group
UPDATE emaj.emaj_relation
  SET rel_log_seq_last_value = CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END
  FROM emaj.emaj_sequence
  WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND sequ_time_id = upper(rel_time_range)
    AND rel_kind = 'r' AND NOT upper_inf(rel_time_range);

--
-- process the emaj_rlbk_plan table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_rlbk_plan_old (LIKE emaj.emaj_rlbk_plan);

INSERT INTO emaj_rlbk_plan_old SELECT * FROM emaj.emaj_rlbk_plan;

-- drop the old table
DROP TABLE emaj.emaj_rlbk_plan CASCADE;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
-- table containing rollback events
CREATE TABLE emaj.emaj_rlbk_plan (
  rlbp_rlbk_id                 INT         NOT NULL,       -- rollback id
  rlbp_step                    emaj._rlbk_step_enum
                                           NOT NULL,       -- kind of elementary step in the rollback processing
  rlbp_schema                  TEXT        NOT NULL,       -- schema object of the step
  rlbp_table                   TEXT        NOT NULL,       -- table name
  rlbp_fkey                    TEXT        NOT NULL,       -- foreign key name for step on foreign key, or ''
  rlbp_batch_number            INT,                        -- identifies a set of tables linked by foreign keys
  rlbp_session                 INT,                        -- session number the step is affected to
  rlbp_fkey_def                TEXT,                       -- foreign key definition used to recreate it, or NULL
  rlbp_target_time_id          BIGINT,                     -- for RLBK_TABLE and DELETE_LOG, time_id to rollback to, or NULL
  rlbp_estimated_quantity      BIGINT,                     -- for RLBK_TABLE, estimated number of updates to rollback
                                                           -- for DELETE_LOG, estimated number of rows to delete
                                                           -- for fkeys, estimated number of keys to check
  rlbp_estimated_duration      INTERVAL,                   -- estimated elapse time for the step processing
  rlbp_estimate_method         INT,                        -- method used to compute the estimated duration
                                                           --  1: use rollback stats with volume in same order of magnitude
                                                           --  2: use all previous rollback stats
                                                           --  3: use only parameters (from emaj_param or default values)
  rlbp_start_datetime          TIMESTAMPTZ,                -- clock start time of the step, NULL is not yet started
  rlbp_quantity                BIGINT,                     -- for RLBK_TABLE, number of effectively rolled back updates
                                                           -- for DELETE_LOG, number of effectively deleted log rows
                                                           -- null for fkeys
  rlbp_duration                INTERVAL,                   -- real elapse time of the step, NULL is not yet completed
  PRIMARY KEY (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey),
  FOREIGN KEY (rlbp_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_plan IS
$$Contains description of elementary steps for rollback operations.$$;

-- populate the new table
-- the new column is set to NULL
INSERT INTO emaj.emaj_rlbk_plan (
         rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
         rlbp_session, rlbp_fkey_def, rlbp_estimated_quantity, rlbp_estimated_duration,
         rlbp_estimate_method, rlbp_start_datetime, rlbp_quantity, rlbp_duration)
  SELECT rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
         rlbp_session, rlbp_fkey_def, rlbp_estimated_quantity, rlbp_estimated_duration,
         rlbp_estimate_method, rlbp_start_datetime, rlbp_quantity, rlbp_duration
    FROM emaj_rlbk_plan_old;

-- create indexes
-- recreate the foreign keys that point on this table
-- set the last value for the sequence associated to the serial column

--
-- add created or recreated tables and sequences to the list of content to save by pg_dump
--
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_plan','');

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

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._create_tbl(r_grpdef emaj.emaj_group_def, v_timeId BIGINT, v_isRollbackable BOOLEAN)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: the emaj_group_def row related to the application table to process, the time id of the operation, a boolean indicating whether the group is rollbackable
-- Are created in the log schema:
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
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
    v_colList                TEXT;
    v_pkColList              TEXT;
    v_pkCondList             TEXT;
    v_stmt                   TEXT;
    v_triggerList            TEXT;
  BEGIN
-- the checks on the table properties are performed by the calling functions
-- build the prefix of all emaj object to create, by default <schema>_<table>
    v_emajNamesPrefix = coalesce(r_grpdef.grpdef_emaj_names_prefix, r_grpdef.grpdef_schema || '_' || r_grpdef.grpdef_tblseq);
-- build the name of emaj components associated to the application table (non schema qualified and not quoted)
    v_baseLogTableName     = v_emajNamesPrefix || '_log';
    v_baseLogIdxName       = v_emajNamesPrefix || '_log_idx';
    v_baseLogFnctName      = v_emajNamesPrefix || '_log_fnct';
    v_baseSequenceName     = v_emajNamesPrefix || '_log_seq';
-- build the different name for table, trigger, functions,...
    v_logSchema        = coalesce(v_schemaPrefix || r_grpdef.grpdef_log_schema_suffix, v_emajSchema);
    v_fullTableName    = quote_ident(r_grpdef.grpdef_schema) || '.' || quote_ident(r_grpdef.grpdef_tblseq);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogTableName);
    v_logIdxName       = quote_ident(v_baseLogIdxName);
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogFnctName);
    v_sequenceName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseSequenceName);
-- prepare TABLESPACE clauses for data and index
    v_dataTblSpace = coalesce('TABLESPACE ' || quote_ident(r_grpdef.grpdef_log_dat_tsp),'');
    v_idxTblSpace = coalesce('TABLESPACE ' || quote_ident(r_grpdef.grpdef_log_idx_tsp),'');
-- Build some pieces of SQL statements that will be needed at table rollback time
--   build the tables's columns list
    SELECT string_agg(col_name, ',') INTO v_colList FROM (
      SELECT 'tbl.' || quote_ident(attname) AS col_name FROM pg_catalog.pg_attribute
        WHERE attrelid = v_fullTableName::regclass
          AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum) AS t;
--   build the pkey columns list and the "equality on the primary key" conditions
    SELECT string_agg(col_pk_name, ','), string_agg(col_pk_cond, ' AND ') INTO v_pkColList, v_pkCondList FROM (
      SELECT quote_ident(attname) AS col_pk_name,
             'tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname) AS col_pk_cond
        FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName::regclass AND indisprimary
          AND attnum > 0 AND attisdropped = FALSE
        ORDER BY attnum) AS t;
-- create the log table: it looks like the application table, with some additional technical columns
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName;
    EXECUTE 'CREATE TABLE ' || v_logTableName
         || ' (LIKE ' || v_fullTableName || ') ' || v_dataTblSpace;
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_verb      VARCHAR(3),'
         || ' ADD COLUMN emaj_tuple     VARCHAR(3),'
         || ' ADD COLUMN emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
         || ' ADD COLUMN emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
         || ' ADD COLUMN emaj_txid      BIGINT      DEFAULT txid_current(),'
         || ' ADD COLUMN emaj_user      VARCHAR(32) DEFAULT session_user,'
         || ' ADD COLUMN emaj_user_ip   INET        DEFAULT inet_client_addr(),'
         || ' ADD COLUMN emaj_user_port INT         DEFAULT inet_client_port()';
-- creation of the index on the log table
    EXECUTE 'CREATE UNIQUE INDEX ' || v_logIdxName || ' ON '
         ||  v_logTableName || ' (emaj_gid, emaj_tuple) ' || v_idxTblSpace;
-- set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE 'ALTER TABLE ONLY ' || v_logTableName || ' CLUSTER ON ' || v_logIdxName;
-- remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables
    SELECT string_agg(action, ',') INTO v_stmt FROM (
      SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
        FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
          AND nspname = v_logSchema AND relname = v_baseLogTableName
          AND attnum > 0 AND attnotnull AND attisdropped = FALSE AND attname NOT LIKE E'emaj\\_%') AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE 'ALTER TABLE ' || v_logTableName || v_stmt;
    END IF;
-- create the sequence associated to the log table
    EXECUTE 'CREATE SEQUENCE ' || v_sequenceName;
-- creation of the log fonction that will be mapped to the log trigger later
-- The new row is logged for each INSERT, the old row is logged for each DELETE
-- and the old and the new rows are logged for each UPDATE.
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || v_logFnctName || '() RETURNS TRIGGER AS $logfnct$'
         || 'BEGIN'
-- The sequence associated to the log table is incremented at the beginning of the function ...
         || '  PERFORM NEXTVAL(' || quote_literal(v_sequenceName) || ');'
-- ... and the global id sequence is incremented by the first/only INSERT into the log table.
         || '  IF (TG_OP = ''DELETE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''DEL'', ''OLD'';'
         || '    RETURN OLD;'
         || '  ELSIF (TG_OP = ''UPDATE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''UPD'', ''OLD'';'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''UPD'', ''NEW'', lastval();'
         || '    RETURN NEW;'
         || '  ELSIF (TG_OP = ''INSERT'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''INS'', ''NEW'';'
         || '    RETURN NEW;'
         || '  END IF;'
         || '  RETURN NULL;'
         || 'END;'
         || '$logfnct$ LANGUAGE plpgsql SECURITY DEFINER;';
-- creation of the log trigger on the application table, using the previously created log function
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
    EXECUTE 'CREATE TRIGGER emaj_log_trg'
         || ' AFTER INSERT OR UPDATE OR DELETE ON ' || v_fullTableName
         || '  FOR EACH ROW EXECUTE PROCEDURE ' || v_logFnctName || '()';
    EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_log_trg';
-- creation of the trigger that manage any TRUNCATE on the application table
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
    IF v_isRollbackable THEN
-- For rollbackable groups, use the common _forbid_truncate_fnct() function that blocks the operation
      EXECUTE 'CREATE TRIGGER emaj_trunc_trg'
           || ' BEFORE TRUNCATE ON ' || v_fullTableName
           || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()';
    ELSE
-- For audit_only groups, use the common _log_truncate_fnct() function that records the operation into the log table
      EXECUTE 'CREATE TRIGGER emaj_trunc_trg'
           || ' BEFORE TRUNCATE ON ' || v_fullTableName
           || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._log_truncate_fnct()';
    END IF;
    EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_trunc_trg';
-- register the table into emaj_relation
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function,
                rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions)
        VALUES (r_grpdef.grpdef_schema, r_grpdef.grpdef_tblseq, int8range(v_timeId, NULL, '[)'), r_grpdef.grpdef_group, r_grpdef.grpdef_priority,
                v_logSchema, r_grpdef.grpdef_log_dat_tsp, r_grpdef.grpdef_log_idx_tsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName,
                v_colList, v_pkColList, v_pkCondList);
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    SELECT string_agg(tgname, ', ' ORDER BY tgname) INTO v_triggerList FROM (
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'emaj\\_%\\_trg') AS t;
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers (%). Verify the compatibility with emaj rollback operations (in particular if triggers update one or several other tables). Triggers may have to be manualy disabled before rollback.', v_fullTableName, v_triggerList;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._add_tbl(r_plan emaj.emaj_alter_plan, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_add_tbl$
-- The function adds a table to a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _create_tbl() function.
-- Otherwise, it calls the _create_tbl() function, activates the log trigger and
--    sets a restart value for the log sequence if a previous range exists for the relation.
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group operation
-- The function is defined as SECURITY DEFINER so that emaj_adm role can enable triggers on application tables.
  DECLARE
    v_isRollbackable         BOOLEAN;
    v_logSchema              TEXT;
    v_logSequence            TEXT;
    v_nextVal                BIGINT;
    v_fullTableName          TEXT;
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
  BEGIN
-- get the is_rollbackable status of the related group
    SELECT group_is_rollbackable INTO v_isRollbackable
      FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- get the table description from emaj_group_def
    SELECT * INTO r_grpdef
      FROM emaj.emaj_group_def
      WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- create the table
    PERFORM emaj._create_tbl(r_grpdef, v_timeId, v_isRollbackable);
-- if the group is in logging state, perform additional tasks
    IF r_plan.altr_group_is_logging THEN
-- get the log schema and sequence for the new relation
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the last log sequence value, if any, for this relation (recorded in emaj_relation at a previous REMOVE_TBL operation)
      SELECT max(rel_log_seq_last_value) + 1 INTO v_nextVal FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq
          AND rel_log_seq_last_value IS NOT NULL;
-- ... set the new log sequence next_val, if needed
      IF v_nextVal IS NOT NULL AND v_nextVal > 1 THEN
        EXECUTE 'ALTER SEQUENCE ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logSequence) || ' RESTART ' || v_nextVal;
      END IF;
-- ... record the new log sequence state in the emaj_sequence table for the current alter_group mark
      INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                  sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
        SELECT * FROM emaj._get_current_sequence_state(v_logSchema, v_logSequence, v_timeId) AS
                      t(sequ_schema TEXT, sequ_name TEXT, sequ_time_id BIGINT, sequ_last_val BIGINT, sequ_start_val BIGINT, sequ_increment BIGINT, sequ_max_val BIGINT, sequ_min_val BIGINT, sequ_cache_val BIGINT, sequ_is_cycled BOOLEAN, sequ_is_called BOOLEAN);
-- ... activate the log and truncate triggers
      v_fullTableName  = quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq);
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg';
---- ... and insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('ALTER_GROUP', 'TABLE ADDED', v_fullTableName, 'To logging group ' || r_plan.altr_group);
    END IF;
    RETURN;
  END;
$_add_tbl$;

CREATE OR REPLACE FUNCTION emaj._remove_tbl(r_plan emaj.emaj_alter_plan, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_remove_tbl$
-- The function removes a table from a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _drop_tbl() function.
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group time for instance).
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group operation
-- The function is defined as SECURITY DEFINER so that emaj_adm role can drop triggers on application tables.
  DECLARE
    v_logSchema              TEXT;
    v_currentLogTable        TEXT;
    v_currentLogIndex        TEXT;
    v_logFunction            TEXT;
    v_logSequence            TEXT;
    v_logSequenceLastValue   BIGINT;
    v_namesSuffix            TEXT;
    v_fullTableName          TEXT;
  BEGIN
    IF NOT r_plan.altr_group_is_logging THEN
-- if the group is in idle state, drop the table immediately
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*) FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    ELSE
-- if the group is in logging state, ...
-- ... get the current relation characteristics
      SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_function, rel_log_sequence
        INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logFunction, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- ... get the current log sequence characteristics
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO STRICT v_logSequenceLastValue
        FROM emaj.emaj_sequence
        WHERE sequ_schema = v_logSchema AND sequ_name = v_logSequence AND sequ_time_id = v_timeId;
-- ... compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names
      SELECT '_'|| coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
        FROM
          (SELECT unnest(regexp_matches(rel_log_table,'_(\d+)$'))::INT AS suffix
             FROM emaj.emaj_relation
             WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq
          ) AS t;
-- ... rename the log table and its index
      EXECUTE 'ALTER TABLE ' || quote_ident(v_logSchema) || '.' || quote_ident(v_currentLogTable) ||
              ' RENAME TO '|| quote_ident(v_currentLogTable || v_namesSuffix);
      EXECUTE 'ALTER INDEX ' || quote_ident(v_logSchema) || '.' || quote_ident(v_currentLogIndex) ||
              ' RENAME TO '|| quote_ident(v_currentLogIndex || v_namesSuffix);
--TODO: share some code with _drop_tbl() ?
-- ... drop the log and truncate triggers (the application table is expected to exist)
      v_fullTableName  = quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq);
      EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
      EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
-- ... drop the log function and the log sequence
-- (but we keep the sequence related data in the emaj_sequence and the emaj_seq_hole tables)
      EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logFunction) || '() CASCADE';
      EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logSequence);
-- ... register the end of the relation time frame, the last value of the log sequence, the log table and index names change,
-- and reset the content of now useless columns
-- (but do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range), v_timeId, '[)'),
            rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix,
            rel_log_function = NULL, rel_sql_columns = NULL, rel_sql_pk_columns = NULL, rel_sql_pk_eq_conditions = NULL,
            rel_log_seq_last_value = v_logSequenceLastValue
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- ... and insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('ALTER_GROUP', 'TABLE REMOVED', quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq),
                'From logging group ' || r_plan.altr_group);
    END IF;
    RETURN;
  END;
$_remove_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
  BEGIN
    v_fullTableName    = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- if the table has been unlinked from its logging group, only the renamed log table has to be removed
    IF upper_inf(r_rel.rel_time_range) THEN
-- check the table exists before dropping its triggers
      PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = r_rel.rel_schema AND relname = r_rel.rel_tblseq AND relkind = 'r';
      IF FOUND THEN
-- drop the log and truncate triggers on the application table
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
      END IF;
-- drop the log function
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() CASCADE';
      END IF;
-- drop the sequence associated to the log table
      EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence);
    END IF;
-- drop the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
-- delete rows related to the log sequence from emaj_sequence table (it my delete rows for other not yet processed time_ranges for the same table)
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- delete rows related to the table from emaj_seq_hole table (it may delete rows for other not yet processed time_ranges for the same table)
    DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq;
-- and finaly delete the table reference from the emaj_relation table
    DELETE FROM emaj.emaj_relation WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range;
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj._add_seq(r_plan emaj.emaj_alter_plan, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_add_seq$
-- The function adds a sequence to a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _create_seq() function.
-- Otherwise, it calls the _create_seql() function, and record the current state of the sequence
-- Required inputs: row from emaj_alter_plan corresponding to the appplication sequence to proccess, time stamp id of the alter group operation
  DECLARE
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
  BEGIN
-- get the table description from emaj_group_def
    SELECT * INTO r_grpdef
      FROM emaj.emaj_group_def
      WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- create the table
    PERFORM emaj._create_seq(r_grpdef, v_timeId);
-- if the group is in logging state, perform additional tasks
    IF r_plan.altr_group_is_logging THEN
-- ... record the new sequence state in the emaj_sequence table for the current alter_group mark
      INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                  sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
        SELECT * FROM emaj._get_current_sequence_state(r_plan.altr_schema, r_plan.altr_tblseq, v_timeId) AS
                      t(sequ_schema TEXT, sequ_name TEXT, sequ_time_id BIGINT, sequ_last_val BIGINT, sequ_start_val BIGINT, sequ_increment BIGINT, sequ_max_val BIGINT, sequ_min_val BIGINT, sequ_cache_val BIGINT, sequ_is_cycled BOOLEAN, sequ_is_called BOOLEAN);
---- ... and insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('ALTER_GROUP', 'SEQUENCE ADDED', quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq),
                'To logging group ' || r_plan.altr_group);
    END IF;
    RETURN;
  END;
$_add_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: row from emaj_relation corresponding to the appplication sequence to proccess
  BEGIN
-- delete rows from emaj_sequence
-- if several rows exist in emaj_relation for the same sequence, due to removals and additions while the group was in logging state,
--   the first function call deletes all emaj_sequence rows for the sequence
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ' || quote_literal(r_rel.rel_schema) || ' AND sequ_name = ' || quote_literal(r_rel.rel_tblseq);
-- and finaly delete the sequence reference from the emaj_relation table
    DELETE FROM emaj.emaj_relation WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range;
    RETURN;
  END;
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark
-- The function is called by emaj.emaj._rlbk_end()
-- Input: the emaj_group_def row related to the application sequence to process, time id of the mark to rollback to
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it is not the owner of the application sequence.
  DECLARE
    v_fullSeqName            TEXT;
    v_stmt                   TEXT;
    mark_seq_rec             RECORD;
    curr_seq_rec             RECORD;
  BEGIN
-- Read sequence's characteristics at mark time
    BEGIN
      SELECT sequ_schema, sequ_name, sequ_last_val, sequ_start_val, sequ_increment,
             sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called
        INTO STRICT mark_seq_rec
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq AND sequ_time_id = v_timeId;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".', v_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END;
-- Read the current sequence's characteristics
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    IF emaj._pg_version_num() < 100000 THEN
      EXECUTE 'SELECT last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM '
               || v_fullSeqName
              INTO STRICT curr_seq_rec;
    ELSE
      EXECUTE 'SELECT rel.last_value, start_value, increment_by, max_value, min_value, cache_size as cache_value, cycle as is_cycled, rel.is_called FROM '
               || v_fullSeqName || ' rel, pg_catalog.pg_sequences '
               || 'WHERE schemaname = '|| quote_literal(r_rel.rel_schema) || ' AND sequencename = ' || quote_literal(r_rel.rel_tblseq)
              INTO STRICT curr_seq_rec;
    END IF;
-- Build the ALTER SEQUENCE statement, depending on the differences between the present values and the related
--   values at the requested mark time
    v_stmt='';
    IF curr_seq_rec.last_value <> mark_seq_rec.sequ_last_val OR
       curr_seq_rec.is_called <> mark_seq_rec.sequ_is_called THEN
      IF mark_seq_rec.sequ_is_called THEN
        v_stmt=v_stmt || ' RESTART ' || mark_seq_rec.sequ_last_val + mark_seq_rec.sequ_increment;
      ELSE
        v_stmt=v_stmt || ' RESTART ' || mark_seq_rec.sequ_last_val;
      END IF;
    END IF;
    IF curr_seq_rec.start_value <> mark_seq_rec.sequ_start_val THEN
      v_stmt=v_stmt || ' START ' || mark_seq_rec.sequ_start_val;
    END IF;
    IF curr_seq_rec.increment_by <> mark_seq_rec.sequ_increment THEN
      v_stmt=v_stmt || ' INCREMENT ' || mark_seq_rec.sequ_increment;
    END IF;
    IF curr_seq_rec.min_value <> mark_seq_rec.sequ_min_val THEN
      v_stmt=v_stmt || ' MINVALUE ' || mark_seq_rec.sequ_min_val;
    END IF;
    IF curr_seq_rec.max_value <> mark_seq_rec.sequ_max_val THEN
      v_stmt=v_stmt || ' MAXVALUE ' || mark_seq_rec.sequ_max_val;
    END IF;
    IF curr_seq_rec.cache_value <> mark_seq_rec.sequ_cache_val THEN
      v_stmt=v_stmt || ' CACHE ' || mark_seq_rec.sequ_cache_val;
    END IF;
    IF curr_seq_rec.is_cycled <> mark_seq_rec.sequ_is_cycled THEN
      IF mark_seq_rec.sequ_is_cycled = 'f' THEN
        v_stmt=v_stmt || ' NO ';
      END IF;
      v_stmt=v_stmt || ' CYCLE ';
    END IF;
-- and execute the statement if at least one parameter has changed

    IF v_stmt <> '' THEN
      EXECUTE 'ALTER SEQUENCE ' || v_fullSeqName || v_stmt;
    END IF;
-- insert event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
    RETURN;
  END;
$_rlbk_seq$;

CREATE OR REPLACE FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, v_beginTimeId BIGINT, v_endTimeId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_log_stat_tbl$
-- This function returns the number of log rows for a single table between 2 time stamps or between a time stamp and the current situation.
-- It is called by the emaj_log_stat_group(), _rlbk_planning(), _rlbk_start_mark() and _gen_sql_groups() functions.
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time or
-- rollback consolidation time.
-- Input: row from emaj_relation corresponding to the appplication table to proccess, the time stamp ids defining the time range to examine
--        (a end time stamp id set to NULL indicates the current situation)
-- Output: number of log rows between both marks for the table
  DECLARE
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- get the log table id at begin time id
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO STRICT v_beginLastValue
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_log_schema
        AND sequ_name = r_rel.rel_log_sequence
        AND sequ_time_id = v_beginTimeId;
    IF v_endTimeId IS NULL THEN
-- last time id is NULL, so examine the current state of the log table id
      IF emaj._pg_version_num() < 100000 THEN
        EXECUTE 'SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM '
             || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence) INTO v_endLastValue;
      ELSE
        EXECUTE 'SELECT CASE WHEN rel.is_called THEN rel.last_value ELSE rel.last_value - increment_by END FROM '
             || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)  || ' rel, pg_sequences'
             || ' WHERE schemaname = '|| quote_literal(r_rel.rel_log_schema) || ' AND sequencename = ' || quote_literal(r_rel.rel_log_sequence)
          INTO v_endLastValue;
      END IF;
--   and count the sum of hole from the start time to now
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_begin_time_id >= v_beginTimeId;
    ELSE
-- last time id is not NULL, so get the log table id at end time id
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_endLastValue
         FROM emaj.emaj_sequence
         WHERE sequ_schema = r_rel.rel_log_schema
           AND sequ_name = r_rel.rel_log_sequence
           AND sequ_time_id = v_endTimeId;
--   and count the sum of hole from the start time to the end time
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_begin_time_id >= v_beginTimeId AND sqhl_end_time_id <= v_endTimeId;
    END IF;
-- return the stat row for the table
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole);
  END;
$_log_stat_tbl$;

CREATE OR REPLACE FUNCTION emaj._get_current_sequence_state(v_schema TEXT, v_sequence TEXT, v_timeId BIGINT)
RETURNS RECORD LANGUAGE plpgsql AS
$_get_current_sequence_state$
-- The function returns the current state of a single sequence
-- Input: schema and sequence name,
--        time_id to set the sequ_time_id
-- Output: an emaj_sequence record
  DECLARE
    r_sequ                   emaj.emaj_sequence%ROWTYPE;
  BEGIN
    IF emaj._pg_version_num() < 100000 THEN
      EXECUTE 'SELECT '|| quote_literal(v_schema) || ', ' || quote_literal(v_sequence) || ', ' ||
               v_timeId || ', last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
              'FROM ' || quote_ident(v_schema) || '.' || quote_ident(v_sequence)
        INTO STRICT r_sequ;
    ELSE
      EXECUTE 'SELECT schemaname, sequencename, ' ||
               v_timeId || ', rel.last_value, start_value, increment_by, max_value, min_value, cache_size, cycle, rel.is_called ' ||
              'FROM ' || quote_ident(v_schema) || '.' || quote_ident(v_sequence) ||
              ' rel, pg_catalog.pg_sequences ' ||
              ' WHERE schemaname = '|| quote_literal(v_schema) || ' AND sequencename = ' || quote_literal(v_sequence)
        INTO STRICT r_sequ;
    END IF;
    RETURN r_sequ;
  END;
$_get_current_sequence_state$;

CREATE OR REPLACE FUNCTION emaj._get_current_sequences_state(v_groupNames TEXT[], v_relKind TEXT, v_timeId BIGINT)
RETURNS SETOF emaj.emaj_sequence LANGUAGE plpgsql AS
$_get_current_sequences_state$
-- The function returns the current state of all log or application sequences for a tables groups array
-- Input: group names array,
--        kind of relations ('r' for log sequences, 'S' for application sequences),
--        time_id to set the sequ_time_id (if the time id is NULL, get the greatest BIGINT value, i.e. 9223372036854775807)
-- Output: a set of records of type emaj_sequence, with a sequ_time_id set to the supplied v_timeId value
  DECLARE
    v_schema                 TEXT;
    v_sequence               TEXT;
    r_tblsq                  RECORD;
    r_sequ                   emaj.emaj_sequence%ROWTYPE;
  BEGIN
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_log_sequence FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = v_relKind
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
      IF v_relKind = 'r' THEN
        v_schema = r_tblsq.rel_log_schema;
        v_sequence = r_tblsq.rel_log_sequence;
      ELSE
        v_schema = r_tblsq.rel_schema;
        v_sequence = r_tblsq.rel_tblseq;
      END IF;
----TODO: call the _get_current_sequence_state() function instead of the 13 next lines
      IF emaj._pg_version_num() < 100000 THEN
        EXECUTE 'SELECT '|| quote_literal(v_schema) || ', ' || quote_literal(v_sequence) || ', ' ||
                 v_timeId || ', last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                'FROM ' || quote_ident(v_schema) || '.' || quote_ident(v_sequence)
          INTO STRICT r_sequ;
      ELSE
        EXECUTE 'SELECT schemaname, sequencename, ' ||
                 v_timeId || ', rel.last_value, start_value, increment_by, max_value, min_value, cache_size, cycle, rel.is_called ' ||
                'FROM ' || quote_ident(v_schema) || '.' || quote_ident(v_sequence) ||
                ' rel, pg_catalog.pg_sequences ' ||
                ' WHERE schemaname = '|| quote_literal(v_schema) || ' AND sequencename = ' || quote_literal(v_sequence)
          INTO STRICT r_sequ;
      END IF;
      RETURN NEXT r_sequ;
    END LOOP;
    RETURN;
  END;
$_get_current_sequences_state$;

CREATE OR REPLACE FUNCTION emaj._verify_groups(v_groups TEXT[], v_onErrorStop BOOLEAN)
RETURNS SETOF emaj._verify_groups_type LANGUAGE plpgsql AS
$_verify_groups$
-- The function verifies the consistency of a tables groups array.
-- Input: - tables groups array,
--        - a boolean indicating whether the function has to raise an exception in case of detected unconsistency.
-- If onErrorStop boolean is false, it returns a set of _verify_groups_type records, one row per detected unconsistency, including the faulting schema and table or sequence names and a detailed message.
-- If no error is detected, no row is returned.
  DECLARE
    v_hint                   TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected
-- check the postgres version: E-Maj needs postgres 9.2+
    IF emaj._pg_version_num() < 90200 THEN
      RAISE EXCEPTION '_verify_groups : The current postgres version (%) is not compatible with this E-Maj version. It should be at least 9.2.', version();
    END IF;
-- OK, now look for groups unconsistency
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist
-- check all application relations referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq,
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                    -- all relations currently belonging to the groups
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groups) AND upper_inf(rel_time_range)
            EXCEPT                                -- all relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (1): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log table for all tables referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups)
          AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_log_schema AND relname = rel_log_table
                   AND relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (2): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log function for each table referenced in the emaj_relation table still exists
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
                 WHERE nspname = rel_log_schema AND proname = rel_log_function
                   AND pronamespace = pg_namespace.oid)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (3): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_log_trg'
                   AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (4): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   then truncate trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
      WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_trunc_trg'
                   AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
      ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (5): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    FOR r_object IN
      WITH cte_app_tables_columns AS (                -- application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)),
           cte_log_tables_columns AS (                -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
              AND relname = rel_log_table
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range))
      SELECT DISTINCT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the structure of the application table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
             rel_log_schema || '"."' || rel_log_table || '").' AS msg
        FROM (
          (                                        -- application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM cte_app_tables_columns
          EXCEPT                                   -- minus log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM cte_log_tables_columns
          )
          UNION
          (                                         -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM cte_log_tables_columns
          EXCEPT                                    -- minus application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM cte_app_tables_columns
          )) AS t
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (6): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check all tables have their primary key if they belong to a rollbackable group
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
        FROM emaj.emaj_relation, emaj.emaj_group
        WHERE rel_group = group_name
          AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND group_is_rollbackable
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                 WHERE nspname = rel_schema AND relname = rel_tblseq
                   AND relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                   AND contype = 'p')
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (7): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check all tables are persistent tables (i.e. have not been altered as UNLOGGED after their tables group creation)
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND relpersistence <> 'p'
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (8): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check no table has been altered as WITH OIDS after tables groups creation
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is declared WITH OIDS.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND relhasoids
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the primary key structure of all tables belonging to rollbackable groups is unchanged
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
                   'In rollbackable group "' || rel_group || '", the primary key of the table "' ||
                   rel_schema || '"."' || rel_tblseq || '" has changed (' || rel_sql_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM (
          SELECT rel_schema, rel_tblseq, rel_group, rel_sql_pk_columns,
                 string_agg(quote_ident(attname), ',' ORDER BY attnum) AS current_pk_columns
            FROM emaj.emaj_relation, emaj.emaj_group, pg_catalog.pg_attribute, pg_catalog.pg_index, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE -- join conditions
                  rel_group = group_name
              AND relname = rel_tblseq AND nspname = rel_schema
              AND pg_attribute.attrelid = pg_index.indrelid
              AND indrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                  -- filter conditions
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND attnum = ANY (indkey)
              AND indisprimary
              AND attnum > 0 AND attisdropped = FALSE
            GROUP BY rel_schema, rel_tblseq, rel_group, rel_sql_pk_columns
          ) AS t
          WHERE rel_sql_pk_columns <> current_pk_columns
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (10): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- It also drops secondary schemas that are not useful any more
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that secondary schemas can be dropped
  DECLARE
    v_eventTriggers          TEXT[];
    v_nbTb                   INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- delete the emaj objets for each table of the group
    FOR r_rel IN
        SELECT * FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
      LOOP
        PERFORM CASE WHEN r_rel.rel_kind = 'r' THEN emaj._drop_tbl(r_rel)
                     WHEN r_rel.rel_kind = 'S' THEN emaj._drop_seq(r_rel) END;
    END LOOP;
-- drop the E-Maj secondary schemas that are now useless (i.e. not used by any other created group)
    PERFORM emaj._drop_log_schemas(CASE WHEN v_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END, v_isForced);
-- delete group row from the emaj_group table.
--   By cascade, it also deletes rows from emaj_mark
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName
      RETURNING group_nb_table + group_nb_sequence INTO v_nbTb;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj._alter_plan(v_groupNames TEXT[], v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_alter_plan$
-- This function build the elementary steps that will be needed to perform an alter_groups operation.
-- Looking at emaj_relation and emaj_group_def tables, it populates the emaj_alter_plan table that will be used by the _alter_exec() function.
-- Input: group names array, timestamp id of the operation (it will be used to identify rows in the emaj_alter_plan table)
  DECLARE
    v_schemaPrefix           TEXT = 'emaj';
    v_groups                 TEXT;
  BEGIN
-- determine the relations that do not belong to the groups anymore
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REMOVE_TBL' ELSE 'REMOVE_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, rel_priority
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames) AND upper_inf(rel_time_range)
          AND NOT EXISTS (
              SELECT NULL FROM emaj.emaj_group_def
                WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
                  AND grpdef_group = ANY (v_groupNames));
-- determine the tables that need to be "repaired" (damaged or out of sync E-Maj components)
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REPAIR_TBL' ELSE 'REPAIR_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, rel_priority
        FROM (                                   -- all damaged or out of sync tables
          SELECT DISTINCT ver_schema, ver_tblseq FROM emaj._verify_groups(v_groupNames, FALSE)
             ) AS t, emaj.emaj_relation
        WHERE rel_schema = ver_schema AND rel_tblseq = ver_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
--   exclude tables that will have been removed in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step IN ('REMOVE_TBL', 'REMOVE_SEQ'));
-- determine the groups that will be reset (i.e. those in IDLE state)
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'RESET_GROUP', '', '', group_name, NULL
        FROM emaj.emaj_group
        WHERE group_name = ANY (v_groupNames)
          AND NOT group_is_logging;
-- determine the tables whose log schema in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CHANGE_TBL_LOG_SCHEMA', rel_schema, rel_tblseq, rel_group, grpdef_priority
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND rel_log_schema <> (v_schemaPrefix || coalesce(grpdef_log_schema_suffix, ''))
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the tables whose emaj names prefix in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CHANGE_TBL_NAMES_PREFIX', rel_schema, rel_tblseq, rel_group, grpdef_priority
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND rel_log_table <> (coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) || '_log')
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the tables whose log data tablespace in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CHANGE_TBL_LOG_DATA_TSP', rel_schema, rel_tblseq, rel_group, grpdef_priority
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND coalesce(rel_log_dat_tsp,'') <> coalesce(grpdef_log_dat_tsp,'')
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the tables whose log data tablespace in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CHANGE_TBL_LOG_INDEX_TSP', rel_schema, rel_tblseq, rel_group, grpdef_priority
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND coalesce(rel_log_idx_tsp,'') <> coalesce(grpdef_log_idx_tsp,'')
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the relations that change their group ownership
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_group)
      SELECT v_timeId, 'ASSIGN_REL', rel_schema, rel_tblseq, rel_group, grpdef_priority, grpdef_group
      FROM emaj.emaj_relation, emaj.emaj_group_def
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
        AND rel_group = ANY (v_groupNames)
        AND grpdef_group = ANY (v_groupNames)
        AND rel_group <> grpdef_group;
-- determine the relation that change their priority level
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_priority)
      SELECT v_timeId, 'CHANGE_REL_PRIORITY', rel_schema, rel_tblseq, rel_group, rel_priority, grpdef_priority
      FROM emaj.emaj_relation, emaj.emaj_group_def
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
        AND rel_group = ANY (v_groupNames)
        AND grpdef_group = ANY (v_groupNames)
        AND ( (rel_priority IS NULL AND grpdef_priority IS NOT NULL) OR
              (rel_priority IS NOT NULL AND grpdef_priority IS NULL) OR
              (rel_priority <> grpdef_priority) );
-- determine the relations to add to the groups
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN relkind = 'r' THEN 'ADD_TBL' ELSE 'ADD_SEQ' END AS emaj._alter_step_enum),
             grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_group = ANY (v_groupNames)
          AND NOT EXISTS (
              SELECT NULL FROM emaj.emaj_relation
                WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
                  AND rel_group = ANY (v_groupNames))
          AND relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq;
-- set the altr_group_is_logging column value
    UPDATE emaj.emaj_alter_plan SET altr_group_is_logging = group_is_logging
      FROM emaj.emaj_group
      WHERE altr_group = group_name
        AND altr_time_id = v_timeId AND altr_group <> '';
-- set the altr_new_group_is_logging column value for the cases when the group ownership changes
    UPDATE emaj.emaj_alter_plan SET altr_new_group_is_logging = group_is_logging
      FROM emaj.emaj_group
      WHERE altr_new_group = group_name
        AND altr_time_id = v_timeId AND altr_new_group IS NOT NULL;
-- check groups LOGGING state, depending on the steps to perform
    SELECT string_agg(DISTINCT altr_group, ', ' ORDER BY altr_group) INTO v_groups
      FROM emaj.emaj_alter_plan
      WHERE altr_time_id = v_timeId
        AND altr_step IN ('RESET_GROUP', 'REPAIR_TBL', 'ASSIGN_REL')
        AND altr_group_is_logging;
    IF v_groups IS NOT NULL THEN
      RAISE EXCEPTION '_alter_plan: The groups "%" cannot be altered because they are in LOGGING state.', v_groups;
    END IF;
-- and return
    RETURN;
  END;
$_alter_plan$;

CREATE OR REPLACE FUNCTION emaj._alter_exec(v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_alter_exec$
-- This function executes the alter groups operation that has been planned by the _alter_plan() function.
-- It looks at the emaj_alter_plan table and executes elementary step in proper order.
-- Input: timestamp id of the operation
  DECLARE
    v_logSchemaSuffix        TEXT;
    v_emajNamesPrefix        TEXT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_isRollbackable         BOOLEAN;
    r_plan                   emaj.emaj_alter_plan%ROWTYPE;
    r_rel                    emaj.emaj_relation%ROWTYPE;
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
  BEGIN
-- scan the emaj_alter_plan table and execute each elementary item in the proper order
    FOR r_plan IN
      SELECT *
        FROM emaj.emaj_alter_plan
        WHERE altr_time_id = v_timeId
        ORDER BY altr_step, altr_priority, altr_schema, altr_tblseq, altr_group
      LOOP
      CASE r_plan.altr_step
        WHEN 'REMOVE_TBL' THEN
-- remove a table from its group
          PERFORM emaj._remove_tbl(r_plan, v_timeId);
--
        WHEN 'REMOVE_SEQ' THEN
-- remove a sequence from its group
          PERFORM emaj._remove_seq(r_plan, v_timeId);
--
        WHEN 'RESET_GROUP' THEN
-- reset a group
          PERFORM emaj._reset_groups(ARRAY[r_plan.altr_group]);
--
        WHEN 'REPAIR_TBL' THEN
          IF r_plan.altr_group_is_logging THEN
            RAISE EXCEPTION 'alter_exec: Cannot repair the table %.%. Its group % is in LOGGING state.', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
          ELSE
-- get the is_rollbackable status of the related group
            SELECT group_is_rollbackable INTO v_isRollbackable
              FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- get the table description from emaj_group_def
            SELECT * INTO r_grpdef
              FROM emaj.emaj_group_def
             WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- remove the table from its group
            PERFORM emaj._drop_tbl(emaj.emaj_relation.*) FROM emaj.emaj_relation
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- and recreate it
            PERFORM emaj._create_tbl(r_grpdef, v_timeId, v_isRollbackable);
          END IF;
--
        WHEN 'REPAIR_SEQ' THEN
          IF r_plan.altr_group_is_logging THEN
            RAISE EXCEPTION 'alter_exec: Cannot repair the sequence %.%. Its group % is in LOGGING state.', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
          ELSE
-- get the sequence description from emaj_group_def
            SELECT * INTO r_grpdef
              FROM emaj.emaj_group_def
             WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- remove the sequence from its group
            PERFORM emaj._drop_seq(emaj.emaj_relation.*) FROM emaj.emaj_relation
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- and recreate it
            PERFORM emaj._create_seq(r_grpdef, v_timeId);
          END IF;
--
        WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_log_schema_suffix INTO v_logSchemaSuffix FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_schema_tbl(r_rel, v_logSchemaSuffix);
--
        WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_emaj_names_prefix INTO v_emajNamesPrefix FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_emaj_names_prefix(r_rel, v_emajNamesPrefix);
--
        WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_log_dat_tsp INTO v_logDatTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_data_tsp_tbl(r_rel, v_logDatTsp);
--
        WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_log_idx_tsp INTO v_logIdxTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_index_tsp_tbl(r_rel, v_logIdxTsp);
--
        WHEN 'ASSIGN_REL' THEN
-- currently, this can only be done when the relation belongs to an IDLE group
-- update the emaj_relation table to report the group ownership change
          UPDATE emaj.emaj_relation SET rel_group = r_plan.altr_new_group
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'CHANGE_REL_PRIORITY' THEN
-- update the emaj_relation table to report the priority change
          UPDATE emaj.emaj_relation SET rel_priority = r_plan.altr_new_priority
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
--
        WHEN 'ADD_TBL' THEN
-- add a table to a group
          PERFORM emaj._add_tbl(r_plan, v_timeId);
--
        WHEN 'ADD_SEQ' THEN
-- add a sequence to a group
          PERFORM emaj._add_seq(r_plan, v_timeId);
--
      END CASE;
    END LOOP;
    RETURN;
  END;
$_alter_exec$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN, v_loggedRlbkTargetMark TEXT DEFAULT NULL, v_timeId BIGINT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into all log tables between this previous mark and the new mark.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like functions that start or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
--        name of the rollback target mark when this mark is created by the logged_rollback functions (NULL by default)
--        time stamp identifier to reuse (NULL by default) (this parameter is set when the mark is a rollback start mark)
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- if requested, record the set mark begin in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_mark);
    END IF;
-- get the time stamp of the operation, if not supplied as input parameter
    IF v_timeId IS NULL THEN
      SELECT emaj._set_time_stamp('M') INTO v_timeId;
    END IF;
-- record sequences state as early as possible (no lock protects them from other transactions activity)
    INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
      SELECT * FROM emaj._get_current_sequences_state(v_groupNames, 'S', v_timeId);
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- record the number of log rows for the old last mark of each group
--   the statement updates no row in case of emaj_start_group(s)
    WITH stat_group1 AS (                                         -- for each group, the mark id and time id of the last active mark
      SELECT mark_group, max(mark_id) as last_mark_id, max(mark_time_id) AS last_mark_time_id
        FROM emaj.emaj_mark
        WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted
        GROUP BY mark_group),
         stat_group2 AS (                                         -- compute the number of log rows for all tables currently belonging to these groups
      SELECT mark_group, last_mark_id, coalesce(
          (SELECT sum(emaj._log_stat_tbl(emaj_relation, greatest(last_mark_time_id, lower(rel_time_range)),NULL))
             FROM emaj.emaj_relation
             WHERE rel_group = mark_group AND rel_kind = 'r' AND upper_inf(rel_time_range)), 0) AS mark_stat
        FROM stat_group1 )
    UPDATE emaj.emaj_mark m SET mark_log_rows_before_next = mark_stat
      FROM stat_group2 s
      WHERE s.mark_group = m.mark_group AND s.last_mark_id = m.mark_id;
-- for tables currently belonging to the groups, record the associated log sequence state into the emaj sequence table
    INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
      SELECT * FROM emaj._get_current_sequences_state(v_groupNames, 'r', v_timeId);
    GET DIAGNOSTICS v_nbTbl = ROW_COUNT;
-- record the mark for each group into the emaj_mark table
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, v_mark, v_timeId, FALSE, FALSE, v_loggedRlbkTargetMark
        FROM emaj.emaj_group WHERE group_name = ANY(v_groupNames) ORDER BY group_name;
-- before exiting, cleanup the state of the pending rollback events from the emaj_rlbk table
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
-- ... either through dblink if we are currently performing a rollback with a dblink connection already opened
--     this is mandatory to avoid deadlock
      PERFORM 0 FROM dblink('rlbk#1','SELECT emaj._cleanup_rollback_state()') AS (dummy INT);
    ELSE
-- ... or directly
      PERFORM emaj._cleanup_rollback_state();
    END IF;
-- if requested, record the set mark end in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_mark);
    END IF;
--
    RETURN v_nbSeq + v_nbTbl;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function.
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_markId                 BIGINT;
    v_markTimeId             BIGINT;
    v_previousMarkTimeId     BIGINT;
    v_previousMarkName       TEXT;
    v_previousMarkGlobalSeq  BIGINT;
    v_nextMarkTimeId         BIGINT;
    v_nextMarkName           TEXT;
    v_nextMarkGlobalSeq      BIGINT;
    v_idNewMin               BIGINT;
    v_markNewMin             TEXT;
    v_count                  INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := '');
-- check the mark name
    SELECT emaj._check_mark_name(v_groupNames := ARRAY[v_groupName], v_mark := v_mark, v_checkList := '') INTO v_mark;
-- count the number of marks in the group
    SELECT count(*) INTO v_count FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- and check there are at least 2 marks for the group
    IF v_count < 2 THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: "%" is the only mark of the group. It cannot be deleted.', v_mark;
    END IF;
-- OK, now get the id and time stamp id of the mark to delete
    SELECT mark_id, mark_time_id INTO v_markId, v_markTimeId
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark;
-- ... and the id and timestamp of the future first mark
    SELECT mark_id, mark_name INTO v_idNewMin, v_markNewMin
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name <> v_mark ORDER BY mark_id LIMIT 1;
-- ... and the name, the time id and the last global sequence value of the previous mark
    SELECT emaj._get_previous_mark_group(v_groupName, v_mark) INTO v_previousMarkName;
    SELECT mark_time_id, time_last_emaj_gid INTO v_previousMarkTimeId, v_previousMarkGlobalSeq
      FROM emaj.emaj_mark, emaj.emaj_time_stamp WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_previousMarkName;
-- ... and the name, the time id and the last global sequence value of the next mark
    SELECT mark_name INTO v_nextMarkName FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_time_id >
        (SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark)
      ORDER BY mark_time_id ASC LIMIT 1;
    SELECT mark_time_id, time_last_emaj_gid INTO v_nextMarkTimeId, v_nextMarkGlobalSeq
      FROM emaj.emaj_mark, emaj.emaj_time_stamp WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_nextMarkName;
-- effectively delete the mark for the group
    IF v_previousMarkTimeId IS NULL THEN
-- if the mark to delete is the first one, process its deletion with _delete_before_mark_group(), as the first rows of log tables become useless
      PERFORM emaj._delete_before_mark_group(v_groupName, v_markNewMin);
    ELSE
-- otherwise, the mark to delete is an intermediate mark for the group
-- process the mark deletion with _delete_intermediate_mark_group()
      PERFORM emaj._delete_intermediate_mark_group(v_groupName, v_mark, v_markId, v_markTimeId);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_mark);
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
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
    v_markId                 BIGINT;
    v_markGlobalSeq          BIGINT;
    v_markTimeId             BIGINT;
    v_nbMark                 INT;
    r_rel                    RECORD;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- retrieve the id, the timestamp and the emaj_gid value and the time stamp id of the mark
    SELECT mark_id, time_last_emaj_gid, mark_time_id INTO v_markId, v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_mark;
-- drop obsolete old log tables (whose end time stamp is older than the new first mark time stamp)
    FOR r_rel IN
        SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r' AND upper(rel_time_range) <= v_markTimeId
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
    END LOOP;
-- delete obsolete emaj_sequence and emaj_relation rows (those corresponding to the just dropped log tables)
--TODO delete also holes
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper(rel_time_range) <= v_markTimeId
        AND sequ_time_id < v_markTimeId;
    DELETE FROM emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND upper(rel_time_range) <= v_markTimeId;
-- drop the E-Maj secondary schemas that are now useless (i.e. not used by any created group)
    PERFORM emaj._drop_log_schemas('DELETE_BEFORE_MARK_GROUP', FALSE);
-- delete rows from all other log tables
    FOR r_rel IN
        SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r' AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
          ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
-- delete log rows prior to the new first mark
      EXECUTE 'DELETE FROM ' || r_rel.log_table_name || ' WHERE emaj_gid <= ' || v_markGlobalSeq;
    END LOOP;
-- process emaj_seq_hole content
-- delete all existing holes (if any) before the mark
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id < v_markTimeId;
-- now the sequences related to the mark to delete can be suppressed
--   delete first application sequences related data for the group
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   delete then emaj sequences related data for the group
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--    and that may have one of the deleted marks as target mark from a previous logged rollback operation
    UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = v_groupName AND mark_id >= v_markId
        AND mark_logged_rlbk_target_mark IN (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_id < v_markId
            );
-- delete oldest marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_id < v_markId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;

--TODO: drop useless application tables (when a feature will need it)

-- deletes obsolete versions of emaj_relation rows
    DELETE FROM emaj.emaj_relation
      WHERE upper(rel_time_range) < v_markTimeId AND rel_group = v_groupName;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- purge the emaj history, if needed (even if no mark as been really dropped)
    PERFORM emaj._purge_hist();
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE OR REPLACE FUNCTION emaj._delete_intermediate_mark_group(v_groupName TEXT, v_markName TEXT, v_markId BIGINT, v_markTimeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_delete_intermediate_mark_group$
-- This function effectively deletes an intermediate mark for a group.
-- It is called by the emaj_delete_mark_group() function.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained
-- Input: group name, mark name, mark id and mark time stamp id of the mark to delete
  DECLARE
    v_previousMark           TEXT;
    v_nextMark               TEXT;
    v_previousMarkTimeId     BIGINT;
    v_nextMarkTimeId         BIGINT;
  BEGIN
-- delete the sequences related to the mark to delete
--   delete first data related to the application sequences (those attached to the group at the set mark time, but excluding the time range bounds)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_time_id = v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   delete then data related to the log sequences for tables (those attached to the group at the set mark time, but excluding the time range bounds)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_time_id = v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
-- physically delete the mark from emaj_mark
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_markName;
-- adjust the mark_log_rows_before_next column of the previous mark
-- get the name of the mark immediately preceeding the mark to delete
    SELECT mark_name, mark_time_id INTO v_previousMark, v_previousMarkTimeId FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_id < v_markId ORDER BY mark_id DESC LIMIT 1;
-- get the name of the first mark succeeding the mark to delete
    SELECT mark_name, mark_time_id INTO v_nextMark, v_nextMarkTimeId FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_id > v_markId ORDER BY mark_id LIMIT 1;
    IF NOT FOUND THEN
-- no next mark, so update the previous mark with NULL
      UPDATE emaj.emaj_mark SET mark_log_rows_before_next = NULL
        WHERE mark_group = v_groupName AND mark_name = v_previousMark;
    ELSE
-- update the previous mark by computing the sum of _log_stat_tbl() call's result
--   for all relations that belonged to the group at the time when the mark before the deleted mark had been set
      UPDATE emaj.emaj_mark SET mark_log_rows_before_next =
          (SELECT sum(emaj._log_stat_tbl(emaj_relation, v_previousMarkTimeId, v_nextMarkTimeId))
             FROM emaj.emaj_relation
             WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_time_range @> v_previousMarkTimeId)
        WHERE mark_group = v_groupName AND mark_name = v_previousMark;
    END IF;
-- reset the mark_logged_rlbk_target_mark column to null for other marks of the group
--   that may have the deleted mark as target mark from a previous logged rollback operation
    UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = v_groupName AND mark_logged_rlbk_target_mark = v_markName;
    RETURN;
  END;
$_delete_intermediate_mark_group$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(v_rlbkId INT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viwer role can write into rollback tables without having specific privileges to do it.
  DECLARE
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_markTimeId             BIGINT;
    v_nbSession              INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_effNbTable             INT;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- get the rollack characteristics for the emaj_rlbk event
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_ctrlStepName
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
if v_markTimeId is null then raise notice '§§§ attention pour mark % v_markTimeId NULL', v_mark; end if;
-- get all duration parameters that will be needed later from the emaj_param table,
--   or get default values for rows that are not present in emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_fkey_check_duration'),'5 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_step_rollback_duration'),'2.5 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::INTERVAL)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk;
-- insert into emaj_rlbk_plan a row per table currently belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey)
      SELECT v_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, ''
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_group = ANY(v_groupNames) AND rel_kind = 'r';
-- insert into emaj_rlbk_plan a row per table to effectively rollback.
-- the numbers of log rows is computed using the _log_stat_tbl() function.
-- a final check will be performed after tables will be locked to be sure no new table will have been updated
     INSERT INTO emaj.emaj_rlbk_plan
            (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_target_time_id, rlbp_estimated_quantity)
      SELECT v_rlbkId, 'RLBK_TABLE', rel_schema, rel_tblseq, '', greatest(v_markTimeId, lower(rel_time_range)),
             emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL)
        FROM (SELECT * FROM emaj.emaj_relation
                WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'r') AS t
        WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0;
     GET DIAGNOSTICS v_effNbTable = ROW_COUNT;
--
-- group tables into batchs to process all tables linked by foreign keys as a batch
--
    v_batchNumber = 1;
--   allocate tables with rows to rollback to batch number starting with the heaviest to rollback tables
--     as reported by emaj_log_stat_group() function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE' ORDER BY rlbp_estimated_quantity DESC
      LOOP
--   is the table already allocated to a batch number (it may have been already allocated because of a fkey link) ?
      PERFORM 0 FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table AND rlbp_batch_number IS NULL;
--   no,
      IF FOUND THEN
--   allocate the table to the batch number, with all other tables linked by foreign key constraints
        PERFORM emaj._rlbk_set_batch_number(v_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table);
        v_batchNumber = v_batchNumber + 1;
      END IF;
    END LOOP;
--
-- if unlogged rollback, register into emaj_rlbk_plan "disable log triggers", "deletes from log tables"
-- and "enable log trigger" steps
--
    IF NOT v_isLoggedRlbk THEN
-- compute the cost for each DIS_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DIS_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all DIS_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'DIS_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- insert all DELETE_LOG steps. But the duration estimates will be computed later
-- the estimated number of log rows to delete is set to the the estimated number of updates. This is underestimated
--   in particular when SQL UPDATES are logged. But the collected statistics used for duration estimates are also
--   based on the estimated number of updates.
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
        ) SELECT v_rlbkId, 'DELETE_LOG', rlbp_schema, rlbp_table, '', rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- compute the cost for each ENA_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'ENA_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all ENA_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'ENA_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
    END IF;
--
-- process foreign key to define which action to perform on them
--
-- First compute the fixed duration estimates for each 'DROP_FK' and 'SET_FK_DEF' steps
--   if DROP_FK statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDropFkDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'DROP_FK';
    v_estimDropFkMethod = 2;
    IF v_estimDropFkDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimDropFkDuration = v_fixed_step_rlbk;
      v_estimDropFkMethod = 3;
    END IF;
--   if SET_FK_DEF statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimSetFkDefDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'SET_FK_DEF';
    v_estimSetFkDefMethod = 2;
    IF v_estimSetFkDefDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimSetFkDefDuration = v_fixed_step_rlbk;
      v_estimSetFkDefMethod = 3;
    END IF;
-- select all foreign keys belonging to or referencing the tables to process
    FOR r_fk IN
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND n.nspname = r.rlbp_schema AND t.relname = r.rlbp_table     -- join on emaj_rlbk_plan table
      UNION
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace rn, pg_catalog.pg_class rt, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace
          AND rn.nspname = r.rlbp_schema AND rt.relname = r.rlbp_table   -- join on emaj_rlbk_plan table
      ORDER BY nspname, relname, conname
      LOOP
-- depending on the foreign key characteristics, record as 'to be dropped' or 'to be set deffered' or 'to just be reset immediate'
      IF NOT r_fk.condeferrable OR r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
-- non deferrable fkeys and deferrable fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
          ) VALUES (
          v_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
          v_estimDropFkDuration, v_estimDropFkMethod
          );
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number, rlbp_fkey_def, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, r_fk.def, r_fk.reltuples
          );
      ELSE
-- other deferrable but not deferred fkeys need to be set deferred
        IF NOT r_fk.condeferred THEN
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            v_rlbkId, 'SET_FK_DEF', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimSetFkDefDuration, v_estimSetFkDefMethod
            );
        END IF;
-- deferrable fkeys are recorded as 'to be set immediate at the end of the rollback operation'
-- compute the number of fkey values to check at set immediate time
        SELECT (coalesce(
--   get the number of rolled back rows in the referencing table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND rlbp_schema = r_fk.nspname AND rlbp_table = r_fk.relname)  -- referencing schema.table
            , 0)) + (coalesce(
--   get the number of rolled back rows in the referenced table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan, pg_catalog.pg_constraint c, pg_catalog.pg_namespace rn, pg_catalog.pg_class rt
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND c.oid = r_fk.conoid                                        -- constraint id
                AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced schema.table
                AND rn.nspname = rlbp_schema AND rt.relname = rlbp_table)      -- join on emaj_rlbk_plan
            , 0)) INTO v_checks;
-- and record the SET_FK_IMM step
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'SET_FK_IMM', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, v_checks
          );
      END IF;
    END LOOP;
--
-- Now compute the estimation duration for each complex step ('RLBK_TABLE', 'DELETE_LOG', 'ADD_FK', 'SET_FK_IMM')
--
-- Compute the rollback duration estimates for the tables
-- for each table with content to rollback
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_rlbk * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the log rows delete duration for the tables
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_del_log * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the fkey recreation duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
      LOOP
      IF r_fk.rlbp_estimated_quantity = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estimDuration = '0 SECONDS'::INTERVAL;
        v_estimMethod = 3;
      ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'ADD_FK' AND rlbt_quantity > 0
            AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey;
        v_estimMethod = 1;
        IF v_estimDuration IS NULL THEN
-- non empty table, but no statistics with at least one row are available => take the last duration for this fkey, if any
          SELECT rlbt_duration INTO v_estimDuration FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = 'ADD_FK'
              AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_tbl.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey
              AND rlbt_rlbk_id =
               (SELECT max(rlbt_rlbk_id) FROM emaj.emaj_rlbk_stat WHERE rlbt_step = 'ADD_FK'
                  AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey);
          v_estimMethod = 2;
          IF v_estimDuration IS NULL THEN
-- definitely no statistics available, compute with the avg_fkey_check_duration parameter
            v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
            v_estimMethod = 3;
          END IF;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_fkey = r_fk.rlbp_fkey;
    END LOOP;
-- Compute the fkey checks duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
      LOOP
-- if fkey checks statistics are available for this fkey, compute an average cost
      SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'SET_FK_IMM' AND rlbt_quantity > 0
          AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
-- if no statistics are available for this fkey, use the avg_fkey_check parameter
        v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_fkey = r_fk.rlbp_fkey;
    END LOOP;
--
-- Allocate batch number to sessions to spread the load on sessions as best as possible
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys
--
--   initialisation
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = '0 SECONDS'::INTERVAL;
    END LOOP;
--   allocate tables batch to sessions, starting with the heaviest to rollback batch
    FOR r_batch IN
        SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number IS NOT NULL
          GROUP BY rlbp_batch_number
          ORDER BY sum(rlbp_estimated_duration) DESC
      LOOP
--   compute the least loaded session
      v_minSession=1; v_minDuration = v_sessionLoad [1];
      FOR v_session IN 2 .. v_nbSession LOOP
        IF v_sessionLoad [v_session] < v_minDuration THEN
          v_minSession = v_session;
          v_minDuration = v_sessionLoad [v_session];
        END IF;
      END LOOP;
--   allocate the batch to the session
      UPDATE emaj.emaj_rlbk_plan SET rlbp_session = v_minSession
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number = r_batch.rlbp_batch_number;
      v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + r_batch.batch_duration;
    END LOOP;
-- assign session 1 to all 'LOCK_TABLE' steps not yet affected
    UPDATE emaj.emaj_rlbk_plan SET rlbp_session = 1
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_session IS NULL;
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate
-- get the number of recorded steps (except LOCK_TABLE)
    SELECT count(*) INTO v_nbStep FROM emaj.emaj_rlbk_plan
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step <> 'LOCK_TABLE';
    IF v_nbStep > 0 THEN
-- if CTRLxDBLINK statistics are available, compute an average cost
      SELECT sum(rlbt_duration) * v_nbStep / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat WHERE rlbt_step = v_ctrlStepName AND rlbt_quantity > 0;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_dblink_rlbk * v_nbStep;
        v_estimMethod = 3;
      END IF;
-- insert the 'CTRLxDBLINK' pseudo step
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_estimated_quantity,
          rlbp_estimated_duration, rlbp_estimate_method
        ) VALUES (
          v_rlbkId, v_ctrlStepName, '', '', '', v_nbStep, v_estimDuration, v_estimMethod
        );
    END IF;
-- return the number of tables to effectively rollback
    RETURN v_effNbTable;
  END;
$_rlbk_planning$;

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_start_mark$
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
-- Before setting the mark, it checks no update has been recorded between the planning step and the locks set
-- for tables for which no rollback was needed at planning time.
-- It also sets the rollback status to EXECUTING.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = FALSE;
    v_timeId                 BIGINT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_markTimeId             BIGINT;
    v_markName               TEXT;
    v_errorMsg               TEXT;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = TRUE;
    END IF;
-- get a time stamp for the rollback operation
    v_stmt = 'SELECT emaj._set_time_stamp(''R'')';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      SELECT time_id INTO v_timeId FROM dblink('rlbk#1',v_stmt) AS (time_id BIGINT);
    ELSE
-- ... or directly
      EXECUTE v_stmt INTO v_timeId;
    END IF;
-- update the emaj_rlbk table to record the time stamp and adjust the rollback status
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_time_id = ' || v_timeId || ', rlbk_status = ''EXECUTING''' ||
             ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, time_clock_timestamp
      INTO v_groupNames, v_mark, v_timeId, v_isLoggedRlbk, v_rlbkDatetime
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_time_id = time_id AND rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- check that no update has been recorded between planning time and lock time for tables that did not need to
-- be rolled back at planning time.
-- This may occur and cannot be avoided because tables cannot be locked before processing the rollback planning.
-- (Sessions must lock the tables they will rollback and the planning processing distribute those tables to sessions.)
    PERFORM 1 FROM (SELECT * FROM emaj.emaj_relation
                      WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'r'
                        AND NOT EXISTS
                            (SELECT NULL FROM emaj.emaj_rlbk_plan
                              WHERE rlbp_schema = rel_schema AND rlbp_table = rel_tblseq
                                AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE')
                    ) AS t
      WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0;
    IF FOUND THEN
      v_errorMsg = 'the rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables to process.';
      PERFORM emaj._rlbk_error(v_rlbkId, v_errorMsg, 'rlbk#1');
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If rollback is "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbkDatetime, 'HH24.MI.SS.MS') || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, TRUE, NULL, v_timeId);
    END IF;
    RETURN;
-- trap and record exception during the rollback operation
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(v_rlbkId, 'In _rlbk_start_mark(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_start_mark$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it doesn't own the application tables.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = FALSE;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_rlbkMarkTimeId         BIGINT;
    v_rlbkTimeId             BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_maxGlobalSeq           BIGINT;
    v_rlbkMarkId             BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
      v_isDblinkUsable = TRUE;
    END IF;
-- get the rollback characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_nb_session, time_last_emaj_gid
      INTO v_groupNames, v_mark, v_rlbkTimeId, v_isLoggedRlbk, v_nbSession, v_maxGlobalSeq
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_id = v_rlbkId AND rlbk_time_id = time_id;
-- fetch the mark_id, the last global sequence at set_mark time for the first group of the groups array (they all share the same values - except for the mark_id)
    SELECT mark_id, mark_time_id, time_last_emaj_gid
      INTO v_rlbkMarkId, v_rlbkMarkTimeId, v_lastGlobalSeq
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupNames[1] AND mark_name = v_mark;
-- scan emaj_rlbp_plan to get all steps to process that have been affected to this session, in batch_number and step order
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_fkey_def, rlbp_target_time_id
        FROM emaj.emaj_rlbk_plan,
             (VALUES ('DIS_LOG_TRG',1),('DROP_FK',2),('SET_FK_DEF',3),('RLBK_TABLE',4),
                     ('DELETE_LOG',5),('SET_FK_IMM',6),('ADD_FK',7),('ENA_LOG_TRG',8)) AS step(step_name, step_order)
        WHERE rlbp_step::TEXT = step.step_name
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step NOT IN ('LOCK_TABLE','CTRL-DBLINK','CTRL+DBLINK')
          AND rlbp_session = v_session
        ORDER BY rlbp_batch_number, step_order, rlbp_table, rlbp_fkey
      LOOP
-- update the emaj_rlbk_plan table to set the step start time
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = clock_timestamp() ' ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || 'AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_fkey = ' || quote_literal(r_step.rlbp_fkey) || ' RETURNING 1';
      IF v_isDblinkUsable THEN
-- ... either through dblink if possible
        PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      ELSE
-- ... or directly
        EXECUTE v_stmt;
      END IF;
-- process the step depending on its type
      CASE r_step.rlbp_step
        WHEN 'DIS_LOG_TRG' THEN
-- process a log trigger disable
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' DISABLE TRIGGER emaj_log_trg';
        WHEN 'DROP_FK' THEN
-- process a foreign key deletion
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' DROP CONSTRAINT ' || quote_ident(r_step.rlbp_fkey);
        WHEN 'SET_FK_DEF' THEN
-- set a foreign key deferred
          EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                  ' DEFERRED';
        WHEN 'RLBK_TABLE' THEN
-- process a table rollback
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._rlbk_tbl(emaj_relation.*,
                                CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                     ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id) END,
                                v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'DELETE_LOG' THEN
-- process the deletion of log rows
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._delete_log_tbl(emaj_relation.*, r_step.rlbp_target_time_id, v_rlbkTimeId,
                                      CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                           ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id) END)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'SET_FK_IMM' THEN
-- set a foreign key immediate
          EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                  ' IMMEDIATE';
        WHEN 'ADD_FK' THEN
-- process a foreign key creation
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' ADD CONSTRAINT ' || quote_ident(r_step.rlbp_fkey) || ' ' || r_step.rlbp_fkey_def;
        WHEN 'ENA_LOG_TRG' THEN
-- process a log trigger enable
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' ENABLE TRIGGER emaj_log_trg';
      END CASE;
-- update the emaj_rlbk_plan table to set the step duration
-- NB: the computed duration does not include the time needed to update the emaj_rlbk_plan table
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_duration = ' || quote_literal(clock_timestamp()) || ' - rlbp_start_datetime';
      IF r_step.rlbp_step = 'RLBK_TABLE' OR r_step.rlbp_step = 'DELETE_LOG' THEN
--   and the effective number of processed rows for RLBK_TABLE and DELETE_LOG steps
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbRows;
      END IF;
      v_stmt = v_stmt ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || 'AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_fkey = ' || quote_literal(r_step.rlbp_fkey) || ' RETURNING 1';
      IF v_isDblinkUsable THEN
-- ... either through dblink if possible
        PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      ELSE
-- ... or directly
        EXECUTE v_stmt;
      END IF;
    END LOOP;
-- update the emaj_rlbk_session table to set the timestamp representing the end of work for the session
    v_stmt = 'UPDATE emaj.emaj_rlbk_session SET rlbs_end_datetime = clock_timestamp()' ||
             ' WHERE rlbs_rlbk_id = ' || v_rlbkId || ' AND rlbs_session = ' || v_session ||
             ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
--     and then close the connection for session > 1
      IF v_session > 1 THEN
        PERFORM emaj._dblink_close_cnx('rlbk#'||v_session);
      END IF;
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
    RETURN;
-- trap and record exception during the rollback operation
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(v_rlbkId, 'In _rlbk_session_exec() for session ' || v_session || ': ' || SQLERRM, 'rlbk#'||v_session);
      RAISE;
  END;
$_rlbk_session_exec$;

CREATE OR REPLACE FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_end$
-- This is the last step of a rollback group processing. It :
--    - deletes the marks that are no longer available,
--    - deletes the recorded sequences values for these deleted marks
--    - copy data into the emaj_rlbk_stat table,
--    - rollbacks all sequences of the groups,
--    - set the end rollback mark if logged rollback,
--    - and finaly set the operation as COMPLETED or COMMITED.
-- It returns the execution report of the rollback operation (a set of rows).
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = FALSE;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_isAlterGroupAllowed    BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_ctrlDuration           INTERVAL;
    v_markId                 BIGINT;
    v_markTimeId             BIGINT;
    v_nbSeq                  INT;
    v_markName               TEXT;
    v_messages               TEXT;
    r_msg                    RECORD;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = TRUE;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl, v_rlbkDatetime
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_time_id = time_id AND  rlbk_id = v_rlbkId;
-- get the mark timestamp for the 1st group (they all share the same timestamp)
    SELECT mark_time_id INTO v_markTimeId FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- if "unlogged" rollback, delete all marks later than the now rolled back mark and the associated sequences
    IF NOT v_isLoggedRlbk THEN
-- get the highest mark id of the mark used for rollback, for all groups
      SELECT max(mark_id) INTO v_markId
        FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames) AND mark_name = v_mark;
-- delete the marks that are suppressed by the rollback (the related sequences have been already deleted by rollback functions)
-- with a logging in the history
      WITH deleted AS (
        DELETE FROM emaj.emaj_mark
          WHERE mark_group = ANY (v_groupNames) AND mark_id > v_markId
          RETURNING mark_time_id, mark_group, mark_name),
           sorted_deleted AS (                                       -- the sort is performed to produce stable results in regression tests
        SELECT mark_group, mark_name FROM deleted ORDER BY mark_time_id, mark_group)
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END,
               'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted' FROM sorted_deleted;
-- and reset the mark_log_rows_before_next column for the new last mark
      UPDATE emaj.emaj_mark SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND (mark_group, mark_id) IN                -- select only the last non deleted mark of each concerned group
              (SELECT mark_group, max(mark_id) FROM emaj.emaj_mark
               WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
-- the sequences related to the deleted marks can be also suppressed
--   delete first application sequences related data for the groups
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames) AND rel_kind = 'S'
          AND sequ_time_id > v_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
--   delete then emaj sequences related data for the groups
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          AND sequ_time_id > v_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
    END IF;
-- delete the now useless 'LOCK TABLE' steps from the emaj_rlbk_plan table
    v_stmt = 'DELETE FROM emaj.emaj_rlbk_plan ' ||
             ' WHERE rlbp_rlbk_id = ' || v_rlbkId || ' AND rlbp_step = ''LOCK_TABLE'' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- Prepare the CTRLxDBLINK pseudo step statistic by computing the global time spent between steps
    SELECT coalesce(sum(ctrl_duration),'0'::INTERVAL) INTO v_ctrlDuration FROM (
      SELECT rlbs_session, rlbs_end_datetime - min(rlbp_start_datetime) - sum(rlbp_duration) AS ctrl_duration
        FROM emaj.emaj_rlbk_session rlbs, emaj.emaj_rlbk_plan rlbp
        WHERE rlbp_rlbk_id = rlbs_rlbk_id AND rlbp_session = rlbs_session
          AND rlbs_rlbk_id = v_rlbkID
        GROUP BY rlbs_session, rlbs_end_datetime ) AS t;
-- report duration statistics into the emaj_rlbk_stat table
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_stat (rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey,' ||
             '      rlbt_rlbk_id, rlbt_quantity, rlbt_duration)' ||
--   copy elementary steps for RLBK_TABLE, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_rlbk_id,' ||
             '      rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 4 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_LOG_TRG'') ' ||
             '    GROUP BY 1, 2, 3, 4, 5' ||
             '  UNION ALL ' ||
--   and the final CTRLxDBLINK pseudo step statistic
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      rlbp_estimated_quantity, ' || quote_literal(v_ctrlDuration) ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''CTRL+DBLINK'',''CTRL-DBLINK'') ' ||
             ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- rollback the application sequences belonging to the groups
-- warning, this operation is not transaction safe (that's why it is placed at the end of the operation)!
-- if the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time
    PERFORM emaj._rlbk_seq(t.*, greatest(v_markTimeId, lower(t.rel_time_range)))
      FROM (SELECT * FROM emaj.emaj_relation
              WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automaticaly set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbkDatetime, 'HH24.MI.SS.MS') || '_DONE';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, TRUE, v_mark);
    END IF;
-- build and return the execution report
-- start with the NOTICE messages
    rlbk_severity = 'Notice';
    rlbk_message = format ('%s / %s tables effectively processed.', v_effNbTbl::TEXT, v_nbTbl::TEXT);
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'NOTICE', 'Rollback id ' || v_rlbkId, rlbk_message);
    v_messages = quote_literal(rlbk_severity || ': ' || rlbk_message);
    IF v_isAlterGroupAllowed IS NULL THEN
-- for old style calling functions just return the number of processed tables and sequences
      rlbk_message = (v_effNbTbl + v_nbSeq)::TEXT;
      RETURN NEXT;
    ELSE
      RETURN NEXT;
    END IF;
-- return the execution report to new style calling functions
-- ... the general notice messages with counters
    IF v_nbSeq > 0 THEN
      rlbk_message = format ('%s sequences processed.', v_nbSeq::TEXT);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'NOTICE', 'Rollback id ' || v_rlbkId, rlbk_message);
      v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
      IF v_isAlterGroupAllowed IS NOT NULL THEN
        RETURN NEXT;
      END IF;
    END IF;
-- then, for new style calling functions, return the WARNING messages for any elementary action from alter group operations that has not been rolled back
    IF v_isAlterGroupAllowed IS NOT NULL THEN
--TODO: add missing cases
      rlbk_severity = 'Warning';
      FOR r_msg IN
-- steps are splitted into 2 groups to filter them differently
        SELECT altr_time_id, altr_step, altr_schema, altr_tblseq,
               (CASE altr_step
                  WHEN 'ADD_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back up to its latest group attachment state (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'ADD_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back up to its latest group attachment (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
            FROM (
-- suppress duplicate ADD_TBL / REMOVE_TBL or ADD_SEQ / REMOVE_SEQ for same table or sequence, by keeping the most recent step
              SELECT altr_schema, altr_tblseq, altr_time_id, altr_step FROM (
                SELECT altr_schema, altr_tblseq, altr_time_id, altr_step,
                       rank() OVER (PARTITION BY altr_schema, altr_tblseq ORDER BY altr_time_id DESC) AS altr_rank
                FROM emaj.emaj_alter_plan
                WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> '' AND altr_rlbk_id IS NULL AND
                      altr_step IN ('ADD_TBL','REMOVE_TBL','ADD_SEQ','REMOVE_SEQ')
                ) AS t1
              WHERE altr_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE altr_time_id = time_id
        UNION
        SELECT altr_time_id, altr_step, altr_schema, altr_tblseq,
               (CASE altr_step
                  WHEN 'CHANGE_REL_PRIORITY' THEN
                    'Tables group change not rolled back: E-Maj priority for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN
                    'Tables group change not rolled back: E-Maj log schema for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN
                    'Tables group change not rolled back: E-Maj names prefix for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
                    'Tables group change not rolled back: log data tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
                    'Tables group change not rolled back: log index tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  ELSE altr_step::TEXT || ' / ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  END)::TEXT AS message
            FROM (
-- suppress duplicates for other steps for each table or sequence
              SELECT altr_schema, altr_tblseq, altr_time_id, altr_step FROM (
                SELECT altr_schema, altr_tblseq, altr_time_id, altr_step,
                       rank() OVER (PARTITION BY altr_schema, altr_tblseq ORDER BY altr_time_id DESC) AS altr_rank
                FROM emaj.emaj_alter_plan
                WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> '' AND altr_rlbk_id IS NULL AND
                      altr_step NOT IN ('ADD_TBL','REMOVE_TBL','ADD_SEQ','REMOVE_SEQ')
                ) AS t1
              WHERE altr_rank = 1
            ) AS t2
          ORDER BY altr_time_id, altr_step, altr_schema, altr_tblseq
        LOOP
          INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
            VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'WARNING', 'Rollback id ' || v_rlbkId, r_msg.message);
          rlbk_message = r_msg.message;
          v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
          RETURN NEXT;
      END LOOP;
    END IF;
-- update the alter steps that have been covered by the rollback
    UPDATE emaj.emaj_alter_plan SET altr_rlbk_id = v_rlbkId
      WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_rlbk_id IS NULL;
-- update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the result message
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMPLETED'', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_messages || ']' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
--     and then close the connection
      PERFORM emaj._dblink_close_cnx('rlbk#1');
    ELSE
-- ... or directly (the status can be directly set to committed, the update being in the same transaction)
      EXECUTE 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMMITTED'', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_messages || ']' ||
               ' WHERE rlbk_id = ' || v_rlbkId;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END',
              array_to_string(v_groupNames,','),
              'Rollback_id ' || v_rlbkId || ', ' || v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed'
             );
-- end of the function
    RETURN;
-- trap and record exception during the rollback operation
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(v_rlbkId, 'In _rlbk_end(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_end$;

CREATE OR REPLACE FUNCTION emaj._delete_between_marks_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, OUT v_nbMark INT, OUT v_nbTbl INT)
RETURNS RECORD LANGUAGE plpgsql AS
$_delete_between_marks_group$
-- This function deletes all logs and intermediate marks set between two given marks.
-- The function is called by the emaj_consolidate_rollback_group() function.
-- It deletes rows corresponding to the marks to delete from emaj_mark and emaj_sequence.
-- It deletes rows from emaj_relation corresponding to old versions that become unreacheable.
-- It deletes rows from all concerned log tables.
-- It also manages sequence holes in emaj_seq_hole.
-- Input: group name, name of both marks that defines the range to delete.
-- Output: number of deleted marks, number of tables effectively processed (for which at least one log row has been deleted)
  DECLARE
    v_firstMarkId            BIGINT;
    v_firstMarkGlobalSeq     BIGINT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkId             BIGINT;
    v_lastMarkGlobalSeq      BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_nbUpd                  BIGINT;
    r_rel                    RECORD;
  BEGIN
-- retrieve the id, the timestamp and the emaj_gid value and the time stamp id of the first mark
    SELECT mark_id, time_last_emaj_gid, mark_time_id INTO v_firstMarkId, v_firstMarkGlobalSeq, v_firstMarkTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_firstMark;
-- retrieve the id, the timestamp and the emaj_gid value and the time stamp id of the last mark
    SELECT mark_id, time_last_emaj_gid, mark_time_id INTO v_lastMarkId, v_lastMarkGlobalSeq, v_lastMarkTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_lastMark;
-- delete rows from all log tables (no need to try to delete if v_firstMarkGlobalSeq and v_lastMarkGlobalSeq are equal)
    v_nbTbl = 0;
    IF v_firstMarkGlobalSeq < v_lastMarkGlobalSeq THEN
-- loop on all tables that belonged to the group at the end of the period
      FOR r_rel IN
          SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
            WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_time_range @> v_lastMarkTimeId
            ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- delete log rows
        EXECUTE 'DELETE FROM ' || r_rel.log_table_name || ' WHERE emaj_gid > ' || v_firstMarkGlobalSeq || ' AND emaj_gid <= ' || v_lastMarkGlobalSeq;
        GET DIAGNOSTICS v_nbUpd = ROW_COUNT;
        IF v_nbUpd > 0 THEN
           v_nbTbl = v_nbTbl + 1;
        END IF;
      END LOOP;
    END IF;
-- process emaj_seq_hole content
-- delete all existing holes (if any) between both marks for tables that belonged to the group at the end of the period
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_time_range @> v_lastMarkTimeId
        AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id >= v_firstMarkTimeId AND sqhl_begin_time_id < v_lastMarkTimeId;
-- create holes representing the deleted logs
    INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)
      SELECT rel_schema, rel_tblseq, greatest(v_firstMarkTimeId, lower(rel_time_range)), v_lastMarkTimeId,
             (SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM emaj.emaj_sequence
                WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND sequ_time_id = v_lastMarkTimeId)
             -
             (SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM emaj.emaj_sequence
                WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence
                  AND sequ_time_id = greatest(v_firstMarkTimeId, lower(rel_time_range)))
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_time_range @> v_lastMarkTimeId
          AND 0 <
             (SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM emaj.emaj_sequence
                WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND sequ_time_id = v_lastMarkTimeId)
             -
             (SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM emaj.emaj_sequence
                WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence
                  AND sequ_time_id = greatest(v_firstMarkTimeId, lower(rel_time_range)));
-- now the sequences related to the mark to delete can be suppressed
--   delete first application sequences related data for the group (excluding the time range bounds)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND rel_time_range @> v_lastMarkTimeId
        AND rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_time_id > v_firstMarkTimeId AND sequ_time_id < v_lastMarkTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   delete then emaj sequences related data for the group (excluding the time range bounds)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND rel_time_range @> v_lastMarkTimeId
        AND rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_time_id > v_firstMarkTimeId AND sequ_time_id < v_lastMarkTimeId
        AND lower(rel_time_range) <> sequ_time_id;
-- in emaj_mark, reset the mark_logged_rlbk_target_mark column to null for marks of the group that will remain
--    and that may have one of the deleted marks as target mark from a previous logged rollback operation
    UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = v_groupName AND mark_id >= v_lastMarkId
        AND mark_logged_rlbk_target_mark IN (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_id > v_firstMarkId AND mark_id < v_lastMarkId
            );
-- set the mark_log_rows_before_next of the first mark to 0
    UPDATE emaj.emaj_mark SET mark_log_rows_before_next = 0
      WHERE mark_group = v_groupName AND mark_name = v_firstMark;
-- and finaly delete all intermediate marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_id > v_firstMarkId AND mark_id < v_lastMarkId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
    RETURN;
  END;
$_delete_between_marks_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_consolidable_rollbacks()
RETURNS SETOF emaj.emaj_consolidable_rollback_type LANGUAGE plpgsql AS
$emaj_get_consolidable_rollbacks$
-- This function returns the list of logged rollback operations that can be consolidated, defined as a marks range for a group.
-- It doesn't need input parameter.
-- It returns a set of emaj_consolidable_rollback_type records, sorted by ascending rollback time.
-- The cons_group and cons_end_rlbk_mark_name returned columns can be used as input parameters for the emaj_consolidate_rollback_group() function.
  BEGIN
-- search and return all marks range corresponding to any logged rollback operation
    RETURN QUERY
      SELECT m1.mark_group AS cons_group,
             m2.mark_name AS cons_target_rlbk_mark_name, m2.mark_id AS cons_target_rlbk_mark_id,
             m1.mark_name AS cons_end_rlbk_mark_name, m1.mark_id AS cons_end_rlbk_mark_id,
             cast(coalesce(
                  (SELECT sum(emaj._log_stat_tbl(emaj_relation,
                                                 greatest(m2.mark_time_id, lower(rel_time_range)),
                                                 m1.mark_time_id))
                     FROM emaj.emaj_relation
                           -- for tables belonging to the group at the rollback time
                     WHERE rel_group = m1.mark_group AND rel_kind = 'r' AND rel_time_range @> m1.mark_time_id)
                          ,0) AS BIGINT) AS cons_rows,
             cast((SELECT count(*) FROM emaj.emaj_mark m3
                   WHERE m3.mark_group = m1.mark_group AND m3.mark_id > m2.mark_id AND m3.mark_id < m1.mark_id) AS INT) AS cons_marks
        FROM emaj.emaj_mark m1
          JOIN emaj.emaj_mark m2 ON (m2.mark_name = m1.mark_logged_rlbk_target_mark AND m2.mark_group = m1.mark_group)
          WHERE m1.mark_logged_rlbk_target_mark IS NOT NULL
          ORDER BY m1.mark_id;
  END;
$emaj_get_consolidable_rollbacks$;
COMMENT ON FUNCTION emaj.emaj_get_consolidable_rollbacks() IS
$$Returns the list of logged rollback operations that can be consolidated.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation for a single group.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing)
-- Input: group name, the 2 mark names defining a range
-- Output: set of log rows by table (including tables with 0 rows to rollback)
  BEGIN
    RETURN QUERY SELECT * FROM emaj._log_stat_groups(ARRAY[v_groupName], FALSE, v_firstMark, v_lastMark);
  END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged events for an E-Maj group between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_groups$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation for a groups array.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing)
-- Input: group names array, the 2 mark names defining a range
-- Output: set of log rows by table (including tables with 0 rows to rollback)
  BEGIN
    RETURN QUERY SELECT * FROM emaj._log_stat_groups(v_groupNames, TRUE, v_firstMark, v_lastMark);
  END;
$emaj_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns global statistics about logged events for an E-Maj group between 2 marks for a groups array.$$;

CREATE OR REPLACE FUNCTION emaj._log_stat_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$_log_stat_groups$
-- This function effectively returns statistics on row updates executed between 2 marks or between a mark and the current situation for 1 or several groups.
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function, the 2 mark names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of log rows by table (including tables with 0 rows to rollback)
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
  BEGIN
-- check the groups name
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '') INTO v_groupNames;
    IF v_groupNames IS NOT NULL THEN
-- check the marks range
      SELECT * FROM emaj._check_marks_range(v_groupNames, v_firstMark, v_lastMark)
        INTO v_firstMark, v_lastMark, v_firstMarkTimeId, v_lastMarkTimeId;
-- get additional data for both mark timestamps (in some cases, v_firstMarkTimeId may be NULL)
      SELECT time_clock_timestamp INTO v_firstMarkTs
        FROM emaj.emaj_time_stamp WHERE time_id = v_firstMarkTimeId;
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
        SELECT time_clock_timestamp INTO v_lastMarkTs
          FROM emaj.emaj_time_stamp WHERE time_id = v_lastMarkTimeId;
      END IF;
-- for each table of the group, get the number of log rows and return the statistics
-- shorten the timeframe if the table did not belong to the group on the entire requested time frame
      RETURN QUERY
        SELECT rel_group, rel_schema, rel_tblseq,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMark
                    ELSE coalesce((SELECT mark_name FROM emaj.emaj_mark
                            WHERE mark_time_id = lower(rel_time_range) AND mark_group = rel_group),'[deleted mark]')
                 END AS stat_first_mark,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMarkTs
                    ELSE (SELECT time_clock_timestamp FROM emaj.emaj_time_stamp
                            WHERE time_id = lower(rel_time_range))
                 END AS stat_first_mark_datetime,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN coalesce((SELECT mark_name FROM emaj.emaj_mark
                                 WHERE mark_time_id = upper(rel_time_range) AND mark_group = rel_group),'[deleted mark]')
                    ELSE v_lastMark
                 END AS stat_last_mark,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN (SELECT time_clock_timestamp FROM emaj.emaj_time_stamp
                                 WHERE time_id = upper(rel_time_range))
                    ELSE v_lastMarkTs
                 END AS stat_last_mark_datetime,
               CASE WHEN v_firstMarkTimeId IS NULL THEN 0                                              -- group just created but without any mark
                    ELSE emaj._log_stat_tbl(emaj_relation,
                                            CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                   THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                            CASE WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                   THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
                 END AS nb_rows
          FROM emaj.emaj_relation
          WHERE rel_group = ANY(v_groupNames) AND rel_kind = 'r'                                       -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range;
    ELSE
      RETURN;
    END IF;
  END;
$_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks as viewed through the log tables for 1 tables group
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range
-- Output: table of updates by user and table
  BEGIN
    RETURN QUERY SELECT * FROM emaj._detailed_log_stat_groups(ARRAY[v_groupName], FALSE, v_firstMark, v_lastMark);
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks for a group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_groups$
-- This function returns statistics on row updates executed between 2 marks as viewed through the log tables for several tables group
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group names array, the 2 marks names defining a range
-- Output: table of updates by user and table
  BEGIN
    RETURN QUERY SELECT * FROM emaj._detailed_log_stat_groups(v_groupNames, TRUE, v_firstMark, v_lastMark);
  END;
$emaj_detailed_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks for several groups.$$;

CREATE OR REPLACE FUNCTION emaj._detailed_log_stat_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$_detailed_log_stat_groups$
-- This function effectively returns statistics on row updates executed between 2 marks as viewed through the log tables for one or several groups
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function, the 2 mark names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of updates by user and table
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_lowerBoundMark         TEXT;
    v_lowerBoundMarkTs       TIMESTAMPTZ;
    v_lowerBoundGid          BIGINT;
    v_upperBoundMark         TEXT;
    v_upperBoundMarkTs       TIMESTAMPTZ;
    v_upperBoundGid          BIGINT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '');
    IF v_groupNames IS NOT NULL THEN
-- check the marks range
      SELECT * FROM emaj._check_marks_range(v_groupNames, v_firstMark, v_lastMark)
        INTO v_firstMark, v_lastMark, v_firstMarkTimeId, v_lastMarkTimeId;
-- if there is no first mark, return quickly
      IF v_firstMark IS NULL THEN
        RETURN;
      END IF;
-- get additional data for both mark timestamps
      SELECT time_last_emaj_gid, time_clock_timestamp INTO v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_time_stamp WHERE time_id = v_firstMarkTimeId;
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
        SELECT time_last_emaj_gid, time_clock_timestamp INTO v_lastEmajGid, v_lastMarkTs
          FROM emaj.emaj_time_stamp WHERE time_id = v_lastMarkTimeId;
      END IF;
-- for each table currently belonging to the group
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_group, rel_time_range, rel_log_schema, rel_log_table
            FROM emaj.emaj_relation
            WHERE rel_group = ANY(v_groupNames) AND rel_kind = 'r'                                       -- tables belonging to the groups
              AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
              AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
            ORDER BY rel_schema, rel_tblseq, rel_time_range
          LOOP
-- count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the lower bound for this table
        IF v_firstMarkTimeId >= lower(r_tblsq.rel_time_range) THEN
-- usual case: the table belonged to the group at statistics start mark
          v_lowerBoundMark = v_firstMark;
          v_lowerBoundMarkTs = v_firstMarkTs;
          v_lowerBoundGid = v_firstEmajGid;
        ELSE
-- special case: the table has been added to the group after the statistics start mark
          SELECT coalesce(mark_name, '[deleted mark]') INTO v_lowerBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = lower(r_tblsq.rel_time_range) AND mark_group = ANY(v_groupNames);
          IF v_lowerBoundMark IS NULL THEN
-- the mark set at alter_group time may have been deleted
            v_lowerBoundMark = '[deleted mark]';
          END IF;
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_lowerBoundMarkTs, v_lowerBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = lower(r_tblsq.rel_time_range);
        END IF;
-- compute the upper bound for this table
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- no supplied end mark and the table has not been removed from its group => the current situation
          v_upperBoundMark = NULL;
          v_upperBoundMarkTs = NULL;
          v_upperBoundGid = NULL;
        ELSIF NOT upper_inf(r_tblsq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_tblsq.rel_time_range) < v_lastMarkTimeId) THEN
-- special case: the table has been removed from its group before the statistics end mark
          SELECT mark_name INTO STRICT v_upperBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = upper(r_tblsq.rel_time_range) AND mark_group = ANY(v_groupNames);
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_upperBoundMarkTs, v_upperBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = upper(r_tblsq.rel_time_range);
        ELSE
-- usual case: the table belonged to the group at statistics end mark
          v_upperBoundMark = v_lastMark;
          v_upperBoundMarkTs = v_lastMarkTs;
          v_upperBoundGid = v_lastEmajGid;
        END IF;
-- build the statement
        v_stmt= 'SELECT ' || quote_literal(r_tblsq.rel_group) || '::TEXT AS stat_group, '
             || quote_literal(r_tblsq.rel_schema) || '::TEXT AS stat_schema, '
             || quote_literal(r_tblsq.rel_tblseq) || '::TEXT AS stat_table, '
             || quote_literal(v_lowerBoundMark) || '::TEXT AS stat_first_mark, '
             || quote_literal(v_lowerBoundMarkTs) || '::TIMESTAMPTZ AS stat_first_mark_datetime, '
             || coalesce(quote_literal(v_upperBoundMark),'NULL') || '::TEXT AS stat_last_mark, '
             || coalesce(quote_literal(v_upperBoundMarkTs),'NULL') || '::TIMESTAMPTZ AS stat_last_mark_datetime, '
             || ' emaj_user AS stat_user,'
             || ' CASE emaj_verb WHEN ''INS'' THEN ''INSERT'''
             ||                ' WHEN ''UPD'' THEN ''UPDATE'''
             ||                ' WHEN ''DEL'' THEN ''DELETE'''
             ||                             ' ELSE ''?'' END::VARCHAR(6) AS stat_verb,'
             || ' count(*) AS stat_rows'
             || ' FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table)
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')'
             || ' AND emaj_gid > '|| v_lowerBoundGid
             || coalesce(' AND emaj_gid <= '|| v_upperBoundGid, '')
             || ' GROUP BY stat_group, stat_schema, stat_table, stat_user, stat_verb'
             || ' ORDER BY stat_user, stat_verb';
-- and execute the statement
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END LOOP;
    END IF;
    RETURN;
  END;
$_detailed_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql SECURITY DEFINER AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: group names array, a boolean indicating whether the groups array may contain several groups,
--        the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
    v_nbTblseq               INT;
  BEGIN
-- check the group names (the groups state checks are delayed for later)
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '') INTO v_groupNames;
-- if the group names array is null, immediately return NULL
    IF v_groupNames IS NULL THEN
      RETURN NULL;
    END IF;
-- check supplied group names and mark parameters with the isAlterGroupAllowed and isRollbackSimulation flags set to true
    SELECT emaj._rlbk_check(v_groupNames, v_mark, TRUE, TRUE) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::INT INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session)
        SELECT v_rlbkId, v_groupNames, v_markName, mark_time_id, v_isLoggedRlbk, FALSE, 1
          FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_markName;
-- call the _rlbk_planning function
      PERFORM emaj._rlbk_planning(v_rlbkId);
-- compute the sum of the duration estimates of all elementary steps (except LOCK_TABLE)
      SELECT coalesce(sum(rlbp_estimated_duration), '0 SECONDS'::INTERVAL) INTO v_estimDuration
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step <> 'LOCK_TABLE';
-- cancel the effect of the rollback planning
      RAISE EXCEPTION '';
    EXCEPTION
      WHEN RAISE_EXCEPTION THEN                 -- catch the raised exception and continue
    END;
-- get the "fixed_table_rollback_duration" parameter from the emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::INTERVAL)
           INTO v_fixed_table_rlbk;
-- get the the number of tables to lock and sequences to rollback
    SELECT sum(group_nb_table)+sum(group_nb_sequence) INTO v_nbTblseq
      FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames);
-- compute the final estimated duration
    v_estimDuration = v_estimDuration + (v_nbTblseq * v_fixed_table_rlbk);
    RETURN v_estimDuration;
  END;
$_estimate_rollback_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
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
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before
-- emaj_snap_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name, the absolute pathname of the directory where the files are to be created and the options to used in the COPY TO statements
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
          IF emaj._pg_version_num() < 100000 THEN
            v_stmt= 'COPY (SELECT sequence_name, last_value, start_value, increment_by, max_value, ' ||
                    'min_value, cache_value, is_cycled, is_called FROM ' || v_fullTableName ||
                    ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
          ELSE
            v_stmt= 'COPY (SELECT sequencename, rel.last_value, start_value, increment_by, max_value, ' ||
                    'min_value, cache_size, cycle, rel.is_called ' ||
                    'FROM ' || v_fullTableName || ' rel, pg_sequences ' ||
                    'WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_tblseq) ||
                    ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
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

CREATE OR REPLACE FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark
-- For log tables, files contain all rows related to the time frame, sorted on emaj_gid.
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no end mark is specified.
--   They contain one row per sequence belonging to the group at the related time
--   (a sequence may belong to a group at the start mark time and not at the end mark time for instance).
-- To do its job, the function performs COPY TO statement, using the options provided by the caller.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before emaj_snap_log_group function call, and
--   - to maintain its content outside E-maj.
-- Input: group name, the 2 mark names defining a range,
--        the absolute pathname of the directory where the files are to be created,
--        options for COPY TO statements
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string can be used as last_mark indicating the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: number of generated files (for tables and sequences, including the _INFO file)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it.
  DECLARE
    v_nbFile                 INT = 3;        -- start with 3 = 2 files for sequences + _INFO
    v_noSuppliedLastMark     BOOLEAN;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_logTableName           TEXT;
    v_fileName               TEXT;
    v_conditions             TEXT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'BEGIN', v_groupName,
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_dir);
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := FALSE, v_checkList := '');
-- check the marks range
    v_noSuppliedLastMark = (v_lastMark IS NULL OR v_lastMark = '');
    SELECT * FROM emaj._check_marks_range(ARRAY[v_groupName], v_firstMark, v_lastMark)
      INTO v_firstMark, v_lastMark, v_firstMarkTimeId, v_lastMarkTimeId;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_log_group: The directory parameter cannot be NULL.';
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%'  THEN
      RAISE EXCEPTION 'emaj_snap_log_group: The COPY options parameter format is invalid.';
    END IF;
-- get additional data for the first mark (in some cases, v_firstMarkTimeId may be NULL)
    SELECT time_last_emaj_gid, time_clock_timestamp INTO v_firstEmajGid, v_firstMarkTs
      FROM emaj.emaj_time_stamp WHERE time_id = v_firstMarkTimeId;
    IF v_noSuppliedLastMark THEN
-- the end mark is not supplied (look for the current state)
-- get a simple time stamp and its attributes
      SELECT emaj._set_time_stamp('S') INTO v_lastMarkTimeId;
      SELECT time_last_emaj_gid, time_clock_timestamp INTO v_lastEmajGid, v_lastMarkTs
        FROM emaj.emaj_time_stamp
        WHERE time_id = v_lastMarkTimeId;
    ELSE
-- the end mark is supplied, get additional data for the last mark
      SELECT mark_time_id, time_last_emaj_gid, time_clock_timestamp INTO v_lastMarkTimeId, v_lastEmajGid, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_lastMark;
    END IF;
-- build the conditions on emaj_gid corresponding to this marks frame, used for the COPY statements dumping the tables
    v_conditions = 'TRUE';
    IF NOT v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
      v_conditions = v_conditions || ' AND emaj_gid > '|| v_firstEmajGid;
    END IF;
    IF NOT v_noSuppliedLastMark THEN
      v_conditions = v_conditions || ' AND emaj_gid <= '|| v_lastEmajGid;
    END IF;
-- process all log tables of the emaj_relation table that enter in the marks range
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_log_table FROM emaj.emaj_relation
          WHERE rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)') AND rel_group = v_groupName AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
--   build names
      v_fileName = v_dir || '/' || translate(r_tblsq.rel_log_table || '.snap', E' /\\$<>*', '_______');
      v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table);
--   prepare the execute the COPY statement
      v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE ' || v_conditions
           || ' ORDER BY emaj_gid ASC) TO ' || quote_literal(v_fileName)
           || ' ' || coalesce (v_copyOptions, '');
      EXECUTE v_stmt;
      v_nbFile = v_nbFile + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || v_firstMark, E' /\\$<>*', '_______');
-- and execute the COPY statement
    v_stmt = 'COPY (SELECT emaj_sequence.*' ||
             ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
             ' WHERE sequ_time_id = ' || v_firstMarkTimeId ||
             '   AND rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) ||
             '   AND sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
             ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
             coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
-- prepare the file for sequences state at end mark
-- generate the full file name and the COPY statement
    IF v_noSuppliedLastMark THEN
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || to_char(v_lastMarkTs,'HH24.MI.SS.MS'), E' /\\$<>*', '_______');
      v_stmt = 'SELECT * FROM ' ||
               'emaj._get_current_sequences_state(ARRAY[' || quote_literal(v_groupName) || '], ''S'', ' || v_lastMarkTimeId || ')';
    ELSE
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || v_lastMark, E' /\\$<>*', '_______');
      v_stmt = 'SELECT emaj_sequence.*' ||
               ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
               ' WHERE sequ_time_id = ' || v_lastMarkTimeId ||
               '   AND rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) ||
               '   AND sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
               ' ORDER BY sequ_schema, sequ_name';
    END IF;
-- and create the file
    EXECUTE 'COPY (' || v_stmt || ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj log tables snap of group ' || v_groupName ||
            ' between marks ' || v_firstMark || ' and ' ||
            CASE WHEN v_noSuppliedLastMark THEN 'current state' ELSE v_lastMark END || ' at ' || statement_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO') || ' ' || coalesce (v_copyOptions, '');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbFile || ' generated files');
    RETURN v_nbFile;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a tables groups array between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- The function can process groups that are in LOGGING state or not.
-- The sql statements are placed between a BEGIN TRANSACTION and a COMMIT statements.
-- The output file can be reused as input file to a psql command to replay the updates scenario. Just '\\'
-- character strings (double antislash), if any, must be replaced by '\' (single antislash) before feeding
-- the psql command.
-- Input: - tables groups array
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - optional array of schema qualified table and sequence names to only process those tables and sequences
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_tblList                TEXT;
    v_count                  INT;
    v_firstMarkTimeId        BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_lastEmajGid            BIGINT;
    v_tblseqErr              TEXT;
    v_nbSQL                  BIGINT;
    v_nbSeq                  INT;
    v_cumNbSQL               BIGINT = 0;
    v_fullSeqName            TEXT;
    v_endComment             TEXT;
    v_conditions             TEXT;
    v_endTimeId              BIGINT;
    v_rqSeq                  TEXT;
    r_tblsq                  RECORD;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','),
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END ||
       ' towards ' || v_location ||
       CASE WHEN v_tblseqs IS NOT NULL THEN ' with tables/sequences filtering' ELSE '' END );
-- check the group name
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '') INTO v_groupNames;
-- if there is at least 1 group to process, go on
    IF v_groupNames IS NOT NULL THEN
-- check that there is no tables without pkey
      SELECT string_agg(rel_schema || '.' || rel_tblseq,', ' ORDER BY rel_schema || '.' || rel_tblseq), count(*) INTO v_tblList, v_count
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_relation
        WHERE relnamespace = pg_namespace.oid
          AND nspname = rel_schema AND relname = rel_tblseq
          AND rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                            AND contype = 'p' AND nspname = rel_schema AND relname = rel_tblseq);
      IF v_count > 0 THEN
        IF v_count = 1 THEN
          RAISE EXCEPTION '_gen_sql_groups: % table of the group(s) has no pkey (%).', v_count, v_tblList;
        ELSE
          RAISE EXCEPTION '_gen_sql_groups: % tables of the group(s) have no pkey (%).', v_count, v_tblList;
        END IF;
      END IF;
-- check the marks range
      SELECT * FROM emaj._check_marks_range(v_groupNames, v_firstMark, v_lastMark)
        INTO v_firstMark, v_lastMark, v_firstMarkTimeId, v_lastMarkTimeId;
-- if table/sequence names are supplied, check them
      IF v_tblseqs IS NOT NULL THEN
-- remove duplicates values, NULL and empty strings from the supplied tables/sequences names array
        SELECT array_agg(DISTINCT table_seq_name) INTO v_tblseqs FROM unnest(v_tblseqs) AS table_seq_name
          WHERE table_seq_name IS NOT NULL AND table_seq_name <> '';
        IF v_tblseqs IS NULL THEN
          RAISE EXCEPTION '_gen_sql_groups: The filtered table/sequence names array cannot be empty.';
        END IF;
      END IF;
-- retrieve the global sequence value of the supplied first mark
      SELECT time_last_emaj_gid INTO v_firstEmajGid
        FROM emaj.emaj_time_stamp WHERE time_id = v_firstMarkTimeId;
-- if last mark is NULL or empty, there is no timestamp to register
      IF v_lastMark IS NULL OR v_lastMark = '' THEN
        v_lastEmajGid = NULL;
      ELSE
-- else, retrieve the global sequence value of the supplied end mark
        SELECT time_last_emaj_gid INTO v_lastEmajGid
          FROM emaj.emaj_time_stamp WHERE time_id = v_lastMarkTimeId;
      END IF;
-- check the array of tables and sequences to filter, if supplied.
-- each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups
      IF v_tblseqs IS NOT NULL THEN
        SELECT string_agg(t,', ' ORDER BY t), count(*) INTO v_tblseqErr, v_count FROM (
          SELECT t FROM unnest(v_tblseqs) AS t
            EXCEPT
          SELECT rel_schema || '.' || rel_tblseq FROM emaj.emaj_relation
            WHERE rel_time_range @> v_firstMarkTimeId AND rel_group = ANY (v_groupNames)    -- tables/sequences that belong to their group at the start mark time
          ) AS t2;
        IF v_count > 0 THEN
          IF v_count = 1 THEN
            RAISE EXCEPTION '_gen_sql_groups: 1 table/sequence (%) did not belong to any of the selected tables groups at % mark time.', v_tblseqErr, v_firstMark;
          ELSE
            RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) did not belong to any of the selected tables groups at % mark time.', v_count, v_tblseqErr, v_firstMark;
          END IF;
        END IF;
      END IF;
-- if there is no first mark for all groups, return quickly with a warning message
      IF v_firstMark IS NULL THEN
        RAISE WARNING '_gen_sql_groups: No mark exists for the group(s) "%".', array_to_string(v_groupNames,', ');
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
          VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
                  array_to_string(v_groupNames,','), 'No mark in the group(s) => no file has been generated');
        RETURN 0;
      END IF;
-- test the supplied output file name by inserting a temporary line (trap NULL or bad file name)
      BEGIN
        EXECUTE 'COPY (SELECT ''-- _gen_sql_groups() function in progress - started at '
                       || statement_timestamp() || ''') TO ' || quote_literal(v_location);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE EXCEPTION '_gen_sql_groups: The file "%" cannot be used as script output file.', v_location;
      END;
-- end of checks
-- create temporary table
      CREATE TEMP TABLE emaj_temp_script (
        scr_emaj_gid           BIGINT,              -- the emaj_gid of the corresponding log row,
                                                    --   0 for initial technical statements,
                                                    --   NULL for final technical statements
        scr_subid              INT,                 -- used to distinguish several generated sql per log row
        scr_emaj_txid          BIGINT,              -- for future use, to insert commit statement at each txid change
        scr_sql                TEXT                 -- the generated sql text
      );
-- for each application table referenced in the emaj_relation table, build SQL statements and process the related log table
-- build the restriction conditions on emaj_gid, depending on supplied mark range (the same for all tables)
      v_conditions = 'o.emaj_gid > ' || v_firstEmajGid;
      IF v_lastMarkTimeId IS NOT NULL THEN
        v_conditions = v_conditions || ' AND o.emaj_gid <= ' || v_lastEmajGid;
      END IF;
      FOR r_rel IN
          SELECT * FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                               -- tables belonging to the groups
              AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))        -- filtered or not by the user
              AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId,                            -- only tables having updates to process
                                    least(v_lastMarkTimeId, upper(rel_time_range))) > 0
            ORDER BY rel_priority, rel_schema, rel_tblseq
          LOOP
-- process the application table, by calling the _gen_sql_tbl() function
        SELECT emaj._gen_sql_tbl(r_rel, v_conditions) INTO v_nbSQL;
        v_cumNbSQL = v_cumNbSQL + v_nbSQL;
      END LOOP;
-- process sequences
      v_nbSeq = 0;
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_time_range FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              AND rel_time_range @> v_firstMarkTimeId                                              -- sequences belonging to the groups at the start mark
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))         -- filtered or not by the user
            ORDER BY rel_priority DESC, rel_schema DESC, rel_tblseq DESC
          LOOP
        v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- no supplied last mark and the sequence currently belongs to its group, so get current sequence characteritics
          IF emaj._pg_version_num() < 100000 THEN
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                    || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                   || 'FROM ' || v_fullSeqName INTO v_rqSeq;
          ELSE
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                    || ''' || '' RESTART '' || CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_size || CASE WHEN NOT cycle THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                   || 'FROM ' || v_fullSeqName  || ' rel, pg_catalog.pg_sequences ' ||
                  ' WHERE schemaname = ' || quote_literal(r_tblsq.rel_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_tblseq) INTO v_rqSeq;
          END IF;
        ELSE
-- a last mark is supplied, or the sequence does not belong to its groupe anymore, so get sequence characteristics from the emaj_sequence table
          v_endTimeId = CASE WHEN upper_inf(r_tblsq.rel_time_range) OR v_lastMarkTimeId < upper(r_tblsq.rel_time_range) THEN v_lastMarkTimeId
                             ELSE upper(r_tblsq.rel_time_range) END;
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                 || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                 || 'FROM emaj.emaj_sequence '
                 || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
                 || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
                 || '  AND sequ_time_id = ' || v_endTimeId INTO v_rqSeq;
        END IF;
-- insert into temp table
        v_nbSeq = v_nbSeq + 1;
        EXECUTE 'INSERT INTO emaj_temp_script '
             || 'SELECT NULL, -1 * ' || v_nbSeq || ', txid_current(), ' || quote_literal(v_rqSeq);
      END LOOP;
-- add initial comments
      IF v_lastMarkTimeId IS NOT NULL THEN
        v_endComment = ' and mark ' || v_lastMark;
      ELSE
        v_endComment = ' and the current situation';
      END IF;
      INSERT INTO emaj_temp_script SELECT 0, 1, 0, '-- SQL script generated by E-Maj at ' || statement_timestamp();
      INSERT INTO emaj_temp_script SELECT 0, 2, 0, '--    for tables group(s): ' || array_to_string(v_groupNames,',');
      INSERT INTO emaj_temp_script SELECT 0, 3, 0, '--    processing logs between mark ' || v_firstMark || v_endComment;
      IF v_tblseqs IS NOT NULL THEN
        INSERT INTO emaj_temp_script SELECT 0, 4, 0, '--    only for the following tables/sequences: ' || array_to_string(v_tblseqs,',');
      END IF;
-- encapsulate the sql statements inside a TRANSACTION
-- and manage the standard_conforming_strings option to properly handle special characters
      INSERT INTO emaj_temp_script SELECT 0, 10, 0, 'SET standard_conforming_strings = ON;';
      INSERT INTO emaj_temp_script SELECT 0, 11, 0, 'BEGIN TRANSACTION;';
      INSERT INTO emaj_temp_script SELECT NULL, 1, txid_current(), 'COMMIT;';
      INSERT INTO emaj_temp_script SELECT NULL, 2, txid_current(), 'RESET standard_conforming_strings;';
-- write the SQL script on the external file
      EXECUTE 'COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid ) TO '
            || quote_literal(v_location);
-- drop temporary table
      DROP TABLE IF EXISTS emaj_temp_script;
-- return the number of sql verbs generated into the output file
      v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    END IF;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
              array_to_string(v_groupNames,','), v_cumNbSQL || ' generated statements');
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  BEGIN
-- check the postgres version at groups creation time is compatible (i.e. >= 8.4)
    RETURN QUERY
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                   to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804
        ORDER BY msg;
-- check all application schemas referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'The application schema "' || rel_schema || '" does not exist any more.' AS msg
        FROM (
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range)
            EXCEPT
          SELECT nspname FROM pg_catalog.pg_namespace
             ) AS t
        ORDER BY msg;
-- check all application relations referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                        -- all expected application relations
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range)
            EXCEPT                                    -- minus relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r             -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range)
        ORDER BY t.rel_schema, t.rel_tblseq, 1;
-- check the log table for all tables referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_log_schema AND relname = rel_log_table
                   AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check the log function for each table referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
                 WHERE nspname = rel_log_schema AND proname = rel_log_function
                   AND pronamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log triggers
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_log_trg'
                   AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
--   then truncate triggers
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_trunc_trg'
                   AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM (
        WITH cte_app_tables_columns AS (                -- application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
                AND upper_inf(rel_time_range) AND rel_kind = 'r'),
             cte_log_tables_columns AS (                -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_log_table
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE AND attname NOT LIKE 'emaj%'
                AND upper_inf(rel_time_range) AND rel_kind = 'r')
        SELECT DISTINCT rel_schema, rel_tblseq,
               'In group "' || rel_group || '", the structure of the application table "' ||
                 rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
               rel_log_schema || '"."' || rel_log_table || '").' AS msg
          FROM (
            (                                           -- application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM cte_app_tables_columns
            EXCEPT                                      -- minus log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM cte_log_tables_columns
            )
            UNION
            (                                           -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM cte_log_tables_columns
            EXCEPT                                      --  minus application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM cte_app_tables_columns
            )) AS t
                           -- do not issue a row if the log or application table does not exist,
                           -- these cases have been already detected
        WHERE (rel_log_schema, rel_log_table) IN
              (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE relnamespace = pg_namespace.oid)
          AND (rel_schema, rel_tblseq) IN
              (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
        ) AS t;
-- check all tables of rollbackable groups have their primary key
    RETURN QUERY
      SELECT 'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
        FROM emaj.emaj_relation, emaj.emaj_group
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r' AND rel_group = group_name AND group_is_rollbackable
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                 WHERE nspname = rel_schema AND relname = rel_tblseq
                   AND relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                   AND contype = 'p')
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check all tables are persistent tables (i.e. have not been altered as UNLOGGED after their tables group creation)
    RETURN QUERY
      SELECT 'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND relpersistence <> 'p'
        ORDER BY rel_schema, rel_tblseq, 1;
-- check all tables are WITHOUT OIDS (i.e. have not been altered as WITH OIDS after their tables group creation)
    RETURN QUERY
      SELECT 'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is WITH OIDS.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND relhasoids
        ORDER BY rel_schema, rel_tblseq, 1;
-- check the primary key structure of all tables belonging to rollbackable groups is unchanged
    RETURN QUERY
      SELECT 'In rollbackable group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' || rel_sql_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM (
          SELECT rel_schema, rel_tblseq, rel_group, rel_sql_pk_columns,
                 string_agg(quote_ident(attname), ',' ORDER BY attnum) AS current_pk_columns
            FROM emaj.emaj_relation, emaj.emaj_group, pg_catalog.pg_attribute, pg_catalog.pg_index, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE -- join conditions
                  rel_group = group_name
              AND relname = rel_tblseq AND nspname = rel_schema
              AND pg_attribute.attrelid = pg_index.indrelid
              AND indrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                  -- filter conditions
              AND rel_kind = 'r' AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND attnum = ANY (indkey)
              AND indisprimary
              AND attnum > 0 AND attisdropped = FALSE
            GROUP BY rel_schema, rel_tblseq, rel_group, rel_sql_pk_columns
          ) AS t
          WHERE rel_sql_pk_columns <> current_pk_columns
        ORDER BY rel_schema, rel_tblseq, 1;
--
    RETURN;
  END;
$_verify_all_groups$;

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

GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._detailed_log_stat_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;

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
    IF emaj._pg_version_num() >= 90300 THEN
-- build the event trigger names array from the pg_event_trigger table
      SELECT coalesce(array_agg(evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
        FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- call the _enable_event_triggers() function
      PERFORM emaj._enable_event_triggers(v_event_trigger_array);
    END IF;
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
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';

-- insert the upgrade record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <devel>', 'Upgrade from 2.2.3 completed');

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
