--
-- E-Maj: migration from 2.0.0 to <NEXT_VERSION>
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
-- the emaj version registered in emaj_param must be '2.0.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '2.0.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 2.0.0',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.1
    IF current_setting('server_version_num')::int < 90100 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with E-Maj <NEXT_VERSION>. The PostgreSQL version should be at least 9.1.', current_setting('server_version');
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

DO
$do$
  DECLARE
  BEGIN
-- if tspemaj tablespace exists, use it as default_tablespace for emaj tables creation
--   and grant the create rights on it to emaj_adm
    PERFORM 0 FROM pg_tablespace WHERE spcname = 'tspemaj';
    IF FOUND THEN
      SET default_tablespace TO tspemaj;
      GRANT CREATE ON TABLESPACE tspemaj TO emaj_adm;
    END IF;
  END;
$do$;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

---------------------------------------
--                                   --
-- emaj tables, views and sequences  --
--                                   --
---------------------------------------

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
--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------
DROP FUNCTION emaj._rlbk_check(V_GROUPNAMES TEXT[],V_MARK TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._create_tbl(r_grpdef emaj.emaj_group_def, v_groupName TEXT, v_isRollbackable BOOLEAN, v_defTsp TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: the emaj_group_def row related to the application table to process, the group name, a boolean indicating whether the group is rollbackable, and the default tablespace to use if no specific tablespace is set for this application table
-- Are created in the log schema:
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_emajNamesPrefix        TEXT;
    v_logSchema              TEXT;
    v_fullTableName          TEXT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_baseLogTableName       TEXT;
    v_baseLogIdxName         TEXT;
    v_baseLogFnctName        TEXT;
    v_baseSequenceName       TEXT;
    v_logTableName           TEXT;
    v_logIdxName             TEXT;
    v_logFnctName            TEXT;
    v_sequenceName           TEXT;
    v_relPersistence         CHAR(1);
    v_relhaspkey             BOOLEAN;
    v_stmt                   TEXT;
    v_triggerList            TEXT;
  BEGIN
-- check the table is neither a temporary nor an unlogged table
    SELECT relpersistence INTO v_relPersistence FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid AND nspname = r_grpdef.grpdef_schema AND relname = r_grpdef.grpdef_tblseq;
    IF v_relPersistence = 't' THEN
      RAISE EXCEPTION '_create_tbl: table "%" is a temporary table.', r_grpdef.grpdef_tblseq;
    ELSIF v_relPersistence = 'u' THEN
      RAISE EXCEPTION '_create_tbl: table "%.%" is an unlogged table.', r_grpdef.grpdef_schema, r_grpdef.grpdef_tblseq;
    END IF;
-- check the table has a primary key
    SELECT true INTO v_relhaspkey FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
      WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
        AND contype = 'p' AND nspname = r_grpdef.grpdef_schema AND relname = r_grpdef.grpdef_tblseq;
    IF NOT FOUND THEN
      v_relhaspkey = false;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table "%.%" has no PRIMARY KEY.', r_grpdef.grpdef_schema, r_grpdef.grpdef_tblseq;
    END IF;
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
    v_logDatTsp = coalesce(r_grpdef.grpdef_log_dat_tsp, v_defTsp);
    v_logIdxTsp = coalesce(r_grpdef.grpdef_log_idx_tsp, v_defTsp);
    v_dataTblSpace = coalesce('TABLESPACE ' || quote_ident(v_logDatTsp),'');
    v_idxTblSpace = coalesce('TABLESPACE ' || quote_ident(v_logIdxTsp),'');
-- creation of the log table: the log table looks like the application table, with some additional technical columns
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
          AND attnum > 0 AND attnotnull AND attisdropped = false AND attname NOT LIKE E'emaj\\_%') AS t;
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
               (rel_schema, rel_tblseq, rel_group, rel_priority, rel_log_schema,
                rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function)
        VALUES (r_grpdef.grpdef_schema, r_grpdef.grpdef_tblseq, r_grpdef.grpdef_group, r_grpdef.grpdef_priority, v_logSchema,
                v_logDatTsp, v_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName);
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    SELECT string_agg(tgname, ', ') INTO v_triggerList FROM (
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'emaj\\_%\\_trg') AS t;
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: table "%" has triggers (%). Verify the compatibility with emaj rollback operations (in particular if triggers update one or several other tables). Triggers may have to be manualy disabled before rollback.', v_fullTableName, v_triggerList;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._delete_log_tbl(r_rel emaj.emaj_relation, v_beginTimeId BIGINT, v_endTimeId BIGINT, v_lastGlobalSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_delete_log_tbl$
-- This function deletes the part of a log table corresponding to updates that have been rolled back.
-- The function is only called by emaj._rlbk_session_exec(), for unlogged rollbacks.
-- It deletes sequences records corresponding to marks that are not visible anymore after the rollback.
-- It also registers the hole in sequence numbers generated by the deleted log rows.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        begin and end time stamp ids to define the time range identifying the hole to create in the log sequence
--        global sequence value limit for rollback, mark timestamp,
--        flag to specify if the rollback is logged
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- delete obsolete log rows
    EXECUTE 'DELETE FROM ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' WHERE emaj_gid > ' || v_lastGlobalSeq;
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- record the sequence holes generated by the delete operation
-- this is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_group() and emaj_estimate_rollback_groups())
-- first delete, if exist, sequence holes that have disappeared with the rollback
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
        AND sqhl_begin_time_id >= v_beginTimeId AND sqhl_begin_time_id < v_endTimeId;
-- and then insert the new sequence hole
    EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size) VALUES ('
      || quote_literal(r_rel.rel_schema) || ',' || quote_literal(r_rel.rel_tblseq) || ',' || v_beginTimeId || ',' || v_endTimeId || ', ('
      || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM '
      || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)
      || ')-('
      || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
      || ' emaj.emaj_sequence WHERE'
      || ' sequ_schema = ' || quote_literal(r_rel.rel_log_schema)
      || ' AND sequ_name = ' || quote_literal(r_rel.rel_log_sequence)
      || ' AND sequ_time_id = ' || v_beginTimeId || '))';
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_group$
-- This function sets a protection on a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name
-- Output: 1 if successful, 0 if the group was already in protected state
  DECLARE
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_groupIsProtected       BOOLEAN;
    v_status                 INT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_rollbackable, group_is_logging, group_is_rlbk_protected INTO v_groupIsRollbackable, v_groupIsLogging, v_groupIsProtected
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_protect_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_protect_group: The group "%" cannot be protected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_protect_group: The group "%" cannot be protected because it is not in LOGGING state.', v_groupName;
    END IF;
-- OK, set the protection
    IF v_groupIsProtected THEN
      v_status = 0;
    ELSE
      UPDATE emaj.emaj_group SET group_is_rlbk_protected = TRUE WHERE group_name = v_groupName;
      v_status = 1;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_GROUP', v_groupName, 'Status ' || v_status);
    RETURN v_status;
  END;
$emaj_protect_group$;
COMMENT ON FUNCTION emaj.emaj_protect_group(TEXT) IS
$$Sets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_mark_group$
-- This function sets a protection on a mark for a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name, mark to protect
-- Output: 1 if successful, 0 if the mark was already in protected state
-- The group must be ROLLBACKABLE and in LOGGING state.
  DECLARE
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_realMark               TEXT;
    v_markIsProtected        BOOLEAN;
    v_status                 INT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: group "%" has not been created.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: A mark on the group "%" cannot be protected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: A mark on the group "%" cannot be protected because it is not in LOGGING state.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: mark "%" does not exist for group "%".', v_mark, v_groupName;
    END IF;
-- OK, set the protection
    SELECT mark_is_rlbk_protected INTO v_markIsProtected FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
    IF v_markIsProtected THEN
      v_status = 0;
    ELSE
      UPDATE emaj.emaj_mark SET mark_is_rlbk_protected = TRUE WHERE mark_group = v_groupName AND mark_name = v_realMark;
      v_status = 1;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_MARK_GROUP', v_groupName, 'Mark ' || v_realMark || ' ; status ' || v_status);
    RETURN v_status;
  END;
$emaj_protect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_protect_mark_group(TEXT,TEXT) IS
$$Sets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT, isRollbackSimulation BOOLEAN DEFAULT FALSE)
RETURNS TEXT LANGUAGE plpgsql AS
$_rlbk_check$
-- This functions performs checks on group names and mark names supplied as parameter for the emaj_rollback_groups()
-- and emaj_estimate_rollback_groups() functions.
-- It returns the real mark name.
  DECLARE
    v_aGroupName             TEXT;
    v_groupIsLogging         BOOLEAN;
    v_groupIsProtected       BOOLEAN;
    v_groupIsRollbackable    BOOLEAN;
    v_markName               TEXT;
    v_markId                 BIGINT;
    v_markIsDeleted          BOOLEAN;
    v_protectedMarkList      TEXT;
    v_cpt                    INT;
  BEGIN
-- check that each group ...
-- ...is recorded in emaj_group table
    FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
      SELECT group_is_logging, group_is_rollbackable, group_is_rlbk_protected INTO v_groupIsLogging, v_groupIsRollbackable, v_groupIsProtected
        FROM emaj.emaj_group WHERE group_name = v_aGroupName;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_rlbk_check: group "%" has not been created.', v_aGroupName;
      END IF;
-- ... is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION '_rlbk_check: Group "%" is not in LOGGING state.', v_aGroupName;
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_groupIsRollbackable THEN
        RAISE EXCEPTION '_rlbk_check: Group "%" has been created for audit only purpose.', v_aGroupName;
      END IF;
-- ... is not protected against rollback (check disabled for rollback simulation)
      IF v_groupIsProtected AND NOT isRollbackSimulation THEN
        RAISE EXCEPTION '_rlbk_check: Group "%" is currently protected against rollback.', v_aGroupName;
      END IF;
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_aGroupName,v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_check: No mark "%" exists for group "%".', v_mark, v_aGroupName;
      END IF;
-- ... and this mark is ACTIVE
      SELECT mark_id, mark_is_deleted INTO v_markId, v_markIsDeleted FROM emaj.emaj_mark
        WHERE mark_group = v_aGroupName AND mark_name = v_markName;
      IF v_markIsDeleted THEN
        RAISE EXCEPTION '_rlbk_check: mark "%" for group "%" is not usable for rollback.', v_markName, v_aGroupName;
      END IF;
-- ... and the rollback wouldn't delete protected marks (check disabled for rollback simulation)
      IF NOT isRollbackSimulation THEN
        SELECT string_agg(mark_name,', ') INTO v_protectedMarkList FROM (
          SELECT mark_name FROM emaj.emaj_mark
            WHERE mark_group = v_aGroupName AND mark_id > v_markId AND mark_is_rlbk_protected
            ORDER BY mark_id) AS t;
        IF v_protectedMarkList IS NOT NULL THEN
          RAISE EXCEPTION '_rlbk_check: protected marks (%) for group "%" block the rollback to mark "%".', v_protectedMarkList, v_aGroupName, v_markName;
        END IF;
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_time_id(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_check: Mark "%" does not represent the same point in time for all groups.', v_mark;
    END IF;
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql SECURITY DEFINER AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: a group names array, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
    v_nbTblseq               INT;
  BEGIN
-- check supplied group names and mark parameters with the isRollbackSimulation flag set to true
    SELECT emaj._rlbk_check(v_groupNames, v_mark, TRUE) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::int INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_nb_session)
        VALUES (v_rlbkId, v_groupNames, v_mark, emaj._get_mark_time_id(v_groupNames[1], v_markName), v_isLoggedRlbk, 1);
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
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::interval)
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

CREATE OR REPLACE FUNCTION emaj.emaj_enable_protection_by_event_triggers()
 RETURNS INT LANGUAGE plpgsql AS
$emaj_enable_protection_by_event_triggers$
-- This function enables all known E-Maj event triggers that are in disabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively enabled event triggers
  DECLARE
    v_event_trigger_array    TEXT[];
  BEGIN
    IF emaj._pg_version_num() >= 90300 THEN
-- build the event trigger names array from the pg_event_trigger table
-- (pg_event_trigger table doesn't exists in 9.2- postgres versions)
      SELECT coalesce(array_agg(evtname  ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
        FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- call the _enable_event_triggers() function
      PERFORM emaj._enable_event_triggers(v_event_trigger_array);
    END IF;
-- insert a row into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS ENABLED',
              CASE WHEN v_event_trigger_array <> ARRAY[]::TEXT[] THEN array_to_string(v_event_trigger_array, ', ') ELSE '<none>' END);
-- return the number of disabled event triggers
    RETURN coalesce(array_length(v_event_trigger_array,1),0);
  END;
$emaj_enable_protection_by_event_triggers$;
COMMENT ON FUNCTION emaj.emaj_enable_protection_by_event_triggers() IS
$$Enables the protection of E-Maj components by event triggers.$$;

CREATE OR REPLACE FUNCTION emaj._disable_event_triggers()
 RETURNS TEXT[] LANGUAGE plpgsql SECURITY DEFINER AS
$_disable_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- The function is called by functions that alter or drop E-Maj components, such as emaj_drop_group().
-- It is also called by the user emaj_disable_event_triggers_protection() function.
-- Output: array of effectively disabled event trigger names. It can be reused as input when calling _enable_event_triggers()
  DECLARE
    v_event_trigger          TEXT;
    v_event_trigger_array    TEXT[] = ARRAY[]::TEXT[];
  BEGIN
    IF emaj._pg_version_num() >= 90300 THEN
-- build the event trigger names array from the pg_event_trigger table
-- (pg_event_trigger table doesn't exists in 9.2- postgres versions)
      SELECT coalesce(array_agg(evtname ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
        FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled <> 'D';
-- disable each event trigger
      FOREACH v_event_trigger IN ARRAY v_event_trigger_array
      LOOP
        EXECUTE 'ALTER EVENT TRIGGER ' || v_event_trigger || ' DISABLE';
      END LOOP;
    END IF;
    RETURN v_event_trigger_array;
  END;
$_disable_event_triggers$;

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
UPDATE emaj.emaj_param SET param_value_text = '<NEXT_VERSION>' WHERE param_key = 'emaj_version';

-- insert the upgrade record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <NEXT_VERSION>', 'Upgrade from 2.0.0 completed');

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

