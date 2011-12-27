--
-- E-Maj : extension migration from 0.10.0 to 0.10.1
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script migrates an existing E-Maj extension.
--

------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Creation of a specific function to check the migration conditions are met.
-- The function generates an exception if at least one condition is not met.
DO LANGUAGE plpgsql
$tmp$
  DECLARE
    v_emajVersion   TEXT;
    v_groupList     TEXT :='';
    r_group         RECORD;
  BEGIN
-- the emaj version registered in emaj_param must be '0.10'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '0.10.0' THEN
      RAISE EXCEPTION 'The current E-Maj version is not 0.10.0';
    END IF;
-- issue a warning regarding already created groups
-- no lock is performed on logging groups (the logging activity can continue during E-Maj version update)
    FOR r_group IN
        SELECT group_name FROM emaj.emaj_group
        LOOP
      v_groupList = v_groupList || ', '|| r_group.group_name;
    END LOOP;
    IF v_groupList <> '' THEN
      RAISE NOTICE 'All created groups (%) will need to be dropped and re-created to take benefit of all improvements brought by this E-Maj version update.', substr(v_groupList,3);
    END IF;
    RETURN;
  END;
$tmp$;

-- OK, upgrade...

UPDATE emaj.emaj_param SET param_value_text = '0.10.1' WHERE param_key = 'emaj_version';

------------------------------------
--                                --
-- emaj tables                    --
--                                --
------------------------------------
-- No emaj table structure change is required in this migration

------------------------------------
--                                --
-- emaj functions                 --
--                                --
------------------------------------
CREATE or REPLACE FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: schema name (mandatory even for the 'public' schema), table name, boolean indicating whether the table belongs to a rollbackable group
-- Are created: 
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
--    - the rollback function (one per table)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
-- variables for the name of tables, functions, triggers,...
    v_fullTableName         TEXT;
    v_emajSchema            TEXT := 'emaj';
    v_emajTblSpace          TEXT := 'tspemaj';
    v_logTableName          TEXT;
    v_logFnctName           TEXT;
    v_rlbkFnctName          TEXT;
    v_exceptionRlbkFnctName TEXT;
    v_logTriggerName        TEXT;
    v_truncTriggerName      TEXT;
    v_sequenceName          TEXT;
-- variables to hold pieces of SQL
    v_pkCondList            TEXT;
    v_colList               TEXT;
    v_setList               TEXT;
-- other variables
    v_attname               TEXT;
    v_relhaspkey            BOOLEAN;
    v_pgVersion             TEXT := emaj._pg_version();
    r_trigger               RECORD;
    v_triggerList           TEXT := '';
-- cursor to retrieve all columns of the application table
    col1_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute 
        WHERE attrelid = tbl 
          AND attnum > 0
          AND attisdropped = false;
-- cursor to retrieve all columns of table's primary key
-- (taking column names in pg_attribute from the table's definition instead of index definition is mandatory 
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
    col2_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute, pg_index 
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey) 
          AND indrelid = tbl AND indisprimary
          AND attnum > 0 AND attisdropped = false;
  BEGIN
-- check the table has a primary key
    SELECT true INTO v_relhaspkey FROM pg_class, pg_namespace, pg_constraint WHERE 
        relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
        contype = 'p' AND nspname = v_schemaName AND relname = v_tableName;
    IF NOT FOUND THEN
      v_relhaspkey = false;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', v_tableName;
    END IF;
-- OK, build the different name for table, trigger, functions,...
    v_fullTableName    := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_rlbkFnctName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_exceptionRlbkFnctName=substring(quote_literal(v_rlbkFnctName) FROM '^.(.*).$');
    v_logTriggerName   := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');
    v_sequenceName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_emaj_id_seq');

-- creation of the log table: the log table looks like the application table, with some additional technical columns
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName;
    EXECUTE 'CREATE TABLE ' || v_logTableName
         || '( LIKE ' || v_fullTableName || ') TABLESPACE ' || v_emajTblSpace ;
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_verb VARCHAR(3)';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_tuple VARCHAR(3)';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_id BIGSERIAL PRIMARY KEY';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_changed TIMESTAMPTZ DEFAULT clock_timestamp()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_txid BIGINT DEFAULT emaj._txid_current()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_user VARCHAR(32) DEFAULT session_user';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr()';
-- alter the sequence associated to the emaj_id column to set the increment to 2 (so that an update operation can safely have its 2 log rows)
    EXECUTE 'ALTER SEQUENCE ' || v_sequenceName || ' INCREMENT 2';

-- creation of the log fonction that will be mapped to the log trigger later
-- The new row is logged for each INSERT, the old row is logged for each DELETE 
-- and the old and the new rows are logged for each UPDATE
    EXECUTE 'CREATE or REPLACE FUNCTION ' || v_logFnctName || '() RETURNS trigger AS $logfnct$'
         || 'DECLARE'
         || '  V_EMAJ_ID   BIGINT;'
         || 'BEGIN'
         || '  IF (TG_OP = ''DELETE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''DEL'', ''OLD'';'
         || '    RETURN OLD;'
         || '  ELSIF (TG_OP = ''UPDATE'') THEN'
         || '    SELECT NEXTVAL(' || quote_literal(v_sequenceName) || ') INTO V_EMAJ_ID;'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''UPD'', ''OLD'', V_EMAJ_ID;'
         || '    V_EMAJ_ID = V_EMAJ_ID + 1;'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''UPD'', ''NEW'', V_EMAJ_ID;'
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
    EXECUTE 'DROP TRIGGER IF EXISTS ' || v_logTriggerName || ' ON ' || v_fullTableName;
    EXECUTE 'CREATE TRIGGER ' || v_logTriggerName
         || ' AFTER INSERT OR UPDATE OR DELETE ON ' || v_fullTableName
         || '  FOR EACH ROW EXECUTE PROCEDURE ' || v_logFnctName || '()';
    EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
-- creation of the trigger that blocks any TRUNCATE on the application table, using the common _forbid_truncate_fnct() function 
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    IF v_pgVersion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
      EXECUTE 'CREATE TRIGGER ' || v_truncTriggerName
           || ' BEFORE TRUNCATE ON ' || v_fullTableName
           || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()';
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
    END IF;
--
-- create the rollback function, if the table belongs to a rollbackable group 
--
    IF v_isRollbackable THEN
-- First build some pieces of the CREATE FUNCTION statement
--   build the tables's columns list
--     and the SET clause for the UPDATE, from the same columns list
      v_colList := '';
      v_setList := '';
      OPEN col1_curs (v_fullTableName);
      LOOP
        FETCH col1_curs INTO v_attname;
        EXIT WHEN NOT FOUND;
        IF v_colList = '' THEN
           v_colList := 'rec_log.' || quote_ident(v_attname);
           v_setList := quote_ident(v_attname) || ' = rec_old_log.' || quote_ident(v_attname);
        ELSE
           v_colList := v_colList || ', rec_log.' || quote_ident(v_attname);
           v_setList := v_setList || ', ' || quote_ident(v_attname) || ' = rec_old_log.' || quote_ident(v_attname);
        END IF;
      END LOOP;
      CLOSE col1_curs;
--   build "equality on the primary key" conditions, from the list of the primary key's columns
      v_pkCondList := '';
      OPEN col2_curs (v_fullTableName);
      LOOP
        FETCH col2_curs INTO v_attname;
        EXIT WHEN NOT FOUND;
        IF v_pkCondList = '' THEN
           v_pkCondList := quote_ident(v_attname) || ' = rec_log.' || quote_ident(v_attname);
        ELSE
           v_pkCondList := v_pkCondList || ' AND ' || quote_ident(v_attname) || ' = rec_log.' || quote_ident(v_attname);
        END IF;
      END LOOP;
      CLOSE col2_curs;
-- Then create the rollback function associated to the table
-- At execution, it will loop on each row from the log table in reverse order
--  It will insert the old deleted rows, delete the new inserted row 
--  and update the new rows by setting back the old rows
-- The function returns the number of rollbacked elementary operations or rows
-- All these functions will be called by the emaj_rlbk_tbl function, which is activated by the
--  emaj_rollback_group function
      EXECUTE 'CREATE or REPLACE FUNCTION ' || v_rlbkFnctName || ' (v_rollback_id_limit bigint)'
           || ' RETURNS bigint AS $rlbkfnct$'
           || '  DECLARE'
           || '    v_nb_rows       bigint := 0;'
           || '    v_nb_proc_rows  integer;'
           || '    rec_log     ' || v_logTableName || '%ROWTYPE;'
           || '    rec_old_log ' || v_logTableName || '%ROWTYPE;'
           || '    log_curs CURSOR FOR '
           || '      SELECT * FROM ' || v_logTableName
           || '        WHERE emaj_id >= v_rollback_id_limit '
           || '        ORDER BY emaj_id DESC;'
           || '  BEGIN'
           || '    OPEN log_curs;'
           || '    LOOP '
           || '      FETCH log_curs INTO rec_log;'
           || '      EXIT WHEN NOT FOUND;'
           || '      IF rec_log.emaj_verb = ''INS'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; INS'', rec_log.emaj_id;'
           || '          DELETE FROM ' || v_fullTableName || ' WHERE ' || v_pkCondList || ';'
           || '      ELSIF rec_log.emaj_verb = ''UPD'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; UPD ; %'', rec_log.emaj_id,rec_log.emaj_tuple;'
           || '          FETCH log_curs into rec_old_log;'
--         || '          RAISE NOTICE ''emaj_id = % ; UPD ; %'', rec_old_log.emaj_id,rec_old_log.emaj_tuple;'
           || '          UPDATE ' || v_fullTableName || ' SET ' || v_setList || ' WHERE ' || v_pkCondList || ';'
           || '      ELSIF rec_log.emaj_verb = ''DEL'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; DEL'', rec_log.emaj_id;'
           || '          INSERT INTO ' || v_fullTableName || ' VALUES (' || v_colList || ');'
           || '      ELSE'
           || '          RAISE EXCEPTION ''' || v_exceptionRlbkFnctName || ': internal error - emaj_verb = % unknown, emaj_id = %.'','
           || '            rec_log.emaj_verb, rec_log.emaj_id;' 
           || '      END IF;'
           || '      GET DIAGNOSTICS v_nb_proc_rows = ROW_COUNT;'
           || '      IF v_nb_proc_rows <> 1 THEN'
           || '        RAISE EXCEPTION ''' || v_exceptionRlbkFnctName || ': internal error - emaj_verb = %, emaj_id = %, # processed rows = % .'''
           || '           ,rec_log.emaj_verb, rec_log.emaj_id, v_nb_proc_rows;' 
           || '      END IF;'
           || '      v_nb_rows := v_nb_rows + 1;'
           || '    END LOOP;'
           || '    CLOSE log_curs;'
--         || '    RAISE NOTICE ''Table ' || v_fullTableName || ' -> % rollbacked rows.'', v_nb_rows;'
           || '    RETURN v_nb_rows;'
           || '  END;'
           || '$rlbkfnct$ LANGUAGE plpgsql;';
      END IF;
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger,
-- This check is not done for postgres 8.2 because column tgconstraint doesn't exist
    IF v_pgVersion >= '8.3' THEN
      FOR r_trigger IN 
        SELECT tgname FROM pg_trigger WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'%emaj\\_%\\_trg'
      LOOP
        IF v_triggerList = '' THEN
          v_triggerList = v_triggerList || r_trigger.tgname;
        ELSE
          v_triggerList = v_triggerList || ', ' || r_trigger.tgname;
        END IF;
      END LOOP;
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
      IF v_triggerList <> '' THEN
        RAISE WARNING '_create_tbl: table % has triggers (%). Verify the compatibility with emaj rollback operations (in particular if triggers update one or several other tables). Triggers may have to be manualy disabled before rollback.', v_fullTableName, v_triggerList;
      END IF;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE or REPLACE FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) 
RETURNS void LANGUAGE plpgsql AS 
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: schema name and sequence name
  BEGIN
-- delete rows from emaj_sequence 
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ' || quote_literal(v_schemaName) || ' AND sequ_name = ' || quote_literal(v_seqName);
    RETURN;
  END;
$_drop_seq$;

CREATE or REPLACE FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ,  v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_rlbk_tbl$
-- This function rollbacks one table to a given timestamp
-- The function is called by emaj._rlbk_groups_step5()
-- Input: schema name and table name, timestamp limit for rollback, flag to specify if log trigger 
--        must be disable during rollback operation, flag to specify if rollbacked log rows must be deleted,
--        last sequence and last hole identifiers to keep (greater ones being to be deleted)
-- These flags must be respectively:
--   - true and true   for common (unlogged) rollback,
--   - false and false for logged rollback, 
--   - true and false  for unlogged rollback with undeleted log rows (for emaj_rollback_and_stop_group function)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_fullTableName  TEXT;
    v_logTableName   TEXT;
    v_rlbkFnctName   TEXT;
    v_logTriggerName TEXT;
    v_fullSeqName    TEXT;
    v_seqName        TEXT;
    v_emaj_id        BIGINT;
    v_nb_rows        BIGINT;
    v_tsrlbk_start   TIMESTAMP;
    v_tsrlbk_end     TIMESTAMP;
    v_tsdel_start    TIMESTAMP;
    v_tsdel_end      TIMESTAMP;
  BEGIN
    v_fullTableName  := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName   := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_rlbkFnctName   := quote_ident(v_emajSchema) || '.' || 
                        quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_logTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_seqName        := v_schemaName || '_' || v_tableName || '_log_emaj_id_seq';
    v_fullSeqName    := quote_ident(v_seqName);
-- get the emaj_id to rollback to from the sequence (first emaj_id to delete)
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_emaj_id
      FROM emaj.emaj_sequence
      WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_timestamp;
    IF NOT FOUND THEN
       RAISE EXCEPTION '_rlbk_tbl: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_timestamp;
    END IF;
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'Up to log_id ' || v_emaj_id);
-- deactivate the log trigger on the application table, if needed (unlogged rollback)
    IF v_disableTrigger THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
    END IF;
-- record the time at the rollback start
    SELECT clock_timestamp() INTO v_tsrlbk_start;
-- rollback the table
    EXECUTE 'SELECT ' || v_rlbkFnctName || '(' || v_emaj_id || ')' INTO v_nb_rows;
-- record the time at the rollback
    SELECT clock_timestamp() INTO v_tsrlbk_end;
-- insert rollback duration into the emaj_rlbk_stat table, if at least 1 row has been processed
    IF v_nb_rows > 0 THEN
      INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
         VALUES ('rlbk', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsrlbk_end - v_tsrlbk_start);
    END IF;
-- if the caller requires it, suppress the rollbacked log part 
    IF v_deleteLog THEN
-- record the time at the delete start
      SELECT clock_timestamp() INTO v_tsdel_start;
-- delete obsolete log rows
      EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_id >= ' || v_emaj_id;
-- ... and suppress from emaj_sequence table the rows regarding the emaj log sequence for this application table
--     corresponding to potential later intermediate marks that disappear with the rollback operation
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_id > v_lastSequenceId;
-- record the sequence holes generated by the delete operation 
-- this is due to the fact that log sequences are not rollbacked, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_duration())
-- first delete, if exist, sequence holes that have disappeared with the rollback
      DELETE FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName AND sqhl_id > v_lastSeqHoleId;
-- and then insert the new sequence hole
      EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_hole_size) VALUES (' 
        || quote_literal(v_schemaName) || ',' || quote_literal(v_tableName) || ', ('
        || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM ' 
        || v_emajSchema || '.' || v_fullSeqName || ')-('
        || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
        || ' emaj.emaj_sequence WHERE'
        || ' sequ_schema = ''' || v_emajSchema 
        || ''' AND sequ_name = ' || quote_literal(v_seqName) 
        || ' AND sequ_datetime = ' || quote_literal(v_timestamp) || '))';
-- record the time at the delete
      SELECT clock_timestamp() INTO v_tsdel_end;
-- insert delete duration into the emaj_rlbk_stat table, if at least 1 row has been processed
      IF v_nb_rows > 0 THEN
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
           VALUES ('del_log', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsdel_end - v_tsdel_start);
      END IF;
    END IF;
-- re-activate the log trigger on the application table, if previously disabled
    IF v_disableTrigger THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
    END IF;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nb_rows || ' rollbacked rows');
    RETURN;
  END;
$_rlbk_tbl$;

CREATE or REPLACE FUNCTION emaj.emaj_verify_all() 
RETURNS SETOF TEXT LANGUAGE plpgsql AS 
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and 
-- emaj objects related to tables and sequences referenced in emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_pgVersion      TEXT := emaj._pg_version();
    v_finalMsg       TEXT := 'Global checking: no error encountered';
    r_object         RECORD;
    r_group          RECORD;
  BEGIN
-- detect if the current postgres version is at least 8.1
    IF v_pgVersion < '8.2' THEN
      RETURN NEXT 'Global checking: the current postgres version (' || version() || ') is not compatible with E-Maj.';
      v_finalMsg = '';
    END IF;
-- detect log tables that don't correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Global checking: table ' || relname || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_class, pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname LIKE E'%\\_log'
          AND relname NOT IN (SELECT rel_schema || '_' || rel_tblseq || '_log' FROM emaj.emaj_relation) 
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- verify that all log, rollback and truncate functions correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Global checking: function ' || proname  || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_proc, pg_namespace
        WHERE pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND 
          ((proname LIKE E'%\\_log\\_fnct' AND proname NOT IN (
            SELECT rel_schema || '_' || rel_tblseq || '_log_fnct' FROM emaj.emaj_relation))
           OR
           (proname LIKE E'%\\_rlbk\\_fnct' AND proname NOT IN (
            SELECT rel_schema || '_' || rel_tblseq || '_rlbk_fnct' FROM emaj.emaj_relation))
          )
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- final message for global check if no error has been yet detected 
    IF v_finalMsg <> '' THEN
      RETURN NEXT v_finalMsg;
    END IF;
-- verify all groups defined in emaj_group
    FOR r_group IN
      SELECT group_name FROM emaj.emaj_group ORDER BY 1
    LOOP 
      FOR r_object IN 
        SELECT msg FROM emaj._verify_group(r_group.group_name, false) msg
      LOOP
        RETURN NEXT r_object.msg;
      END LOOP;
    END LOOP;
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

CREATE OR REPLACE FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) 
RETURNS SETOF TEXT LANGUAGE plpgsql AS 
$_verify_group$
-- The function verifies the consistency between log and application tables for a group
-- Input: group name, boolean to specify if a detected error must raise an exception
-- If onErrorStop boolean is false, it returns a set of warning messages for discovered discrepancies.
-- If no error is detected, a single row is returned.
  DECLARE
    v_emajSchema        TEXT := 'emaj';
    v_pgVersion         TEXT := emaj._pg_version();
    v_finalMsg          TEXT;
    v_isRollbackable    BOOLEAN;
    v_creationPgVersion TEXT;
    v_msgPrefix         TEXT;
    v_msg               TEXT;
    v_fullTableName     TEXT;
    v_logTableName      TEXT;
    v_logFnctName       TEXT;
    v_rlbkFnctName      TEXT;
    v_logTriggerName    TEXT;
    v_truncTriggerName  TEXT;
    r_tblsq             RECORD;
  BEGIN
-- for 8.1-, E-Maj is not compatible
    IF v_pgVersion < '8.2' THEN
      RAISE EXCEPTION 'The current postgres version (%) is not compatible with E-Maj.', version();
    END IF;
-- get some characteristics of the group
    SELECT group_is_rollbackable, group_pg_version INTO v_isRollbackable, v_creationPgVersion 
      FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_verify_group: group % has not been created.', v_groupName;
    END IF;
-- Build message parts
    v_msgPrefix = 'Checking ' || v_groupName || ': ';
    v_finalMsg = v_msgPrefix || 'no error encountered';
-- check the postgres version at creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric. 
--          But we suppose these tests will not be useful anymore when pg 10.0 will appear!
--   for 8.2 and 8.3, both major versions must be the same
    IF ((v_pgVersion = '8.2' OR v_pgVersion = '8.3') AND substring (v_creationPgVersion FROM E'(\\d+\\.\\d+)') <> v_pgVersion) OR
--   for 8.4+, both major versions must be 8.4+
       (v_pgVersion >= '8.4' AND substring (v_creationPgVersion FROM E'(\\d+\\.\\d+)') < '8.4') THEN
      v_msg = v_msgPrefix || 'the group has been created with a non compatible postgresql version (' || v_creationPgVersion || '). It must be dropped and recreated.';
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
      RETURN NEXT v_msg;
      v_finalMsg = '';
    END IF;
-- per table verifications
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- check the class is unchanged
      IF r_tblsq.rel_kind <> emaj._check_class(r_tblsq.rel_schema, r_tblsq.rel_tblseq) THEN
        v_msg = v_msgPrefix || 'the relation type for ' || r_tblsq.rel_schema || '.' || r_tblsq.rel_tblseq || ' has changed (was ''' || r_tblsq.rel_kind || ''' at emaj_create_group time).';
        if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
        RETURN NEXT v_msg;
        v_finalMsg = '';
      END IF;
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, ...
        v_logTableName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log';
        v_logFnctName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_fnct';
        v_rlbkFnctName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_rlbk_fnct';
        v_logTriggerName   := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg';
        v_truncTriggerName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg';
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
--   -> check boths functions exists
        PERFORM proname FROM pg_proc , pg_namespace WHERE 
          pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_logFnctName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log function ' || v_logFnctName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        END IF;
        IF v_isRollbackable THEN
          PERFORM proname FROM pg_proc , pg_namespace WHERE 
            pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_rlbkFnctName;
          IF NOT FOUND THEN
            v_msg = v_msgPrefix || 'rollback function ' || v_rlbkFnctName || ' not found.';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
--   -> check both triggers exist
        PERFORM tgname FROM pg_trigger WHERE tgname = v_logTriggerName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log trigger ' || v_logTriggerName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        END IF;
        IF v_pgVersion >= '8.4' THEN
          PERFORM tgname FROM pg_trigger WHERE tgname = v_truncTriggerName;
          IF NOT FOUND THEN
            v_msg = v_msgPrefix || 'truncate trigger ' || v_truncTriggerName || ' not found.';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
--   -> check the log table exists
        PERFORM relname FROM pg_class, pg_namespace WHERE 
          relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname = v_logTableName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log table ' || v_logTableName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        ELSE
--   -> check that the log tables structure is consistent with the application tables structure
--      (same columns and same formats)
--      - added or changed column in application table
          PERFORM attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace 
            WHERE nspname = r_tblsq.rel_schema AND relnamespace = pg_namespace.oid AND relname = r_tblsq.rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
          EXCEPT
          SELECT attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace
            WHERE nspname = v_emajSchema AND relnamespace = pg_namespace.oid AND relname = v_logTableName
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%';
          IF FOUND THEN
            v_msg = v_msgPrefix || 'the structure of log table ' || v_logTableName || ' is not coherent with ' || v_fullTableName || ' (added or changed column?).';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
--      - missing or changed column in application table
          PERFORM attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace
            WHERE nspname = v_emajSchema AND relnamespace = pg_namespace.oid AND relname = v_logTableName
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
          EXCEPT
          SELECT attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace 
            WHERE nspname = r_tblsq.rel_schema AND relnamespace = pg_namespace.oid AND relname = r_tblsq.rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false;
          IF FOUND THEN
            v_msg = v_msgPrefix || 'the structure of log table ' || v_logTableName || ' is not coherent with ' || v_fullTableName || ' (dropped or changed column?).';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
-- if it is a sequence, nothing to do
      END IF;
    END LOOP;
    IF v_finalMsg <> '' THEN
-- OK, no error for the group
      RETURN NEXT v_finalMsg;
    END IF;
    RETURN;
  END;
$_verify_group$;

CREATE or REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_reset BOOLEAN) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time 
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion        TEXT := emaj._pg_version();
    v_i                INT;
    v_groupState       TEXT;
    v_nbTb             INT := 0;
    v_markName         TEXT;
    v_logTableName     TEXT;
    v_fullTableName    TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_cpt              BIGINT;
    r_tblsq            RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group is recorded in emaj_group table
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_start_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... and is in IDLE (i.e. not in a LOGGING) state
      IF v_groupState <> 'IDLE' THEN
        RAISE EXCEPTION '_start_group: The group % cannot be started because it is not in idle state. An emaj_stop_group function must be previously executed.', v_groupNames[v_i];
      END IF;
-- ... and is not damaged
      PERFORM 0 FROM emaj._verify_group(v_groupNames[v_i], true);
    END LOOP;
-- check and process the supplied mark name
-- (the group names array is set to NULL to not check the existence of the mark against groups as groups may have old deleted marks)
    SELECT emaj._check_new_mark(v_mark, NULL) INTO v_markName;
-- for each group, 
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      if v_reset THEN
-- ... if requested by the user, call the emaj_reset_group function to erase remaining traces from previous logs
        SELECT emaj._rst_group(v_groupNames[v_i]) INTO v_nbTb;
        IF v_nbTb = 0 THEN
          RAISE EXCEPTION '_start_group: Internal error - emaj_res_group for group % returned 0.', v_groupNames[v_i];
        END IF;
      END IF;
-- ... and check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(array[v_groupNames[v_i]]);
    END LOOP;
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
    PERFORM emaj._lock_groups(v_groupNames,'',v_multiGroup);
-- ... and enable all log triggers for the group
    v_nbTb = 0;
-- for each relation of the group,
    FOR r_tblsq IN
       SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
         WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
       LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_logTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
        v_truncTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
        EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
        IF v_pgVersion >= '8.4' THEN
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_truncTriggerName;
        END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- update the state of the group row from the emaj_group table
    UPDATE emaj.emaj_group SET group_state = 'LOGGING' WHERE group_name = ANY (v_groupNames);
-- Set the first mark for each group
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_markName);
    PERFORM emaj._set_mark_groups(v_groupNames, v_markName);
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_markName);
--
    RETURN v_nbTb;
  END;
$_start_groups$;

CREATE or REPLACE FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group
-- Input: group name, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_groupState    TEXT;
    v_markName      TEXT;
    v_nbTb          INT;
  BEGIN
-- insert begin into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'BEGIN', v_groupName, v_markName);
-- check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_set_mark_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE EXCEPTION 'emaj_set_mark_group: A mark cannot be set for group % because it is not in logging state. An emaj_start_group function must be previously executed.', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM 0 FROM emaj._verify_group(v_groupName, true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, array[v_groupName]) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(array[v_groupName],'ROW EXCLUSIVE',false);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(array[v_groupName], v_markName) into v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'END', v_groupName, v_markName);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time
-- Input: array of group names, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_validGroupNames TEXT[];
    v_groupState      TEXT;
    v_markName        TEXT;
    v_nbTb            INT;
  BEGIN
-- validate the group names array
    v_validGroupNames=emaj._check_group_names_array(v_groupNames);
-- if the group names array is null, immediately return 0
    IF v_validGroupNames IS NULL THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES ('SET_MARK_GROUPS', 'BEGIN', NULL, v_mark);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES ('SET_MARK_GROUPS', 'END', NULL, v_mark);
      RETURN 0;
    END IF;
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUPS', 'BEGIN', array_to_string(v_groupNames,','), v_mark);
-- for each group...
    FOR v_i in 1 .. array_upper(v_validGroupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
      SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_validGroupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: group % has not been created.', v_validGroupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF v_groupState <> 'LOGGING' THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: A mark cannot be set for group % because it is not in logging state. An emaj_start_group function must be previously executed.', v_validGroupNames[v_i];
      END IF;
-- ... check if the group is OK
      PERFORM 0 FROM emaj._verify_group(v_validGroupNames[v_i], true);
    END LOOP;
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_validGroupNames) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(v_validGroupNames,'ROW EXCLUSIVE',true);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(v_validGroupNames, v_markName) into v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(v_groupNames,','), v_mark);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT) IS 
$$Sets a mark on several E-Maj groups.$$;

CREATE or REPLACE FUNCTION emaj._set_mark_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like functions that start or rollback groups.
-- Input: group names array, mark to set, boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_pgVersion       TEXT := emaj._pg_version();
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    v_timestamp       TIMESTAMPTZ;
    v_lastSequenceId  BIGINT;
    v_lastSeqHoleId   BIGINT;
    v_fullSeqName     TEXT;
    v_seqName         TEXT;
    v_stmt            TEXT;
    r_tblsq           RECORD;
  BEGIN
-- look at the clock to get the 'official' timestamp representing the mark
    v_timestamp = clock_timestamp();
-- for each member of the groups, ...
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, record the emaj_id associated sequence parameters in the emaj sequence table
        v_seqName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_fullSeqName := quote_ident(v_emajSchema) || '.' || quote_ident(v_seqName);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' || 
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT '|| quote_literal(v_emajSchema) || ', ' || quote_literal(v_seqName) || ', ' ||
                 quote_literal(v_timestamp) || ', ' || quote_literal(v_mark) || ', ' || 'last_value, ';
        IF v_pgVersion <= '8.3' THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt || 
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- ... if it is a sequence, record the sequence parameters in the emaj sequence table
        v_fullSeqName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' || 
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' || 
                 quote_literal(r_tblsq.rel_tblseq) || ', ' || quote_literal(v_timestamp) || 
                 ', ' || quote_literal(v_mark) || ', last_value, ';
        IF v_pgVersion <= '8.3' THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt || 
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
      END IF;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the marks
-- get the last id for emaj_sequence and emaj_seq_hole tables insert the marks into the emaj_mark table
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSequenceId 
      FROM emaj.emaj_sequence_sequ_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSeqHoleId 
      FROM emaj.emaj_seq_hole_sqhl_id_seq;
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_datetime, mark_state, mark_last_sequence_id, mark_last_seq_hole_id) 
        VALUES (v_groupNames[v_i], v_mark, v_timestamp, 'ACTIVE', v_lastSequenceId, v_lastSeqHoleId);
    END LOOP;
-- 
    RETURN v_nbTb;
  END;
$_set_mark_groups$;

CREATE or REPLACE FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS integer LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function. 
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence 
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_groupState     TEXT;
    v_realMark       TEXT;
    v_markId         BIGINT;
    v_datetimeMark   TIMESTAMPTZ;
    v_idNewMin       BIGINT;
    v_markNewMin     TEXT;
    v_datetimeNewMin TIMESTAMPTZ;
    v_cpt            INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- count the number of mark in the group
    SELECT count(*) INTO v_cpt FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- and check there are at least 2 marks for the group
    IF v_cpt < 2 THEN
       RAISE EXCEPTION 'emaj_delete_mark_group: % is the only mark. It cannot be deleted.', v_mark;
    END IF;
-- OK, now get the id and timestamp of the mark to delete
    SELECT mark_id, mark_datetime INTO v_markId, v_datetimeMark
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- ... and the id and timestamp of the future first mark
    SELECT mark_id, mark_name, mark_datetime INTO v_idNewMin, v_markNewMin, v_datetimeNewMin 
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name <> v_realMark ORDER BY mark_id LIMIT 1;
    IF v_markId < v_idNewMin THEN
-- if the mark to delete is the first one, 
--   ... process its deletion with _delete_before_mark_group(), as the first rows of log tables become useless
      PERFORM emaj._delete_before_mark_group(v_groupName, v_markNewMin);
    ELSE
-- otherwise, 
--   ... the sequences related to the mark to delete can be suppressed
--         Delete first application sequences related data for the group
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation 
        WHERE sequ_mark = v_realMark AND sequ_datetime = v_datetimeMark
          AND rel_group = v_groupName AND rel_kind = 'S'
          AND sequ_schema = rel_schema AND sequ_name = rel_tblseq;
--         Delete then emaj sequences related data for the group
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation 
        WHERE sequ_mark = v_realMark AND sequ_datetime = v_datetimeMark
          AND rel_group = v_groupName AND rel_kind = 'r'
          AND sequ_schema = 'emaj' AND sequ_name = rel_schema || '_' || rel_tblseq || '_log_emaj_id_seq';
--   ... and the mark to delete can be physicaly deleted
      DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_mark);
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS integer LANGUAGE plpgsql AS
$emaj_delete_before_mark_group$
-- This function deletes all marks set before a given mark. 
-- Then, any rollback on the deleted marks will not be possible.
-- It deletes rows corresponding to the marks to delete from emaj_mark, emaj_sequence, emaj_seq_hole.  
-- It also deletes rows from all concerned log tables.
-- Input: group name, name of the new first mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark name.
-- Output: number of deleted marks
--   or NULL if the provided mark name is NULL
  DECLARE
    v_groupState     TEXT;
    v_realMark       TEXT;
    v_markId         BIGINT;
    v_datetimeMark   TIMESTAMPTZ;
    v_nbMark         INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: group % has not been created.', v_groupName;
    END IF;
-- return NULL if mark name is NULL
    IF v_mark IS NULL THEN
      RETURN NULL;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- effectively delete all marks before the supplied mark
    SELECT emaj._delete_before_mark_group(v_groupName, v_realMark) INTO v_nbMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', v_groupName,  v_nbMark || ' marks deleted');
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups_step1$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- It builds the requested number of sessions with the list of tables to process, trying to spread the load over all sessions.
-- It finaly inserts into the history the event about the rollback start
  DECLARE
    v_i                   INT;
    v_groupState          TEXT;
    v_isRollbackable      BOOLEAN;
    v_markName            TEXT;
    v_markState           TEXT;
    v_cpt                 INT;
    v_nbTblInGroup        INT;
    v_nbUnchangedTbl      INT;
    v_timestampMark       TIMESTAMPTZ;
    v_session             INT;
    v_sessionLoad         INT [];
    v_minSession          INT;
    v_minRows             INT;
    v_fullTableName       TEXT;
    v_msg                 TEXT;
    r_tbl                 RECORD;
    r_tbl2                RECORD;
  BEGIN
-- check that each group ...
-- ...is recorded in emaj_group table
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_state, group_is_rollbackable INTO v_groupState, v_isRollbackable FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_rlbk_groups_step1: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... is in LOGGING state
      IF v_groupState <> 'LOGGING' THEN
        RAISE EXCEPTION '_rlbk_groups_step1: Group % cannot be rollbacked because it is not in logging state.', v_groupNames[v_i];
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_isRollbackable THEN
        RAISE EXCEPTION '_rlbk_groups_step1: Group % has been created for audit only purpose. It cannot be rollbacked.', v_groupNames[v_i];
      END IF;
-- ... is not damaged
      PERFORM 0 FROM emaj._verify_group(v_groupNames[v_i],true);
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_groups_step1: No mark % exists for group %.', v_mark, v_groupNames[v_i];
      END IF;
-- ... and this mark is ACTIVE
      SELECT mark_state INTO v_markState FROM emaj.emaj_mark 
        WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
      IF v_markState <> 'ACTIVE' THEN
        RAISE EXCEPTION '_rlbk_groups_step1: mark % for group % is not in ACTIVE state.', v_markName, v_groupNames[v_i];
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(distinct emaj._get_mark_datetime(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_groups_step1: Mark % does not represent the same point in time for all groups.', v_mark;
    END IF;
-- get the mark timestamp for the 1st group (as we know this timestamp is the same for all groups of the array)
    SELECT emaj._get_mark_datetime(v_groupNames[1],v_mark) INTO v_timestampMark;
-- insert begin in the history
    IF v_unloggedRlbk THEN
      v_msg = 'Unlogged';
    ELSE
      v_msg = 'Logged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN', 
              array_to_string(v_groupNames,','), v_msg || ' rollback to mark ' || v_mark || ' [' || v_timestampMark || ']');
-- get the total number of tables for these groups
    SELECT sum(group_nb_table) INTO v_nbTblInGroup FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- issue warnings in case of foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(v_groupNames);
-- create sessions, using the number of sessions requested by the caller
-- session id for sequences will remain NULL
--   initialisation
--     accumulated counters of number of log rows to rollback for each parallel session 
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = 0;
    END LOOP;
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
--     fkey table
      DELETE FROM emaj.emaj_fk WHERE v_groupNames[v_i] = ANY (fk_groups);
--     relation table: for each group, session set to NULL and 
--       numbers of log rows computed by emaj_log_stat_group function
      UPDATE emaj.emaj_relation SET rel_session = NULL, rel_rows = stat_rows 
        FROM emaj.emaj_log_stat_group (v_groupNames[v_i], v_mark, NULL) stat
        WHERE rel_group = v_groupNames[v_i]
          AND rel_group = stat_group AND rel_schema = stat_schema AND rel_tblseq = stat_table;
    END LOOP;
--   count the number of tables that have no update to rollback
    SELECT count(*) INTO v_nbUnchangedTbl FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames) AND rel_rows = 0;
--   allocate tables with rows to rollback to sessions starting with the heaviest to rollback tables
--     as reported by emaj_log_stat_group function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' ORDER BY rel_rows DESC
        LOOP
--   is the table already allocated to a session (it may have been already allocated because of a fkey link) ?
      PERFORM 1 FROM emaj.emaj_relation 
        WHERE rel_group = ANY (v_groupNames) AND rel_schema = r_tbl.rel_schema AND rel_tblseq = r_tbl.rel_tblseq 
          AND rel_session IS NULL;
--   no, 
      IF FOUND THEN
--   compute the least loaded session
        v_minSession=1; v_minRows = v_sessionLoad [1];
        FOR v_session IN 2 .. v_nbSession LOOP
          IF v_sessionLoad [v_session] < v_minRows THEN
            v_minSession = v_session;
            v_minRows = v_sessionLoad [v_session];
          END IF;
        END LOOP;
--   allocate the table to the session, with all other tables linked by foreign key constraints
        v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + 
                 emaj._rlbk_groups_set_session(v_groupNames, r_tbl.rel_schema, r_tbl.rel_tblseq, v_minSession, r_tbl.rel_rows);
      END IF;
    END LOOP;
    RETURN v_nbTblInGroup - v_nbUnchangedTbl;
  END;
$_rlbk_groups_step1$;

CREATE or REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) 
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS 
$emaj_detailed_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks as viewed through the log tables
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of updates by user and table
  DECLARE
    v_groupState         TEXT;
    v_emajSchema         TEXT := 'emaj';
    v_realFirstMark      TEXT;
    v_realLastMark       TEXT;
    v_firstMarkId        BIGINT;
    v_lastMarkId         BIGINT;
    v_tsFirstMark        TIMESTAMPTZ;
    v_tsLastMark         TIMESTAMPTZ;
    v_firstEmajId        BIGINT;
    v_lastEmajId         BIGINT;
    v_logTableName       TEXT;
    v_seqName            TEXT;
    v_stmt               TEXT;
    r_tblsq              RECORD;
    r_stat               RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: group % has not been created.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_detailed_log_stat_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_lastMarkId, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the log table name and its sequence name for this table
        v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- get the next emaj_id for the first mark from the sequence
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_firstEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsFirstMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_detailed_log_stat_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsFirstMark;
          END IF;
        END IF;
-- get the next emaj_id for the last mark from the sequence
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_lastEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsLastMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_detailed_log_stat_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsLastMark;
          END IF;
        END IF;
-- prepare and execute the statement
        v_stmt= 'SELECT ' || quote_literal(v_groupName) || '::TEXT as emaj_group,'
             || ' ' || quote_literal(r_tblsq.rel_schema) || '::TEXT as emaj_schema,'
             || ' ' || quote_literal(r_tblsq.rel_tblseq) || '::TEXT as emaj_table,'
             || ' emaj_user,'
             || ' CASE WHEN emaj_verb = ''INS'' THEN ''INSERT'''
             ||      ' WHEN emaj_verb = ''UPD'' THEN ''UPDATE'''
             ||      ' WHEN emaj_verb = ''DEL'' THEN ''DELETE'''
             ||      ' ELSE ''?'' END::VARCHAR(6) as emaj_verb,'
             || ' count(*) as emaj_rows'
             || ' FROM ' || v_logTableName 
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN v_stmt = v_stmt 
             || ' AND emaj_id >= '|| v_firstEmajId ;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN v_stmt = v_stmt
             || ' AND emaj_id < '|| v_lastEmajId ;
        END IF;
        v_stmt = v_stmt
             || ' GROUP BY emaj_group, emaj_schema, emaj_table, emaj_user, emaj_verb'
             || ' ORDER BY emaj_user, emaj_verb';
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END IF;
    END LOOP;
    RETURN;
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks.$$;

CREATE or REPLACE FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) 
RETURNS interval LANGUAGE plpgsql AS 
$emaj_estimate_rollback_duration$
-- This function computes an approximate duration of a rollback to a predefined mark for a group.
-- It takes into account the content of emaj_rollback_stat table filled by previous rollback operations.
-- It also uses several parameters from emaj_param table.
-- "Logged" and "Unlogged" rollback durations are estimated with the same algorithm. (the cost of log insertion
-- for logged rollback balances the cost of log deletion of unlogged rollback) 
-- Input: group name, the mark name of the rollback operation
-- Output: the approximate duration that the rollback would need as time interval
  DECLARE
    v_nbTblSeq              INTEGER;
    v_markName              TEXT;
    v_markState             TEXT;
    v_estim_duration        INTERVAL;
    v_avg_row_rlbk          INTERVAL;
    v_avg_row_del_log       INTERVAL;
    v_fixed_table_rlbk      INTERVAL;
    v_fixed_table_with_rlbk INTERVAL;
    v_estim                 INTERVAL;
    r_tblsq                 RECORD;
    r_fkey		            RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table and get the number of tables and sequences
    SELECT group_nb_table + group_nb_sequence INTO v_nbTblSeq FROM emaj.emaj_group 
      WHERE group_name = v_groupName and group_state = 'LOGGING';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: group % has not been created or is not in LOGGING state.', v_groupName;
    END IF;
-- check the mark exists
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_markName;
    IF NOT FOUND OR v_markName IS NULL THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: no mark % exists for group %.', v_mark, v_groupName;
    END IF;
-- check the mark is ACTIVE
    SELECT mark_state INTO v_markState FROM emaj.emaj_mark 
      WHERE mark_group = v_groupName AND mark_name = v_markName;
    IF v_markState <> 'ACTIVE' THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: mark % for group % is not in ACTIVE state.', v_markName, v_groupName;
    END IF;
-- get all needed duration parameters from emaj_param table, 
--   or get default values for rows that are not present in emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'fixed_table_rollback_duration'),'5 millisecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'fixed_table_with_rollback_duration'),'2.5 millisecond'::interval)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_fixed_table_rlbk, v_fixed_table_with_rlbk;
-- compute the fixed cost for the group
    v_estim_duration = v_nbTblSeq * v_fixed_table_rlbk;
--
-- walk through the list of tables with their number of rows to rollback as returned by the emaj_log_stat_group function
--
-- for each table with content to rollback
    FOR r_tblsq IN
        SELECT stat_schema, stat_table, stat_rows FROM emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) WHERE stat_rows > 0
        LOOP
--
-- compute the rollback duration estimate for the table
--
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'rlbk' AND rlbk_nb_rows > 0
          AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'rlbk' AND rlbk_nb_rows > 0
            AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
        IF v_estim IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estim = v_avg_row_rlbk * r_tblsq.stat_rows;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_fixed_table_with_rlbk + v_estim;
--
-- compute the log rows delete duration for the table
--
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'del_log' AND rlbk_nb_rows > 0
          AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'del_log' AND rlbk_nb_rows > 0 
            AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
        IF v_estim IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estim = v_avg_row_del_log * r_tblsq.stat_rows;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_estim;
    END LOOP;
--
-- walk through the list of foreign key constraints concerned by the estimated rollback
--
-- for each foreign key
    FOR r_fkey IN
      SELECT c.conname, n.nspname, t.relname, t.reltuples
        FROM pg_constraint c, pg_namespace n, pg_class t, emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) s
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND stat_rows > 0                                              -- table to effectively rollback only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND n.nspname = s.stat_schema AND t.relname = s.stat_table     -- join on group tables to effectively rollback
      UNION
--           and the foreign keys referenced by tables that are concerned by the rollback operation
      SELECT c.conname, n.nspname, t.relname, t.reltuples
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace rn, pg_class rt, emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) s
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND stat_rows > 0                                              -- table to effectively rollback only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace 
          AND rn.nspname = s.stat_schema AND rt.relname = s.stat_table   -- join on group tables to effectively rollback
        LOOP
-- estimate the recreation duration of a fkey 
      IF r_fkey.reltuples = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estim = 0;
	  ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbk_duration) * r_fkey.reltuples / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat
          WHERE rlbk_operation = 'add_fk' AND rlbk_nb_rows > 0
            AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname;
        IF v_estim IS NULL THEN
-- non empty table, but no statistics with at least one row are available => take the last duration for this fkey, if any
          SELECT rlbk_duration INTO v_estim FROM emaj.emaj_rlbk_stat
            WHERE rlbk_operation = 'add_fk' AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname AND rlbk_datetime =
             (SELECT max(rlbk_datetime) FROM emaj.emaj_rlbk_stat
                WHERE rlbk_operation = 'add_fk' AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname);
          IF v_estim IS NULL THEN
-- definitely no statistics available
            v_estim = 0;
          END IF;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_estim;
    END LOOP;
    RETURN v_estim_duration;
  END;
$emaj_estimate_rollback_duration$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_duration(TEXT,TEXT) IS
$$Estimates the duration of a potential rollback of an E-Maj group to a given mark.$$;

CREATE or REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_snap_group$
-- This function creates a file for each table and sequence belonging to the group.
-- For tables, these files contain all rows sorted on primary key.
-- For sequences, they contain a single row describing the sequence.
-- To do its job, the function performs COPY TO statement, with all default parameters.
-- For table without primary key, rows are sorted on all columns.
-- There is no need for the group to be in IDLE state.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before 
-- emaj_snap_group function call, and 
--   - maintain its content outside E-maj.
-- Input: group name, the absolute pathname of the directory where the files are to be created
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    r_tblsq           RECORD;
    v_fullTableName   TEXT;
    r_col             RECORD;
    v_colList         TEXT;
    v_fileName        TEXT;
    v_stmt text;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: group % has not been created.', v_groupName;
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName := v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
      v_fullTableName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   first build the order by column list
        v_colList := '';
        PERFORM 0 FROM pg_class, pg_namespace, pg_constraint 
          WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
                contype = 'p' AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
        IF FOUND THEN
--   the table has a pkey,
          FOR r_col IN
              SELECT attname FROM pg_attribute, pg_index 
                WHERE pg_attribute.attrelid = pg_index.indrelid 
                  AND attnum = ANY (indkey) 
                  AND indrelid = v_fullTableName::regclass AND indisprimary
                  AND attnum > 0 AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList := r_col.attname;
            ELSE
               v_colList := v_colList || ',' || r_col.attname;
            END IF;
          END LOOP;
        ELSE
--   the table has no pkey
          FOR r_col IN
              SELECT attname FROM pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList := r_col.attname;
            ELSE
               v_colList := v_colList || ',' || r_col.attname;
            END IF;
          END LOOP;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO ' || quote_literal(v_fileName) || ' CSV';
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, the statement has no order by
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ') TO ' || quote_literal(v_fileName) || ' CSV';
      END IF;
-- and finaly perform the COPY
--    raise notice 'emaj_snap_group: Executing %',v_stmt;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' || 
            quote_literal('E-Maj snap of tables group ' || v_groupName || ' at ' || to_char(transaction_timestamp(),'DD/MM/YYYY HH24:MI:SS')) || 
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE or REPLACE FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark
-- For log tables, files contain all rows related to the time frame, sorted on emaj_id.
-- For sequences, files are names <group>_sequences_at_<mark>. They contain one row per sequence.
-- To do its job, the function performs COPY TO statement, using the CSV option.
-- There is no need for the group to be in IDLE state.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before 
-- emaj_snap_log_group function call, and 
--   - maintain its content outside E-maj.
-- Input: group name, the 2 mark names defining a range, the absolute pathname of the directory where the files are to be created
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string can NOT be used as last_mark (the current sequences state is not recorded in to the emaj_sequence table)
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    r_tblsq           RECORD;
    v_seqName         TEXT;
    v_realFirstMark   TEXT;
    v_realLastMark    TEXT;
    v_firstMarkId     BIGINT;
    v_lastMarkId      BIGINT;
    v_tsFirstMark     TIMESTAMPTZ;
    v_tsLastMark      TIMESTAMPTZ;
    v_firstEmajId     BIGINT;
    v_lastEmajId      BIGINT;
    v_logTableName    TEXT;
    v_fileName        TEXT;
    v_stmt text;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_log_group: group % has not been created.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_snap_log_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    ELSE
      SELECT mark_name, mark_id, mark_datetime INTO v_realFirstMark, v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_snap_log_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_lastMarkId, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    ELSE
      RAISE EXCEPTION 'emaj_snap_log_group: an explicit end mark must be supplied.';
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_snap_log_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- process all log tables of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- process tables
-- compute names
        v_fileName     := v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log.snap';
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
-- get the next emaj_id for the first mark from the sequence
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_firstEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsFirstMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_snap_log_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsFirstMark;
          END IF;
        END IF;
-- get the next emaj_id for the last mark from the sequence
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_lastEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsLastMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_snap_log_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsLastMark;
          END IF;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE TRUE';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          v_stmt = v_stmt || ' AND emaj_id >= '|| v_firstEmajId ;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          v_stmt = v_stmt || ' AND emaj_id < '|| v_lastEmajId ;
        END IF;
        v_stmt = v_stmt || ' ORDER BY emaj_id ASC) TO ' || quote_literal(v_fileName) || ' CSV';
-- and finaly perform the COPY
--      raise notice 'emaj_snap_log_group: Executing %',v_stmt;
        EXECUTE v_stmt;
      END IF;
-- for sequences, just adjust the counter
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName := v_dir || '/' || v_groupName || '_sequences_at_' || v_realFirstMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM ' || v_emajSchema || '.emaj_sequence, ' || v_emajSchema || '.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realFirstMark) || ' AND ' || 
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' CSV';
--  raise notice 'emaj_snap_log_group: Executing %',v_stmt;
    EXECUTE v_stmt;
-- generate the file for sequences state at end mark
    v_fileName := v_dir || '/' || v_groupName || '_sequences_at_' || v_realLastMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM ' || v_emajSchema || '.emaj_sequence, ' || v_emajSchema || '.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realLastMark) || ' AND ' || 
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' CSV';
--  raise notice 'emaj_snap_log_group: Executing %',v_stmt;
    EXECUTE v_stmt;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' || 
            quote_literal('E-Maj log tables snap of group ' || v_groupName || ' between marks ' || v_realFirstMark || ' and ' || v_realLastMark || ' at ' || to_char(transaction_timestamp(),'DD/MM/YYYY HH24:MI:SS')) || 
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------
-- revoke grants on all function from PUBLIC
REVOKE ALL ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) FROM PUBLIC;

-- and give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) TO emaj_adm; 

-- and give appropriate rights on functions to emaj_viewer role
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) TO emaj_viewer;

------------------------------------
--                                --
-- commit migration               --
--                                --
------------------------------------

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 0.10.1', 'Migration from 0.10.0 completed');

