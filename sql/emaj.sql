--
-- E-Maj : logs and rollbacks table updates : V 0.10.0
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script creates all objects needed to log and rollback table updates.
--
-- These objects are recorded into a specific schema, named "emaj". 
-- It mainly contains few technical tables, few types, a set of functions, and one "log table" per processed application table.
--
-- This script must be executed by a role having SUPERUSER privileges.
-- Before its execution:
-- 	-> the concerned cluster must contain a tablespace named "tspemaj", for instance previously created by
--	   CREATE TABLESPACE tspemaj LOCATION '/.../tspemaj',
--	-> the plpgsql language must have been created in the concerned database.

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO WARNING;
\echo 'E-maj objects creation...'

BEGIN TRANSACTION;

------------------------------------
--                                --
-- emaj schema and tables         --
--                                --
------------------------------------

-- (re)creation of the schema 'emaj' containing all the needed objets

DROP SCHEMA IF EXISTS emaj CASCADE;
CREATE SCHEMA emaj;

-- uncomment the next line to let emaj schema visible to all user (for test purpose)
--GRANT USAGE ON SCHEMA emaj TO PUBLIC;

-- creation of a specific function to create a dummy txid_current function with postgres 8.2
CREATE or REPLACE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_pgversion     TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_stmt          TEXT;
  BEGIN
    v_stmt = 'CREATE or REPLACE FUNCTION emaj.emaj_txid_current() RETURNS BIGINT LANGUAGE SQL AS $$ SELECT ';
    IF v_pgversion < '8.3' THEN
      v_stmt = v_stmt || '0::BIGINT;$$';
    ELSE
      v_stmt = v_stmt || 'txid_current();$$';
    END IF;
    EXECUTE v_stmt;
    RETURN;
  END;
$tmp$;
SELECT emaj.tmp();
DROP FUNCTION emaj.tmp();

-- creation of the technical tables

-- table containing Emaj parameters
CREATE TABLE emaj.emaj_param (
    param_key                TEXT        NOT NULL,
    param_value_text         TEXT,
    param_value_int          BIGINT,
    param_value_boolean      BOOLEAN,
    param_value_interval     INTERVAL,
    PRIMARY KEY (param_key) 
    ) TABLESPACE tspemaj;

-- table containing the history of operations 
CREATE TABLE emaj.emaj_hist (
    hist_id                  SERIAL      NOT NULL,
    hist_datetime            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    hist_function            TEXT        NOT NULL,
    hist_event               TEXT,
    hist_object              TEXT,
    hist_wording             TEXT,
    hist_user                TEXT        DEFAULT session_user,
    PRIMARY KEY (hist_id)
    ) TABLESPACE tspemaj;

-- table containing the definition of groups' content. Filled and maintained by the user, it is used by emaj_create_group function.
CREATE TABLE emaj.emaj_group_def (
    grpdef_group             TEXT        NOT NULL,       -- name of the group containing this table or sequence
    grpdef_schema            TEXT        NOT NULL,       -- schema name of this table or sequence
    grpdef_tblseq            TEXT        NOT NULL,       -- table or sequence name
    PRIMARY KEY (grpdef_group, grpdef_schema, grpdef_tblseq)
    ) TABLESPACE tspemaj;

-- table containing the defined groups
--     rows are created at emaj_create_group time and deleted at emaj_drop_group time
CREATE TABLE emaj.emaj_group (
    group_name               TEXT        NOT NULL,
    group_state              TEXT        NOT NULL,       -- 2 possibles states: 'LOGGING' between emaj_start_group and emaj_stop_group
                                                         --                     'IDLE' in other cases
    group_nb_table           INT,                        -- number of tables at emaj_create_group time
    group_nb_sequence        INT,                        -- number of sequences at emaj_create_group time
    group_creation_datetime  TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (group_name)
    ) TABLESPACE tspemaj;

-- table containing the relations (tables and sequences) of created tables groups
CREATE TABLE emaj.emaj_relation (
    rel_schema               TEXT        NOT NULL,
    rel_tblseq               TEXT        NOT NULL,
    rel_group                TEXT        NOT NULL,
    rel_kind                 TEXT,                       -- similar to the relkind column of pg_class table ('r' = table, 'S' = sequence) 
    rel_subgroup             INT,                        -- subgroup id, computed at rollback time
    rel_rows                 BIGINT,                     -- number of rows to rollback, computed at rollback time
    PRIMARY KEY (rel_schema, rel_tblseq),
    FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
    ) TABLESPACE tspemaj;

-- table containing the marks
CREATE TABLE emaj.emaj_mark (
    mark_group               TEXT        NOT NULL,
    mark_name                TEXT        NOT NULL,
    mark_datetime            TIMESTAMPTZ NOT NULL,
    mark_state               TEXT,
    mark_txid                BIGINT      DEFAULT emaj.emaj_txid_current(),
    PRIMARY KEY (mark_group, mark_name),
    FOREIGN KEY (mark_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
    ) TABLESPACE tspemaj;

-- table containing the sequences log 
-- (to record the state at mark time of application sequences and sequences used by log tables) 
CREATE TABLE emaj.emaj_sequence (
    sequ_schema              TEXT        NOT NULL,
    sequ_name                TEXT        NOT NULL,
    sequ_datetime            TIMESTAMPTZ NOT NULL,
    sequ_mark                TEXT        NOT NULL,
    sequ_last_val            BIGINT      NOT NULL,
    sequ_start_val           BIGINT      NOT NULL,
    sequ_increment           BIGINT      NOT NULL,
    sequ_max_val             BIGINT      NOT NULL,
    sequ_min_val             BIGINT      NOT NULL,
    sequ_cache_val           BIGINT      NOT NULL,
    sequ_is_cycled           BOOLEAN     NOT NULL,
    sequ_is_called           BOOLEAN     NOT NULL,
    PRIMARY KEY (sequ_schema, sequ_name, sequ_datetime)
    ) TABLESPACE tspemaj;

-- table containing the holes in sequences log
-- these holes are due to rollback operations that do not adjust log sequences
-- the hole size = difference of sequence's current last_value and last value at the rollback mark
CREATE TABLE emaj.emaj_seq_hole (
    sqhl_schema              TEXT        NOT NULL,
    sqhl_table               TEXT        NOT NULL,
    sqhl_datetime            TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    sqhl_hole_size           BIGINT      NOT NULL,
    PRIMARY KEY (sqhl_schema, sqhl_table, sqhl_datetime)
    ) TABLESPACE tspemaj;

-- table containing statistics about previously executed rollback operations
-- and used to estimate rollback durations 
CREATE TABLE emaj.emaj_rlbk_stat (
    rlbk_operation           TEXT        NOT NULL,     -- can contains 'rlbk', 'del_log', 'cre_fk'
    rlbk_schema              TEXT        NOT NULL,
    rlbk_tbl_fk              TEXT        NOT NULL,     -- table name of fk name
    rlbk_datetime            TIMESTAMPTZ NOT NULL,
    rlbk_nb_rows             BIGINT      NOT NULL,
    rlbk_duration            INTERVAL    NOT NULL,
    PRIMARY KEY (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime)
    ) TABLESPACE tspemaj;

-- working storage table containing foreign key definition
-- (used at table rollback time to drop and later recreate foreign keys)
CREATE TABLE emaj.emaj_fk (
    fk_group                 TEXT        NOT NULL,
    fk_subgroup              INT         NOT NULL,
    fk_name                  TEXT        NOT NULL,
    fk_schema                TEXT        NOT NULL,
    fk_table                 TEXT        NOT NULL,
    fk_def                   TEXT        NOT NULL,
    PRIMARY KEY (fk_group, fk_name),
    FOREIGN KEY (fk_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
    ) TABLESPACE tspemaj;

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

CREATE TYPE emaj.emaj_log_stat_type AS (
    stat_group     TEXT,
    stat_schema    TEXT,
    stat_table     TEXT,
    stat_rows      BIGINT
    );

CREATE TYPE emaj.emaj_detailed_log_stat_type AS (
    stat_group     TEXT,
    stat_schema    TEXT,
    stat_table     TEXT,
    stat_role      VARCHAR(32),
    stat_verb      VARCHAR(6),
    stat_rows      BIGINT
    );

------------------------------------
--                                --
-- Default parameters             --
--                                --
------------------------------------
INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('emaj_version','0.9');
-- The history_retention parameter defines the time interval when a row remains in the emaj history table - default is 1 month
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','1 month'::interval);
-- The log_only parameter defines if the rollback capability is activated or not. 
-- If log_only is on, the rollback functions are not created and rollback functions are not possible. 
-- This behaviour is available to test logging mechanism on tables that have no primary key.
INSERT INTO emaj.emaj_param (param_key, param_value_boolean) VALUES ('log_only','false');
-- The avg_row_rollback_duration parameter defines the average duration needed to rollback a row.
-- The avg_row_delete_log_duration parameter defines the average duration needed to delete log rows.
-- The fixed_table_rollback_duration parameter defines the fixed rollback cost for any table or sequence belonging to a group
-- The fixed_table_with_rollback_duration parameter defines the additional fixed rollback cost for any table that has effective rows to rollback
-- They are used by the emaj_estimate_rollback_duration function as a default value to compute the approximate duration of a rollback operation.
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','100 microsecond'::interval);
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','10 microsecond'::interval);
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','5 millisecond'::interval);
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_with_rollback_duration','2.5 millisecond'::interval);
------------------------------------
--                                --
-- Low level Functions            --
--                                --
------------------------------------

CREATE or REPLACE FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) 
RETURNS TEXT LANGUAGE plpgsql AS
$_check_class$
-- This function verifies that an application table or sequence exists in pg_class
-- It also protects from a recursive use : tables or sequences from emaj schema cannot be managed by EMAJ
-- Input: the names of the schema and the class (table or sequence)
-- Output: the relkind of the class : 'r' for a table and 's' for a sequence
-- If the schema or the class is not known, the function stops.
  DECLARE
    v_relkind      TEXT;
    v_schemaOid    OID;
  BEGIN
    IF v_schemaName = 'emaj' THEN
      RAISE EXCEPTION '_check_class : object from schema % cannot be managed by EMAJ', v_schemaName;
    END IF;
    SELECT oid INTO v_schemaOid FROM pg_namespace WHERE nspname = v_schemaName;
    IF NOT found THEN
      RAISE EXCEPTION '_check_class : schema % doesn''t exist', v_schemaName;
    END IF;
    SELECT relkind INTO v_relkind FROM pg_class 
      WHERE relNameSpace = v_schemaOid AND relName = v_className AND relkind in ('r','S');
    IF NOT found THEN
      RAISE EXCEPTION '_check_class : table or sequence % doesn''t exist', v_className;
    END IF; 
    RETURN v_relkind;
  END;
$_check_class$;

CREATE or REPLACE FUNCTION emaj._create_log(v_schemaName TEXT, v_tableName TEXT, v_logOnly BOOLEAN) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_create_log$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: schema name (mandatory even for the 'public' schema) and table name
-- Are created: 
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
--    - the rollback function (one per table)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.

  DECLARE
-- variables for the name of tables, functions, triggers,...
    v_fullTableName    TEXT;
    v_emajSchema       TEXT := 'emaj';
    v_emajTblSpace     TEXT := 'tspemaj';
    v_logTableName     TEXT;
    v_logFnctName      TEXT;
    v_rlbkFnctName     TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_sequenceName     TEXT;
-- variables to hold pieces of SQL
    v_pkCondList       TEXT;
    v_colList          TEXT;
    v_setList          TEXT;
-- other variables
    v_attname          TEXT;
    v_relhaspkey       BOOLEAN;
    v_pgversion        TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    r_trigger          RECORD;
    v_triggerList      TEXT := '';
-- cursor to retrieve all columns of the application table
    col1_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute 
        WHERE attrelid = tbl 
          AND attnum > 0
          AND attisdropped = false;
-- cursor to retrieve all columns of table's primary key
    col2_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute, pg_index 
        WHERE pg_attribute.attrelid = pg_index.indexrelid 
          AND indrelid = tbl AND indisprimary
          AND attnum > 0 AND attisdropped = false;
  BEGIN
-- check the table has a primary key
    BEGIN
      SELECT relhaspkey INTO v_relhaspkey FROM pg_class, pg_namespace WHERE 
        relnamespace = pg_namespace.oid AND nspname = v_schemaName AND relname = v_tableName;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_create_log: Internal error - schema.table not found in pg_class';
      END IF;
    END;
    IF NOT v_logOnly AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_log : table % has no PRIMARY KEY', v_tableName;
    END IF;
-- OK, build the different name for table, trigger, functions,...
    v_fullTableName    := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_rlbkFnctName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
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
         || ' ADD COLUMN emaj_id BIGSERIAL';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_changed TIMESTAMPTZ DEFAULT clock_timestamp()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_txid BIGINT DEFAULT emaj.emaj_txid_current()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_user VARCHAR(32) DEFAULT session_user';
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
         || '    SELECT NEXTVAL(''' || v_sequenceName || ''') INTO V_EMAJ_ID;'
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
    IF v_pgversion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
      EXECUTE 'CREATE TRIGGER ' || v_truncTriggerName
           || ' BEFORE TRUNCATE ON ' || v_fullTableName
           || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()';
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
    END IF;
--
-- create the rollback function
--
    IF NOT v_logOnly THEN
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
           v_colList := 'rec_log.' || v_attname;
           v_setList := v_attname || ' = rec_old_log.' || v_attname;
        ELSE
           v_colList := v_colList || ', rec_log.' || v_attname;
           v_setList := v_setList || ', ' || v_attname || ' = rec_old_log.' || v_attname;
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
           v_pkCondList := v_attname || ' = rec_log.' || v_attname;
        ELSE
           v_pkCondList := v_pkCondList || ' AND ' || v_attname || ' = rec_log.' || v_attname;
        END IF;
      END LOOP;
      CLOSE col2_curs;
-- Then create the rollback function associated to the table
-- At execution, it will loop on each row from the log table in reverse order
--  It will insert the old deleted rows, delete the new inserted row 
--  and update the new rows by setting back the old rows
-- The function returns the number of rollbacked elementary operations or rows
-- All these functions will be called by the emaj_rlbk_table function, which is activated by the
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
           || '          RAISE EXCEPTION ''' || v_rlbkFnctName || ': internal error - emaj_verb = % unknown, emaj_id = %'','
           || '            rec_log.emaj_verb, rec_log.emaj_id;' 
           || '      END IF;'
           || '      GET DIAGNOSTICS v_nb_proc_rows = ROW_COUNT;'
           || '      IF v_nb_proc_rows <> 1 THEN'
           || '        RAISE EXCEPTION ''' || v_rlbkFnctName || ': internal error - emaj_verb = %, emaj_id = %, # processed rows = % '''
           || '           ,rec_log.emaj_verb, rec_log.emaj_id, v_nb_proc_rows;' 
           || '      END IF;'
           || '      v_nb_rows := v_nb_rows + 1;'
           || '    END LOOP;'
           || '    CLOSE log_curs;'
--         || '    RAISE NOTICE ''Table ' || v_fullTableName || ' -> % rollbacked rows'', v_nb_rows;'
           || '    RETURN v_nb_rows;'
           || '  END;'
           || '$rlbkfnct$ LANGUAGE plpgsql;';
      END IF;
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger,
-- This check is not done for postgres 8.2 because column tgconstraint doesn't exist
    IF v_pgversion >= '8.3' THEN
      FOR r_trigger IN 
        SELECT tgname FROM pg_trigger WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE '%emaj_log_trg'
      LOOP
        IF v_triggerList = '' THEN
          v_triggerList = v_triggerList || r_trigger.tgname;
        ELSE
          v_triggerList = ', ' || v_triggerList || r_trigger.tgname;
        END IF;
      END LOOP;
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
      IF v_triggerList <> '' THEN
        RAISE WARNING '_create_log: table % has triggers (%). Verify the compatibility with emaj rollback operations (in particular if triggers update one or several other tables). Triggers may have to be manualy disabled before rollback.', v_fullTableName, v_triggerList;
      END IF;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_log$;

CREATE or REPLACE FUNCTION emaj._delete_log(v_schemaName TEXT, v_tableName TEXT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_delete_log$
-- The function deletes all what has been created by _create_log function
-- Required inputs: schema name (mandatory even if "public") and table name
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema       TEXT := 'emaj';
    v_pgversion        TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_fullTableName    TEXT;
    v_logTableName     TEXT;
    v_logFnctName      TEXT;
    v_rlbkFnctName     TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;

  BEGIN
    v_fullTableName    := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_rlbkFnctName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_logTriggerName   := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');

-- delete the log trigger on the application table
    EXECUTE 'DROP TRIGGER IF EXISTS ' || v_logTriggerName || ' ON ' || v_fullTableName;
-- delete the truncate trigger on the application table
    IF v_pgversion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
    END IF;
-- delete both log and rollback functions
    EXECUTE 'DROP FUNCTION IF EXISTS ' || v_logFnctName || '()';
    EXECUTE 'DROP FUNCTION IF EXISTS ' || v_rlbkFnctName || '(bigint)';
-- delete the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName || ' CASCADE';

    RETURN;
  END;
$_delete_log$;

CREATE or REPLACE FUNCTION emaj._delete_seq(v_schemaName TEXT, v_seqName TEXT) 
RETURNS void LANGUAGE plpgsql AS 
$_delete_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: schema name and sequence name
  BEGIN
-- delete rows from emaj_sequence 
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ''' || v_schemaName || ''' AND sequ_name = ''' || v_seqName || '''';
    RETURN;
  END;
$_delete_seq$;

CREATE or REPLACE FUNCTION emaj._rlbk_table(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ,  v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_rlbk_table$
-- This function rollbacks one table to a given timestamp
-- The function is called by emaj.emaj_rollback_group
-- Input: schema name and table name, timestamp limit for rollback, flag to specify if log trigger 
--        must be disable during rollback operation and flag to specify if rollbacked log rows must be deleted.
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
       RAISE EXCEPTION '_rlbk_table: internal error - sequence for % and % not found in emaj_sequence',v_seqName, v_timestamp;
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
-- record the time at the rollback and insert rollback duration into the emaj_rlbk_stat table
    SELECT clock_timestamp() INTO v_tsrlbk_end;
    INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
       VALUES ('rlbk', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsrlbk_end - v_tsrlbk_start);
-- if the caller requires it, suppress the rollbacked log part 
    IF v_deleteLog THEN
-- record the time at the delete start
      SELECT clock_timestamp() INTO v_tsdel_start;
-- delete obsolete log rows
      EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_id >= ' || v_emaj_id;
-- ... and suppress from emaj_sequence table the rows regarding the emaj log sequence for this application table
--     corresponding to potential later intermediate marks that disappear with the rollback operation
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime > v_timestamp;
-- record the sequence holes generated by the delete operation 
-- this is due to the fact that log sequences are not rollbacked, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_duration())
-- first delete, if exist, sequence holes that have disappeared with the rollback
      DELETE FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName AND sqhl_datetime > v_timestamp;
-- and then insert the new sequence hole
      EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_hole_size) VALUES (''' 
        || v_schemaName || ''',''' || v_tableName || ''', ('
        || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM ' 
        || v_emajSchema || '.' || v_fullSeqName || ')-('
        || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
        || ' emaj.emaj_sequence WHERE'
        || ' sequ_schema = ''' || v_emajSchema 
        || ''' AND sequ_name = ''' || v_seqName 
        || ''' AND sequ_datetime = ''' || v_timestamp || '''))';
-- record the time at the delete and insert delete duration into the emaj_rlbk_stat table
      SELECT clock_timestamp() INTO v_tsdel_end;
      INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
         VALUES ('del_log', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsdel_end - v_tsdel_start);
   END IF;
-- re-activate the log trigger on the application table, if previously disabled
   IF v_disableTrigger THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
   END IF;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nb_rows || ' rollbacked rows ');
    RETURN;
  END;
$_rlbk_table$;

CREATE or REPLACE FUNCTION emaj._rlbk_sequence(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_rlbk_sequence$
-- This function rollbacks one sequence to a given mark
-- The function is used by emaj.emaj_rollback_group
-- Input: schema name and table name, mark
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application sequence.
  DECLARE
    v_pgversion     TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_fullSeqName   TEXT;
    v_stmt          TEXT;
    mark_seq_rec    RECORD;
    curr_seq_rec    RECORD;

  BEGIN
-- Read sequence's characteristics at mark time
    BEGIN
      SELECT   sequ_schema, sequ_name, sequ_mark, sequ_last_val, sequ_start_val, sequ_increment,
               sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called
        INTO STRICT mark_seq_rec 
        FROM emaj.emaj_sequence 
        WHERE sequ_schema = v_schemaName AND sequ_name = v_seqName AND sequ_datetime = v_timestamp;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RAISE EXCEPTION '_rlbk_sequence: Mark at % not found for sequence %.%', v_timestamp, v_schemaName, v_seqName;
        WHEN TOO_MANY_ROWS THEN
          RAISE EXCEPTION '_rlbk_sequence: Internal error 1';
    END;
-- Read the current sequence's characteristics
    v_fullSeqName := quote_ident(v_schemaName) || '.' || quote_ident(v_seqName);
    v_stmt = 'SELECT last_value, ';
    IF v_pgversion <= '8.3' THEN
       v_stmt = v_stmt || '0 as start_value, '; 
    ELSE
       v_stmt = v_stmt || 'start_value, ';
    END IF;
    v_stmt = v_stmt || 'increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM '
                    || v_fullSeqName;
    EXECUTE v_stmt INTO STRICT curr_seq_rec;
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
--    RAISE NOTICE 'Rollback sequence % with%', v_fullSeqName, v_stmt;
      EXECUTE 'ALTER SEQUENCE ' || v_fullSeqName || v_stmt;
    END IF;
-- if the caller requires it, delete the rollbacked intermediate sequences from the sequence table
    IF v_deleteLog THEN
      DELETE FROM emaj.emaj_sequence 
        WHERE sequ_schema = v_schemaName AND sequ_name = v_seqName AND sequ_datetime > v_timestamp;
    END IF;
-- insert event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) 
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, SUBSTR(v_stmt,2));
    RETURN;
  END;
$_rlbk_sequence$;

CREATE or REPLACE FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ) 
RETURNS BIGINT LANGUAGE plpgsql AS 
$_log_stat_table$
-- This function returns the number of log rows for a single table between 2 marks or between a mark and the current situation.
-- It is called by emaj_log_stat_group function
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: schema name and table name, the timestamps of both marks 
--   a NULL value as last timestamp mark indicates the current situation
-- Output: number of log rows between both marks for the table
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_fullSeqName     TEXT;
    v_beginLastValue  BIGINT;
    v_endLastValue    BIGINT;
    v_sumHole         BIGINT;
  BEGIN
-- get the log table id at first mark time 
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_beginLastValue
       FROM emaj.emaj_sequence
       WHERE sequ_schema = v_emajSchema 
         AND sequ_name = v_schemaName || '_' || v_tableName || '_log_emaj_id_seq'
         AND sequ_datetime = v_tsFirstMark;
    IF v_tsLastMark IS NULL THEN
-- last mark is NULL, so examine the current state of the log table id
      v_fullSeqName := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_emaj_id_seq');
      EXECUTE 'SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM ' || v_fullSeqName INTO v_endLastValue;
--   and count the sum of hole from the start mark time until now
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole 
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_datetime >= v_tsFirstMark;
    ELSE
-- last mark is not NULL, so get the log table id at last mark time
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_endLastValue
         FROM emaj.emaj_sequence
         WHERE sequ_schema = v_emajSchema 
           AND sequ_name = v_schemaName || '_' || v_tableName || '_log_emaj_id_seq'
           AND sequ_datetime = v_tsLastMark;
--   and count the sum of hole from the start mark time to the end mark time
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole 
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_datetime BETWEEN v_tsFirstMark AND v_tsLastMark;
    END IF;
-- return the stat row for the table
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole)/2;
  END;
$_log_stat_table$;

CREATE or REPLACE FUNCTION emaj.emaj_verify_all() 
RETURNS SETOF TEXT LANGUAGE plpgsql AS 
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and 
-- emaj objects related to tables and sequences referenced in emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_finalMsg       TEXT := 'No error encountered';
    r_object         RECORD;
  BEGIN
-- detect log tables that don't correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Table ' || relname || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_class, pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname LIKE '%_log'
          AND relname NOT IN (SELECT rel_schema || '_' || rel_tblseq || '_log' FROM emaj.emaj_relation) 
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- verify that all log functions correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Function ' || proname  || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_proc, pg_namespace
        WHERE pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname LIKE '%_log_fnct'
          AND proname NOT IN (SELECT rel_schema || '_' || rel_tblseq || '_log_fnct' FROM emaj.emaj_relation)
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- verify that all rollback functions correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Function ' || proname  || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_proc, pg_namespace
        WHERE pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname LIKE '%_rlbk_fnct'
          AND proname NOT IN (SELECT rel_schema || '_' || rel_tblseq || '_rlbk_fnct' FROM emaj.emaj_relation)
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
    IF v_finalMsg <> '' THEN
      RETURN NEXT v_finalMsg;
    END IF;
    RETURN;
  END;
$emaj_verify_all$;

CREATE or REPLACE FUNCTION emaj._forbid_truncate_fnct() RETURNS TRIGGER AS $_forbid_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of a group in logging mode.
-- It can only be called with postgresql in a version greater or equal 8.4
BEGIN
  IF (TG_OP = 'TRUNCATE') THEN
    RAISE EXCEPTION 'emaj._forbid_truncate_fnct: TRUNCATE is not allowed while updates on this table (%) are currently protected by E-Maj. Consider tomay stop the group before issuing a TRUNCATE.', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
END;
$_forbid_truncate_fnct$ LANGUAGE plpgsql SECURITY DEFINER;

------------------------------------------------
----                                        ----
----       Functions to manage groups       ----
----                                        ----
------------------------------------------------

CREATE or REPLACE FUNCTION emaj._verify_group(v_groupName TEXT) 
RETURNS void LANGUAGE plpgsql AS 
$_verify_group$
-- The function verifies the consistency between log and application tables for a group
-- It generates an error if the check fails
  DECLARE
    v_emajSchema       TEXT := 'emaj';
    v_pgversion        TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_fullTableName    TEXT;
    v_logTableName     TEXT;
    v_logFnctName      TEXT;
    v_rlbkFnctName     TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_logOnly          BOOLEAN := false;          -- emaj parameter telling if rollback functions have to be skipped (for test)
    r_tblsq            RECORD;
  BEGIN
-- get the log_only parameter
    SELECT param_value_boolean INTO v_logOnly FROM emaj.emaj_param WHERE param_key = 'log_only';
-- per table verifications
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
-- check the class is unchanged
      IF r_tblsq.rel_kind <> emaj._check_class(r_tblsq.rel_schema, r_tblsq.rel_tblseq) THEN
        RAISE EXCEPTION '_verify_group: The relation type for %.% has changed (was ''%'' at emaj_create_group time)', r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_kind;
      END IF;
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, ...
        v_logTableName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log';
        v_logFnctName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_fnct';
        v_rlbkFnctName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_rlbk_fnct';
        v_logTriggerName   := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg';
        v_truncTriggerName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg';
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
--   -> check the log table exists
        PERFORM relname FROM pg_class, pg_namespace WHERE 
          relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname = v_logTableName;
        IF NOT FOUND THEN
          RAISE EXCEPTION '_verify_group: Log table % not found',v_logTableName;
        END IF;
--   -> check boths functions exists
        PERFORM proname FROM pg_proc , pg_namespace WHERE 
          pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_logFnctName;
        IF NOT FOUND THEN
          RAISE EXCEPTION '_verify_group: Log function % not found',v_logFnctName;
        END IF;
        IF NOT v_logOnly THEN
           PERFORM proname FROM pg_proc , pg_namespace WHERE 
             pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_rlbkFnctName;
           IF NOT FOUND THEN
             RAISE EXCEPTION '_verify_group: Rollback function % not found',v_rlbkFnctName;
           END IF;
        END IF;
--   -> check both triggers exist
        PERFORM tgname FROM pg_trigger WHERE tgname = v_logTriggerName;
        IF NOT FOUND THEN
          RAISE EXCEPTION '_verify_group: Log trigger % not found',v_logTriggerName;
        END IF;
        IF v_pgversion >= '8.4' THEN
          PERFORM tgname FROM pg_trigger WHERE tgname = v_truncTriggerName;
          IF NOT FOUND THEN
            RAISE EXCEPTION '_verify_group: Truncate trigger % not found',v_truncTriggerName;
          END IF;
        END IF;
--   -> check that the log tables structure is consistent with the application tables structure
--      (same columns and same formats)
        PERFORM attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace 
          WHERE nspname = r_tblsq.rel_schema AND relnamespace = pg_namespace.oid AND relname = r_tblsq.rel_tblseq
            AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
        EXCEPT
        SELECT attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace
          WHERE nspname = v_emajSchema AND relnamespace = pg_namespace.oid AND relname = v_logTableName
            AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%';
        IF FOUND THEN
          RAISE EXCEPTION '_verify_group: The structure of log table % is not coherent with %' ,v_logTableName,v_fullTableName;
        END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
      END IF;
    END LOOP;
    RETURN;
  END;
$_verify_group$;

CREATE or REPLACE FUNCTION emaj._check_fk_group (v_groupName TEXT) 
RETURNS void LANGUAGE plpgsql AS 
$_check_fk_group$
-- this function checks foreign key constraints for tables of a group:
-- Input: group name
  DECLARE
    r_fk             RECORD;
  BEGIN
-- issue a warning if a table of the group has a foreign key that reference a table outside the group
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname 
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace nf,pg_class tf, emaj.emaj_relation r
        WHERE contype = 'f'                                      -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid   -- join for table and namespace 
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid  -- join for referenced table and namespace
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq AND r.rel_group = v_groupName  -- join with emaj_relation table
          AND (nf.nspname,tf.relname) NOT IN
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = v_groupName)    -- referenced table outside the group
      LOOP
      RAISE WARNING '_check_fk_group: Foreign key %, for %.% references %.% that is ouside the group %',
                r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,v_groupName;
    END LOOP;
-- issue a warning if a table of the group is referenced by a table outside the group
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq 
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace nf,pg_class tf, emaj.emaj_relation r
        WHERE contype = 'f'                                      -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid   -- join for table and namespace 
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid  -- join for referenced table and namespace
          AND nf.nspname = r.rel_schema AND tf.relname = r.rel_tblseq AND r.rel_group = v_groupName  -- join with emaj_relation table
          AND (n.nspname,t.relname) NOT IN
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = v_groupName)    -- referenced table outside the group
      LOOP
      RAISE WARNING '_check_fk_group: table %.% is referenced by foreign key % of %.% that is ouside the group %',
                r_fk.rel_schema,r_fk.rel_tblseq,r_fk.conname,r_fk.nspname,r_fk.relname,v_groupName;
    END LOOP;
    RETURN;
  END;
$_check_fk_group$;

CREATE or REPLACE FUNCTION emaj._lock_group(v_groupName TEXT, v_lockMode TEXT)
RETURNS void LANGUAGE plpgsql AS 
$_lock_group$
-- This function locks all tables of a group. 
-- The lock mode is provided by the calling function
-- Input: group name, lock mode
  DECLARE
    v_nbRetry       SMALLINT := 0;
    v_ok            BOOLEAN := false;
    v_fullTableName TEXT;
    v_mode          TEXT;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('LOCK_GROUP' ,'BEGIN', v_groupName);
-- set the value for the lock mode that will be used in the LOCK statement
    IF v_lockMode = '' THEN
      v_mode = 'ACCESS EXCLUSIVE';
    ELSE
      v_mode = v_lockMode;
    END IF;
-- acquire lock on all tables
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- scan all tables of the group
        FOR r_tblsq IN
            SELECT rel_schema, rel_tblseq FROM emaj.emaj_relation 
               WHERE rel_group = v_groupName AND rel_kind = 'r'
            LOOP
-- lock the table
          v_fullTableName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE 'LOCK TABLE ' || v_fullTableName || ' IN ' || v_mode || ' MODE';
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_lock_group: a deadlock has been trapped while locking tables of group %.', v_groupName;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_lock_group: too many (5) deadlocks encountered while locking tables of group %.',v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('LOCK_GROUP', 'END', v_groupName, v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_lock_group$;

CREATE or REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTbl         INT := 0;
    v_nbSeq         INT := 0;
    v_logOnly       BOOLEAN := false;          -- emaj parameter telling if rollback functions have to be skipped (for Emaj test)
    v_relkind       TEXT;
    v_stmt          TEXT;
    v_nb_trg        INT;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName);
-- check that the group is not yet recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: group % is already created', v_groupName;
    END IF;
-- OK, insert group row in the emaj_group table
    INSERT INTO emaj.emaj_group (group_name, group_state) VALUES (v_groupName, 'IDLE');
-- get the log_only parameter
    SELECT param_value_boolean INTO v_logOnly FROM emaj.emaj_param WHERE param_key = 'log_only';
-- scan all classes of the group
    FOR r_tblsq IN
        SELECT grpdef_schema, grpdef_tblseq FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName
        LOOP
-- check the class is valid
      v_relkind = emaj._check_class(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq);
      IF v_relkind = 'r' THEN
-- if it is a table, create the related emaj objects
         PERFORM emaj._create_log (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_logOnly);
         v_nbTbl = v_nbTbl + 1;
        ELSEIF v_relkind = 'S' THEN
-- if it is a sequence, just count
         v_nbSeq = v_nbSeq + 1;
      END IF;
-- record this table or sequence in the emaj_relation table
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_kind)
          VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, v_relkind);
    END LOOP;
    IF v_nbTbl + v_nbSeq = 0 THEN
       RAISE EXCEPTION 'emaj_create_group: Group % is unknown in emaj_relation table', v_groupName;
    END IF;
-- update tables and sequences counters in the emaj_group table
    UPDATE emaj.emaj_group SET group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
      WHERE group_name = v_groupName;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_group (v_groupName);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('CREATE_GROUP', 'END', v_groupName, v_nbTbl + v_nbSeq || ' tables/sequences processed');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_create_group$;

CREATE or REPLACE FUNCTION emaj.emaj_drop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_drop_group$
-- This function deletes the emaj objects for all tables of a group
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb          INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, TRUE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_drop_group$;

CREATE or REPLACE FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_force_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- It differs from emaj_drop_group by the fact that no check is done on group's state.
-- This allows to drop a group that is not consistent, following hasardeous operations.
-- This functions should not be used, except if the emaj_drop_group fails. 
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb          INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('FORCE_DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, FALSE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('FORCE_DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_force_drop_group$;

CREATE or REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS 
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- Input: group name, and a boolean indicating whether the group's state has to be checked 
-- Output: number of processed tables and sequences
  DECLARE
    v_groupState    TEXT;
    v_nbTb          INT := 0;
    r_tblsq         RECORD; 
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_group: group % has not been created', v_groupName;
    END IF;
-- if the state of the group has to be checked,
    IF v_checkState THEN
--   check that the group is IDLE (i.e. not in a LOGGING) state
      IF v_groupState <> 'IDLE' THEN
        RAISE EXCEPTION '_drop_group: The group % cannot be deleted because it is not in idle state', v_groupName;
      END IF;
    END IF;
-- OK, delete the emaj objets for each table of the group
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
        PERFORM emaj._delete_log (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
          PERFORM emaj._delete_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- delete group row from the emaj_group table. 
--   By cascade, it also deletes rows from emaj_relation, emaj_mark, emaj_sequence and emaj_fk tables
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName;
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE or REPLACE FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_start_group$
-- This function activates the log triggers of all the tables for a group and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: group name, name of the mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgversion        TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_groupState       TEXT;
    v_markName         TEXT;
    v_nbTb             INT := 0;
    v_logOnly          BOOLEAN;
    v_logTableName     TEXT;
    v_fullTableName    TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_cpt              BIGINT;
    r_tblsq            RECORD;
  BEGIN
-- purge the emaj history by deleting all rows prior the 'history_retention' parameter 
--   and not deleting rows generated by groups that are currently in logging state
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < 
      (SELECT MIN(datetime) FROM (
                 -- compute the least recent start_group time of groups in logging state
         (SELECT MIN(hist_datetime) FROM emaj.emaj_group, emaj.emaj_hist 
           WHERE group_state = 'LOGGING' AND hist_function = 'START_GROUP BEGIN' AND group_name = hist_object)
         UNION 
                 -- compute the timestamp of now minus the history_retention
         (SELECT current_timestamp - (SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'))
        ) AS tmst(datetime));
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('START_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_start_group: group % has not been created', v_groupName;
    END IF;
-- check that the group is IDLE (i.e. not in a LOGGING) state
    IF v_groupState <> 'IDLE' THEN
      RAISE EXCEPTION 'emaj_start_group: The group % cannot be started because it is not in idle state. An emaj_stop_group function must be previously executed', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM emaj._verify_group(v_groupName);
-- check the mark name is not 'EMAJ_LAST_MARK'
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      RAISE EXCEPTION 'emaj_start_group: % is not an allowed name for a mark', v_mark;
    END IF;
-- process % wild characters in mark name
    v_markName = replace(v_mark, '%', to_char(current_timestamp, 'HH24.MI.SS.MS'));
-- process null or empty supplied mark name
    IF v_markName = '' OR v_markName IS NULL THEN
      v_markName = 'MARK_' || to_char(current_timestamp, 'HH24.MI.SS.MS');
    END IF;
-- call the emaj_reset_group function to erase remaining traces from previous logs if any
    SELECT emaj._rst_group(v_groupName) INTO v_nbTb;
    IF v_nbTb = 0 THEN
      RAISE EXCEPTION 'emaj_start_group: Internal error - emaj_res_group for group % returned 0', v_groupName;
    END IF;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_group (v_groupName);
    v_nbTb = 0;
-- get the log_only parameter and issue a warning if emaj is in log_only mode
    SELECT param_value_boolean INTO v_logOnly FROM emaj.emaj_param WHERE param_key = 'log_only';
    if v_logOnly THEN
      RAISE WARNING 'emaj_start_group: E-maj is in log_only mode. Rollback operations are disabled.';
    END IF;
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
    PERFORM emaj._lock_group(v_groupName,'');
-- ... and enable all log triggers for the group
-- for each relation of the group,
    FOR r_tblsq IN
       SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
       LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_logTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
        v_truncTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
        EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
        IF v_pgversion >= '8.4' THEN
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_truncTriggerName;
        END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- update the state of the group row from the emaj_group table
    UPDATE emaj.emaj_group SET group_state = 'LOGGING' WHERE group_name = v_groupName;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('START_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
-- Set the first mark
    PERFORM emaj.emaj_set_mark_group(v_groupName, v_markName);
    RETURN v_nbTb;
  END;
$emaj_start_group$;

CREATE or REPLACE FUNCTION emaj.emaj_stop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_stop_group$
-- This function de-activates the log triggers of all the tables for a group. 
-- Execute several emaj_stop_group functions for the same group doesn't produce any error.
-- Input: group name
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgversion        TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_groupState       TEXT;
    v_nbTb             INT := 0;
    v_fullTableName    TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    r_tblsq            RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('STOP_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_stop_group: group % has not been created', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE WARNING 'emaj_stop_group: Group % cannot be stopped because it is not in logging state.', v_groupName;
    ELSE
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
      PERFORM emaj._lock_group(v_groupName,'');
-- for each relation of the group,
      FOR r_tblsq IN
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
          LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, disable the emaj log and truncate triggers
          v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          v_logTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
          v_truncTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
          IF v_pgversion >= '8.4' THEN
            EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
          END IF;
          ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
        END IF;
        v_nbTb = v_nbTb + 1;
      END LOOP;
-- set all marks for the group from the emaj_mark table in deleted state to avoid any further rollback
      UPDATE emaj.emaj_mark SET mark_state = 'DELETED' WHERE mark_group = v_groupName; 
-- update the state of the group row from the emaj_group table
      UPDATE emaj.emaj_group SET group_state = 'IDLE' WHERE group_name = v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('STOP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_stop_group$;

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
-- check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_set_mark_group: group % has not been created', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE EXCEPTION 'emaj_set_mark_group: A mark cannot be set for group % because it is not in logging state. An emaj_start_group function must be previously executed', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM emaj._verify_group(v_groupName);
-- check the mark name is not 'EMAJ_LAST_MARK'
    IF v_mark = 'EMAJ_LAST_MARK' THEN
       RAISE EXCEPTION 'emaj_set_mark_group: % is not an allowed name for a new mark', v_mark;
    END IF;
-- process % wild characters in mark name
    v_markName = replace(v_mark, '%', to_char(current_timestamp, 'HH24.MI.SS.MS'));
-- process null or empty supplied mark name
    IF v_markName = '' OR v_markName IS NULL THEN
      v_markName = 'MARK_' || to_char(current_timestamp, 'HH24.MI.SS.MS');
    END IF;
-- if a mark with the same name already exists for the group, stop
    PERFORM 1 FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_markName;
    IF FOUND THEN
       RAISE EXCEPTION 'emaj_set_mark_group: Group % already contains a name %.', v_groupName, v_markName;
    END IF;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_group(v_groupName,'ROW EXCLUSIVE');
-- Effectively set the mark using the internal _set_mark_group() function
    SELECT emaj._set_mark_group(v_groupName, v_markName) into v_nbTb;
    RETURN v_nbTb;
  END;
$emaj_set_mark_group$;

CREATE or REPLACE FUNCTION emaj._set_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$_set_mark_group$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group. 
-- It is called by emaj_set_mark_group function but may be called by other functions to set an internal mark.
-- Input: group name, mark to set
-- Output: number of processed tables and sequences
  DECLARE
    v_pgversion     TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)\\.');
    v_emajSchema    TEXT := 'emaj';
    v_nbTb          INT := 0;
    v_fullSeqName   TEXT;
    v_seqName       TEXT;
    v_timestamp     TIMESTAMPTZ;
    v_stmt          TEXT;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- look at the clock and insert the mark into the emaj_mark table
    v_timestamp = clock_timestamp();
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_datetime, mark_state) 
      VALUES (v_groupName, v_mark, v_timestamp, 'ACTIVE');
-- then, examine the group's definition
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, record the emaj_id associated sequence parameters in the emaj sequence table
        v_seqName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_fullSeqName := quote_ident(v_emajSchema) || '.' || quote_ident(v_seqName);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' || 
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT '''|| v_emajSchema || ''', ''' || v_seqName || ''', ''' || v_timestamp || ''', ''' || v_mark || 
                 ''', ' || 'last_value, ';
        IF v_pgversion <= '8.3' THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt || 
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, record the sequence parameters in the emaj sequence table
        v_fullSeqName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' || 
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT ''' || r_tblsq.rel_schema || ''', ''' || 
                 r_tblsq.rel_tblseq || ''', ''' || v_timestamp || ''', ''' || v_mark || ''', ' ||
                 'last_value, ';
        IF v_pgversion <= '8.3' THEN
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
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'END', v_groupName, v_mark);
    RETURN v_nbTb;
  END;
$_set_mark_group$;

CREATE or REPLACE FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS void LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group function. 
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence 
-- If this mark is the first mark, it also physically deletes rows from all concerned log tables.
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_groupState     TEXT;
    v_realMark       TEXT;
    v_datetimeNewMin TIMESTAMPTZ;
    v_datetimeMark   TIMESTAMPTZ;
    v_seqName        TEXT;
    v_logTableName   TEXT;
    v_emaj_id        BIGINT;
    v_cpt            INT;
    r_mark           RECORD;
    r_tblsq          RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: group % has not been created', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: A mark cannot be deleted for group % because it is not in logging state. A emaj_reset_group function can be performed if you wish to reclaim disk space', v_groupName;
    END IF;
-- if mark = 'EMAJ_LAST_MARK', retrieve the last mark name for the group
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      SELECT mark_name INTO v_realMark FROM emaj.emaj_mark WHERE mark_datetime =
        (SELECT MAX(mark_datetime) FROM emaj.emaj_mark WHERE mark_group = v_groupName);
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_delete_mark_group: No mark can be used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
      END IF;
    ELSE
-- otherwise, check if the supplied mark exists 
      PERFORM 1 FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_delete_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
      END IF;
      v_realMark=v_mark;
    END IF;
-- count the number of mark in the group
    SELECT count(*) INTO v_cpt FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- and check there are at least 2 marks for the group
    IF v_cpt < 2 THEN
       RAISE EXCEPTION 'emaj_delete_mark_group: % is the only mark. It cannot be deleted.', v_mark;
    END IF;
-- OK, now get the timestamp of the mark to delete
    SELECT mark_datetime INTO v_datetimeMark FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- OK, now get the timestamp of the future first mark
    SELECT min (mark_datetime) INTO v_datetimeNewMin FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name <> v_realMark;
-- if the mark to delete is the first mark, delete data from log tables and emaj_sequence that will becomes useless
    IF v_datetimeMark < v_datetimeNewMin THEN
-- loop on all tables of the group
      FOR r_tblsq IN
          SELECT rel_schema, rel_tblseq FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'r'
      LOOP
        v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- get the emaj_id corresponding to the new first mark
        SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_emaj_id
          FROM emaj.emaj_sequence
          WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_datetimeNewMin;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'emaj_delete_mark_group: internal error - sequence for % and % not found in emaj_sequence',v_seqName, v_datetimeNewMin;
        END IF;
-- delete log rows prior to the new first mark
        EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_id < ' || v_emaj_id;
      END LOOP;
-- delete also all sequence holes that are prior the new first mark for the tables of the group
      DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
          AND sqhl_timestamp < v_datetimeNewMin;
    END IF;
-- now the sequences related to the mark to delete can be suppressed
    DELETE FROM emaj.emaj_sequence WHERE sequ_mark = v_realMark AND sequ_datetime = v_datetimeMark;
-- and the mark to delete can be physicaly deleted
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_mark || ' (' || v_cpt || ' marks physically deleted)');
    RETURN;
  END;
$emaj_delete_mark_group$;

CREATE or REPLACE FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT)
RETURNS void LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be either in LOGGING or IDLE state.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly t is not worth having a group in LOGGING state !).
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  DECLARE
--    v_emajSchema    TEXT := 'emaj';
    v_realMark       TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: group % has not been created', v_groupName;
    END IF;
-- if mark = 'EMAJ_LAST_MARK', retrieve the last mark name for the group
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      SELECT mark_name INTO v_realMark FROM emaj.emaj_mark WHERE mark_datetime =
        (SELECT MAX(mark_datetime) FROM emaj.emaj_mark WHERE mark_group = v_groupName);
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_rename_mark_group: No mark can be used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
      END IF;
    ELSE
-- otherwise, check if the supplied mark exists 
      PERFORM 1 FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_name = v_mark LIMIT 1;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_rename_mark_group: mark % doesn''t exist for group %.', v_mark, v_groupName;
      END IF;
      v_realMark=v_mark;
    END IF;
-- check the new mark name is not 'EMAJ_LAST_MARK'
    IF v_newName = 'EMAJ_LAST_MARK' THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: % is not an allowed name for a new mark', v_newName;
    END IF;
-- check if the new mark name doesn't exist for the group 
    PERFORM 1 FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_newName LIMIT 1;
    IF FOUND THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: a mark % already exists for group %.', v_newName, v_groupName;
    END IF;
-- OK, update the sequences table as well
    UPDATE emaj.emaj_sequence SET sequ_mark = v_newName 
      WHERE sequ_datetime = (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark);
-- and then update the emaj_mark table
    UPDATE emaj.emaj_mark SET mark_name = v_newName
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('RENAME_MARK_GROUP', 'END', v_groupName, v_realMark || ' renamed ' || v_newName);
    RETURN;
  END;
$emaj_rename_mark_group$;

CREATE or REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the group, with log table deletion
    return emaj._rlbk_group(v_groupName, v_mark, true, true);
  END;
$emaj_rollback_group$;

CREATE or REPLACE FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_and_stop_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- and then stop the group without deleting log tables.
-- Input: group name, mark to rollback to
-- Output: number of tables and sequences processed by the rollback function
  DECLARE
    v_ret_rollback   INT;
    v_ret_stop       INT;
  BEGIN
-- (unlogged) rollback the group without log table deletion
    SELECT emaj._rlbk_group(v_groupName, v_mark, true, false) INTO v_ret_rollback;
-- and stop
    SELECT emaj.emaj_stop_group(v_groupName) INTO v_ret_stop;
-- return the number of rollbacked tables and sequences
    RETURN v_ret_rollback;
  END;
$emaj_rollback_and_stop_group$;

CREATE or REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rollbacked! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rollbacked log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the group, with log table deletion
    return emaj._rlbk_group(v_groupName, v_mark, false, false);
  END;
$emaj_logged_rollback_group$;

CREATE or REPLACE FUNCTION emaj._rlbk_group(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history.
-- It is called by emaj_rollback_group and emaj_rollback_and_stop_group.
-- It effectively manages the rollback operation for each table or sequence, deleting rows from log tables 
-- only when asked by the calling functions.
-- Its activity is split into 6 smaller functions that are also called by the parallel restore php function
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group
--        and a boolean saying if the log rows must be deleted from log table or not
-- Output: number of tables and sequences effectively processed
  DECLARE
    v_nbTbl             INT;
    v_nbTblInGroup      INT;
    v_nbSeq             INT;
  BEGIN
-- Step 1: prepare the rollback operation
    SELECT emaj._rlbk_group_step1(v_groupName, v_mark, v_unloggedRlbk, 1) INTO v_nbTblInGroup;
-- Step 2: lock all tables
    PERFORM emaj._rlbk_group_step2(v_groupName, 1);
-- Step 3: set a rollback start mark if logged rollback
    PERFORM emaj._rlbk_group_step3(v_groupName, v_mark, v_unloggedRlbk);
-- Step 4: record and drop foreign keys
    PERFORM emaj._rlbk_group_step4(v_groupName, 1);
-- Step 5: effectively rollback tables
    SELECT emaj._rlbk_group_step5(v_groupName, v_mark, 1, v_unloggedRlbk, v_deleteLog) INTO v_nbTbl;
-- checks that we have the expected number of processed tables
    IF v_nbTbl <> v_nbTblInGroup THEN
       RAISE EXCEPTION '_rlbk_group: Internal error 1 (%,%)',v_nbTbl,v_nbTblInGroup;
    END IF;
-- Step 6: recreate foreign keys
    PERFORM emaj._rlbk_group_step6(v_groupName, 1);
-- Step 7: process sequences and complete the rollback operation record
    SELECT emaj._rlbk_group_step7(v_groupName, v_mark, v_nbTbl, v_unloggedRlbk, v_deleteLog) INTO v_nbSeq;
    RETURN v_nbTbl + v_nbSeq;
  END;
$_rlbk_group$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step1(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSubGroup INT) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_group_step1$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- It builds the requested number of sub-groups with the list of tables to process, trying to spread the load over all sub-groups.
-- It finaly inserts into the history the event about the rollback start
  DECLARE
    v_logOnly             BOOLEAN;
    v_groupState          TEXT;
    v_nbTblInGroup        INT;
    v_nbUnchangedTbl      INT;
    v_timestampMark       TIMESTAMPTZ;
    v_subGroup            INT;
    v_subGroupLoad        INT [];
    v_minSubGroup         INT;
    v_minRows             INT;
    v_fullTableName       TEXT;
    v_msg                 TEXT;
    r_tbl                 RECORD;
    r_tbl2                RECORD;
  BEGIN
-- check emaj is not configured in log_only mode
    SELECT param_value_boolean INTO v_logOnly FROM emaj.emaj_param WHERE param_key = 'log_only';
    IF v_logOnly THEN
      RAISE EXCEPTION '_rlbk_group_step1: Emaj is configured in LogOnly mode. It cannot perform any rollback operation';
    END IF;
-- check that the group is recorded in emaj_group table and get the total number of tables for this group
    SELECT group_state, group_nb_table INTO v_groupState, v_nbTblInGroup FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_rlbk_group_step1: group % has not been created', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE EXCEPTION '_rlbk_group_step1: Group % cannot be rollbacked because it is not in logging state. An emaj_start_group function must be previously executed', v_groupName;
    END IF;
-- if mark = 'EMAJ_LAST_MARK', retrieve the timestamp of the last mark for the group
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      SELECT MAX(mark_datetime) INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName;
      IF NOT FOUND THEN
         RAISE EXCEPTION '_rlbk_group_step1: No mark can be used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
      END IF;
    ELSE
-- otherwise, check the requested mark exists and get its timestamp
      SELECT mark_datetime INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_name = v_mark;
      IF NOT FOUND THEN
         RAISE EXCEPTION '_rlbk_group_step1: No mark % exists for group % ', v_mark, v_groupName;
      END IF;
    END IF;
-- insert begin in the history
    IF v_unloggedRlbk THEN
      v_msg = 'Unlogged';
    ELSE
      v_msg = 'Logged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_GROUP', 'BEGIN', v_groupName, v_msg || ' rollback to mark ' || v_mark || ' [' || v_timestampMark || ']');
-- check that emaj group is OK 
    PERFORM emaj._verify_group(v_groupName);
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_group (v_groupName);
-- create sub_groups, using the number of sub-groups requested by the caller
-- subgroup for sequences will remain NULL
--   initialisation
--     accumulated counters of number of log rows to rollback for each parallel connection 
    FOR v_subGroup IN 1 .. v_nbSubGroup LOOP
      v_subGroupLoad [v_subGroup] = 0;
    END LOOP;
--     fkey table
    DELETE FROM emaj.emaj_fk WHERE fk_group = v_groupName;
--     relation table: subgroup set to NULL and numbers of log rows computed by emaj_log_stat_group function
    UPDATE emaj.emaj_relation SET rel_subgroup = NULL, rel_rows = stat_rows 
      FROM emaj.emaj_log_stat_group (v_groupName, v_mark, NULL) stat
      WHERE rel_group = v_groupName
        AND rel_group = stat_group AND rel_schema = stat_schema AND rel_tblseq = stat_table;
--   count the number of tables that have no update to rollback
    SELECT count(*) INTO v_nbUnchangedTbl FROM emaj.emaj_relation WHERE rel_rows = 0;
--   allocate tables with rows to rollback to sub-groups starting with the heaviest to rollback tables as reported by emaj_log_stat_group function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_rows DESC
        LOOP
--   is the table already allocated to a subgroup (it may have been already allocated because of a fkey link) ?
      PERFORM 1 FROM emaj.emaj_relation 
        WHERE rel_group = v_groupName AND rel_schema = r_tbl.rel_schema AND rel_tblseq = r_tbl.rel_tblseq 
          AND rel_subgroup IS NULL;
--   no, 
      IF FOUND THEN
--   compute the least loaded sub-group
        v_minSubGroup=1; v_minRows = v_subGroupLoad [1];
        FOR v_subGroup IN 2 .. v_nbSubGroup LOOP
          IF v_subGroupLoad [v_subGroup] < v_minRows THEN
            v_minSubGroup = v_subGroup;
            v_minRows = v_subGroupLoad [v_subGroup];
          END IF;
        END LOOP;
--   allocate the table to the sub-group, with all other tables linked by foreign key constraints
        v_subGroupLoad [v_minSubGroup] = v_subGroupLoad [v_minSubGroup] + 
                 emaj._rlbk_group_set_subgroup(v_groupName, r_tbl.rel_schema, r_tbl.rel_tblseq, v_minSubGroup, r_tbl.rel_rows);
      END IF;
    END LOOP;
    RETURN v_nbTblInGroup - v_nbUnchangedTbl;
  END;
$_rlbk_group_step1$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_set_subgroup(v_groupName TEXT, v_schema TEXT, v_table TEXT, v_subGroup INT, v_rows BIGINT) 
RETURNS BIGINT LANGUAGE plpgsql AS
$_rlbk_group_set_subgroup$
-- This functions updates the emaj_relation table and set the predefined sub-group number for one table. 
-- It also looks for all tables that are linked to this table by foreign keys to force them to be allocated to the same sub-group.
-- As those linked table can also be linked to other tables by other foreign keys, the function has to be recursiley called.
-- The function returns the accumulated number of rows contained into all log tables of these linked by foreign keys tables.
  DECLARE
    v_cumRows       BIGINT;    -- accumulate the number of rows of the related log tables ; will be returned by the function
    v_fullTableName TEXT;
    r_tbl           RECORD;
  BEGIN
    v_cumRows=v_rows;
-- first set the sub-group of the emaj_relation table for this application table
    UPDATE emaj.emaj_relation SET rel_subgroup = v_SubGroup 
      WHERE rel_group = v_groupName AND rel_schema = v_schema AND rel_tblseq = v_table;
-- then look for other application tables linked by foreign key relationships
    v_fullTableName := quote_ident(v_schema) || '.' || quote_ident(v_table);
    FOR r_tbl IN
        SELECT rel_schema, rel_tblseq, rel_rows FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName
            AND rel_subgroup IS NULL                          -- not yet allocated
            AND (rel_schema, rel_tblseq) IN (                  -- list of (schema,table) linked to the original table by foreign keys
            SELECT nspname, relname FROM pg_constraint, pg_class t, pg_namespace n 
              WHERE contype = 'f' AND confrelid = v_fullTableName::regclass 
                AND t.oid = conrelid AND relnamespace = n.oid
            UNION
            SELECT nspname, relname FROM pg_constraint, pg_class t, pg_namespace n
              WHERE contype = 'f' AND conrelid = v_fullTableName::regclass 
                AND t.oid = confrelid AND relnamespace = n.oid
            ) 
        LOOP
-- recursive call to allocate these linked tables to the same sub-group and get the accumulated number of rows to rollback
      SELECT v_cumRows + emaj._rlbk_group_set_subgroup(v_groupName, r_tbl.rel_schema, r_tbl.rel_tblseq, v_subGroup, r_tbl.rel_rows) INTO v_cumRows;
    END LOOP;
    RETURN v_cumRows;
  END;
$_rlbk_group_set_subgroup$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step2(v_groupName TEXT, v_subGroup INT) 
RETURNS void LANGUAGE plpgsql AS
$_rlbk_group_step2$
-- This is the second step of a rollback group processing. It just locks the table for a sub-group.
  DECLARE
    v_nbRetry       SMALLINT := 0;
    v_ok            BOOLEAN := false;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('LOCK_SUBGROUP', 'BEGIN', v_groupName, 'Sub-group ' || v_subGroup);
-- acquire lock on all tables
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- scan all tables of the sub-group
        FOR r_tblsq IN
            SELECT rel_schema, rel_tblseq FROM emaj.emaj_relation 
              WHERE rel_group = v_groupName AND rel_subgroup = v_subGroup AND rel_kind = 'r'
            LOOP
--   lock each table
          EXECUTE 'LOCK TABLE ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_group_step2: a deadlock has been trapped while locking tables of group %.', v_groupName;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_rlbk_group_step2: too many (5) deadlocks encountered while locking tables of group %.',v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('LOCK_SUBGROUP', 'END', v_groupName, 'Sub-group ' || v_subGroup || ' ; ' ||v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_rlbk_group_step2$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step3(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN) 
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_group_step3$
-- This is the third step of a rollback group processing.
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback. 
-- All concerned tables are already locked.
  DECLARE
    v_markName       TEXT := v_mark;
  BEGIN
    IF NOT v_unloggedRlbk THEN
-- if rollback is "logged" rollback, build a mark name with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the current time
      IF v_mark = 'EMAJ_LAST_MARK' THEN
-- if mark = 'EMAJ_LAST_MARK', retrieve the name of the last mark for the group
        SELECT mark_name INTO v_markName 
          FROM emaj.emaj_mark
          WHERE mark_group = v_groupName ORDER BY mark_datetime DESC LIMIT 1;
        IF NOT FOUND THEN
           RAISE EXCEPTION '_rlbk_group_step3: Internal error - ''EMAJ_LAST_MARK'' not found for group % ', v_groupName;
        END IF;
      END IF;
-- set the mark
      PERFORM emaj._set_mark_group(v_groupName, 'RLBK_' || v_markName || '_' || to_char(current_timestamp, 'HH24.MI.SS.MS') || '_START');
    END IF;
    RETURN;
  END;
$_rlbk_group_step3$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step4(v_groupName TEXT, v_subGroup INT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_group_step4$
-- This is the fourth step of a rollback group processing. It drops all foreign keys involved in a table sub-group.
-- Before dropping, it records them to be able to recreate them at step 5.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    r_fk                RECORD;
  BEGIN
-- record and drop the foreign keys involved in all tables of the group, if any
    DELETE FROM emaj.emaj_fk WHERE fk_group = v_groupName and fk_subgroup = v_subGroup;
    INSERT INTO emaj.emaj_fk (fk_group, fk_subgroup, fk_name, fk_schema, fk_table, fk_def)
--    record the foreign keys of the sub-group's tables
      SELECT v_groupName, v_subGroup, c.conname, n.nspname, t.relname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c, pg_namespace n, pg_class t, emaj.emaj_relation r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq      -- join on group table
          AND r.rel_group = v_groupName AND r.rel_subgroup = v_subGroup
      UNION
--           and the foreign keys referenced the sub-group's tables
      SELECT v_groupName, v_subGroup, c.conname, n.nspname, t.relname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace rn, pg_class rt, emaj.emaj_relation r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace 
          AND rn.nspname = r.rel_schema AND rt.relname = r.rel_tblseq    -- join on group table
          AND r.rel_group = v_groupName AND r.rel_subgroup = v_subGroup;
--    and drop all these foreign keys
    FOR r_fk IN
      SELECT fk_schema, fk_table, fk_name FROM emaj.emaj_fk WHERE fk_group = v_groupName  AND fk_subgroup = v_subGroup
      LOOP
        RAISE NOTICE '_rlbk_group_step4: group %, sub-group % -> foreign key constraint % dropped for table %.%', v_groupName, v_subGroup, r_fk.fk_name, r_fk.fk_schema, r_fk.fk_table;
        EXECUTE 'ALTER TABLE ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_table) || ' DROP CONSTRAINT ' || quote_ident(r_fk.fk_name);
    END LOOP;
  END;
$_rlbk_group_step4$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step5(v_groupName TEXT, v_mark TEXT, v_subGroup INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_group_step5$
-- This is the fifth step of a rollback group processing. It performs the rollback of all tables of a sub-group of a group.
  DECLARE
    v_nbTbl             INT := 0;
    v_timestampMark     TIMESTAMPTZ;
  BEGIN
-- fetch the timestamp mark again (its existence has been already checked at step 1)
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      SELECT MAX(mark_datetime) INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName;
    ELSE
      SELECT mark_datetime INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_name = v_mark;
    END IF;
-- rollback all tables of the sub-group, having rows to rollback (sequences are processed later)
-- (the disableTrigger boolean for the _rlbk_table() function always equal unloggedRlbk boolean)
    PERFORM emaj._rlbk_table(rel_schema, rel_tblseq, v_timestampMark, v_unloggedRlbk, v_deleteLog)
      FROM emaj.emaj_relation 
      WHERE rel_group = v_groupName AND rel_subgroup = v_subGroup AND rel_kind = 'r' AND rel_rows > 0;
-- and return the number of processed tables
    GET DIAGNOSTICS v_nbTbl = ROW_COUNT;
    RETURN v_nbTbl;
  END;
$_rlbk_group_step5$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step6(v_groupName TEXT, v_subGroup INT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_group_step6$
-- This is the sixth step of a rollback group processing. It recreates the previously deleted foreign keys.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_ts_start          TIMESTAMP;
    v_ts_end            TIMESTAMP;
    r_fk                RECORD;
  BEGIN
    FOR r_fk IN
-- get all recorded fk plus the number of rows of the related table as estimated by postgresql (pg_class.reltuples)
      SELECT fk_schema, fk_table, fk_name, fk_def, pg_class.reltuples 
        FROM emaj.emaj_fk, pg_namespace, pg_class
        WHERE fk_group = v_groupName AND fk_subgroup = v_subGroup AND                         -- restrictions
              pg_namespace.oid = relnamespace AND relname = fk_table AND nspname = fk_schema  -- joins
      LOOP
-- record the time at the alter table start
        SELECT clock_timestamp() INTO v_ts_start;
-- recreate the foreign key
        EXECUTE 'ALTER TABLE ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_table) || ' ADD CONSTRAINT ' || quote_ident(r_fk.fk_name) || ' ' || r_fk.fk_def;
-- record the time after the alter table and insert FK creation duration into the emaj_rlbk_stat table
        SELECT clock_timestamp() INTO v_ts_end;
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
           VALUES ('add_fk', r_fk.fk_schema, r_fk.fk_name, v_ts_start, r_fk.reltuples, v_ts_end - v_ts_start);
-- send a message about the fk creation completion 
        RAISE NOTICE '_rlbk_group_step6: group %, sub-group % -> foreign key constraint % recreated for table %.%', v_groupName, v_subGroup, r_fk.fk_name, r_fk.fk_schema, r_fk.fk_table;
    END LOOP;
    RETURN;
  END;
$_rlbk_group_step6$;

CREATE or REPLACE FUNCTION emaj._rlbk_group_step7(v_groupName TEXT, v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_group_step7$
-- This is the last step of a rollback group processing. It :
--    - deletes the marks that are no longer available,
--    - rollbacks all sequences of the group.
-- It returns the number of processed sequences.
  DECLARE
    v_timestampMark     TIMESTAMPTZ;
    v_nbSeq             INT;
    v_markName          TEXT;
  BEGIN
-- fetch the timestamp mark again (its existence has been already checked at step 1)
    IF v_mark = 'EMAJ_LAST_MARK' THEN
      SELECT MAX(mark_datetime) INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName;
--      SELECT mark_name INTO v_actualMark FROM emaj.emaj_mark
--        WHERE mark_group = v_groupName AND mark_datetime = v_timestampMark;
    ELSE
      SELECT mark_datetime INTO v_timestampMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_name = v_mark;
    END IF;
-- if "unlogged" rollback, delete all marks and sequence holes later than the now rollbacked mark
-- (not needed if mark = 'EMAJ_LAST_MARK')
    IF v_unloggedRlbk AND v_mark <> 'EMAJ_LAST_MARK' THEN
-- log in the history the name of all marks that must be deleted due to the rollback
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        SELECT 'ROLLBACK_GROUP', 'MARK DELETED', v_groupName, 'mark ' || mark_name || ' has been deleted' FROM emaj.emaj_mark 
          WHERE mark_group = v_groupName AND mark_datetime > v_timestampMark;
-- and finaly delete these useless marks (the related sequences have been already deleted by rollback functions)
      DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_datetime > v_timestampMark; 
    END IF;
-- rollback the application sequences belonging to the group
-- warning, this operation is not transaction safe (that's why it is placed at the end of the operation)!
    PERFORM emaj._rlbk_sequence(rel_schema, rel_tblseq, v_timestampMark, v_deleteLog) 
       FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'S';
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automaticaly set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the current time
    IF NOT v_unloggedRlbk THEN
-- get the mark name set at the beginning of the rollback operation
      SELECT mark_name INTO v_markName 
        FROM emaj.emaj_mark
        WHERE mark_group = v_groupName ORDER BY mark_datetime DESC LIMIT 1;
      IF NOT FOUND OR substr(v_markName,1,5) <> 'RLBK_' THEN
        RAISE EXCEPTION '_rlbk_group_step7: Internal error - rollback start mark not found for group % ', v_groupName;
      END IF;
-- set the mark, replacing the '_START' suffix of the rollback start mark by '_DONE'
      PERFORM emaj._set_mark_group(v_groupName, substring(v_markName from '(.*)_START$') || '_DONE');
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_GROUP', 'END', v_groupName, v_nbTb || ' tables and ' || v_nbSeq || ' sequences effectively processed');
    RETURN v_nbSeq;
  END;
$_rlbk_group_step7$;

CREATE or REPLACE FUNCTION emaj.emaj_reset_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves
-- It calls the emaj_rst_group function to do the job
-- Input: group name
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_groupState  TEXT;
    v_nbTb        INT := 0;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('RESET_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_reset_group: group % has not been created', v_groupName;
    END IF;
-- check that the group is in IDLE state (i.e. not in LOGGING) state
    IF v_groupState <> 'IDLE' THEN
      RAISE EXCEPTION 'emaj_reset_group: Group % cannot be reset because it is not in idle state. An emaj_stop_group function must be previously executed', v_groupName;
    END IF;
-- perform the reset operation
    SELECT emaj._rst_group(v_groupName) INTO v_nbTb;
    IF v_nbTb = 0 THEN
       RAISE EXCEPTION 'emaj_reset_group: Group % is unknown', v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('RESET_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_reset_group$;

CREATE or REPLACE FUNCTION emaj._rst_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$_rst_group$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences saves
-- It is called by both emaj_reset_group and emaj_start_group functions
-- Input: group name
-- Output: number of processed tables
-- There is no check of the group state
  DECLARE
    v_nbTb          INT  := 0;
    v_emajSchema    TEXT := 'emaj';
    v_logTableName  TEXT;
    v_fullSeqName   TEXT;
    r_tblsq         RECORD;
  BEGIN
-- delete all marks for the group from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- delete all sequence holes for the tables of the group
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table;
-- then, truncate log tables
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, 
--   truncate the related log table
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        EXECUTE 'TRUNCATE ' || v_logTableName;
--   delete rows from emaj_sequence related to the associated emaj_id sequence
        v_fullSeqName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        DELETE FROM emaj.emaj_sequence WHERE sequ_name = v_fullSeqName;
--   and reset the log sequence
        PERFORM setval(quote_ident(v_emajSchema) || '.' || quote_ident(v_fullSeqName), 1, false);
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
        PERFORM emaj._delete_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
    IF v_nbTb = 0 THEN
       RAISE EXCEPTION '_rst_group: Internal error (Group % is empty)', v_groupName;
    END IF;
    RETURN v_nbTb;
  END;
$_rst_group$;

CREATE or REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) 
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS 
$emaj_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing) 
-- It is also used to estimate the cost of a rollback to a specified mark
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: group name, the 2 marks names defining a range 
--   a NULL value as first_mark indicates the first recorded mark ; a NULL value as last_mark indicates the current situation
--   Use a NULL as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of log rows by table (including tables with 0 rows to rollback)
  DECLARE
    v_groupState      TEXT;
    v_emajSchema      TEXT := 'emaj';
    v_tsFirstMark     TIMESTAMPTZ;
    v_tsLastMark      TIMESTAMPTZ;
    v_fullSeqName     TEXT;
    v_beginLastValue  BIGINT;
    v_endLastValue    BIGINT;
    v_sumHole         BIGINT;
    r_tblsq           RECORD;
    r_stat            RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_log_stat_group: group % has not been created', v_groupName;
    END IF;
-- if first mark is NULL, retrieve the timestamp of the first mark for the group
    IF v_firstMark IS NULL THEN
      SELECT MIN(mark_datetime) INTO v_tsFirstMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_log_stat_group: No initial mark can be found for group % ', v_groupName;
      END IF;
    ELSE
-- else, if first mark = 'EMAJ_LAST_MARK', retrieve the timestamp of the last mark for the group
      IF v_firstMark = 'EMAJ_LAST_MARK' THEN
        SELECT MAX(mark_datetime) INTO v_tsFirstMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_log_stat_group: No mark can be used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
        END IF;
      ELSE
-- otherwise, check the requested first mark exists and get its timestamp
        SELECT mark_datetime INTO v_tsFirstMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName AND mark_name = v_firstMark;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_log_stat_group: no mark % exists for group %', v_firstMark, v_groupName;
        END IF;
      END IF;
    END IF;
-- if last mark is NULL, there is no timestamp to register
    IF v_lastMark IS NULL THEN
      v_tsLastMark = NULL;
    ELSE
-- else, if last mark = 'EMAJ_LAST_MARK', retrieve the timestamp of the last mark for the group
      IF v_lastMark = 'EMAJ_LAST_MARK' THEN
        SELECT MAX(mark_datetime) INTO v_tsLastMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_log_stat_group: No mark can ge used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
        END IF;
      ELSE
-- otherwise, check the requested last mark exists and get its timestamp
        SELECT mark_datetime INTO v_tsLastMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName AND mark_name = v_lastMark;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_log_stat_group: no mark % exists for group %', v_lastMark, v_groupName;
        END IF;
      END IF;
    END IF;
-- check that the first_mark < end_mark
    IF v_tsLastMark IS NOT NULL AND v_tsFirstMark > v_tsLastMark THEN
      RAISE EXCEPTION 'emaj_log_stat_group: mark time for % (%) is greater than mark time for % (%)', v_firstMark, v_tsFirstMark, v_lastMark, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table, get the number of log rows and return the statistic
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, emaj._log_stat_table(rel_schema, rel_tblseq, v_tsFirstMark, v_tsLastMark) as nb_rows FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'r'
        LOOP
      SELECT v_groupName, r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.nb_rows INTO r_stat;
      RETURN NEXT r_stat;
    END LOOP;
    RETURN;
  END;
$emaj_log_stat_group$;

CREATE or REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) 
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS 
$emaj_detailed_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks as viewed through the log tables
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range (either one or both marks can be NULL)
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of updates by user and table
  DECLARE
    v_groupState    TEXT;
    v_emajSchema    TEXT := 'emaj';
    v_logTableName  TEXT;
    v_tsFirstMark   TIMESTAMPTZ;
    v_tsLastMark    TIMESTAMPTZ;
    v_stmt          TEXT;
    r_tblsq         RECORD;
    r_stat          RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: group % has not been created', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL THEN
-- if first mark = 'EMAJ_LAST_MARK', retrieve the timestamp of the last mark for the group
      IF v_firstMark = 'EMAJ_LAST_MARK' THEN
        SELECT MAX(mark_datetime) INTO v_tsFirstMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_detailed_log_stat_group: No mark can be used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
        END IF;
      ELSE
-- otherwise, check the requested first mark exists and get its timestamp
        SELECT mark_datetime INTO v_tsFirstMark
          FROM emaj.emaj_mark
          WHERE mark_group = v_groupName AND mark_name = v_firstMark;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: Start mark % is unknown for group %', v_firstMark, v_groupName;
        END IF;
      END IF;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL THEN
-- else, if last mark = 'EMAJ_LAST_MARK', retrieve the timestamp of the last mark for the group
      IF v_lastMark = 'EMAJ_LAST_MARK' THEN
        SELECT MAX(mark_datetime) INTO v_tsLastMark FROM emaj.emaj_mark
          WHERE mark_group = v_groupName;
        IF NOT FOUND THEN
           RAISE EXCEPTION 'emaj_detailed_log_stat_group: No mark can ge used as ''EMAJ_LAST_MARK'' for group % ', v_groupName;
        END IF;
      ELSE
-- otherwise, check the requested last mark exists and get its timestamp
        SELECT mark_datetime INTO v_tsLastMark
          FROM emaj.emaj_mark
          WHERE mark_group = v_groupName AND mark_name = v_lastMark;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: End mark % is unknown for group %', v_lastMark, v_groupName;
        END IF;
      END IF;
    END IF;
-- check that the first_mark < end_mark
    IF v_firstMark IS NOT NULL AND v_LastMark IS NOT NULL AND v_tsFirstMark > v_tsLastMark THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: mark time for % (%) is greater than mark time for % (%)', v_firstMark, v_tsFirstMark, v_lastMark, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, count the number of operation per type (INSERT, UPDATE and DELETE) and role
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        v_stmt= 'SELECT ''' || v_groupName || '''::TEXT as emaj_group,'
             || ' ''' || r_tblsq.rel_schema || '''::TEXT as emaj_schema,'
             || ' ''' || r_tblsq.rel_tblseq || '''::TEXT as emaj_table,'
             || ' emaj_user,'
             || ' CASE WHEN emaj_verb = ''INS'' THEN ''INSERT'''
             ||      ' WHEN emaj_verb = ''UPD'' THEN ''UPDATE'''
             ||      ' WHEN emaj_verb = ''DEL'' THEN ''DELETE'''
             ||      ' ELSE ''?'' END::VARCHAR(6) as emaj_verb,'
             || ' count(*) as emaj_rows'
             || ' FROM ' || v_logTableName 
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')';
        IF v_firstMark IS NOT NULL THEN v_stmt = v_stmt 
             || ' AND emaj_changed >= timestamp '''|| v_tsFirstMark || '''';
        END IF;
        IF v_lastMark IS NOT NULL THEN v_stmt = v_stmt
             || ' AND emaj_changed < timestamp '''|| v_tsLastMark || '''';
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
    SELECT group_nb_table + group_nb_sequence INTO v_nbTblSeq FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: group % has not been created', v_groupName;
    END IF;
-- get all needed duration parameters from emaj_param table
    SELECT param_value_interval INTO v_avg_row_rlbk FROM emaj.emaj_param 
        WHERE param_key = 'avg_row_rollback_duration';
    SELECT param_value_interval INTO v_avg_row_del_log FROM emaj.emaj_param 
        WHERE param_key = 'avg_row_delete_log_duration';
    SELECT param_value_interval INTO v_fixed_table_rlbk FROM emaj.emaj_param 
        WHERE param_key = 'fixed_table_rollback_duration';
    SELECT param_value_interval INTO v_fixed_table_with_rlbk FROM emaj.emaj_param 
        WHERE param_key = 'fixed_table_with_rollback_duration';
-- compute the fixed cost for the group
    v_estim_duration = v_nbTblSeq * v_fixed_table_rlbk;
--
-- walk through the list of tables with their number of rows to rollback as returned by the emaj_log_stat_group function
--
-- for each table
    FOR r_tblsq IN
        SELECT stat_schema, stat_table, stat_rows FROM emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) WHERE stat_rows > 0
        LOOP
--
-- compute the rollback duration estimate for the table
--
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbk_duration) / sum(rlbk_nb_rows) * r_tblsq.stat_rows INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'rlbk' AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) / sum(rlbk_nb_rows) * r_tblsq.stat_rows INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'rlbk' AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
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
      SELECT sum(rlbk_duration) / sum(rlbk_nb_rows) * r_tblsq.stat_rows INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'del_log' AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) / sum(rlbk_nb_rows) * r_tblsq.stat_rows INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'del_log' AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
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
--raise notice 'schema % table % (% tuples) -> fkey %',r_fkey.nspname, r_fkey.relname, r_fkey.reltuples, r_fkey.conname;
-- estimate the recreation duration of a fkey 
      IF r_fkey.reltuples = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estim = 0;
	  ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbk_duration) / sum(rlbk_nb_rows) * r_fkey.reltuples INTO v_estim FROM emaj.emaj_rlbk_stat
          WHERE rlbk_operation = 'add_fk' AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname AND rlbk_nb_rows > 0;
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

CREATE or REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_snap_group$
-- This function creates a file for each table and sequence belonging to the group.
-- For tables, these files contain all rows sorted on primary key.
-- For sequences, they contain a single row describing the sequence.
-- It is used for test purpose. It creates a snap of the group that can be compared to another snap.
-- To do its job, the function performs COPY TO statement, with all default parameters.
-- For table without primary key (in log_only mode), rows are sorted on all columns.
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
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    r_tblsq           RECORD;
    v_fullTableName   TEXT;
    r_col             RECORD;
    v_colList         TEXT;
    v_fileName        TEXT;
    v_relhaspkey      BOOLEAN;
    v_stmt text;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: group % has not been created', v_groupName;
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = v_groupName
        LOOP
      v_fileName := v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
      v_fullTableName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   first build the order by column list
        v_colList := '';
        SELECT relhaspkey INTO v_relhaspkey FROM pg_class, pg_namespace WHERE 
          relnamespace = pg_namespace.oid AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
        IF v_relhaspkey THEN
--   the table has a pkey,
          FOR r_col IN
              SELECT attname FROM pg_attribute, pg_index 
                WHERE pg_attribute.attrelid = pg_index.indexrelid 
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
                  AND attnum > 0
                  AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList := r_col.attname;
            ELSE
               v_colList := v_colList || ',' || r_col.attname;
            END IF;
          END LOOP;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO ''' || v_fileName || '''';
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, the statement has no order by
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ') TO ''' || v_fileName || '''';
      END IF;
-- and finaly perform the COPY
      raise notice 'emaj_snap_group: Executing %',v_stmt;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
    RETURN v_nbTb;
  END;
$emaj_snap_group$;

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------

-- Create the emaj NOLOGIN roles and give them the appropriate rights, using a temporary function

CREATE or REPLACE FUNCTION emaj.tmp_create_role() 
RETURNS VOID LANGUAGE plpgsql AS 
$tmp_create_role$
-- This temporary function verifies if emaj roles already exist. If not, it creates them.
  BEGIN
-- Does 'emaj_adm' already exist ?
    PERFORM 1 FROM pg_roles WHERE rolname = 'emaj_adm';
-- If no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_adm;
    END IF;
-- Does 'emaj_viewer' already exist ?
    PERFORM 1 FROM pg_roles WHERE rolname = 'emaj_viewer';
-- If no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_viewer;
    END IF;
    RETURN;
  END;
$tmp_create_role$;

SELECT emaj.tmp_create_role();

DROP FUNCTION emaj.tmp_create_role();

-- Give rights to emaj roles

-- -> emaj_viewer can view the emaj objects (content of emaj and log tables)
GRANT USAGE ON SCHEMA emaj TO emaj_viewer;

GRANT SELECT ON emaj.emaj_param      TO emaj_viewer;
GRANT SELECT ON emaj.emaj_hist       TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group_def  TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group      TO emaj_viewer;
GRANT SELECT ON emaj.emaj_relation   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_mark       TO emaj_viewer;
GRANT SELECT ON emaj.emaj_sequence   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_seq_hole   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_fk         TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk_stat  TO emaj_viewer;

-- -> emaj_adm can execute all emaj functions (except emaj_snap_group)

GRANT CREATE ON TABLESPACE tspemaj TO emaj_adm;

GRANT ALL   ON SCHEMA emaj TO emaj_adm;

GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_param      TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_hist       TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group_def  TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group      TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_relation   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_mark       TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_sequence   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_seq_hole   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_fk         TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk_stat  TO emaj_adm;

GRANT ALL ON SEQUENCE emaj.emaj_hist_hist_id_seq TO emaj_adm;

-- revoke grants on all function from PUBLIC
REVOKE ALL ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._create_log(v_schemaName TEXT, v_tableName TEXT, v_logOnly BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_log(v_schemaName TEXT, v_tableName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_seq(v_schemaName TEXT, v_seqName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_table(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_sequence(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_verify_all() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._verify_group(v_groupName TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._check_fk_group (v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._lock_group(v_groupName TEXT, v_lockMode TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._set_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_step1(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSubGroup INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_set_subgroup(v_groupName TEXT, v_schema TEXT, v_table TEXT, v_subGroup INT, v_rows BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_step2(v_groupName TEXT, v_subGroup INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_step3(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_step4(v_groupName TEXT, v_subGroup INT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rlbk_group_step5(v_groupName TEXT, v_mark TEXT, v_subGroup INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_group_step6(v_groupName TEXT, v_subGroup INT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rlbk_group_step7(v_groupName TEXT, v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rst_group(v_groupName TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;

-- and give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._create_log(v_schemaName TEXT, v_tableName TEXT, v_logOnly BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_log(v_schemaName TEXT, v_tableName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_seq(v_schemaName TEXT, v_seqName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_table(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_sequence(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._verify_group(v_groupName TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._check_fk_group (v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._lock_group(v_groupName TEXT, v_lockMode TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._set_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rlbk_group(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step1(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSubGroup INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_set_subgroup(v_groupName TEXT, v_schema TEXT, v_table TEXT, v_subGroup INT, v_rows BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step2(v_groupName TEXT, v_subGroup INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step3(v_groupName TEXT, v_mark TEXT, v_unloggedRlbk BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step4(v_groupName TEXT, v_subGroup INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step5(v_groupName TEXT, v_mark TEXT, v_subGroup INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step6(v_groupName TEXT, v_subGroup INT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rlbk_group_step7(v_groupName TEXT, v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rst_group(v_groupName TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) TO emaj_adm;

-- and give appropriate rights on functions to emaj_viewer role
GRANT EXECUTE ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer; 
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) TO emaj_viewer;

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_wording) VALUES ('EMAJ_INIT','E-Maj initialisation completed');

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-Maj objects successfully created'

