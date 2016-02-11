--
-- E-Maj: migration from 1.2.0 to 1.3.0
--
-- This software is distributed under the GNU General Public License.
--
-- This script migrates an existing installation of E-Maj extension.
-- If E-Maj is not yet installed, use the emaj.sql script. 
--

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO WARNING;
--SET client_min_messages TO NOTICE;
\echo 'E-maj upgrade from version 1.2.0 to version 1.3.0'
\echo 'Checking...'
------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Creation of a specific function to check the migration conditions are met.
-- The function generates an exception if at least one condition is not met.
DROP FUNCTION IF EXISTS emaj.tmp();
CREATE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_emajVersion            TEXT;
    v_pgVersion              INTEGER = emaj._pg_version_num();
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj installation: the current user (%) is not a superuser.', current_user;
    END IF;
-- the emaj version registered in emaj_param must be '1.2.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '1.2.0' THEN
      RAISE EXCEPTION 'The current E-Maj version (%) is not 1.2.0',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 8.3
    IF v_pgVersion < 803 THEN
      RAISE EXCEPTION 'The current PostgreSQL version (%) is not compatible with E-Maj 1.3.0 (8.3 minimum)',v_pgVersion;
    END IF;
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

CREATE OR REPLACE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
  BEGIN
-- if tspemaj tablespace exists, use it as default_tablespace for emaj tables creation
--   and grant the create rights on it to emaj_adm
    PERFORM 0 FROM pg_tablespace WHERE spcname = 'tspemaj';
    IF FOUND THEN
      SET LOCAL default_tablespace TO tspemaj;
      GRANT CREATE ON TABLESPACE tspemaj TO emaj_adm;
    END IF;
    RETURN;
  END;
$tmp$;
SELECT emaj.tmp();
DROP FUNCTION emaj.tmp();

\echo 'Updating E-Maj internal objects ...'

---------------------------------------
--                                   --
-- emaj tables, views and sequences  --
--                                   --
---------------------------------------
-- drop the foreign keys impacted by the migration
ALTER TABLE emaj.emaj_mark DROP CONSTRAINT emaj_mark_mark_group_fkey;
ALTER TABLE emaj.emaj_relation DROP CONSTRAINT emaj_relation_rel_group_fkey;

--
-- process the emaj_group table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE tmp_group_old (LIKE emaj.emaj_group);

INSERT INTO tmp_group_old SELECT * FROM emaj.emaj_group;

-- drop the old table
DROP TABLE emaj.emaj_group;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
CREATE TABLE emaj.emaj_group (
  group_name                 TEXT        NOT NULL,
  group_is_logging           BOOLEAN     NOT NULL,       -- are log triggers activated ?
                                                         -- true between emaj_start_group(s) and emaj_stop_group(s)
                                                         -- flase in other cases
  group_is_rlbk_protected    BOOLEAN     NOT NULL,       -- is the group currently protected against rollback ?
                                                         -- always true for AUDIT_ONLY groups
  group_nb_table             INT,                        -- number of tables at emaj_create_group time
  group_nb_sequence          INT,                        -- number of sequences at emaj_create_group time
  group_is_rollbackable      BOOLEAN     NOT NULL,       -- false for 'AUDIT_ONLY' and true for 'ROLLBACKABLE' groups
  group_creation_datetime    TIMESTAMPTZ NOT NULL        -- start time of the transaction that created the group
                             DEFAULT transaction_timestamp(),
  group_last_alter_datetime  TIMESTAMPTZ,                -- date and time of the last emaj_alter_group() exec,
                                                         -- set to NULL at emaj_create_group() time
  group_pg_version           TEXT        NOT NULL        -- postgres version at emaj_create_group() time
                             DEFAULT substring (version() from E'PostgreSQL\\s([.,0-9,A-Z,a-z]*)'),
  group_comment              TEXT,                       -- optional user comment
  PRIMARY KEY (group_name)
  );
COMMENT ON TABLE emaj.emaj_group IS
$$Contains created E-Maj groups.$$;

-- populate the new table
INSERT INTO emaj.emaj_group
  SELECT group_name, group_is_logging, FALSE, group_nb_table, group_nb_sequence, group_is_rollbackable, 
         group_creation_datetime, group_last_alter_datetime, group_pg_version, group_comment
    FROM tmp_group_old;

-- and drop the temporary table
DROP TABLE tmp_group_old;

--
-- process the emaj_relation table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE tmp_relation_old (LIKE emaj.emaj_relation);

INSERT INTO tmp_relation_old SELECT * FROM emaj.emaj_relation;

-- drop the old table
DROP TABLE emaj.emaj_relation CASCADE;
-- The CASCADE clause is needed for the emaj_relation table, because its structure is referenced in 7 functions:
--   emaj._drop_tbl(), emaj._drop_seq(), emaj._rlbk_tbl(), emaj._delete_log_tbl(), emaj._rlbk_seq(), emaj._log_stat_tbl(), emaj._gen_sql_tbl()
-- These functions need to be recreated as well as their grants.

-- create the new table, with its indexes, comment, constraints (except foreign key)...
CREATE TABLE emaj.emaj_relation (
  rel_schema                 TEXT        NOT NULL,       -- schema name containing the relation
  rel_tblseq                 TEXT        NOT NULL,       -- application table or sequence name
  rel_group                  TEXT        NOT NULL,       -- name of the group that owns the relation
  rel_kind                   TEXT,                       -- similar to the relkind column of pg_class table
                                                         --   ('r' = table, 'S' = sequence)
  rel_priority               INTEGER,                    -- priority level of processing inside the group
  rel_log_schema             TEXT,                       -- schema for the log table, functions and sequence
  rel_log_table              TEXT,                       -- name of the log table associated (NULL for sequence)
  rel_log_dat_tsp            TEXT,                       -- tablespace for the log table (NULL for sequences)
  rel_log_index              TEXT,                       -- name of the index of the log table
  rel_log_idx_tsp            TEXT,                       -- tablespace for the log index (NULL for sequences)
  rel_log_sequence           TEXT,                       -- name of the log sequence
  rel_log_function           TEXT,                       -- name of the function associated to the log trigger
                                                         --   created on the application table
  PRIMARY KEY (rel_schema, rel_tblseq)
  );
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;
-- index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- index on emaj_relation used to speedup _verify_schema() with large E-Maj configuration
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- populate the new table
INSERT INTO emaj.emaj_relation
  SELECT rel_schema, rel_tblseq, rel_group, rel_kind, rel_priority, rel_log_schema,
         rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function
    FROM tmp_relation_old;

-- the tmp_relation_old temporary table will be dropped later, after its content will be used to rename the triggers

--
-- process the emaj_mark table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE tmp_mark_old (LIKE emaj.emaj_mark);

INSERT INTO tmp_mark_old SELECT * FROM emaj.emaj_mark;

-- create a sequence to keep a trace of the sequence current values associated to the mark_id column
CREATE TEMPORARY SEQUENCE tmp_mark_old_mark_id_seq;
SELECT setval('tmp_mark_old_mark_id_seq',last_value,is_called) FROM emaj.emaj_mark_mark_id_seq;

-- drop the old table
DROP TABLE emaj.emaj_mark;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
CREATE TABLE emaj.emaj_mark (
  mark_group                 TEXT        NOT NULL,       -- group for which the mark has been set
  mark_name                  TEXT        NOT NULL,       -- mark name
  mark_id                    BIGSERIAL   NOT NULL,       -- serial id used to order rows (not to rely on timestamps
                                                         -- that are not safe if system time changes)
  mark_datetime              TIMESTAMPTZ NOT NULL,       -- precise timestamp of the mark creation, used as a reference
                                                         --   for other tables like emaj_sequence and all log tables
  mark_global_seq            BIGINT      NOT NULL,       -- emaj_global_seq last value at mark set (used to rollback)
  mark_is_deleted            BOOLEAN     NOT NULL,       -- boolean to indicate if the mark is deleted
  mark_is_rlbk_protected     BOOLEAN     NOT NULL,       -- boolean to indicate if the mark is protected from rollbacks (false by default)
  mark_comment               TEXT,                       -- optional user comment
  mark_txid                  BIGINT                      -- id of the tx that has set the mark
                             DEFAULT txid_current(),
  mark_last_sequence_id      BIGINT,                     -- last sequ_id for the group at the end of the _set_mark_groups operation
  mark_last_seq_hole_id      BIGINT,                     -- last sqhl_id for the group at _set_mark_groups time
  mark_log_rows_before_next  BIGINT,                     -- number of log rows recorded for the group between the mark
                                                         -- and the next one (NULL if last mark)
                                                         -- used to speedup marks list display in phpPgAdmin plugin
  PRIMARY KEY (mark_group, mark_name)
  );
COMMENT ON TABLE emaj.emaj_mark IS
$$Contains marks set on E-Maj tables groups.$$;

-- populate the new table
INSERT INTO emaj.emaj_mark
  SELECT mark_group, mark_name, mark_id, mark_datetime, mark_global_seq, mark_is_deleted, FALSE, 
         mark_comment, mark_txid, mark_last_sequence_id, mark_last_seq_hole_id, mark_log_rows_before_next
    FROM tmp_mark_old;

-- reset the original current value of the sequence associated to the mark_id column
SELECT setval('emaj.emaj_mark_mark_id_seq',last_value,is_called) FROM tmp_mark_old_mark_id_seq;

-- and drop the temporary table and sequence
DROP TABLE tmp_mark_old;
DROP SEQUENCE tmp_mark_old_mark_id_seq;

-- create the foreign keys dropped at the beginning of the tables migration or not yet created for the new tables
ALTER TABLE emaj.emaj_relation ADD FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE;
ALTER TABLE emaj.emaj_mark ADD FOREIGN KEY (mark_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE;

------------------------------------
--                                --
-- emaj functions: drop obsolete  --
--    or with modified interface  --
--                                --
------------------------------------
DROP FUNCTION emaj._pg_version();

------------------------------------
--                                --
-- emaj functions: recreate all   --
--                                --
------------------------------------

------------------------------------
--                                --
-- Low level Functions            --
--                                --
------------------------------------
CREATE OR REPLACE FUNCTION emaj._pg_version_num()
RETURNS INTEGER LANGUAGE sql IMMUTABLE AS
$$
-- This function returns as an integer the 2 major parts of the current postgresql version
-- x.y.z => x*100 + y. For instance, Postgres version 9.4.12 returns 904
SELECT cast(to_number(substring (version() from E'PostgreSQL\\s(\\d+)'),'99') * 100 +
            to_number(substring (version() from E'PostgreSQL\\s\\d+\\.(\\d+)'),'99') AS INTEGER);
$$;

---------------------------------------------------
--                                               --
-- Elementary functions for tables and sequences --
--                                               --
---------------------------------------------------

CREATE OR REPLACE FUNCTION emaj._create_tbl(r_grpdef emaj.emaj_group_def, v_groupName TEXT, v_isRollbackable BOOLEAN, v_defTsp TEXT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: the emaj_group_def row related to the application table to process, the group name, a boolean indicating whether the group is rollbackable, and the default tablespace to use if no specific tablespace is set for this application table
-- Are created in the log schema:
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
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
    v_relIsTemp              BOOLEAN;
    v_relhaspkey             BOOLEAN;
    v_stmt                   TEXT = '';
    v_triggerList            TEXT = '';
    r_column                 RECORD;
    r_trigger                RECORD;
  BEGIN
-- check the table is neither a temporary nor an unlogged table
    IF v_pgVersion >= 901 THEN
      SELECT relpersistence INTO v_relPersistence FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = r_grpdef.grpdef_schema AND relname = r_grpdef.grpdef_tblseq;
      IF v_relPersistence = 't' THEN
        RAISE EXCEPTION '_create_tbl: table % is a temporary table.', r_grpdef.grpdef_tblseq;
      ELSIF v_relPersistence = 'u' THEN
        RAISE EXCEPTION '_create_tbl: table % is an unlogged table.', r_grpdef.grpdef_tblseq;
      END IF;
    ELSIF v_pgVersion >= 804 THEN
      SELECT relistemp INTO v_relIsTemp FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = r_grpdef.grpdef_schema AND relname = r_grpdef.grpdef_tblseq;
      IF v_relIsTemp THEN
        RAISE EXCEPTION '_create_tbl: table % is a temporary table.', r_grpdef.grpdef_tblseq;
      END IF;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', r_grpdef.grpdef_tblseq;
    END IF;
-- check the table has a primary key
    SELECT true INTO v_relhaspkey FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
      WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
        AND contype = 'p' AND nspname = r_grpdef.grpdef_schema AND relname = r_grpdef.grpdef_tblseq;
    IF NOT FOUND THEN
      v_relhaspkey = false;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', r_grpdef.grpdef_tblseq;
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
         || ' ( LIKE ' || v_fullTableName || ') ' || v_dataTblSpace;
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
    FOR r_column IN
      SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
        FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
          AND nspname = v_logSchema AND relname = v_baseLogTableName
          AND attnum > 0 AND attnotnull AND attisdropped = false AND attname NOT LIKE E'emaj\\_%'
    LOOP
      IF v_stmt = '' THEN
        v_stmt = v_stmt || r_column.action;
      ELSE
        v_stmt = v_stmt || ',' || r_column.action;
      END IF;
    END LOOP;
    IF v_stmt <> '' THEN
      EXECUTE 'ALTER TABLE ' || v_logTableName || v_stmt;
    END IF;
-- create the sequence associated to the log table
    EXECUTE 'CREATE SEQUENCE ' || v_sequenceName;
-- creation of the log fonction that will be mapped to the log trigger later
-- The new row is logged for each INSERT, the old row is logged for each DELETE
-- and the old and the new rows are logged for each UPDATE.
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || v_logFnctName || '() RETURNS trigger AS $logfnct$'
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
    IF v_pgVersion >= 804 THEN
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
    END IF;
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
    FOR r_trigger IN
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'emaj\\_%\\_trg'
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
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_fullTableName          TEXT;
  BEGIN
    v_fullTableName    = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- check the table exists before dropping its triggers
    PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = r_rel.rel_schema AND relname = r_rel.rel_tblseq AND relkind = 'r';
    IF FOUND THEN
-- delete the log trigger on the application table
      EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
-- delete the truncate trigger on the application table
      IF v_pgVersion >= 804 THEN
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
      END IF;
    END IF;
-- delete the log function
    EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() CASCADE';
-- delete the sequence associated to the log table
    EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence);
-- delete the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
-- delete rows related to the log sequence from emaj_sequence table
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- delete rows related to the table from emaj_seq_hole table
    DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = quote_ident(r_rel.rel_schema) AND sqhl_table = quote_ident(r_rel.rel_tblseq);
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation)
RETURNS void LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: row from emaj_relation corresponding to the appplication sequence to proccess
  BEGIN
-- delete rows from emaj_sequence
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ' || quote_literal(r_rel.rel_schema) || ' AND sequ_name = ' || quote_literal(r_rel.rel_tblseq);
    RETURN;
  END;
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_lastGlobalSeq BIGINT, v_nbSession INT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence
-- The function is called by emaj._rlbk_session_exec()
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence value limit for rollback
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_nbPk                   BIGINT;
    v_cols                   TEXT[] = '{}';
    v_colsPk                 TEXT[] = '{}';
    v_condsPk                TEXT[] = '{}';
    v_colList                TEXT;
    v_pkColList              TEXT;
    v_pkCondList             TEXT;
    r_col                    RECORD;
  BEGIN
    v_fullTableName  = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName   = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'All log rows with emaj_gid > ' || v_lastGlobalSeq);
-- Build some pieces of SQL statements
--   build the tables's columns list
    FOR r_col IN
      SELECT 'tbl.' || quote_ident(attname) AS col_name FROM pg_catalog.pg_attribute
        WHERE attrelid = v_fullTableName::regclass
          AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
      LOOP
      v_cols = array_append(v_cols, r_col.col_name);
    END LOOP;
    v_colList = array_to_string(v_cols,',');
-- TODO when pg 8.3 will not be supported any more :
--   SELECT array_to_string(array_agg('tbl.' || quote_ident(attname)),', ') INTO v_colList
--     FROM pg_catalog.pg_attribute
--     WHERE attrelid = 'myschema1.mytbl1'::regclass AND attnum > 0 AND NOT attisdropped;
--   build the pkey columns list and the "equality on the primary key" conditions
    FOR r_col IN
      SELECT quote_ident(attname) AS col_pk_name,
             'tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname) AS col_pk_cond
        FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName::regclass AND indisprimary
          AND attnum > 0 AND attisdropped = false
        ORDER BY attnum
      LOOP
      v_colsPk = array_append(v_colsPk, r_col.col_pk_name);
      v_condsPk = array_append(v_condsPk, r_col.col_pk_cond);
    END LOOP;
    v_pkColList = array_to_string(v_colsPk,',');
    v_pkCondList = array_to_string(v_condsPk, ' AND ');
-- create the temporary table containing all primary key values with their earliest emaj_gid
    IF v_nbSession = 1 THEN
      v_tableType = 'TEMP';
      v_tmpTable = 'emaj_tmp_' || pg_backend_pid();
    ELSE
--   with multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
--   but it may be an UNLOGGED table with pg 9.1+
      IF v_pgVersion >= 901 THEN
        v_tableType = 'UNLOGGED';
      ELSE
        v_tableType = '';
      END IF;
      v_tmpTable = 'emaj.emaj_tmp_' || pg_backend_pid();
    END IF;
    EXECUTE 'CREATE ' || v_tableType || ' TABLE ' || v_tmpTable || ' AS '
         || '  SELECT ' || v_pkColList || ', min(emaj_gid) as emaj_gid'
         || '    FROM ' || v_logTableName
         || '    WHERE emaj_gid > ' || v_lastGlobalSeq
         || '    GROUP BY ' || v_pkColList;
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- delete all rows from the application table corresponding to each touched primary key
--   this deletes rows inserted or updated during the rolled back period
    EXECUTE 'DELETE FROM ONLY ' || v_fullTableName || ' tbl USING ' || v_tmpTable || ' keys '
         || '  WHERE ' || v_pkCondList;
-- inserted into the application table rows that were deleted or updated during the rolled back period
    EXECUTE 'INSERT INTO ' || v_fullTableName
         || '  SELECT ' || v_colList
         || '    FROM ' || v_logTableName || ' tbl, ' || v_tmpTable || ' keys '
         || '    WHERE ' || v_pkCondList || ' AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD''';
-- drop the now useless temporary table
    EXECUTE 'DROP TABLE ' || v_tmpTable;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

CREATE OR REPLACE FUNCTION emaj._delete_log_tbl(r_rel emaj.emaj_relation, v_timestamp TIMESTAMPTZ, v_lastGlobalSeq BIGINT, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_delete_log_tbl$
-- This function deletes the part of a log table corresponding to updates that have been rolled back.
-- The function is only called for unlogged rollback.
-- It deletes marks that are not visible anymore after to the rollback.
-- It also registers the hole in sequence number generated by the deleted log rows.
-- The function is called by emaj._rlbk_session_exec()
-- Input: row from emaj_relation corresponding to the appplication table to proccess, global sequence value limit for rollback, mark timestamp,
--        flag to specify if the rollback is logged,
--        last sequence and last hole identifiers to keep (greater ones being to be deleted)
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- delete obsolete log rows
    EXECUTE 'DELETE FROM ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' WHERE emaj_gid > ' || v_lastGlobalSeq;
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- suppress from emaj_sequence table the rows regarding the emaj log sequence for this application table
--     corresponding to potential later intermediate marks that disappear with the rollback operation
    DELETE FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence AND sequ_id > v_lastSequenceId;
-- record the sequence holes generated by the delete operation
-- this is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_duration())
-- first delete, if exist, sequence holes that have disappeared with the rollback
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq AND sqhl_id > v_lastSeqHoleId;
-- and then insert the new sequence hole
    EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_hole_size) VALUES ('
      || quote_literal(r_rel.rel_schema) || ',' || quote_literal(r_rel.rel_tblseq) || ', ('
      || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM '
      || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)
      || ')-('
      || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
      || ' emaj.emaj_sequence WHERE'
      || ' sequ_schema = ' || quote_literal(r_rel.rel_log_schema)
      || ' AND sequ_name = ' || quote_literal(r_rel.rel_log_sequence)
      || ' AND sequ_datetime = ' || quote_literal(v_timestamp) || '))';
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, v_timestamp TIMESTAMPTZ, v_isLoggedRlbk BOOLEAN, v_lastSequenceId BIGINT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark
-- The function is called by emaj.emaj._rlbk_end()
-- Input: the emaj_group_def row related to the application sequence to process, mark timestamp, boolean indicating whether the rollback is logged, and the id of the last sequence to keep
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it is not the owner of the application sequence.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_fullSeqName            TEXT;
    v_stmt                   TEXT;
    mark_seq_rec             RECORD;
    curr_seq_rec             RECORD;
  BEGIN
-- Read sequence's characteristics at mark time
    BEGIN
      SELECT sequ_schema, sequ_name, sequ_mark, sequ_last_val, sequ_start_val, sequ_increment,
             sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called
        INTO STRICT mark_seq_rec
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq AND sequ_datetime = v_timestamp;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RAISE EXCEPTION '_rlbk_seq: Mark at % not found for sequence %.%.', v_timestamp, r_rel.rel_schema, r_rel.rel_tblseq;
        WHEN TOO_MANY_ROWS THEN
          RAISE EXCEPTION '_rlbk_seq: Internal error 1.';
    END;
-- Read the current sequence's characteristics
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_stmt = 'SELECT last_value, ';
    IF v_pgVersion <= 803 THEN
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
-- if the caller requires it, delete the rolled back intermediate sequences from the sequence table
    IF NOT v_isLoggedRlbk THEN
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq AND sequ_id > v_lastSequenceId;
    END IF;
-- insert event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
    RETURN;
  END;
$_rlbk_seq$;

CREATE OR REPLACE FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_log_stat_tbl$
-- This function returns the number of log rows for a single table between 2 marks or between a mark and the current situation.
-- It is called by emaj_log_stat_group function
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: schema name and table name, log schema, the timestamps of both marks, the emaj_seq_hole last id of both marks
--   a NULL value as last timestamp mark indicates the current situation
-- Output: number of log rows between both marks for the table
  DECLARE
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- get the log table id at first mark time
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_beginLastValue
       FROM emaj.emaj_sequence
       WHERE sequ_schema = r_rel.rel_log_schema
         AND sequ_name = r_rel.rel_log_sequence
         AND sequ_datetime = v_tsFirstMark;
    IF v_tsLastMark IS NULL THEN
-- last mark is NULL, so examine the current state of the log table id
      EXECUTE 'SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM '
           || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence) INTO v_endLastValue;
--   and count the sum of hole from the start mark time until now
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_id > v_firstLastSeqHoleId;
    ELSE
-- last mark is not NULL, so get the log table id at last mark time
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_endLastValue
         FROM emaj.emaj_sequence
         WHERE sequ_schema = r_rel.rel_log_schema
           AND sequ_name = r_rel.rel_log_sequence
           AND sequ_datetime = v_tsLastMark;
--   and count the sum of hole from the start mark time to the end mark time
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_id > v_firstLastSeqHoleId AND sqhl_id <= v_lastLastSeqHoleId;
    END IF;
-- return the stat row for the table
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole);
  END;
$_log_stat_tbl$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, v_conditions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_tbl$
-- This function generates SQL commands representing all updates performed on a table between 2 marks
-- or beetween a mark and the current situation. These command are stored into a temporary table created
-- by the _gen_sql_groups() calling function.
-- Input: - fully qualified application table to process
--        - fully qualified associated log table
--        - sql conditions corresponding to the marks range to process
-- Output: number of generated SQL statements
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_valList                TEXT;
    v_setList                TEXT;
    v_pkCondList             TEXT;
    v_unquotedType           TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                             'int2','int4','int8','serial','bigserial',
                                             'real','double precision','float','float4','float8','oid'];
    v_rqInsert               TEXT;
    v_rqUpdate               TEXT;
    v_rqDelete               TEXT;
    v_rqTruncate             TEXT;
    v_nbSQL                  INT;
    r_col                    RECORD;
  BEGIN
-- build schema specified table name and log table name
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements
    v_valList = '';
    v_setList = '';
    FOR r_col IN
      SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute
       WHERE attrelid = v_fullTableName ::regclass
         AND attnum > 0 AND NOT attisdropped
       ORDER BY attnum
    LOOP
-- test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric data types)
      IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
--          may be we will need to cast some column types in the future. So keep the comment for the moment...
--          v_valList = v_valList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::text,''NULL'') || ''::' || r_col.format_type || ', ';
        v_valList = v_valList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::text,''NULL'') || '', ';
--          v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.' || quote_ident(r_col.attname) || ' ::text,''NULL'') || ''::' || r_col.format_type || ', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.' || quote_ident(r_col.attname) || ' ::text,''NULL'') || '', ';
      ELSE
-- literal for this column must be quoted
--          v_valList = v_valList || ''' || coalesce(quote_literal(o.' || quote_ident(r_col.attname) || '),''NULL'') || ''::' || r_col.format_type || ', ';
        v_valList = v_valList || ''' || coalesce(quote_literal(o.' || quote_ident(r_col.attname) || '),''NULL'') || '', ';
--          v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(quote_literal(n.' || quote_ident(r_col.attname) || '),''NULL'') || ''::' || r_col.format_type || ', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(quote_literal(n.' || quote_ident(r_col.attname) || '),''NULL'') || '', ';
      END IF;
    END LOOP;
-- suppress the final separators
    v_valList = substring(v_valList FROM 1 FOR char_length(v_valList) - 2);
    v_setList = substring(v_setList FROM 1 FOR char_length(v_setList) - 2);
-- retrieve all columns that represents the pkey and build the "pkey equal" conditions set that will be used in UPDATE and DELETE statements
-- (taking column names in pg_attribute from the table's definition instead of index definition is mandatory
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
    v_pkCondList = '';
    FOR r_col IN
      SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName ::regclass AND indisprimary
          AND attnum > 0 AND NOT attisdropped
    LOOP
-- test if the column format (at least up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric data types)
      IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
--          v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.' || quote_ident(r_col.attname) || ' || ''::' || r_col.format_type || ' AND ';
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.' || quote_ident(r_col.attname) || ' || '' AND ';
      ELSE
-- literal for this column must be quoted
--          v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(r_col.attname) || ') || ''::' || r_col.format_type || ' AND ';
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(r_col.attname) || ') || '' AND ';
      END IF;
    END LOOP;
-- suppress the final separator
    v_pkCondList = substring(v_pkCondList FROM 1 FOR char_length(v_pkCondList) - 5);
-- prepare sql skeletons for each statement type
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''') || ' VALUES (' || v_valList || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''') || ' SET ' || v_setList || ' WHERE ' || v_pkCondList || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''') || ' WHERE ' || v_pkCondList || ';''';
    v_rqTruncate = '''TRUNCATE ' || replace(v_fullTableName,'''','''''') || ';''';
-- now scan the log table to process all statement types at once
    EXECUTE 'INSERT INTO emaj_temp_script '
         || 'SELECT o.emaj_gid, 0, o.emaj_txid, CASE '
         ||   ' WHEN o.emaj_verb = ''INS'' THEN ' || v_rqInsert
         ||   ' WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN ' || v_rqUpdate
         ||   ' WHEN o.emaj_verb = ''DEL'' THEN ' || v_rqDelete
         ||   ' WHEN o.emaj_verb = ''TRU'' THEN ' || v_rqTruncate
         || ' END '
         || ' FROM ' || v_logTableName || ' o'
         ||   ' LEFT OUTER JOIN ' || v_logTableName || ' n ON n.emaj_gid = o.emaj_gid'
         || '        AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'') '
         || ' WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
         || ' AND ' || v_conditions;
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

------------------------------------------------
----                                        ----
----       Functions to manage groups       ----
----                                        ----
------------------------------------------------

CREATE OR REPLACE FUNCTION emaj._verify_groups(v_groups TEXT[], v_onErrorStop BOOLEAN)
RETURNS SETOF emaj._verify_groups_type LANGUAGE plpgsql AS
$_verify_groups$
-- The function verifies the consistency of a tables groups array.
-- Input: - tables groups array,
--        - a boolean indicating whether the function has to raise an exception in case of detected unconsistency.
-- If onErrorStop boolean is false, it returns a set of _verify_groups_type records, one row per detected unconsistency, including the faulting schema and table or sequence names and a detailed message.
-- If no error is detected, no row is returned.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_hint                   TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected
-- check the postgres version: E-Maj is not compatible with 8.2-
    IF v_pgVersion < 803 THEN
      RAISE EXCEPTION 'The current postgres version (%) is not compatible with E-Maj.', version();
    END IF;
-- check the postgres version at groups creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric.
--          But we suppose these tests will not be useful any more when pg 10.0 will appear!
--   for 8.3, both major versions must be the same
    FOR r_object IN
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE group_name = ANY (v_groups)
          AND ((v_pgVersion = 803 AND cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                                           to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) <> v_pgVersion)
--   for 8.4+, both major versions must be 8.4+
            OR (v_pgVersion >= 804 AND cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                                            to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804))
        ORDER BY msg
    LOOP
      RAISE EXCEPTION '_verify_groups: %',r_object.msg;
    END LOOP;
-- OK, now look for groups unconsistency
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist
-- check all application relations referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq,
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                    -- all relations known by E-Maj
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = ANY (v_groups)
            EXCEPT                                -- all relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
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
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log function for each table referenced in the emaj_relation table still exists
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
                 WHERE nspname = rel_log_schema AND proname = rel_log_function
                   AND pronamespace = pg_namespace.oid)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_log_trg'
                   AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   then truncate trigger if pg 8.4+
    IF v_pgVersion >= 804 THEN
      FOR r_object IN
        SELECT rel_schema, rel_tblseq,
               'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
          FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
            AND NOT EXISTS
                (SELECT NULL FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                   WHERE nspname = rel_schema AND relname = rel_tblseq AND tgname = 'emaj_trunc_trg'
                     AND tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
      LOOP
        IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
        RETURN NEXT r_object;
      END LOOP;
-- TODO : merge both triggers check when pg 8.3 will not be supported any more
    END IF;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the structure of the application table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
             rel_log_schema || '"."' || rel_log_table || '").' AS msg
        FROM (
          (                                       -- application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          EXCEPT                                   -- minus log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema AND relname = rel_log_table
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          )
          UNION
          (                                         -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema AND relname = rel_log_table
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          EXCEPT                                    -- minus application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          )) AS t
        ORDER BY 1,2,3
-- TODO : use CTE to improve performance, when pg 8.3 will not be supported any more
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- It also creates the secondary E-Maj schemas when needed
-- Input: group name, boolean indicating wether the group is rollbackable or not
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTbl                  INT = 0;
    v_nbSeq                  INT = 0;
    v_schemaPrefix           TEXT = 'emaj';
    v_defTsp                 TEXT;
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
    r_schema                 RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName, CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- check that the group name is valid
    IF v_groupName IS NULL OR v_groupName = ''THEN
      RAISE EXCEPTION 'emaj_create_group: group name can''t be NULL or empty.';
    END IF;
-- check the group is known in emaj_group_def table
    PERFORM 0 FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName LIMIT 1;
    IF NOT FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: Group % is unknown in emaj_group_def table.', v_groupName;
    END IF;
-- check that the group is not yet recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: group % is already created.', v_groupName;
    END IF;
-- performs various checks on the group's content described in the emaj_group_def table
    PERFORM emaj._check_group_content(v_groupName);
-- OK, insert group row in the emaj_group table
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable)
    INSERT INTO emaj.emaj_group (group_name, group_is_logging, group_is_rollbackable, group_is_rlbk_protected)
      VALUES (v_groupName, FALSE, v_isRollbackable, NOT v_isRollbackable);
-- look for new E-Maj secondary schemas to create
    FOR r_schema IN
      SELECT DISTINCT v_schemaPrefix || grpdef_log_schema_suffix AS log_schema FROM emaj.emaj_group_def
        WHERE grpdef_group = v_groupName
          AND grpdef_log_schema_suffix IS NOT NULL AND grpdef_log_schema_suffix <> ''
      EXCEPT
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
      ORDER BY 1
      LOOP
-- create the schema
      PERFORM emaj._create_log_schema(r_schema.log_schema);
-- and record the schema creation in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('CREATE_GROUP','SCHEMA CREATED',quote_ident(r_schema.log_schema));
    END LOOP;
-- define the default tablespace, NULL if tspemaj tablespace doesn't exist
    SELECT 'tspemaj' INTO v_defTsp FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
-- get and process all tables of the group (in priority order, NULLS being processed last)
    FOR r_grpdef IN
        SELECT emaj.emaj_group_def.*
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_group = v_groupName
            AND relnamespace = pg_namespace.oid
            AND nspname = grpdef_schema AND relname = grpdef_tblseq
            AND relkind = 'r'
          ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
        LOOP
      PERFORM emaj._create_tbl(r_grpdef, v_groupName, v_isRollbackable, v_defTsp);
      v_nbTbl = v_nbTbl + 1;
    END LOOP;
-- get and process all sequences of the group (in priority order, NULLS being processed last)
    FOR r_grpdef IN
        SELECT emaj.emaj_group_def.*
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_group = v_groupName
            AND relnamespace = pg_namespace.oid
            AND nspname = grpdef_schema AND relname = grpdef_tblseq
            AND relkind = 'S'
          ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
        LOOP
      PERFORM emaj._create_seq(r_grpdef, v_groupName);
      v_nbSeq = v_nbSeq + 1;
    END LOOP;
-- update tables and sequences counters in the emaj_group table
    UPDATE emaj.emaj_group SET group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
      WHERE group_name = v_groupName;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_groups (array[v_groupName]);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'END', v_groupName, v_nbTbl + v_nbSeq || ' tables/sequences processed');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT, BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- purge the emaj history, if needed
    PERFORM emaj._purge_hist();
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group is recorded in emaj_group table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_start_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... and is not in LOGGING state
      IF v_groupIsLogging THEN
        RAISE EXCEPTION '_start_group: The group % cannot be started because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_groupNames[v_i];
      END IF;
    END LOOP;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- for each group,
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      if v_resetLog THEN
-- ... if requested by the user, call the emaj_reset_group function to erase remaining traces from previous logs
        SELECT emaj._reset_group(v_groupNames[v_i]) INTO v_nbTb;
      END IF;
-- ... and check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(array[v_groupNames[v_i]]);
    END LOOP;
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
-- OK, lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
--   the requested lock level is based on the lock level of the future ALTER TABLE, which depends on the postgres version.
    IF v_pgVersion >= 905 THEN
      PERFORM emaj._lock_groups(v_groupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
    ELSE
      PERFORM emaj._lock_groups(v_groupNames,'ACCESS EXCLUSIVE',v_multiGroup);
    END IF;
-- enable all log triggers for the groups
    v_nbTb = 0;
-- for each relation of the group,
    FOR r_tblsq IN
       SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
         WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
       LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
        v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER emaj_log_trg';
        IF v_pgVersion >= 804 THEN
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER emaj_trunc_trg';
        END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- update the state of the group row from the emaj_group table
    UPDATE emaj.emaj_group SET group_is_logging = TRUE WHERE group_name = ANY (v_groupNames);
-- Set the first mark for each group
    PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true);
--
    RETURN v_nbTb;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_validGroupNames        TEXT[];
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- for each group of the array,
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_stop_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE WARNING '_stop_group: Group % cannot be stopped because it is not in LOGGING state.', v_groupNames[v_i];
      ELSE
-- ... if OK, add the group into the array of groups to process
        v_validGroupNames = v_validGroupNames || array[v_groupNames[v_i]];
      END IF;
    END LOOP;
-- check and process the supplied mark name (except if the function is called by emaj_force_stop_group())
    IF NOT v_isForced THEN
      SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
    END IF;
--
    IF v_validGroupNames IS NOT NULL THEN
-- OK (no error detected and at least one group in logging state)
-- lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
--   the requested lock level is based on the lock level of the future ALTER TABLE, which depends on the postgres version.
    IF v_pgVersion >= 905 THEN
      PERFORM emaj._lock_groups(v_validGroupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
    ELSE
      PERFORM emaj._lock_groups(v_validGroupNames,'ACCESS EXCLUSIVE',v_multiGroup);
    END IF;
-- for each relation of the groups to process,
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_validGroupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
          LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, disable the emaj log and truncate triggers
--   errors are captured so that emaj_force_stop_group() can be silently executed
          v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          BEGIN
            EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_log_trg';
          EXCEPTION
            WHEN invalid_schema_name THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
              ELSE
                RAISE EXCEPTION '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
              END IF;
            WHEN undefined_table THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Table % does not exist any more.', v_fullTableName;
              ELSE
                RAISE EXCEPTION '_stop_group: Table % does not exist any more.', v_fullTableName;
              END IF;
            WHEN undefined_object THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Trigger "emaj_log_trg" on table % does not exist any more.', v_fullTableName;
              ELSE
                RAISE EXCEPTION '_stop_group: Trigger "emaj_log_trg" on table % does not exist any more.', v_fullTableName;
              END IF;
          END;
          IF v_pgVersion >= 804 THEN
            BEGIN
              EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_trunc_trg';
            EXCEPTION
              WHEN invalid_schema_name THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
                ELSE
                  RAISE EXCEPTION '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
                END IF;
              WHEN undefined_table THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Table % does not exist any more.', v_fullTableName;
                ELSE
                  RAISE EXCEPTION '_stop_group: Table % does not exist any more.', v_fullTableName;
                END IF;
              WHEN undefined_object THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Trigger "emaj_trunc_trg" on table % does not exist any more.', v_fullTableName;
                ELSE
                  RAISE EXCEPTION '_stop_group: Trigger "emaj_trunc_trg" on table % does not exist any more.', v_fullTableName;
                END IF;
            END;
          END IF;
          ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
        END IF;
        v_nbTb = v_nbTb + 1;
      END LOOP;
      IF NOT v_isForced THEN
-- if the function is not called by emaj_force_stop_group(), set the stop mark for each group
        PERFORM emaj._set_mark_groups(v_validGroupNames, v_markName, v_multiGroup, true);
-- and set the number of log rows to 0 for these marks
        UPDATE emaj.emaj_mark m SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (v_validGroupNames)
            AND (mark_group, mark_id) IN                        -- select only last mark of each concerned group
                (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
                 WHERE mark_group = ANY (v_validGroupNames) AND NOT mark_is_deleted GROUP BY mark_group);
      END IF;
-- set all marks for the groups from the emaj_mark table as 'DELETED' to avoid any further rollback and remove protection if any
      UPDATE emaj.emaj_mark SET mark_is_deleted = TRUE, mark_is_rlbk_protected = FALSE
        WHERE mark_group = ANY (v_validGroupNames) AND NOT mark_is_deleted;
-- update the state of the groups rows from the emaj_group table (the rollback protection of rollbackable groups is reset)
      UPDATE emaj.emaj_group SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (v_validGroupNames);
    END IF;
    RETURN v_nbTb;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_group$
-- This function sets a protection on a group against accidental rollback.
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
      RAISE EXCEPTION 'emaj_protect_group: The group % cannot be protected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_protect_group: The group % cannot be protected because it is not in LOGGING state.', v_groupName;
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

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_group$
-- This function unsets a protection on a group against accidental rollback.
-- Input: group name
-- Output: 1 if successful, 0 if the group was not already in protected state
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
      RAISE EXCEPTION 'emaj_unprotect_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_unprotect_group: The group % cannot be unprotected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- OK, unset the protection
    IF NOT v_groupIsProtected THEN
      v_status = 0;
    ELSE
      UPDATE emaj.emaj_group SET group_is_rlbk_protected = FALSE WHERE group_name = v_groupName;
      v_status = 1;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_GROUP', v_groupName, 'Status ' || v_status);
    RETURN v_status;
  END;
$emaj_unprotect_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_group(TEXT) IS
$$Unsets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN)
RETURNS int LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into all log tables between this previous mark and the new mark.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like functions that start or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_nbTb                   INT = 0;
    v_timestamp              TIMESTAMPTZ;
    v_lastSequenceId         BIGINT;
    v_lastSeqHoleId          BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- if requested, record the set mark begin in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_mark);
    END IF;
-- look at the clock to get the 'official' timestamp representing the mark
    v_timestamp = clock_timestamp();
-- process sequences as early as possible (no lock protect them from other transactions activity)
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- for each sequence of the groups, record the sequence parameters into the emaj_sequence table
      v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
               quote_literal(r_tblsq.rel_tblseq) || ', ' || quote_literal(v_timestamp) ||
               ', ' || quote_literal(v_mark) || ', last_value, ';
      IF v_pgVersion <= 803 THEN
         v_stmt = v_stmt || '0, ';
      ELSE
         v_stmt = v_stmt || 'start_value, ';
      END IF;
      v_stmt = v_stmt ||
               'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the number of log rows for the old last mark of each group
--   the statement returns no row in case of emaj_start_group(s)
    UPDATE emaj.emaj_mark m SET mark_log_rows_before_next =
      coalesce( (SELECT sum(stat_rows) FROM emaj.emaj_log_stat_group(m.mark_group,'EMAJ_LAST_MARK',NULL)) ,0)
      WHERE mark_group = ANY (v_groupNames)
        AND (mark_group, mark_id) IN                   -- select only the last non deleted mark of each concerned group
            (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
             WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
-- for each table of the groups, ...
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_log_sequence FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- ... record the associated sequence parameters in the emaj sequence table
      v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT '|| quote_literal(r_tblsq.rel_log_schema) || ', ' || quote_literal(r_tblsq.rel_log_sequence) || ', ' ||
               quote_literal(v_timestamp) || ', ' || quote_literal(v_mark) || ', ' || 'last_value, ';
      IF v_pgVersion <= 803 THEN
         v_stmt = v_stmt || '0, ';
      ELSE
         v_stmt = v_stmt || 'start_value, ';
      END IF;
      v_stmt = v_stmt ||
               'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_sequence);
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the marks
-- get the last id for emaj_sequence and emaj_seq_hole tables, and the last value for emaj_global_seq
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSequenceId
      FROM emaj.emaj_sequence_sequ_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSeqHoleId
      FROM emaj.emaj_seq_hole_sqhl_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastGlobalSeq
      FROM emaj.emaj_global_seq;
-- insert the marks into the emaj_mark table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_datetime, mark_global_seq, mark_is_deleted, mark_is_rlbk_protected,
                                  mark_last_sequence_id, mark_last_seq_hole_id)
        VALUES (v_groupNames[v_i], v_mark, v_timestamp, v_lastGlobalSeq, FALSE, FALSE, v_lastSequenceId, v_lastSeqHoleId);
    END LOOP;
-- before exiting, cleanup the state of the pending rollback events from the emaj_rlbk table
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
-- ... either through dblink if we are currently performing a rollback with a dblink connection already opened
--     this is mandatory to avoid deadlock
      PERFORM 0 FROM dblink('rlbk#1','SELECT emaj.emaj_cleanup_rollback_state()') AS (dummy INT);
    ELSE
-- ... or directly
      PERFORM emaj.emaj_cleanup_rollback_state();
    END IF;
-- if requested, record the set mark end in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_mark);
    END IF;
--
    RETURN v_nbTb;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_mark_group$
-- This function sets a protection on a mark for a group against accidental rollback.
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
      RAISE EXCEPTION 'emaj_protect_mark_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: A mark on the group % cannot be protected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: A mark on the group % cannot be protected because it is not in LOGGING state.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: mark % doesn''t exist for group %.', v_mark, v_groupName;
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

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_mark_group$
-- This function unsets a protection on a mark for a group against accidental rollback.
-- Input: group name, mark to unprotect
-- Output: 1 if successful, 0 if the mark was already in unprotected state
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
      RAISE EXCEPTION 'emaj_unprotect_mark_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_unprotect_mark_group: A mark on the group % cannot be unprotected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_unprotect_mark_group: mark % doesn''t exist for group %.', v_mark, v_groupName;
    END IF;
-- OK, set the protection
    SELECT mark_is_rlbk_protected INTO v_markIsProtected FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
    IF NOT v_markIsProtected THEN
      v_status = 0;
    ELSE
      UPDATE emaj.emaj_mark SET mark_is_rlbk_protected = FALSE WHERE mark_group = v_groupName AND mark_name = v_realMark;
      v_status = 1;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_MARK_GROUP', v_groupName, 'Mark ' || v_realMark || ' ; status ' || v_status);
    RETURN v_status;
  END;
$emaj_unprotect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_mark_group(TEXT,TEXT) IS
$$Unsets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps.
  DECLARE
    v_markName               TEXT;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_startDateTime          TIMESTAMPTZ;
    v_stmt                   TEXT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsable         BOOLEAN = false;
    v_rlbkId                 INT;
    v_timestampMark          TIMESTAMPTZ;
    v_msg                    TEXT;
  BEGIN
-- lock the groups to rollback
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = ANY(v_groupNames) FOR UPDATE;
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames, v_mark) INTO v_markName;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- get the mark timestamp for the 1st group (as we know this timestamp is the same for all groups of the array)
    SELECT emaj._get_mark_datetime(v_groupNames[1], v_mark) INTO v_timestampMark;
-- insert begin in the history
    IF v_isLoggedRlbk THEN
      v_msg = 'Logged';
    ELSE
      v_msg = 'Unlogged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
              array_to_string(v_groupNames,','),
              v_msg || ' rollback to mark ' || v_markName || ' [' || v_timestampMark || ']'
             ) RETURNING hist_id, hist_datetime INTO v_histId, v_startDateTime;
-- get the total number of tables for these groups
    SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
      FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- first try to open a dblink connection
    SELECT emaj._dblink_open_cnx('rlbk#1') INTO v_dbLinkCnxStatus;
    v_isDblinkUsable = (v_dbLinkCnxStatus >= 0);
-- for parallel rollback (nb sessions > 1) the dblink connection must be ok
    IF v_nbSession > 1 AND NOT v_isDblinkUsable THEN
      RAISE EXCEPTION '_rlbk_init: cannot use several sessions without dblink connection capability. (Status of the dblink connection attempt = % - see E-Maj documentation)', v_dbLinkCnxStatus;
    END IF;
-- create the row representing the rollback event in the emaj_rlbk table and get the rollback id back
    v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, ' ||
             'rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
             'rlbk_is_dblink_used, rlbk_start_datetime ) ' ||
             'VALUES (' || quote_literal(v_groupNames) || ',' || quote_literal(v_markName) || ',' ||
             quote_literal(v_timestampMark) || ',' || v_isLoggedRlbk || ',' || v_nbSession || ',' ||
             v_nbTblInGroups || ',' || v_nbSeqInGroups || ', ''PLANNING'',' || v_histId || ',' ||
             v_isDblinkUsable || ',' || quote_literal(v_startDateTime) || ') RETURNING rlbk_id';
    IF v_isDblinkUsable THEN
-- insert a rollback event into the emaj_rlbk table ... either through dblink if possible
      SELECT rlbk_id INTO v_rlbkId FROM dblink('rlbk#1',v_stmt) AS (rlbk_id INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt INTO v_rlbkId;
    END IF;
-- issue warnings in case of foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(v_groupNames);
-- call the rollback planning function to define all the elementary steps to perform,
-- compute their estimated duration and attribute steps to sessions
    v_stmt = 'SELECT emaj._rlbk_planning(' || v_rlbkId || ')';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible (do not try to open a connection, it has already been attempted)
      SELECT eff_nb_table FROM dblink('rlbk#1',v_stmt) AS (eff_nb_table INT) INTO v_effNbTable;
    ELSE
-- ... or directly
      EXECUTE v_stmt INTO v_effNbTable;
    END IF;
-- update the emaj_rlbk table to set the real number of tables to process and adjust the rollback status
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_table = ' || v_effNbTable ||
             ', rlbk_status = ''LOCKING'' ' || ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_rlbk_check$
-- This functions performs checks on group names and mark names supplied as parameter for the emaj_rollback_groups()
-- and emaj_estimate_rollback_groups() functions.
-- It returns the real mark name.
  DECLARE
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_groupIsProtected       BOOLEAN;
    v_groupIsRollbackable    BOOLEAN;
    v_markName               TEXT;
    v_markId                 BIGINT;
    v_markIsDeleted          BOOLEAN;
    v_protectedMarkList      TEXT;
    v_cpt                    INT;
    r_mark                   RECORD;
  BEGIN
-- check that each group ...
-- ...is recorded in emaj_group table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_is_logging, group_is_rollbackable, group_is_rlbk_protected INTO v_groupIsLogging, v_groupIsRollbackable, v_groupIsProtected
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i];
      IF NOT FOUND THEN
        RAISE EXCEPTION '_rlbk_check: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION '_rlbk_check: Group % is not in LOGGING state.', v_groupNames[v_i];
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_groupIsRollbackable THEN
        RAISE EXCEPTION '_rlbk_check: Group % has been created for audit only purpose.', v_groupNames[v_i];
      END IF;
-- ... is not protected against rollback
      IF v_groupIsProtected THEN
        RAISE EXCEPTION '_rlbk_check: Group % is currently protected against rollback.', v_groupNames[v_i];
      END IF;
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_check: No mark % exists for group %.', v_mark, v_groupNames[v_i];
      END IF;
-- ... and this mark is ACTIVE
      SELECT mark_id, mark_is_deleted INTO v_markId, v_markIsDeleted FROM emaj.emaj_mark
        WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
      IF v_markIsDeleted THEN
        RAISE EXCEPTION '_rlbk_check: mark % for group % is not usable for rollback.', v_markName, v_groupNames[v_i];
      END IF;
-- ... and the rollback wouldn't delete protected marks
--TODO : use aggregate when 8.3 will not be supported anymore
      v_protectedMarkList = '';
      FOR r_mark IN
        SELECT mark_name FROM emaj.emaj_mark WHERE mark_group = v_groupNames[v_i] AND mark_id > v_markId AND mark_is_rlbk_protected
        LOOP
        IF v_protectedMarkList = '' THEN
          v_protectedMarkList = r_mark.mark_name;
        ELSE
          v_protectedMarkList = v_protectedMarkList || ', ' || r_mark.mark_name;
        END IF;
      END LOOP;
      IF v_protectedMarkList <> '' THEN
        RAISE EXCEPTION '_rlbk_check: protected marks (%) for group % block the rollback to mark %.', v_protectedMarkList, v_groupNames[v_i], v_markName;
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_check: Mark % does not represent the same point in time for all groups.', v_mark;
    END IF;
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- The function returns the effective number of processed tables.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it doesn't own the application tables.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_effNbTable             INT = 0;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_timestampMark          TIMESTAMPTZ;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_lastGlobalSeq          BIGINT;
    v_lastSequenceId         BIGINT;
    v_lastSeqHoleId          BIGINT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
      v_isDblinkUsable = true;
    END IF;
-- get the rollack characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session
      INTO v_groupNames, v_mark, v_timestampMark, v_isLoggedRlbk, v_nbSession
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- fetch the last global sequence and the last id values of emaj_sequence and emaj_seq_hole tables at set mark time
    SELECT mark_global_seq, mark_last_sequence_id, mark_last_seq_hole_id
      INTO v_lastGlobalSeq, v_lastSequenceId, v_lastSeqHoleId FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- scan emaj_rlbp_plan to get all steps to process that have been affected to this session, in batch_number and step order
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_fkey_def
        FROM emaj.emaj_rlbk_plan,
             (VALUES ('DIS_LOG_TRG',1),('DROP_FK',2),('SET_FK_DEF',3),('RLBK_TABLE',4),
                     ('DELETE_LOG',5),('SET_FK_IMM',6),('ADD_FK',7),('ENA_LOG_TRG',8)) AS step(step_name, step_order)
        WHERE rlbp_step::text = step.step_name
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
      IF r_step.rlbp_step = 'DIS_LOG_TRG' THEN
-- process a log trigger disable
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' DISABLE TRIGGER emaj_log_trg';
      ELSEIF r_step.rlbp_step = 'DROP_FK' THEN
-- process a foreign key deletion
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' DROP CONSTRAINT ' || quote_ident(r_step.rlbp_fkey);
      ELSEIF r_step.rlbp_step = 'SET_FK_DEF' THEN
-- set a foreign key deferred
        EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                ' DEFERRED';
      ELSEIF r_step.rlbp_step = 'RLBK_TABLE' THEN
-- process a table rollback
        SELECT emaj._rlbk_tbl(emaj_relation.*, v_lastGlobalSeq, v_nbSession) INTO v_nbRows
          FROM emaj.emaj_relation
          WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
        v_effNbTable = v_effNbTable + 1;
      ELSEIF r_step.rlbp_step = 'DELETE_LOG' THEN
-- process the deletion of log rows
        SELECT emaj._delete_log_tbl(emaj_relation.*, v_timestampMark, v_lastGlobalSeq, v_lastSequenceId, v_lastSeqHoleId) INTO v_nbRows
          FROM emaj.emaj_relation
          WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
      ELSEIF r_step.rlbp_step = 'SET_FK_IMM' THEN
-- set a foreign key immediate
        EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                ' IMMEDIATE';
      ELSEIF r_step.rlbp_step = 'ADD_FK' THEN
-- process a foreign key creation
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' ADD CONSTRAINT ' || quote_ident(r_step.rlbp_fkey) || ' ' || r_step.rlbp_fkey_def;
      ELSEIF r_step.rlbp_step = 'ENA_LOG_TRG' THEN
-- process a log trigger enable
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' ENABLE TRIGGER emaj_log_trg';
      END IF;
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
    RETURN v_effNbTable;
-- TODO: trap and record exception during the rollback operation when pg 8.3 will not be supported any more
--  EXCEPTION
--    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
--      RAISE;
--    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
--      PERFORM emaj._rlbk_error(v_rlbkId, SQLERRM, 'rlbk#'||v_session);
--      RAISE;
  END;
$_rlbk_session_exec$;

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
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames, v_mark) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::int INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session)
        VALUES (v_rlbkId, v_groupNames, v_mark, v_isLoggedRlbk, 1);
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
    v_pgVersion              INT = emaj._pg_version_num();
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    r_col                    RECORD;
    v_colList                TEXT;
    v_fileName               TEXT;
    v_stmt                   TEXT;
    v_seqCol                 TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: group % has not been created.', v_groupName;
    END IF;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_group: directory parameter cannot be NULL';
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: invalid COPY options parameter format';
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   first build the order by column list
        v_colList = '';
        PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
          WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
                contype = 'p' AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
        IF FOUND THEN
--   the table has a pkey,
          FOR r_col IN
              SELECT attname FROM pg_catalog.pg_attribute, pg_catalog.pg_index
                WHERE pg_attribute.attrelid = pg_index.indrelid
                  AND attnum = ANY (indkey)
                  AND indrelid = v_fullTableName::regclass AND indisprimary
                  AND attnum > 0 AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList = quote_ident(r_col.attname);
            ELSE
               v_colList = v_colList || ',' || quote_ident(r_col.attname);
            END IF;
          END LOOP;
        ELSE
--   the table has no pkey
          FOR r_col IN
              SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList = quote_ident(r_col.attname);
            ELSE
               v_colList = v_colList || ',' || quote_ident(r_col.attname);
            END IF;
          END LOOP;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO '
                || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, the statement has no order by
        IF v_pgVersion <= 803 THEN
          v_seqCol = 'sequence_name, last_value, 0, increment_by, max_value, min_value, cache_value, is_cycled, is_called';
        ELSE
          v_seqCol = 'sequence_name, last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called';
        END IF;
        v_stmt= 'COPY (SELECT ' || v_seqCol || ' FROM ' || v_fullTableName || ') TO '
                || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
      END IF;
-- and finaly perform the COPY
--    raise notice 'emaj_snap_group: Executing %',v_stmt;
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
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no
--   end mark is specified. They contain one row per sequence.
-- To do its job, the function performs COPY TO statement, using the options provided by the caller.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before
-- emaj_snap_log_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name, the 2 mark names defining a range, the absolute pathname of the directory where the files are to be created, options for COPY TO statements
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string can be used as last_mark indicating the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_logTableName           TEXT;
    v_fileName               TEXT;
    v_stmt                   TEXT;
    v_timestamp              TIMESTAMPTZ;
    v_pseudoMark             TEXT;
    v_fullSeqName            TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'BEGIN', v_groupName,
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_log_group: group % has not been created.', v_groupName;
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%'  THEN
      RAISE EXCEPTION 'emaj_snap_log_group: invalid COPY options parameter format';
    END IF;
-- catch the global sequence value and the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the global sequence value and the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_snap_log_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_firstMarkId, v_firstEmajGid, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    ELSE
      SELECT mark_name, mark_id, mark_global_seq, mark_datetime INTO v_realFirstMark, v_firstMarkId, v_firstEmajGid, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    END IF;
-- catch the global sequence value and timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the global sequence value and the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_snap_log_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_lastMarkId, v_lastEmajGid, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    ELSE
      v_lastMarkId = NULL;
      v_lastEmajGid = NULL;
      v_tsLastMark = NULL;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_snap_log_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_log_group: directory parameter cannot be NULL';
    END IF;
-- process all log tables of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_log_schema, rel_log_table FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- process tables
--   build names
        v_fileName     = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log.snap';
        v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table);
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE TRUE';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
          v_stmt = v_stmt || ' AND emaj_gid > '|| v_firstEmajGid;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
          v_stmt = v_stmt || ' AND emaj_gid <= '|| v_lastEmajGid;
        END IF;
        v_stmt = v_stmt || ' ORDER BY emaj_gid ASC) TO ' || quote_literal(v_fileName) || ' '
                        || coalesce (v_copyOptions, '');
-- and finaly perform the COPY
        EXECUTE v_stmt;
      END IF;
-- for sequences, just adjust the counter
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realFirstMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realFirstMark) || ' AND ' ||
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
            coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- generate the file for sequences state at end mark, if specified
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realLastMark;
      v_stmt= 'COPY (SELECT emaj_sequence.*' ||
              ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_realLastMark) || ' AND ' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
              ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
              coalesce (v_copyOptions, '');
      EXECUTE v_stmt;
    ELSE
-- generate the file for sequences in their current state, if no end_mark is specified,
--   by using emaj_sequence table to create temporary rows as if a mark had been set
-- look at the clock to get the 'official' timestamp representing this point in time
--   and build a pseudo mark name with it
      v_timestamp = clock_timestamp();
      v_pseudoMark = to_char(v_timestamp,'HH24.MI.SS.MS');
-- for each sequence of the groups, ...
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
            WHERE rel_group = v_groupName AND rel_kind = 'S' ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- ... temporary record the sequence parameters in the emaj sequence table
        v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
                 quote_literal(r_tblsq.rel_tblseq) || ', ' || quote_literal(v_timestamp) ||
                 ', ' || quote_literal(v_pseudoMark) || ', last_value, ';
        IF v_pgVersion <= 803 THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt ||
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
        EXECUTE v_stmt;
      END LOOP;
-- generate the file for sequences current state
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || to_char(v_timestamp,'HH24.MI.SS.MS');
      v_stmt= 'COPY (SELECT emaj_sequence.*' ||
              ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_pseudoMark) || ' AND ' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
              ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
              coalesce (v_copyOptions, '');
      EXECUTE v_stmt;
-- delete sequences state that have just been inserted into the emaj_sequence table.
      EXECUTE 'DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_pseudoMark) || ' AND' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq';
    END IF;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj log tables snap of group ' || v_groupName ||
            ' between marks ' || v_realFirstMark || ' and ' ||
            coalesce(v_realLastMark,'current state') || ' at ' || transaction_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO') || ' ' || coalesce (v_copyOptions, '');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
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
    v_pgVersion              INT = emaj._pg_version_num();
    v_groupIsLogging         BOOLEAN;
    v_cpt                    INT;
    v_firstMarkCopy          TEXT = v_firstMark;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_firstLastSeqHoleId     BIGINT;
    v_lastLastSeqHoleId      BIGINT;
    v_tblseqErr              TEXT;
    v_nbSQL                  INT;
    v_nbSeq                  INT;
    v_cumNbSQL               INT = 0;
    v_fullSeqName            TEXT;
    v_endComment             TEXT;
    v_conditions             TEXT;
    v_rqSeq                  TEXT;
    r_tblsq                  RECORD;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group ...
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
-- ...is recorded into the emaj_group table
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i];
      IF NOT FOUND THEN
        RAISE EXCEPTION '_gen_sql_groups: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... has no tables without pkey
      SELECT count(*) INTO v_cpt FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_relation
        WHERE relnamespace = pg_namespace.oid
          AND nspname = rel_schema AND relname =  rel_tblseq
          AND rel_group = v_groupNames[v_i] AND rel_kind = 'r'
          AND relhaspkey = false;
      IF v_cpt > 0 THEN
        RAISE EXCEPTION '_gen_sql_groups: Tables group % contains % tables without pkey.', v_groupNames[v_i], v_cpt;
      END IF;
-- If the first mark supplied is NULL or empty, get the first mark for the current processed group
--   (in fact the first one) and override the supplied first mark
      IF v_firstMarkCopy IS NULL OR v_firstMarkCopy = '' THEN
        SELECT mark_name INTO v_firstMarkCopy
          FROM emaj.emaj_mark WHERE mark_group = v_groupNames[v_i] ORDER BY mark_id LIMIT 1;
        IF NOT FOUND THEN
           RAISE EXCEPTION '_gen_sql_groups: No initial mark can be found for group %.', v_groupNames[v_i];
        END IF;
      END IF;
-- ... owns the requested first mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_firstMarkCopy) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: No mark % exists for group %.', v_firstMarkCopy, v_groupNames[v_i];
      END IF;
-- ... and owns the requested last mark, if supplied
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
        SELECT emaj._get_mark_name(v_groupNames[v_i],v_lastMark) INTO v_realLastMark;
        IF v_realLastMark IS NULL THEN
          RAISE EXCEPTION '_gen_sql_groups: No mark % exists for group %.', v_lastMark, v_groupNames[v_i];
        END IF;
      END IF;
    END LOOP;
-- check that the first mark timestamp is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_firstMarkCopy)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_gen_sql_groups: Mark % does not represent the same point in time for all groups.', v_firstMarkCopy;
    END IF;
-- check that the last mark timestamp, if supplied, is the same for all groups of the array
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_lastMark)) INTO v_cpt FROM emaj.emaj_group
        WHERE group_name = ANY (v_groupNames);
      IF v_cpt > 1 THEN
        RAISE EXCEPTION '_gen_sql_groups: Mark % does not represent the same point in time for all groups.', v_lastMark;
      END IF;
    END IF;
-- retrieve the name, the global sequence value and the timestamp of the supplied first mark for the 1st group
--   (the global sequence value and the timestamp are the same for all groups of the array
--    and the mark ids are just used to check that first mark is prior the last mark)
    SELECT mark_id, mark_global_seq, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_firstEmajGid, v_tsFirstMark, v_firstLastSeqHoleId
      FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_realFirstMark;
-- if last mark is NULL or empty, there is no timestamp to register
    IF v_lastMark IS NULL OR v_lastMark = '' THEN
      v_lastMarkId = NULL;
      v_lastEmajGid = NULL;
      v_tsLastMark = NULL;
      v_lastLastSeqHoleId = NULL;
    ELSE
-- else, retrieve the name, timestamp and last sequ_hole id of the supplied end mark for the 1st group
      SELECT mark_id, mark_global_seq, mark_datetime, mark_last_seq_hole_id INTO v_lastMarkId, v_lastEmajGid, v_tsLastMark, v_lastLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION '_gen_sql_groups: mark id for % (% = %) is greater than mark id for % (% = %).', v_firstMarkCopy, v_firstMarkId, v_tsFirstMark, v_lastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- check the array of tables and sequences to filter, if supplied.
-- each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups
    IF v_tblseqs IS NOT NULL THEN
      IF v_tblseqs = array[''] THEN
        RAISE EXCEPTION '_gen_sql_groups: filtered table/sequence names array cannot be empty.';
      END IF;
      v_tblseqErr = '';
      FOR r_tblsq IN
        SELECT t FROM regexp_split_to_table(array_to_string(v_tblseqs,'?'),E'\\?') AS t
          EXCEPT
        SELECT rel_schema || '.' || rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames)
-- TODO regexp_split_to_table(array_to_string) to be transformed into unnest() when pg 8.3 will not be supported any more
        LOOP
        v_tblseqErr = v_tblseqErr || r_tblsq.t || ', ';
      END LOOP;
      IF v_tblseqErr <> '' THEN
        v_tblseqErr = substring(v_tblseqErr FROM 1 FOR char_length(v_tblseqErr) - 2);
        RAISE EXCEPTION '_gen_sql_groups: some tables and/or sequences (%) do not belong to any of the selected tables groups.', v_tblseqErr;
      END IF;
    END IF;
-- test the supplied output file name by inserting a temporary line (trap NULL or bad file name)
    BEGIN
      EXECUTE 'COPY (SELECT ''-- _gen_sql_groups() function in progress - started at '
                     || statement_timestamp() || ''') TO ' || quote_literal(v_location);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION '_gen_sql_groups: file % cannot be used as script output file.', v_location;
    END;
-- create temporary table
    DROP TABLE IF EXISTS emaj_temp_script;
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
    IF v_tsLastMark IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || v_lastEmajGid;
    END IF;
    FOR r_rel IN
        SELECT * FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                        -- tables of the groups
            AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs)) -- filtered or not by the user
                                                                               -- only tables having updates to process
            AND emaj._log_stat_tbl(emaj_relation, v_tsFirstMark, v_tsLastMark, v_firstLastSeqHoleId, v_lastLastSeqHoleId) > 0
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- process the application table, by calling the _gen_sql_tbl function
      SELECT emaj._gen_sql_tbl(r_rel, v_conditions) INTO v_nbSQL;
      v_cumNbSQL = v_cumNbSQL + v_nbSQL;
    END LOOP;
-- process sequences
    v_nbSeq = 0;
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'                        -- sequences of the groups
            AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs)) -- filtered or not by the user
          ORDER BY rel_priority DESC, rel_schema DESC, rel_tblseq DESC
        LOOP
      v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF v_tsLastMark IS NULL THEN
-- no supplied last mark, so get current sequence characteritics
        IF v_pgVersion <= 803 THEN
-- .. in pg 8.3-
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM ' || v_fullSeqName INTO v_rqSeq;
--raise notice '1 - sequence % -> %',v_fullSeqName,v_rqSeq;
        ELSE
-- .. in pg 8.4+
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM ' || v_fullSeqName INTO v_rqSeq;
--raise notice '2 - sequence % -> %',v_fullSeqName,v_rqSeq;
        END IF;
      ELSE
-- a last mark is supplied, so get sequence characteristics from emaj_sequence table
        IF v_pgVersion <= 803 THEN
-- .. in pg 8.3-
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_datetime = ''' || v_tsLastMark || '''' INTO v_rqSeq;
--raise notice '3 - sequence % -> %',v_fullSeqName,v_rqSeq;
        ELSE
-- .. in pg 8.4+
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_datetime = ''' || v_tsLastMark || '''' INTO v_rqSeq;
--raise notice '4 - sequence % -> %',v_fullSeqName,v_rqSeq;
        END IF;
      END IF;
-- insert into temp table
      v_nbSeq = v_nbSeq + 1;
      EXECUTE 'INSERT INTO emaj_temp_script '
           || 'SELECT NULL, -1 * ' || v_nbSeq || ', txid_current(), ' || quote_literal(v_rqSeq);
    END LOOP;
-- add initial comments
    IF v_tsLastMark IS NOT NULL THEN
      v_endComment = ' and mark ' || v_realLastMark;
    ELSE
      v_endComment = ' and the current situation';
    END IF;
    INSERT INTO emaj_temp_script SELECT 0, 1, 0, '-- SQL script generated by E-Maj at ' || statement_timestamp();
    INSERT INTO emaj_temp_script SELECT 0, 2, 0, '--    for tables group(s): ' || array_to_string(v_groupNames,',');
    INSERT INTO emaj_temp_script SELECT 0, 3, 0, '--    processing logs between mark ' || v_realFirstMark || v_endComment;
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
-- drop temporary table ?
--    DROP TABLE IF EXISTS emaj_temp_script;
-- return the number of sql verbs generated into the output file
    v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

------------------------------------
--                                --
-- Global purpose functions       --
--                                --
------------------------------------

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
  BEGIN
-- check the postgres version at creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric.
--          But we suppose these tests will not be useful any more when pg 10.0 will appear!
--   for 8.3, both major versions must be the same
    RETURN QUERY
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE ((v_pgVersion = 803 AND cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                                           to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) <> v_pgVersion)
--   for 8.4+, both major versions must be 8.4+
            OR (v_pgVersion >= 804 AND cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                                            to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804))
        ORDER BY msg;
-- check all application schemas referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'The application schema "' || rel_schema || '" does not exist any more.' AS msg
        FROM (
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
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
            EXCEPT                                    -- minus relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r             -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq
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
        WHERE rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
                 WHERE nspname = rel_log_schema AND proname = rel_log_function
                   AND pronamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log trigger
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
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
--   then truncate trigger if pg 8.4+
    IF v_pgVersion >= 804 THEN
      RETURN QUERY
        SELECT 'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
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
-- TODO : merge both triggers check when pg 8.3 will not be supported any more
    END IF;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM (
        SELECT DISTINCT rel_schema, rel_tblseq,
               'In group "' || rel_group || '", the structure of the application table "' ||
                 rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
               rel_log_schema || '"."' || rel_log_table || '").' AS msg
          FROM (
            (                                           -- application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
                AND rel_kind = 'r'
            EXCEPT                                      -- minus log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_log_table
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
                AND rel_kind = 'r'
            )
            UNION
            (                                           -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_log_table
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
                AND rel_kind = 'r'
            EXCEPT                                      --  minus application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
                AND rel_kind = 'r'
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
-- TODO : use CTE to improve performance, when pg 8.3 will not be supported any more
    RETURN;
  END;
$_verify_all_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_errorFound             BOOLEAN = FALSE;
    r_object                 RECORD;
  BEGIN
-- Global checks
-- detect if the current postgres version is at least 8.2
    IF v_pgVersion < 803 THEN
      RETURN NEXT 'The current postgres version (' || version() || ') is not compatible with E-Maj.';
      v_errorFound = TRUE;
    END IF;
-- check all E-Maj primary and secondary schemas
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_schemas() msg
    LOOP
      RETURN NEXT r_object.msg;
      v_errorFound = TRUE;
    END LOOP;
-- check all groups components
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_groups() msg
    LOOP
      RETURN NEXT r_object.msg;
      v_errorFound = TRUE;
    END LOOP;
-- final message if no error has been yet detected
    IF NOT v_errorFound THEN
      RETURN NEXT 'No error detected';
    END IF;
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

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

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------

-- grants on tables and sequences
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group        TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_relation     TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_mark         TO emaj_adm;
GRANT SELECT ON emaj.emaj_group               TO emaj_viewer;
GRANT SELECT ON emaj.emaj_relation            TO emaj_viewer;
GRANT SELECT ON emaj.emaj_mark                TO emaj_viewer;

GRANT ALL ON SEQUENCE emaj.emaj_mark_mark_id_seq     TO emaj_adm;
GRANT SELECT ON SEQUENCE emaj.emaj_mark_mark_id_seq     TO emaj_viewer;

-- revoke grants on all functions from PUBLIC
REVOKE ALL ON FUNCTION emaj.emaj_protect_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_unprotect_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_protect_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_unprotect_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;

REVOKE ALL ON FUNCTION emaj._drop_tbl(emaj.emaj_relation) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_seq(emaj.emaj_relation) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_tbl(emaj.emaj_relation,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_log_tbl(emaj.emaj_relation,timestamp with time zone,bigint,bigint,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_seq(emaj.emaj_relation,timestamp with time zone,boolean,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._log_stat_tbl(emaj.emaj_relation,timestamp with time zone,timestamp with time zone,bigint,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._gen_sql_tbl(emaj.emaj_relation,text) FROM PUBLIC;

-- give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj.emaj_protect_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_unprotect_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_protect_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_unprotect_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;

GRANT EXECUTE ON FUNCTION emaj._drop_tbl(emaj.emaj_relation) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_seq(emaj.emaj_relation) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_tbl(emaj.emaj_relation,bigint,integer) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_log_tbl(emaj.emaj_relation,timestamp with time zone,bigint,bigint,bigint) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_seq(emaj.emaj_relation,timestamp with time zone,boolean,bigint) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(emaj.emaj_relation,timestamp with time zone,timestamp with time zone,bigint,bigint) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_tbl(emaj.emaj_relation,text) TO emaj_adm;

-- give appropriate rights on functions to emaj_viewer role
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(emaj.emaj_relation,timestamp with time zone,timestamp with time zone,bigint,bigint) TO emaj_viewer;

--------------------------------------------------------------
--                                                          --
-- Apply changes to log components:                         --
-- - Rename the log triggers and truncate triggers on all   --
--   application tables known as belonging to a group       --
--                                                          --
--------------------------------------------------------------

CREATE OR REPLACE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_fullTableName          TEXT;
    r_table                  RECORD;
  BEGIN
-- For each application table known in emaj_relation
    FOR r_table IN
      SELECT rel_schema, rel_tblseq, rel_log_trigger, rel_trunc_trigger
        FROM tmp_relation_old WHERE rel_kind = 'r'
      LOOP
      v_fullTableName = quote_ident(r_table.rel_schema) || '.' || quote_ident(r_table.rel_tblseq);
-- ... rename the log trigger
      EXECUTE 'ALTER TRIGGER ' || quote_ident(r_table.rel_log_trigger) || ' ON ' || v_fullTableName || ' RENAME TO emaj_log_trg';
-- ... and rename the truncate trigger if postgres version >= 8.4
      IF v_pgVersion >= 804 THEN
        EXECUTE 'ALTER TRIGGER ' || quote_ident(r_table.rel_trunc_trigger) || ' ON ' || v_fullTableName || ' RENAME TO emaj_trunc_trg';
      END IF;
    END LOOP;
    RETURN;
  END;
$tmp$;
SELECT emaj.tmp();
DROP FUNCTION emaj.tmp();

-- The temporary table containing the old emaj_relation table's content can now be dropped
DROP TABLE tmp_relation_old;

------------------------------------
--                                --
-- commit migration               --
--                                --
------------------------------------

UPDATE emaj.emaj_param SET param_value_text = '1.3.0' WHERE param_key = 'emaj_version';

-- insert the migration record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 1.3.0', 'Migration from 1.2.0 completed');

-- post installation checks
CREATE OR REPLACE FUNCTION emaj._tmp_post_install()
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_mpt                    INTEGER;
    v_schemaList             TEXT;
    r_sch                    RECORD;
  BEGIN
-- check the max_prepared_transactions GUC value
    SELECT setting INTO v_mpt FROM pg_catalog.pg_settings WHERE name = 'max_prepared_transactions';
    IF v_mpt <= 1 THEN
      RAISE WARNING 'E-Maj migration: as the max_prepared_transactions parameter is set to % on this cluster, no parallel rollback is possible.',v_mpt;
    END IF;
-- check the dblink contrib or extension is already installed and give EXECUTE right to emaj_adm on dblink_connect_u functions
--   Scan all schemas where dblink is installed
--   Note dblink may have been installed in several schemas and we don't know at installation time 
--   what will be the search_path of future users.
--   Grants on this particular dblink_connect_u function are revoked by default to public, 
--   but emaj administrators need it to monitor the rollback operations progress.
    v_schemaList = '';
    FOR r_sch IN 
      SELECT DISTINCT nspname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
          WHERE pg_proc.pronamespace = pg_namespace.oid
            AND proname = 'dblink'
      LOOP
      IF v_schemaList = '' THEN
        v_schemaList = r_sch.nspname;
      ELSE
        v_schemaList = v_schemaList || ',' || r_sch.nspname;
      END IF;
      EXECUTE 'GRANT EXECUTE ON FUNCTION ' || 
              quote_ident(r_sch.nspname) || '.dblink_connect_u(text,text) TO emaj_adm';
    END LOOP;
-- return the output message depending whether dblink is installed or not 
    IF v_schemaList = '' THEN
      RAISE WARNING 'E-Maj migration: the dblink contrib/extension is not installed. It is advisable to have the dblink contrib/extension installed in order to take benefit of the E-Maj rollback monitoring and E-Maj parallel rollback capabilities. (You will need to "GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO emaj_adm" once dblink is installed)';
    ELSE
      RAISE WARNING 'E-Maj migration: the dblink functions exist in schema(s): %. Be sure these functions will be accessible by emaj_adm roles through their schemas search_path.',v_schemaList;
    END IF;
    RETURN;
  END;
$tmp$;
SELECT emaj._tmp_post_install();
DROP FUNCTION emaj._tmp_post_install();

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-Maj successfully migrated to 1.3.0'

