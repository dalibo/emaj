--
-- E-Maj: migration from 3.1.0 to <devel>
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
-- the emaj version registered in emaj_param must be '3.1.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '3.1.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 3.1.0',v_emajVersion;
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 3.1.0 started');

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

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(v_cnxName TEXT, OUT v_status INT, OUT v_schema TEXT)
LANGUAGE plpgsql AS
$_dblink_open_cnx$
-- This function tries to open a named dblink connection.
-- It uses as target: the current cluster (port), the current database and a role defined in the emaj_param table.
-- This connection role must be defined in the emaj_param table with a row having:
--   - param_key = 'dblink_user_password',
--   - param_value_text = 'user=<user> password=<password>' with the rules that apply to usual libPQ connect strings.
-- The password can be omited if the connection doesn't require it.
-- The dblink_connect_u is used to open the connection so that emaj_adm but non superuser roles can access the
--    cluster even when no password is required to log on.
-- The function is directly called by Emaj_web.
-- Input:  connection name
-- Output: integer status return.
--           1 successful connection
--           0 already opened connection
--          -1 dblink is not installed
--          -2 dblink functions are not visible for the session (obsolete)
--          -3 dblink functions execution is not granted to the role
--          -4 the transaction isolation level is not READ COMMITTED
--          -5 no 'dblink_user_password' parameter is defined in the emaj_param table
--          -6 error at dblink_connect() call
--         name of the schema that holds the dblink extension (used later to schema qualify all calls to dblink functions)
  DECLARE
    v_nbCnx                  INT;
    v_UserPassword           TEXT;
    v_connectString          TEXT;
  BEGIN
-- look for the schema holding the dblink functions
--   (NULL if the dblink_connect_u function is not available, which should not happen)
    SELECT nspname INTO v_schema FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND proname = 'dblink_connect_u'
      LIMIT 1;
    IF NOT FOUND THEN
      v_status = -1;                      -- dblink is not installed
    ELSIF NOT has_function_privilege(quote_ident(v_schema) || '.dblink_connect_u(text, text)', 'execute') THEN
      v_status = -3;                      -- current role has not the execute rights on dblink functions
    ELSIF substring(v_cnxName FROM 1 FOR 5) = 'rlbk#' AND
          current_setting('transaction_isolation') <> 'read committed' THEN
      v_status = -4;                      -- 'rlbk#*' connection (used for rollbacks) must only come from a
                                          --   READ COMMITTED transaction
    ELSE
      EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                     v_cnxName, v_schema);
      GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
      IF v_nbCnx > 0 THEN
-- dblink is usable, so search the requested connection name in dblink connections list
        v_status = 0;                       -- the requested connection is already open
      ELSE
-- so, get the 'dblink_user_password' parameter if exists, from emaj_param
        SELECT param_value_text INTO v_UserPassword FROM emaj.emaj_param WHERE param_key = 'dblink_user_password';
        IF NOT FOUND THEN
          v_status = -5;                    -- no 'dblink_user_password' parameter is defined in the emaj_param table
        ELSE
-- ... build the connect string
          v_connectString = 'host=localhost port=' || current_setting('port') ||
                            ' dbname=' || current_database() || ' ' || v_userPassword;
-- ... and try to connect
          BEGIN
            EXECUTE format('SELECT %I.dblink_connect_u(%L ,%L)',
                           v_schema, v_cnxName, v_connectString);
            v_status = 1;                 -- the connection is successful
          EXCEPTION
            WHEN OTHERS THEN
              v_status = -6;              -- the connection attempt failed
          END;
        END IF;
      END IF;
    END IF;
-- for connections used for rollback operations, record the dblink connection attempt in the emaj_hist table
    IF substring(v_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('DBLINK_OPEN_CNX', v_cnxName, 'Status = ' || v_status);
    END IF;
    RETURN;
  END;
$_dblink_open_cnx$;

CREATE OR REPLACE FUNCTION emaj._dblink_sql_exec(v_cnxName TEXT, v_stmt TEXT, v_dblinkSchema TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_dblink_sql_exec$
-- This function executes a SQL statement, either through an opened dblink connection when a schema name is provided or directly.
-- It returns a bigint value. Consequently, all SQL statements to execute must return an integer numeric value.
-- Input:  connection name
--         sql statement
--         name of the schema that holds the dblink extension
-- Output: the single return value
  DECLARE
    v_returnValue            BIGINT;
  BEGIN
    IF v_dblinkSchema IS NOT NULL THEN
-- a dblink schema is provided, so the connection name can be used to execute the requested SQL statement
      EXECUTE format('SELECT return_value FROM %I.dblink(%L, %L) AS (return_value BIGINT)',
                     v_dblinkSchema, v_cnxName, v_stmt)
        INTO v_returnValue;
    ELSE
-- the SQL statement has to be directly executed
      EXECUTE v_stmt INTO v_returnValue;
    END IF;
    RETURN v_returnValue;
  END;
$_dblink_sql_exec$;

CREATE OR REPLACE FUNCTION emaj._dblink_close_cnx(v_cnxName TEXT, v_dblinkSchema TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_dblink_close_cnx$
-- This function closes a named dblink connection.
-- The function is directly called by Emaj_web.
-- Input:  connection name
  DECLARE
    v_nbCnx                  INT;
  BEGIN
-- check the dblink connection exists
    EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                   v_cnxName, v_dblinkSchema);
    GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
    IF v_nbCnx > 0 THEN
-- the connection exists, so disconnect
      EXECUTE format('SELECT %I.dblink_disconnect(%L)',
                     v_dblinkSchema, v_cnxName);
-- for connections used for rollback operations, record the dblink disconnection in the emaj_hist table
      IF substring(v_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
        INSERT INTO emaj.emaj_hist (hist_function, hist_object)
          VALUES ('DBLINK_CLOSE_CNX', v_cnxName);
      END IF;
    END IF;
    RETURN;
  END;
$_dblink_close_cnx$;

CREATE OR REPLACE FUNCTION emaj._check_new_mark(v_groupNames TEXT[], v_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_check_new_mark$
-- This function verifies that a new mark name supplied the user is valid.
-- It processes the possible NULL mark value and the replacement of % wild characters.
-- It also checks that the mark name do not already exist for any group.
-- Input: array of group names, name of the mark to set
-- Output: internal name of the mark
  DECLARE
    v_markName               TEXT = v_mark;
    v_groupList              TEXT;
    v_count                  INTEGER;
  BEGIN
-- check the mark name is not 'EMAJ_LAST_MARK'
    IF v_mark = 'EMAJ_LAST_MARK' THEN
       RAISE EXCEPTION '_check_new_mark: "%" is not an allowed name for a new mark.', v_mark;
    END IF;
-- process null or empty supplied mark name
    IF v_markName = '' OR v_markName IS NULL THEN
      v_markName = 'MARK_%';
    END IF;
-- process % wild characters in mark name
    v_markName = replace(v_markName, '%', substring(to_char(clock_timestamp(), 'HH24.MI.SS.US') from 1 for 13));
-- check that the mark does not exist for any groups
    SELECT string_agg(mark_group,', ' ORDER BY mark_group), count(*) INTO v_groupList, v_count
      FROM emaj.emaj_mark WHERE mark_name = v_markName AND mark_group = ANY(v_groupNames);
    IF v_count > 0 THEN
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_new_mark: The group "%" already contains a mark named "%".', v_groupList, v_markName;
      ELSE
        RAISE EXCEPTION '_check_new_mark: The groups "%" already contain a mark named "%".', v_groupList, v_markName;
      END IF;
    END IF;
    RETURN v_markName;
  END;
$_check_new_mark$;

CREATE OR REPLACE FUNCTION emaj._log_truncate_fnct()
RETURNS TRIGGER  LANGUAGE plpgsql SECURITY DEFINER AS
$_log_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of an audit_only group in logging mode.
  DECLARE
    v_fullLogTableName       TEXT;
  BEGIN
    IF (TG_OP = 'TRUNCATE') THEN
      SELECT quote_ident(rel_log_schema)  || '.' || quote_ident(rel_log_table) INTO v_fullLogTableName FROM emaj.emaj_relation
        WHERE rel_schema = TG_TABLE_SCHEMA AND rel_tblseq = TG_TABLE_NAME AND upper_inf(rel_time_range);
      EXECUTE format('INSERT INTO %s (emaj_verb) VALUES (''TRU'')',
                    v_fullLogTableName);
    END IF;
    RETURN NULL;
  END;
$_log_truncate_fnct$;

CREATE OR REPLACE FUNCTION emaj._create_log_schemas(v_function TEXT, v_groupNames TEXT[])
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_schemas$
-- The function creates all log schemas that will be needed to create new log tables. It gives the appropriate rights to emaj users on
-- these schemas.
-- Input: calling function to record into the emaj_hist table,
--        array of group names
-- The function is created as SECURITY DEFINER so that log schemas can be owned by superuser
  DECLARE
    v_schemaPrefix           TEXT = 'emaj_';
    r_schema                 RECORD;
  BEGIN
    FOR r_schema IN
        SELECT DISTINCT v_schemaPrefix || grpdef_schema AS log_schema FROM emaj.emaj_group_def
          WHERE grpdef_group = ANY (v_groupNames)
            AND NOT EXISTS                                                                -- minus those already created
              (SELECT 0 FROM emaj.emaj_schema WHERE sch_name = v_schemaPrefix || grpdef_schema)
        ORDER BY 1
      LOOP
-- check that the schema doesn't already exist
      PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = r_schema.log_schema;
      IF FOUND THEN
        RAISE EXCEPTION '_create_log_schemas: The schema "%" should not exist. Drop it manually.',r_schema.log_schema;
      END IF;
-- create the schema and give the appropriate rights
      EXECUTE format('CREATE SCHEMA %I',
                     r_schema.log_schema);
      EXECUTE format('GRANT ALL ON SCHEMA %I TO emaj_adm',
                     r_schema.log_schema);
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                     r_schema.log_schema);
-- and record the schema creation into the emaj_schema and the emaj_hist tables
      INSERT INTO emaj.emaj_schema (sch_name) VALUES (r_schema.log_schema);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (v_function, 'LOG_SCHEMA CREATED', quote_ident(r_schema.log_schema));
    END LOOP;
    RETURN;
  END;
$_create_log_schemas$;

CREATE OR REPLACE FUNCTION emaj._drop_log_schemas(v_function TEXT, v_isForced BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_log_schemas$
-- The function looks for log schemas to drop. Drop them if any.
-- Input: calling function to record into the emaj_hist table,
--        boolean telling whether the schema to drop may contain residual objects
-- The function is created as SECURITY DEFINER so that log schemas can be dropped in any case.
  DECLARE
    r_schema                 RECORD;
  BEGIN
-- For each log schema to drop,
    FOR r_schema IN
        SELECT sch_name AS log_schema FROM emaj.emaj_schema                           -- the existing schemas
          WHERE sch_name <> 'emaj'
          EXCEPT
        SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation                        -- the currently needed schemas (after tables drop)
          WHERE rel_kind = 'r' AND rel_log_schema <> 'emaj'
        ORDER BY 1
        LOOP
-- check that the schema really exists
      PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = r_schema.log_schema;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_drop_log_schemas: Internal error (the schema "%" does not exist).',r_schema.log_schema;
      END IF;
      IF v_isForced THEN
-- drop cascade when called by emaj_force_xxx_group()
        EXECUTE format('DROP SCHEMA %I CASCADE',
                       r_schema.log_schema);
      ELSE
-- otherwise, drop restrict with a trap on the potential error
        BEGIN
          EXECUTE format('DROP SCHEMA %I',
                         r_schema.log_schema);
          EXCEPTION
-- trap the 2BP01 exception to generate a more understandable error message
            WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
              RAISE EXCEPTION '_drop_log_schemas: Cannot drop the schema "%". It probably owns unattended objects.'
                              ' Use the emaj_verify_all() function to get details.', r_schema.log_schema;
        END;
      END IF;
-- remove the schema from the emaj_schema table
      DELETE FROM emaj.emaj_schema WHERE sch_name = r_schema.log_schema;
-- record the schema drop in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (v_function,'LOG_SCHEMA DROPPED',quote_ident(r_schema.log_schema));
    END LOOP;
    RETURN;
  END;
$_drop_log_schemas$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_table(v_schema TEXT, v_table TEXT, v_group TEXT, v_properties JSONB DEFAULT NULL,
                                                  v_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_table$
-- The function assigns a table into a tables group.
-- Inputs: schema name, table name, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group, ie. 1
-- The JSONB v_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
  BEGIN
    RETURN emaj._assign_tables(v_schema, ARRAY[v_table], v_group, v_properties, v_mark , FALSE);
  END;
$emaj_assign_table$;
COMMENT ON FUNCTION emaj.emaj_assign_table(TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign a table into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_tables(v_schema TEXT, v_tables TEXT[], v_group TEXT, v_properties JSONB DEFAULT NULL,
                                                   v_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_tables$
-- The function assigns several tables at once into a tables group.
-- Inputs: schema, array of table names, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB v_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
  BEGIN
    RETURN emaj._assign_tables(v_schema, v_tables, v_group, v_properties, v_mark, TRUE);
  END;
$emaj_assign_tables$;
COMMENT ON FUNCTION emaj.emaj_assign_tables(TEXT,TEXT[],TEXT,JSONB,TEXT) IS
$$Assign several tables into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(v_schema TEXT, v_tables TEXT[], v_group TEXT, v_properties JSONB, v_mark TEXT,
                                               v_multiTable BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed
-- Outputs: number of tables effectively assigned to the tables group
  DECLARE
    v_priority               INT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_extraProperties        JSONB;
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_list                   TEXT;
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_schemaPrefix           TEXT = 'emaj_';
    v_logSchema              TEXT;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_logSequence            TEXT;
    v_nextVal                BIGINT;
    v_nbAssignedTbl          INT = 0;
  BEGIN
-- check supplied parameters
-- check the group name and if ok, get some properties of the group
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_group], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := '');
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_group;
-- check the supplied schema exists and is not an E-Maj schema
    PERFORM 1 FROM pg_catalog.pg_namespace
      WHERE nspname = v_schema;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" does not exist.', v_schema;
    END IF;
    PERFORM 1 FROM emaj.emaj_schema
      WHERE sch_name = v_schema;
    IF FOUND THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" is an E-Maj schema.', v_schema;
    END IF;
-- check tables
-- remove duplicates values, NULL and empty strings from the supplied table names array
    SELECT array_agg(DISTINCT table_name) INTO v_tables FROM unnest(v_tables) AS table_name
      WHERE table_name IS NOT NULL AND table_name <> '';
-- process empty array
    IF v_tables IS NULL THEN
      RAISE WARNING '_assign_tables: No table to process.';
      RETURN 0;
    END IF;
-- check that application tables exist
    WITH tables AS (
      SELECT unnest(v_tables) AS table_name)
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(table_name), ', ') INTO v_list
      FROM (
        SELECT table_name FROM tables
        WHERE NOT EXISTS (
          SELECT 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid
              AND nspname = v_schema AND relname = table_name
              AND relkind IN ('r','p'))
      ) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_tables: some tables (%) do not exist.', v_list;
    END IF;
-- check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj)
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(relname), ', ') INTO v_list
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schema AND relname = ANY(v_tables)
        AND relkind = 'p';
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_tables: some tables (%) are partitionned tables (only elementary partitions are supported by E-Maj).',
                      v_list;
    END IF;
-- check no table is a TEMP table
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(relname), ', ') INTO v_list
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schema AND relname = ANY(v_tables)
        AND relkind = 'r' AND relpersistence = 't';
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_tables: some tables (%) are TEMP tables.', v_list;
    END IF;
-- check no table is an unlogged table (blocking rollbackable groups only)
    IF v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(relname), ', ') INTO v_list
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = v_schema AND relname = ANY(v_tables)
          AND relkind = 'r' AND relpersistence = 'u';
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: some tables (%) are UNLOGGED tables.', v_list;
      END IF;
    END IF;
-- with PG11-, check no table is a WITH OIDS table (blocking rollbackable groups only)
    IF emaj._pg_version_num() < 120000 AND v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(relname), ', ') INTO v_list
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = v_schema AND relname = ANY(v_tables)
          AND relkind = 'r' AND relhasoids;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: some tables (%) are declared WITH OIDS.', v_list;
      END IF;
    END IF;
-- check every table has a primary key (blocking rollbackable groups only)
    IF v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(relname), ', ') INTO v_list
        FROM pg_catalog.pg_class t, pg_catalog.pg_namespace
        WHERE t.relnamespace = pg_namespace.oid
          AND nspname = v_schema AND t.relname = ANY(v_tables)
          AND relkind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE c.relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = c.oid
                            AND contype = 'p' AND nspname = v_schema AND c.relname = t.relname);
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: some tables (%) have no PRIMARY KEY.', v_list;
      END IF;
    END IF;
-- check that no table already belongs to a group
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(rel_tblseq), ', ') INTO v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = v_schema AND rel_tblseq = ANY(v_tables) AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_tables: some tables (%) already belong to a group.', v_list;
    END IF;
-- check the priority is numeric
    BEGIN
      v_priority = (v_properties->>'priority')::INT;
    EXCEPTION
      WHEN invalid_text_representation THEN
        RAISE EXCEPTION '_assign_tables: the "priority" property is not numeric.';
    END;
-- check that the tablespaces exist, if supplied
    v_logDatTsp = v_properties->>'log_data_tablespace';
    IF v_logDatTsp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = v_logDatTsp) THEN
      RAISE EXCEPTION '_assign_tables: the log data tablespace "%" does not exists.', v_logDatTsp;
    END IF;
    v_logIdxTsp = v_properties->>'log_index_tablespace';
    IF v_logIdxTsp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = v_logIdxTsp) THEN
      RAISE EXCEPTION '_assign_tables: the log index tablespace "%" does not exists.', v_logIdxTsp;
    END IF;
-- check no properties are unknown
    v_extraProperties = v_properties - 'priority' - 'log_data_tablespace' - 'log_index_tablespace';
    IF v_extraProperties IS NOT NULL AND v_extraProperties <> '{}' THEN
      RAISE EXCEPTION '_assign_tables: properties "%" are unknown.', v_extraProperties;
    END IF;
-- check the supplied mark
    SELECT emaj._check_new_mark(array[v_group], v_mark) INTO v_markName;
-- OK,
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- for LOGGING groups, lock all tables to get a stable point
    IF v_groupIsLogging THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
      PERFORM emaj._lock_groups(ARRAY[v_group], 'ROW EXCLUSIVE', FALSE);
-- and set the mark, using the same time identifier
      PERFORM emaj._set_mark_groups(ARRAY[v_group], v_markName, FALSE, TRUE, NULL, v_timeId);
    END IF;
-- create new log schemas if needed
    v_logSchema = v_schemaPrefix || v_schema;
    IF NOT EXISTS (SELECT 0 FROM emaj.emaj_schema WHERE sch_name = v_logSchema) THEN
-- check that the schema doesn't already exist
      PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchema;
      IF FOUND THEN
        RAISE EXCEPTION '_assign_tables: The schema "%" should not exist. Drop it manually.',v_logSchema;
      END IF;
-- create the schema and give the appropriate rights
      EXECUTE format('CREATE SCHEMA %I',
                     v_logSchema);
      EXECUTE format('GRANT ALL ON SCHEMA %I TO emaj_adm',
                     v_logSchema);
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                     v_logSchema);
-- and record the schema creation into the emaj_schema and the emaj_hist tables
      INSERT INTO emaj.emaj_schema (sch_name) VALUES (v_logSchema);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (CASE WHEN v_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END, 'LOG_SCHEMA CREATED', quote_ident(v_logSchema));
    END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- effectively create the log components for each table
    FOREACH v_oneTable IN ARRAY v_tables
        LOOP
-- create the table
      PERFORM emaj._create_tbl(v_schema, v_oneTable, v_group, v_priority, v_logDatTsp, v_logIdxTsp,
                               v_timeId, v_groupIsRollbackable, v_groupIsLogging);
-- if the group is in logging state, perform additional tasks
      IF v_groupIsLogging THEN
-- ... get the log schema and sequence for the new relation
        SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
          FROM emaj.emaj_relation
          WHERE rel_schema = v_schema AND rel_tblseq = v_oneTable AND upper_inf(rel_time_range);
-- ... get the last log sequence value, if any, for this relation (recorded in emaj_relation at a previous REMOVE_TBL operation)
        SELECT max(rel_log_seq_last_value) + 1 INTO v_nextVal FROM emaj.emaj_relation
          WHERE rel_schema = v_schema AND rel_tblseq = v_oneTable
            AND rel_log_seq_last_value IS NOT NULL;
-- ... set the new log sequence next_val, if needed
        IF v_nextVal IS NOT NULL AND v_nextVal > 1 THEN
          EXECUTE format('ALTER SEQUENCE %I.%I RESTART %s',
                         v_logSchema, v_logSequence, v_nextVal);
        END IF;
-- ... record the new log sequence state in the emaj_sequence table for the current alter_group mark
        INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                    sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
          SELECT * FROM emaj._get_current_sequence_state(v_logSchema, v_logSequence, v_timeId);
      END IF;
-- insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END, 'TABLE ADDED',
                quote_ident(v_schema) || '.' || quote_ident(v_oneTable),
                'To the' || CASE WHEN v_groupIsLogging THEN ' logging' ELSE '' END || ' group ' || v_group);
      v_nbAssignedTbl = v_nbAssignedTbl + 1;
    END LOOP;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- adjust the group characteristics
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = v_timeId,
          group_nb_table = (SELECT count(*) FROM emaj.emaj_relation
                              WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r')
      WHERE group_name = v_group;
-- check foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(array[v_group]);
    RETURN v_nbAssignedTbl;
  END;
$_assign_tables$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_table(v_schema TEXT, v_table TEXT, v_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_table$
-- The function removes a table from its tables group.
-- Inputs: schema name, table name, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively removed to the tables group, ie. 1
  BEGIN
    RETURN emaj._remove_tables(v_schema, ARRAY[v_table], v_mark, FALSE);
  END;
$emaj_remove_table$;
COMMENT ON FUNCTION emaj.emaj_remove_table(TEXT,TEXT,TEXT) IS
$$Remove a table from its tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_tables(v_schema TEXT, v_tables TEXT[], v_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_tables$
-- The function removes several tables at once from their tables group.
-- Inputs: schema, array of table names, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively removed from the tables group
  BEGIN
    RETURN emaj._remove_tables(v_schema, v_tables, v_mark, TRUE);
  END;
$emaj_remove_tables$;
COMMENT ON FUNCTION emaj.emaj_remove_tables(TEXT,TEXT[],TEXT) IS
$$Remove several tables from their tables group.$$;

CREATE OR REPLACE FUNCTION emaj._remove_tables(v_schema TEXT, v_tables TEXT[], v_mark TEXT, v_multiTable BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_tables$
-- The function effectively removes tables from their tables group.
-- Inputs: schema, array of table names, mark to set if for logging groups, boolean to indicate whether several tables need to be processed
-- Outputs: number of tables effectively assigned to the tables group
  DECLARE
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_schemaPrefix           TEXT = 'emaj_';
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_logSchema              TEXT;
    v_nbRemovedTbl           INT = 0;
    v_logSequenceLastValue   BIGINT;
    v_namesSuffix            TEXT;
    v_fullTableName          TEXT;
    r_relation               emaj.emaj_relation%ROWTYPE;
  BEGIN
-- check supplied parameters
-- remove duplicates values, NULL and empty strings from the supplied table names array
    SELECT array_agg(DISTINCT table_name) INTO v_tables FROM unnest(v_tables) AS table_name
      WHERE table_name IS NOT NULL AND table_name <> '';
-- process empty array
    IF v_tables IS NULL THEN
      RAISE WARNING '_remove_tables: No table to process.';
      RETURN 0;
    END IF;
-- check that the tables currently belong to a tables group (not necessarily the same one)
    WITH all_supplied_tables AS (
      SELECT unnest(v_tables) AS table_name),
         tables_in_group AS (
      SELECT rel_tblseq FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = ANY(v_tables) AND upper_inf(rel_time_range))
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(table_name), ', ') INTO v_list
      FROM (
        SELECT table_name FROM all_supplied_tables
          EXCEPT
        SELECT rel_tblseq FROM tables_in_group) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_remove_tables: some tables (%) do not currently belong to any tables group.', v_list;
    END IF;
-- check the supplied mark
    SELECT emaj._check_new_mark(array[v_groupName], v_mark) INTO v_markName;
-- OK,
    v_logSchema = v_schemaPrefix || v_schema;
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- get the lists of groups and logging groups holding these tables, if any.
-- It locks the tables groups so that no other operation simultaneously occurs these groups
    WITH tables_group AS (
      SELECT group_name, group_is_logging FROM emaj.emaj_relation, emaj.emaj_group
        WHERE rel_group = group_name
          AND rel_schema = v_schema AND rel_tblseq = ANY(v_tables) AND upper_inf(rel_time_range)
        FOR UPDATE OF emaj_group
      )
    SELECT (SELECT array_agg(group_name) FROM tables_group),
           (SELECT array_agg(group_name) FROM tables_group WHERE group_is_logging)
      INTO v_groups, v_loggingGroups;
-- for LOGGING groups, lock all tables to get a stable point
    IF v_loggingGroups IS NOT NULL THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
      PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- and set the mark, using the same time identifier
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, FALSE, TRUE, NULL, v_timeId);
    END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- effectively drop the log components for each table
    FOREACH v_oneTable IN ARRAY v_tables
        LOOP
-- get the emaj_relation row of the table to process and some characteristics of the group that holds the table
      SELECT emaj_relation.* INTO r_relation FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_oneTable AND upper_inf(rel_time_range);
      SELECT group_name, group_is_logging INTO v_groupName, v_groupIsLogging FROM emaj.emaj_group
        WHERE group_name = r_relation.rel_group;
      IF NOT v_groupIsLogging THEN
-- if the group is idle, drop the table
        PERFORM emaj._drop_tbl(r_relation, v_timeId);
      ELSE
-- if the group is in logging state, perform additional tasks
-- ... get the current log sequence characteristics
        SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO STRICT v_logSequenceLastValue
          FROM emaj.emaj_sequence
          WHERE sequ_schema = r_relation.rel_log_schema AND sequ_name = r_relation.rel_log_sequence AND sequ_time_id = v_timeId;
-- ... compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names
        SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
          FROM
            (SELECT unnest(regexp_matches(rel_log_table,'_(\d+)$'))::INT AS suffix
               FROM emaj.emaj_relation
               WHERE rel_schema = v_schema AND rel_tblseq = v_oneTable
            ) AS t;
-- ... rename the log table and its index (they may have been dropped)
        EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                       v_logSchema, r_relation.rel_log_table, r_relation.rel_log_table || v_namesSuffix);
        EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                       v_logSchema, r_relation.rel_log_index, r_relation.rel_log_index || v_namesSuffix);
-- ... drop the log and truncate triggers
--     (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
        v_fullTableName  = quote_ident(v_schema) || '.' || quote_ident(v_oneTable);
        PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE relnamespace = pg_namespace.oid
            AND nspname = v_schema AND relname = v_oneTable AND relkind = 'r';
        IF FOUND THEN
          EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                         v_fullTableName);
          EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                         v_fullTableName);
        END IF;
-- ... drop the log function and the log sequence
-- (but we keep the sequence related data in the emaj_sequence and the emaj_seq_hole tables)
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                       v_logSchema, r_relation.rel_log_function);
        EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                       v_logSchema, r_relation.rel_log_sequence);
-- ... register the end of the relation time frame, the last value of the log sequence, the log table and index names change,
-- and reset the content of now useless columns
-- (but do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
        UPDATE emaj.emaj_relation
          SET rel_time_range = int8range(lower(rel_time_range), v_timeId, '[)'),
              rel_log_table = r_relation.rel_log_table || v_namesSuffix , rel_log_index = r_relation.rel_log_index || v_namesSuffix,
              rel_log_function = NULL, rel_sql_columns = NULL, rel_sql_pk_columns = NULL, rel_sql_pk_eq_conditions = NULL,
              rel_log_seq_last_value = v_logSequenceLastValue
          WHERE rel_schema = v_schema AND rel_tblseq = v_oneTable AND upper_inf(rel_time_range);
      END IF;
-- insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END, 'TABLE REMOVED',
                quote_ident(v_schema) || '.' || quote_ident(v_oneTable),
                'From the' || CASE WHEN v_groupIsLogging THEN ' logging' ELSE '' END || ' group ' || v_groupName);
      v_nbRemovedTbl = v_nbRemovedTbl + 1;
    END LOOP;
-- drop the log schema if it is now useless
    IF NOT EXISTS (SELECT 0 FROM emaj.emaj_relation WHERE rel_log_schema = v_logSchema) THEN
-- drop the schema
      EXECUTE format('DROP SCHEMA %I',
                     v_logSchema);
-- and record the schema drop into the emaj_schema and the emaj_hist tables
      DELETE FROM emaj.emaj_schema WHERE sch_name = v_logSchema;
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (CASE WHEN v_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END, 'LOG_SCHEMA DROPPED', quote_ident(v_logSchema));
    END IF;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- adjust the groups characteristics
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = v_timeId,
          group_nb_table = (SELECT count(*) FROM emaj.emaj_relation
                              WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r')
      WHERE group_name = ANY (v_groups);
    RETURN v_nbRemovedTbl;
  END;
$_remove_tables$;

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
    v_colList                TEXT;
    v_pkColList              TEXT;
    v_pkCondList             TEXT;
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
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('CREATE TABLE %s (LIKE %s) %s',
                    v_logTableName, v_fullTableName, v_dataTblSpace);
    EXECUTE format('ALTER TABLE %s'
                   ' ADD COLUMN emaj_verb      VARCHAR(3),'
                   ' ADD COLUMN emaj_tuple     VARCHAR(3),'
                   ' ADD COLUMN emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
                   ' ADD COLUMN emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
                   ' ADD COLUMN emaj_txid      BIGINT      DEFAULT txid_current(),'
                   ' ADD COLUMN emaj_user      VARCHAR(32) DEFAULT session_user',
                   v_logTableName);
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
    PERFORM emaj._create_log_trigger(v_fullTableName, v_logTableName, v_sequenceName, v_logFnctName);
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
-- register the table into emaj_relation
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function,
                rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions,
                rel_emaj_verb_attnum)
        VALUES (v_schema, v_tbl, int8range(v_timeId, NULL, '[)'), v_groupName, v_priority,
                v_logSchema, v_logDatTsp, v_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName,
                v_colList, v_pkColList, v_pkCondList,
                v_attnum);
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
-- grant appropriate rights to both emaj roles
    EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                   v_logTableName);
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE %s TO emaj_adm',
                   v_logTableName);
    EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                   v_sequenceName);
    EXECUTE format('GRANT ALL PRIVILEGES ON SEQUENCE %s TO emaj_adm',
                   v_sequenceName);
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._create_log_trigger(v_fullTableName TEXT, v_logTableName TEXT, v_sequenceName TEXT, v_logFnctName TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_trigger$
-- The function creates the log function and the associated log trigger for an application table.
-- It is called by several functions.
-- Inputs: the full name of the application table, the log table, the log sequence and the log function
-- The function is defined as SECURITY DEFINER so that emaj_adm role can manage the trigger on the application table.
  DECLARE
  BEGIN
-- drop the log trigger if it exists
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                   v_fullTableName);
-- create the log fonction that will be mapped to the log trigger just after
--   the new row is logged for each INSERT, the old row is logged for each DELETE
--   and the old and the new rows are logged for each UPDATE.
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
         || '$logfnct$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;';
-- create the log trigger on the application table, using the previously created log function
    EXECUTE format('CREATE TRIGGER emaj_log_trg'
                   ' AFTER INSERT OR UPDATE OR DELETE ON %s'
                   '  FOR EACH ROW EXECUTE PROCEDURE %s()',
                   v_fullTableName, v_logFnctName);
    RETURN;
  END;
$_create_log_trigger$;

CREATE OR REPLACE FUNCTION emaj._add_tbl(r_plan emaj.emaj_alter_plan, v_timeId BIGINT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_add_tbl$
-- The function adds a table to a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _create_tbl() function.
-- Otherwise, it calls the _create_tbl() function, activates the log trigger and
--    sets a restart value for the log sequence if a previous range exists for the relation.
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group
-- operation.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can enable triggers on application tables.
  DECLARE
    v_groupIsRollbackable    BOOLEAN;
    v_logSchema              TEXT;
    v_logSequence            TEXT;
    v_nextVal                BIGINT;
  BEGIN
-- get the is_rollbackable status of the related group
    SELECT group_is_rollbackable INTO v_groupIsRollbackable
      FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- create the table
    PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp,
                             v_timeId, v_groupIsRollbackable, r_plan.altr_group_is_logging)
      FROM emaj.emaj_group_def
      WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- if the group is in logging state, perform additional tasks
    IF r_plan.altr_group_is_logging THEN
-- ... get the log schema and sequence for the new relation
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- ... get the last log sequence value, if any, for this relation (recorded in emaj_relation at a previous REMOVE_TBL operation)
      SELECT max(rel_log_seq_last_value) + 1 INTO v_nextVal FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq
          AND rel_log_seq_last_value IS NOT NULL;
-- ... set the new log sequence next_val, if needed
      IF v_nextVal IS NOT NULL AND v_nextVal > 1 THEN
        EXECUTE format('ALTER SEQUENCE %I.%I RESTART %s',
                       v_logSchema, v_logSequence, v_nextVal);
      END IF;
-- ... record the new log sequence state in the emaj_sequence table for the current alter_group mark
      INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                  sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
        SELECT * FROM emaj._get_current_sequence_state(v_logSchema, v_logSequence, v_timeId);
    END IF;
-- insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'TABLE ADDED',
                quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq), 'To the group ' || r_plan.altr_group);
    RETURN;
  END;
$_add_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_log_data_tsp_tbl(r_rel emaj.emaj_relation, v_newLogDatTsp TEXT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_change_log_data_tsp_tbl$
-- This function changes the log data tablespace for an application table.
-- Input: the existing emaj_relation row for the table and the new log data tablespace
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = v_newLogDatTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- process the log data tablespace change
    EXECUTE format('ALTER TABLE %I.%I SET TABLESPACE %I',
                   r_rel.rel_log_schema, r_rel.rel_log_table, v_newTsp);
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation SET rel_log_dat_tsp = v_newLogDatTsp
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'LOG DATA TABLESPACE CHANGED',
              quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              coalesce(r_rel.rel_log_dat_tsp, 'Default tablespace') || ' => ' || coalesce(v_newLogDatTsp, 'Default tablespace'));
    RETURN;
  END;
$_change_log_data_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_log_index_tsp_tbl(r_rel emaj.emaj_relation, v_newLogIdxTsp TEXT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_change_log_index_tsp_tbl$
-- This function changes the log index tablespace for an application table.
-- Input: the existing emaj_relation row for the table and the new log index tablespace
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = v_newLogIdxTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- process the log index tablespace change
    EXECUTE format('ALTER INDEX %I.%I SET TABLESPACE %I',
                   r_rel.rel_log_schema, r_rel.rel_log_index, v_newTsp);
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation SET rel_log_idx_tsp = v_newLogIdxTsp
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'LOG INDEX TABLESPACE CHANGED',
              quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              coalesce(r_rel.rel_log_idx_tsp, 'Default tablespace') || ' => ' || coalesce(v_newLogIdxTsp, 'Default tablespace'));
    RETURN;
  END;
$_change_log_index_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._remove_tbl(r_plan emaj.emaj_alter_plan, v_timeId BIGINT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_remove_tbl$
-- The function removes a table from a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _drop_tbl() function.
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group
-- time for instance).
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group
-- operation.
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
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*, v_timeId) FROM emaj.emaj_relation
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
      SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
        FROM
          (SELECT unnest(regexp_matches(rel_log_table,'_(\d+)$'))::INT AS suffix
             FROM emaj.emaj_relation
             WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq
          ) AS t;
-- ... rename the log table and its index (they may have been dropped)
      EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogTable, v_currentLogTable || v_namesSuffix);
      EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogIndex, v_currentLogIndex || v_namesSuffix);
--TODO: share some code with _drop_tbl() ?
-- ... drop the log and truncate triggers
--     (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
      v_fullTableName  = quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq);
      PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = r_plan.altr_schema AND relname = r_plan.altr_tblseq AND relkind = 'r';
      IF FOUND THEN
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                       v_fullTableName);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                       v_fullTableName);
      END IF;
-- ... drop the log function and the log sequence
-- (but we keep the sequence related data in the emaj_sequence and the emaj_seq_hole tables)
      EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                     v_logSchema, v_logFunction);
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     v_logSchema, v_logSequence);
-- ... register the end of the relation time frame, the last value of the log sequence, the log table and index names change,
-- and reset the content of now useless columns
-- (but do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range), v_timeId, '[)'),
            rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix,
            rel_log_function = NULL, rel_sql_columns = NULL, rel_sql_pk_columns = NULL, rel_sql_pk_eq_conditions = NULL,
            rel_log_seq_last_value = v_logSequenceLastValue
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'TABLE REMOVED',
              quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq), 'From the group ' || r_plan.altr_group);
    RETURN;
  END;
$_remove_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function.
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess, time id.
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
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                       v_fullTableName);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                       v_fullTableName);
      END IF;
-- drop the log function
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                       r_rel.rel_log_schema, r_rel.rel_log_function);
      END IF;
-- drop the sequence associated to the log table
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_sequence);
    END IF;
-- drop the log table
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                   r_rel.rel_log_schema, r_rel.rel_log_table);
-- delete rows related to the log sequence from emaj_sequence table (it my delete rows for other not yet processed time_ranges for the
-- same table)
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- delete rows related to the table from emaj_seq_hole table
-- (it may delete rows for other not yet processed time_ranges for the same table).
    DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq;
-- keep a trace of the table group ownership history and finaly delete the table reference from the emaj_relation table
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq,
             CASE WHEN upper_inf(rel_time_range) THEN int8range(lower(rel_time_range), v_timeId, '[)') ELSE rel_time_range END,
             rel_group, rel_kind
        FROM deleted;
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequence(v_schema TEXT, v_sequence TEXT, v_group TEXT, v_properties JSONB DEFAULT NULL,
                                                     v_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequence$
-- The function assigns a sequence into a tables group.
-- Inputs: schema name, sequence name, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group, ie. 1
-- The JSONB v_properties parameter has currenlty only one field '{"priority":...}' the properties being NULL by default
  BEGIN
    RETURN emaj._assign_sequences(v_schema, ARRAY[v_sequence], v_group, v_properties, v_mark , FALSE);
  END;
$emaj_assign_sequence$;
COMMENT ON FUNCTION emaj.emaj_assign_sequence(TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign a sequence into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequences(v_schema TEXT, v_sequences TEXT[], v_group TEXT, v_properties JSONB DEFAULT NULL,
                                                      v_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequences$
-- The function assigns several sequences at once into a tables group.
-- Inputs: schema, array of sequence names, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group
-- The JSONB v_properties parameter has currenlty only one field '{"priority":...}' the properties being NULL by default
  BEGIN
    RETURN emaj._assign_sequences(v_schema, v_sequences, v_group, v_properties, v_mark, TRUE);
  END;
$emaj_assign_sequences$;
COMMENT ON FUNCTION emaj.emaj_assign_sequences(TEXT,TEXT[],TEXT,JSONB,TEXT) IS
$$Assign several sequences into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj._assign_sequences(v_schema TEXT, v_sequences TEXT[], v_group TEXT, v_properties JSONB,
                                                  v_mark TEXT, v_multiSequence BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_sequences$
-- The function effectively assigns sequences into a tables group.
-- Inputs: schema, array of sequence names, group name, properties as JSON structure,
--         mark to set for lonnging groups, a boolean indicating whether several sequences need to be processed
-- Outputs: number of sequences effectively assigned to the tables group
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_priority               INT;
    v_extraProperties        JSONB;
    v_list                   TEXT;
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_oneSequence            TEXT;
    v_nbAssignedSeq          INT = 0;
  BEGIN
-- check supplied parameters
-- check the group name and if ok, get some properties of the group
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_group], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := '');
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_group;
-- check the supplied schema exists and is not an E-Maj schema
    PERFORM 1 FROM pg_catalog.pg_namespace
      WHERE nspname = v_schema;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" does not exist.', v_schema;
    END IF;
    PERFORM 1 FROM emaj.emaj_schema
      WHERE sch_name = v_schema;
    IF FOUND THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" is an E-Maj schema.', v_schema;
    END IF;
-- check sequences
-- remove duplicates values, NULL and empty strings from the supplied sequence names array
    SELECT array_agg(DISTINCT sequence_name) INTO v_sequences FROM unnest(v_sequences) AS sequence_name
      WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- process empty array
    IF v_sequences IS NULL THEN
      RAISE WARNING '_assign_sequences: No sequence to process.';
      RETURN 0;
    END IF;
-- check that application sequences exist
    WITH sequences AS (
      SELECT unnest(v_sequences) AS sequence_name)
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(sequence_name), ', ') INTO v_list
      FROM (
        SELECT sequence_name FROM sequences
        WHERE NOT EXISTS (
          SELECT 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid
              AND nspname = v_schema AND relname = sequence_name
              AND relkind IN ('S'))
      ) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_sequences: some sequences (%) do not exist.', v_list;
    END IF;
-- check that no sequence already belongs to a group
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(rel_tblseq), ', ') INTO v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = v_schema AND rel_tblseq = ANY(v_sequences) AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_assign_sequences: some sequences (%) already belong to a group.', v_list;
    END IF;
-- check the priority is numeric
    BEGIN
      v_priority = (v_properties->>'priority')::INT;
    EXCEPTION
      WHEN invalid_text_representation THEN
        RAISE EXCEPTION '_assign_sequences: the "priority" property is not numeric.';
    END;
-- check no properties are unknown
    v_extraProperties = v_properties - 'priority';
    IF v_extraProperties IS NOT NULL AND v_extraProperties <> '{}' THEN
      RAISE EXCEPTION '_assign_sequences: properties "%" are unknown.', v_extraProperties;
    END IF;
-- check the supplied mark
    SELECT emaj._check_new_mark(array[v_group], v_mark) INTO v_markName;
-- OK,
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- for LOGGING groups, lock all tables to get a stable point
    IF v_groupIsLogging THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
      PERFORM emaj._lock_groups(ARRAY[v_group], 'ROW EXCLUSIVE', FALSE);
-- and set the mark, using the same time identifier
      PERFORM emaj._set_mark_groups(ARRAY[v_group], v_markName, FALSE, TRUE, NULL, v_timeId);
    END IF;
-- effectively create the log components for each table
    FOREACH v_oneSequence IN ARRAY v_sequences
        LOOP
-- create the sequence
      PERFORM emaj._create_seq(v_schema, v_oneSequence, v_group, v_priority, v_timeId);
-- if the group is in logging state, perform additional tasks
      IF v_groupIsLogging THEN
-- ... record the new sequence state in the emaj_sequence table for the current alter_group mark
        INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                    sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
          SELECT * FROM emaj._get_current_sequence_state(v_schema, v_oneSequence, v_timeId);
      END IF;
-- insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiSequence THEN 'ASSIGN_SEQUENCES' ELSE 'ASSIGN_SEQUENCE' END, 'SEQUENCE ADDED',
                quote_ident(v_schema) || '.' || quote_ident(v_oneSequence),
                'To the' || CASE WHEN v_groupIsLogging THEN ' logging' ELSE '' END || ' group ' || v_group);
      v_nbAssignedSeq = v_nbAssignedSeq + 1;
    END LOOP;
-- adjust the group characteristics
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = v_timeId,
          group_nb_sequence = (SELECT count(*) FROM emaj.emaj_relation
                              WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'S')
      WHERE group_name = v_group;
    RETURN v_nbAssignedSeq;
  END;
$_assign_sequences$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_sequence(v_schema TEXT, v_sequence TEXT, v_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_sequence$
-- The function removes a sequence from its tables group.
-- Inputs: schema name, sequence name, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively removed to the tables group, id. 1
  BEGIN
    RETURN emaj._remove_sequences(v_schema, ARRAY[v_sequence], v_mark, FALSE);
  END;
$emaj_remove_sequence$;
COMMENT ON FUNCTION emaj.emaj_remove_sequence(TEXT,TEXT,TEXT) IS
$$Remove a sequence from its tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_sequences(v_schema TEXT, v_sequences TEXT[], v_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_sequences$
-- The function removes several sequences at once from their tables group.
-- Inputs: schema, array of sequence names, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively removed from the tables group
  BEGIN
    RETURN emaj._remove_sequences(v_schema, v_sequences, v_mark, TRUE);
  END;
$emaj_remove_sequences$;
COMMENT ON FUNCTION emaj.emaj_remove_sequences(TEXT,TEXT[],TEXT) IS
$$Remove several sequences from their tables group.$$;

CREATE OR REPLACE FUNCTION emaj._remove_sequences(v_schema TEXT, v_sequences TEXT[], v_mark TEXT, v_multiSequence BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_sequences$
-- The function effectively removes sequences from their sequences group.
-- Inputs: schema, array of sequence names, mark to set if for logging groups,
--         boolean to indicate whether several sequences need to be processed
-- Outputs: number of sequences effectively assigned to the sequences group
  DECLARE
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_oneSequence            TEXT;
    v_nbRemovedSeq           INT = 0;
    r_relation               emaj.emaj_relation%ROWTYPE;
  BEGIN
-- check supplied parameters
-- remove duplicates values, NULL and empty strings from the supplied sequence names array
    SELECT array_agg(DISTINCT sequence_name) INTO v_sequences FROM unnest(v_sequences) AS sequence_name
      WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- process empty array
    IF v_sequences IS NULL THEN
      RAISE WARNING '_remove_sequences: No sequence to process.';
      RETURN 0;
    END IF;
-- check that the sequences currently belong to a tables group (not necessarily the same one)
    WITH all_supplied_sequences AS (
      SELECT unnest(v_sequences) AS sequence_name),
         sequences_in_group AS (
      SELECT rel_tblseq FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = ANY(v_sequences) AND upper_inf(rel_time_range))
    SELECT string_agg(quote_ident(v_schema) || '.' || quote_ident(sequence_name), ', ') INTO v_list
      FROM (
        SELECT sequence_name FROM all_supplied_sequences
          EXCEPT
        SELECT rel_tblseq FROM sequences_in_group) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_remove_sequences: some sequences (%) do not currently belong to any tables group.', v_list;
    END IF;
-- check the supplied mark
    SELECT emaj._check_new_mark(array[v_groupName], v_mark) INTO v_markName;
-- OK,
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- get the lists of groups and logging groups holding these sequences, if any
-- It locks the tables groups so that no other operation simultaneously occurs these groups
    WITH tables_group AS (
      SELECT group_name, group_is_logging FROM emaj.emaj_relation, emaj.emaj_group
        WHERE rel_group = group_name
          AND rel_schema = v_schema AND rel_tblseq = ANY(v_sequences) AND upper_inf(rel_time_range)
        FOR UPDATE OF emaj_group
      )
    SELECT (SELECT array_agg(group_name) FROM tables_group),
           (SELECT array_agg(group_name) FROM tables_group WHERE group_is_logging)
      INTO v_groups, v_loggingGroups;
-- for LOGGING groups, lock all tables to get a stable point
    IF v_loggingGroups IS NOT NULL THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
      PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- and set the mark, using the same time identifier
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, FALSE, TRUE, NULL, v_timeId);
    END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- effectively drop the log components for each sequence
    FOREACH v_oneSequence IN ARRAY v_sequences
        LOOP
-- get the emaj_relation row of the sequence to process and some characteristics of the group that holds the sequence
      SELECT emaj_relation.* INTO r_relation FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_oneSequence AND upper_inf(rel_time_range);
      SELECT group_name, group_is_logging INTO v_groupName, v_groupIsLogging FROM emaj.emaj_group
        WHERE group_name = r_relation.rel_group;
      IF NOT v_groupIsLogging THEN
-- if the group is idle, drop the sequence
        PERFORM emaj._drop_seq(r_relation, v_timeId);
      ELSE
-- if the group is in logging state, just register the end of the relation time frame
        UPDATE emaj.emaj_relation SET rel_time_range = int8range(lower(rel_time_range),v_timeId, '[)')
          WHERE rel_schema = v_schema AND rel_tblseq = v_oneSequence AND upper_inf(rel_time_range);
      END IF;
-- insert an entry into the emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiSequence THEN 'REMOVE_SEQUENCES' ELSE 'REMOVE_SEQUENCE' END, 'SEQUENCE REMOVED',
                quote_ident(v_schema) || '.' || quote_ident(v_oneSequence),
                'From the' || CASE WHEN v_groupIsLogging THEN ' logging' ELSE '' END || ' group ' || v_groupName);
      v_nbRemovedSeq = v_nbRemovedSeq + 1;
    END LOOP;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- adjust the groups characteristics
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = v_timeId,
          group_nb_sequence = (SELECT count(*) FROM emaj.emaj_relation
                              WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r')
      WHERE group_name = ANY (v_groups);
    RETURN v_nbRemovedSeq;
  END;
$_remove_sequences$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence.
-- Required inputs: row from emaj_relation corresponding to the appplication sequence to proccess.
  BEGIN
-- delete rows from emaj_sequence
-- if several rows exist in emaj_relation for the same sequence, due to removals and additions while the group was in logging state,
--   the first function call deletes all emaj_sequence rows for the sequence
    EXECUTE format('DELETE FROM emaj.emaj_sequence WHERE sequ_schema = %L AND sequ_name = %L',
                   r_rel.rel_schema, r_rel.rel_tblseq);
-- keep a trace of the sequence group ownership history and finaly delete the sequence from the emaj_relation table
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq,
             CASE WHEN upper_inf(rel_time_range) THEN int8range(lower(rel_time_range), v_timeId, '[)') ELSE rel_time_range END,
             rel_group, rel_kind
        FROM deleted;
    RETURN;
  END;
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_minGlobalSeq BIGINT, v_maxGlobalSeq BIGINT, v_nbSession INT,
                                          v_isLoggedRlbk BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence.
-- The function is called by emaj._rlbk_session_exec().
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence (non inclusive) lower and (inclusive) upper limits covering the rollback time frame
--        number of sessions and a boolean indicating whether the rollback is logged
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_insertClause           TEXT = '';
    v_nbPk                   BIGINT;
  BEGIN
    v_fullTableName  = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName   = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName,
              'All log rows with emaj_gid > ' || v_minGlobalSeq || ' and <= ' || v_maxGlobalSeq);
-- create the temporary table containing all primary key values with their earliest emaj_gid
    IF v_nbSession = 1 THEN
      v_tableType = 'TEMP';
      v_tmpTable = 'emaj_tmp_' || pg_backend_pid();
    ELSE
--   with multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
--   but it may be an UNLOGGED table
      v_tableType = 'UNLOGGED';
      v_tmpTable = 'emaj.emaj_tmp_' || pg_backend_pid();
    END IF;
    EXECUTE format('CREATE %s TABLE %s AS '
                   '  SELECT %s, min(emaj_gid) as emaj_gid FROM %s'
                   '    WHERE emaj_gid > %s AND emaj_gid <= %s'
                   '    GROUP BY %s',
                   v_tableType, v_tmpTable, r_rel.rel_sql_pk_columns, v_logTableName,
                   v_minGlobalSeq, v_maxGlobalSeq, r_rel.rel_sql_pk_columns);
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- delete all rows from the application table corresponding to each touched primary key
--   this deletes rows inserted or updated during the rolled back period
    EXECUTE format('DELETE FROM ONLY %s tbl USING %s keys WHERE %s',
                   v_fullTableName, v_tmpTable, r_rel.rel_sql_pk_eq_conditions);
-- for logged rollbacks, if the number of pkey to process is greater than 1.000, ANALYZE the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement
    IF v_isLoggedRlbk AND v_nbPk > 1000 THEN
      EXECUTE format('ANALYZE %s',
                     v_logTableName);
    END IF;
-- insert into the application table rows that were deleted or updated during the rolled back period
    IF emaj._pg_version_num() >= 100000 THEN
      v_insertClause = ' OVERRIDING SYSTEM VALUE';
    END IF;
    EXECUTE format('INSERT INTO %s %s'
                   '  SELECT %s FROM %s tbl, %s keys '
                   '    WHERE %s AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
                   '      AND tbl.emaj_gid > %s AND tbl.emaj_gid <= %s',
                   v_fullTableName, v_insertClause, r_rel.rel_sql_columns, v_logTableName, v_tmpTable,
                   r_rel.rel_sql_pk_eq_conditions, v_minGlobalSeq, v_maxGlobalSeq);
-- drop the now useless temporary table
    EXECUTE format('DROP TABLE %s',
                   v_tmpTable);
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

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
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- delete obsolete log rows
    EXECUTE format('DELETE FROM %I.%I WHERE emaj_gid > %s',
                   r_rel.rel_log_schema, r_rel.rel_log_table, v_lastGlobalSeq);
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- record the sequence holes generated by the delete operation
-- this is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group() function
--   (and indirectly by emaj_estimate_rollback_group() and emaj_estimate_rollback_groups())
-- first delete, if exist, sequence holes that have disappeared with the rollback
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
        AND sqhl_begin_time_id >= v_beginTimeId AND sqhl_begin_time_id < v_endTimeId;
-- and then insert the new sequence hole
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE format('INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)'
                     ' VALUES (%L, %L, %s, %s, ('
                     '   SELECT CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END'
                     '     FROM %I.%I rel, pg_sequences'
                     '     WHERE schemaname = %L AND sequencename = %L'
                     '   )-('
                     '   SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END'
                     '     FROM emaj.emaj_sequence'
                     '     WHERE sequ_schema = %L AND sequ_name = %L AND sequ_time_id = %s))',
                     r_rel.rel_schema, r_rel.rel_tblseq, v_beginTimeId, v_endTimeId, r_rel.rel_log_schema, r_rel.rel_log_sequence,
                     r_rel.rel_log_schema, r_rel.rel_log_sequence, r_rel.rel_log_schema, r_rel.rel_log_sequence, v_beginTimeId);
    ELSE
      EXECUTE format('INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)'
                     ' VALUES (%L, %L, %s, %s, ('
                     '   SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM %I.%I'
                     '   )-('
                     '   SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END'
                     '     FROM emaj.emaj_sequence'
                     '     WHERE sequ_schema = %L AND sequ_name = %L AND sequ_time_id = %s))',
                     r_rel.rel_schema, r_rel.rel_tblseq, v_beginTimeId, v_endTimeId, r_rel.rel_log_schema, r_rel.rel_log_sequence,
                     r_rel.rel_log_schema, r_rel.rel_log_sequence, v_beginTimeId);
    END IF;
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark.
-- The function is called by emaj.emaj._rlbk_end().
-- Input: the emaj_group_def row related to the application sequence to process, time id of the mark to rollback to.
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
          RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".',
            v_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END;
-- Read the current sequence's characteristics
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE format('SELECT rel.last_value, start_value, increment_by, max_value, min_value, cache_size as cache_value, '
                     '       cycle as is_cycled, rel.is_called'
                     '  FROM %s rel, pg_catalog.pg_sequences '
                     '  WHERE schemaname = %L AND sequencename = %L',
                     v_fullSeqName, r_rel.rel_schema, r_rel.rel_tblseq)
              INTO STRICT curr_seq_rec;
    ELSE
      EXECUTE format('SELECT last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM %s',
                     v_fullSeqName)
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
      EXECUTE format('ALTER SEQUENCE %s %s',
                     v_fullSeqName, v_stmt);
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
      IF emaj._pg_version_num() >= 100000 THEN
       EXECUTE format('SELECT CASE WHEN rel.is_called THEN rel.last_value ELSE rel.last_value - increment_by END'
                       '  FROM %I.%I rel, pg_sequences'
                       '  WHERE schemaname = %L  AND sequencename = %L ',
                       r_rel.rel_log_schema, r_rel.rel_log_sequence, r_rel.rel_log_schema, r_rel.rel_log_sequence)
          INTO v_endLastValue;
      ELSE
        EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM %I.%I',
                       r_rel.rel_log_schema, r_rel.rel_log_sequence)
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

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, v_firstEmajGid BIGINT, v_lastEmajGid BIGINT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_tbl$
-- This function generates SQL commands representing all updates performed on a table between 2 marks
-- or beetween a mark and the current situation.
-- These command are stored into a temporary table created by the _gen_sql_groups() calling function.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        the global sequence value at requested start and end marks
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
    v_conditions             TEXT;
    v_lastEmajGidRel         BIGINT;
    v_nbSQL                  BIGINT;
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
-- test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric
-- data types)
      IF regexp_replace(r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
        v_valList = v_valList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::TEXT,''NULL'') || '', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.'
                              || quote_ident(r_col.attname) || ' ::TEXT,''NULL'') || '', ';
      ELSE
-- literal for this column must be quoted
        v_valList = v_valList || ''' || quote_nullable(o.' || quote_ident(r_col.attname) || ') || '', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_nullable(n.'
                              || quote_ident(r_col.attname) || ') || '', ';
      END IF;
    END LOOP;
-- suppress the final separators
    v_valList = substring(v_valList FROM 1 FOR char_length(v_valList) - 2);
    v_setList = substring(v_setList FROM 1 FOR char_length(v_setList) - 2);
-- retrieve all columns that represents the pkey and build the "pkey equal" conditions set that will be used in UPDATE and DELETE
--  statements (taking column names in pg_attribute from the table's definition instead of index definition is mandatory
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
    v_pkCondList = '';
    FOR r_col IN
      SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName::regclass AND indisprimary
          AND attnum > 0 AND NOT attisdropped
    LOOP
-- test if the column format (at least up to the parenthesis) belongs to the list of formats that do not require any quotation
-- (like numeric data types)
      IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.'
                                    || quote_ident(r_col.attname) || ' || '' AND ';
      ELSE
-- literal for this column must be quoted
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.'
                                    || quote_ident(r_col.attname) || ') || '' AND ';
      END IF;
    END LOOP;
-- suppress the final separator
    v_pkCondList = substring(v_pkCondList FROM 1 FOR char_length(v_pkCondList) - 5);
-- prepare sql skeletons for each statement type
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''') || ' VALUES (' || v_valList || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''') || ' SET ' || v_setList || ' WHERE ' || v_pkCondList || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''') || ' WHERE ' || v_pkCondList || ';''';
    v_rqTruncate = '''TRUNCATE ' || replace(v_fullTableName,'''','''''') || ';''';
-- build the restriction conditions on emaj_gid, depending on supplied marks range and the relation time range upper bound
    v_conditions = 'o.emaj_gid > ' || v_firstEmajGid;
--   get the EmajGid of the relation time range upper bound, if any
    IF NOT upper_inf(r_rel.rel_time_range) THEN
      SELECT time_last_emaj_gid INTO v_lastEmajGidRel FROM emaj.emaj_time_stamp WHERE time_id = upper(r_rel.rel_time_range);
    END IF;
--   if the relation time range upper bound is before the requested end mark, restrict the EmajGid upper limit
    IF v_lastEmajGidRel IS NOT NULL AND
       (v_lastEmajGid IS NULL OR (v_lastEmajGid IS NOT NULL AND v_lastEmajGidRel < v_lastEmajGid)) THEN
      v_lastEmajGid = v_lastEmajGidRel;
    END IF;
--   complete the restriction conditions
    IF v_lastEmajGid IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || v_lastEmajGid;
    END IF;
-- now scan the log table to process all statement types at once
    EXECUTE format('INSERT INTO emaj_temp_script '
                   'SELECT o.emaj_gid, 0, o.emaj_txid, CASE '
                   '    WHEN o.emaj_verb = ''INS'' THEN %s'
                   '    WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN %s'
                   '    WHEN o.emaj_verb = ''DEL'' THEN %s'
                   '    WHEN o.emaj_verb = ''TRU'' THEN %s'
                   '  END '
                   '  FROM %s o'
                   '       LEFT OUTER JOIN %s n ON n.emaj_gid = o.emaj_gid'
                   '                          AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'') '
                   ' WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
                   ' AND %s',
                   v_rqInsert, v_rqUpdate, v_rqDelete, v_rqTruncate, v_logTableName, v_logTableName, v_conditions);
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._get_current_sequence_state(v_schema TEXT, v_sequence TEXT, v_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql AS
$_get_current_sequence_state$
-- The function returns the current state of a single sequence.
-- Input: schema and sequence name,
--        time_id to set the sequ_time_id
-- Output: an emaj_sequence record
  DECLARE
    r_sequ                   emaj.emaj_sequence%ROWTYPE;
  BEGIN
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE format('SELECT schemaname, sequencename, %s, rel.last_value, start_value, increment_by, max_value, min_value, cache_size,'
                     '       cycle, rel.is_called FROM %I.%I rel, pg_catalog.pg_sequences '
                     '  WHERE schemaname = %L AND sequencename = %L',
                     v_timeId, v_schema, v_sequence, v_schema, v_sequence)
        INTO STRICT r_sequ;
    ELSE
      EXECUTE format('SELECT %L, %L, %s, last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called'
                     ' FROM %I.%I',
                     v_schema, v_sequence, v_timeId, v_schema, v_sequence)
        INTO STRICT r_sequ;
    END IF;
    RETURN r_sequ;
  END;
$_get_current_sequence_state$;

CREATE OR REPLACE FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_lock_groups$
-- This function locks all tables of a groups array.
-- The lock mode is provided by the calling function.
-- It only locks existing tables. It is calling function's responsability to handle cases when application tables are missing.
-- Input: array of group names, lock mode, flag indicating whether the function is called to processed several groups
  DECLARE
    v_nbRetry                SMALLINT = 0;
    v_nbTbl                  INT;
    v_ok                     BOOLEAN = FALSE;
    v_fullTableName          TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END,'BEGIN', array_to_string(v_groupNames,','));
-- acquire lock on all tables
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- scan all tables currently belonging to the groups
        v_nbTbl = 0;
        FOR r_tblsq IN
            SELECT rel_priority, rel_schema, rel_tblseq
               FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
               WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' AND upper_inf(rel_time_range)
                 AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
               ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
-- lock the table
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE format('LOCK TABLE %s IN %s MODE',
                         v_fullTableName, v_lockMode);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_lock_groups: A deadlock has been trapped while locking tables of group "%".', v_groupNames;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_lock_groups: Too many (5) deadlocks encountered while locking tables of group "%".',v_groupNames;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END, 'END',
              array_to_string(v_groupNames,','), v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_lock_groups$;

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
    v_fullTableName          TEXT;
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
-- purge the emaj history, if needed
      PERFORM emaj._purge_hist();
-- if requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs
      if v_resetLog THEN
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
         SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
           WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
         LOOP
        CASE r_tblsq.rel_kind
          WHEN 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
            v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
            EXECUTE format('ALTER TABLE %s ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg',
                           v_fullTableName);
          WHEN 'S' THEN
-- if it is a sequence, nothing to do
        END CASE;
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

CREATE OR REPLACE FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and
-- sequences.
  DECLARE
    v_groupList              TEXT;
    v_count                  INT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    r_schema                 RECORD;
    r_tblsq                  RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT v_multiGroup AND NOT v_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'BEGIN', array_to_string(v_groupNames,','));
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := '')
      INTO v_groupNames;
-- for all groups already IDLE, generate a warning message and remove them from the list of the groups to process
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames) AND NOT group_is_logging;
    IF v_count = 1 THEN
      RAISE WARNING '_stop_groups: The group "%" is already in IDLE state.', v_groupList;
    END IF;
    IF v_count > 1 THEN
      RAISE WARNING '_stop_groups: The groups "%" are already in IDLE state.', v_groupList;
    END IF;
    SELECT array_agg(DISTINCT group_name) INTO v_groupNames FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames) AND group_is_logging;
-- process the LOGGING groups
    IF v_groupNames IS NOT NULL THEN
-- check and process the supplied mark name (except if the function is called by emaj_force_stop_group())
      IF v_mark IS NULL OR v_mark = '' THEN
        v_mark = 'STOP_%';
      END IF;
      IF NOT v_isForced THEN
        SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
      END IF;
-- OK (no error detected and at least one group in logging state)
-- lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
--   risk of deadlock.
      PERFORM emaj._lock_groups(v_groupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
-- verify that all application schemas for the groups still exists
      FOR r_schema IN
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames)
              AND NOT EXISTS (SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = rel_schema)
            ORDER BY rel_schema
        LOOP
        IF v_isForced THEN
          RAISE WARNING '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        END IF;
      END LOOP;
-- for each relation currently belonging to the groups to process,
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames)
            ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
        CASE r_tblsq.rel_kind
          WHEN 'r' THEN
-- if it is a table, check the table still exists
            PERFORM 1 FROM pg_catalog.pg_namespace, pg_catalog.pg_class
              WHERE  relnamespace = pg_namespace.oid AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
            IF NOT FOUND THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
              ELSE
                RAISE EXCEPTION '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
              END IF;
            ELSE
-- and disable the emaj log and truncate triggers
--   errors are captured so that emaj_force_stop_group() can be silently executed
              v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
              BEGIN
                EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_log_trg',
                               v_fullTableName);
              EXCEPTION
                WHEN undefined_object THEN
                  IF v_isForced THEN
                    RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                      r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                      r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  END IF;
              END;
              BEGIN
                EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_trunc_trg',
                               v_fullTableName);
              EXCEPTION
                WHEN undefined_object THEN
                  IF v_isForced THEN
                    RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                      r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                      r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  END IF;
              END;
            END IF;
          WHEN 'S' THEN
-- if it is a sequence, nothing to do
        END CASE;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
      IF NOT v_isForced THEN
-- if the function is not called by emaj_force_stop_group(), set the stop mark for each group
        PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, TRUE);
-- and set the number of log rows to 0 for these marks
        UPDATE emaj.emaj_mark m SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (v_groupNames)
            AND (mark_group, mark_time_id) IN                        -- select only last mark of each concerned group
                (SELECT mark_group, max(mark_time_id) FROM emaj.emaj_mark
                 WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
      END IF;
-- set all marks for the groups from the emaj_mark table as 'DELETED' to avoid any further rollback and remove protection if any
      UPDATE emaj.emaj_mark SET mark_is_deleted = TRUE, mark_is_rlbk_protected = FALSE
        WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted;
-- update the state of the groups rows from the emaj_group table (the rollback protection of rollbackable groups is reset)
      UPDATE emaj.emaj_group SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (v_groupNames);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT v_multiGroup AND NOT v_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_stop_groups$;

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
-- retrieve the timestamp and the emaj_gid value and the time stamp id of the mark
    SELECT time_last_emaj_gid, mark_time_id INTO v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_mark;
-- drop obsolete old log tables (whose end time stamp is older than the new first mark time stamp)
    FOR r_rel IN
          SELECT DISTINCT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_group = v_groupName AND rel_kind = 'r' AND upper(rel_time_range) <= v_markTimeId
        EXCEPT
          SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r' AND (upper(rel_time_range) > v_markTimeId OR upper_inf(rel_time_range))
          ORDER BY 1,2
        LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- delete obsolete emaj_sequence
-- (the related emaj_seq_hole rows will be deleted just later ; they are not directly linked to a emaj_relation row)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper(rel_time_range) <= v_markTimeId
        AND sequ_time_id < v_markTimeId;
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
-- delete rows from all other log tables
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
      WHERE mark_group = v_groupName AND mark_time_id >= v_markTimeId
        AND mark_logged_rlbk_target_mark IN (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_time_id < v_markTimeId
            );
-- delete oldest marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_time_id < v_markTimeId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
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

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_session_lock$
-- It creates the session row in the emaj_rlbk_session table and then locks all the application tables for the session.
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dblinkSchema           TEXT;
    v_dbLinkCnxStatus        INT;
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_nbRetry                SMALLINT = 0;
    v_ok                     BOOLEAN = FALSE;
    v_nbTbl                  INT;
    r_tbl                    RECORD;
  BEGIN
-- get the rollback characteristics from the emaj_rlbk table
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema, rlbk_groups
      INTO v_isDblinkUsed, v_dblinkSchema, v_groupNames
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- for dblink session > 1, open the connection (the session 1 is already opened)
    IF v_session > 1 THEN
      SELECT v_status INTO v_dbLinkCnxStatus
        FROM emaj._dblink_open_cnx('rlbk#'||v_session);
      IF v_dbLinkCnxStatus < 0 THEN
        RAISE EXCEPTION '_rlbk_session_lock: Error while opening the dblink session #% (Status of the dblink connection attempt = %'
                        ' - see E-Maj documentation).',
          v_session, v_dbLinkCnxStatus;
      END IF;
    END IF;
-- create the session row the emaj_rlbk_session table.
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
             'VALUES (' || v_rlbkId || ',' || v_session || ',' || txid_current() || ',' ||
              quote_literal(clock_timestamp()) || ') RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#'||v_session, v_stmt, v_dblinkSchema);
-- insert lock begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'BEGIN', array_to_string(v_groupNames,','), 'Rollback session #' || v_session);
--
-- acquire locks on tables
--
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- scan all tables of the session, in priority ascending order (priority being defined in emaj_group_def and stored in emaj_relation)
        FOR r_tbl IN
          SELECT quote_ident(rlbp_schema) || '.' || quote_ident(rlbp_table) AS fullName,
                 EXISTS (SELECT 1 FROM emaj.emaj_rlbk_plan rlbp2
                         WHERE rlbp2.rlbp_rlbk_id = v_rlbkId AND rlbp2.rlbp_session = v_session AND
                               rlbp2.rlbp_schema = rlbp1.rlbp_schema AND rlbp2.rlbp_table = rlbp1.rlbp_table AND
                               rlbp2.rlbp_step = 'DIS_LOG_TRG') AS disLogTrg
            FROM emaj.emaj_rlbk_plan rlbp1, emaj.emaj_relation
            WHERE rel_schema = rlbp_schema AND rel_tblseq = rlbp_table AND upper_inf(rel_time_range)
              AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'LOCK_TABLE'
              AND rlbp_session = v_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
--   lock each table
--     The locking level is EXCLUSIVE mode.
--     This blocks all concurrent update capabilities of all tables of the groups (including tables with no logged update to rollback),
--     in order to ensure a stable state of the group at the end of the rollback operation).
--     But these tables can be accessed by SELECT statements during the E-Maj rollback.
          EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE',
                         r_tbl.fullName);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_session_lock: A deadlock has been trapped while locking tables for groups "%".',
            array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      PERFORM emaj._rlbk_error(v_rlbkId, '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables',
                               'rlbk#' || v_session);
      RAISE EXCEPTION '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables for groups "%".',
        array_to_string(v_groupNames,',');
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'END', array_to_string(v_groupNames,','),
              'Rollback session #' || v_session || ': ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
-- trap and record exception during the rollback operation
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(v_rlbkId, 'In _rlbk_session_lock() for session ' || v_session || ': ' || SQLERRM, 'rlbk#'||v_session);
      RAISE;
  END;
$_rlbk_session_lock$;

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_start_mark$
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
-- Before setting the mark, it checks no update has been recorded between the planning step and the locks set
-- for tables for which no rollback was needed at planning time.
-- It also sets the rollback status to EXECUTING.
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dblinkSchema           TEXT;
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_timeId                 BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_markTimeId             BIGINT;
    v_markName               TEXT;
    v_errorMsg               TEXT;
  BEGIN
-- get the dblink usage characteristics for the current rollback
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema
      INTO v_isDblinkUsed, v_dblinkSchema
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get a time stamp for the rollback operation
    v_stmt = 'SELECT emaj._set_time_stamp(''R'')';
    SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
-- update the emaj_rlbk table to record the time stamp and adjust the rollback status
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_time_id = ' || v_timeId || ', rlbk_status = ''EXECUTING''' ||
             ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- get the rollback characteristics from the emaj_rlbk table
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
      v_errorMsg = 'the rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables'
                || ' to process.';
      PERFORM emaj._rlbk_error(v_rlbkId, v_errorMsg, 'rlbk#1');
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If rollback is "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, TRUE, NULL, v_timeId, v_dblinkSchema);
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
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it doesn't own the application tables.
  DECLARE
    v_stmt                   TEXT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_rlbkMarkTimeId         BIGINT;
    v_rlbkTimeId             BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_maxGlobalSeq           BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- get the rollback characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_nb_session, rlbk_dblink_schema, rlbk_is_dblink_used,
           time_last_emaj_gid
      INTO v_groupNames, v_mark, v_rlbkTimeId, v_isLoggedRlbk, v_nbSession, v_dblinkSchema, v_isDblinkUsed,
           v_maxGlobalSeq
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_id = v_rlbkId AND rlbk_time_id = time_id;
-- fetch the mark_time_id, the last global sequence at set_mark time for the first group of the groups array
-- (they all share the same values)
    SELECT mark_time_id, time_last_emaj_gid
      INTO v_rlbkMarkTimeId, v_lastGlobalSeq
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupNames[1] AND mark_name = v_mark;
-- scan emaj_rlbp_plan to get all steps to process that have been affected to this session, in batch_number and step order
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_object_def, rlbp_target_time_id
        FROM emaj.emaj_rlbk_plan,
             (VALUES ('DIS_APP_TRG',1),('DIS_LOG_TRG',2),('DROP_FK',3),('SET_FK_DEF',4),
                     ('RLBK_TABLE',5),('DELETE_LOG',6),('SET_FK_IMM',7),('ADD_FK',8),
                     ('ENA_APP_TRG',9),('ENA_LOG_TRG',10)) AS step(step_name, step_order)
        WHERE rlbp_step::TEXT = step.step_name
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step NOT IN ('LOCK_TABLE','CTRL-DBLINK','CTRL+DBLINK')
          AND rlbp_session = v_session
        ORDER BY rlbp_batch_number, step_order, rlbp_table, rlbp_object
      LOOP
-- update the emaj_rlbk_plan table to set the step start time
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = clock_timestamp() ' ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#'||v_session, v_stmt, v_dblinkSchema);
-- process the step depending on its type
      CASE r_step.rlbp_step
        WHEN 'DIS_APP_TRG' THEN
-- process an application trigger disable
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'DIS_LOG_TRG' THEN
-- process a log trigger disable
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
        WHEN 'DROP_FK' THEN
-- process a foreign key deletion
          EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'SET_FK_DEF' THEN
-- set a foreign key deferred
          EXECUTE format('SET CONSTRAINTS %I.%I DEFERRED',
                         r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'RLBK_TABLE' THEN
-- process a table rollback
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._rlbk_tbl(emaj_relation.*,
                                CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                     ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id)
                                END,
                                v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'DELETE_LOG' THEN
-- process the deletion of log rows
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._delete_log_tbl(emaj_relation.*, r_step.rlbp_target_time_id, v_rlbkTimeId,
                                      CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                           ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp
                                                   WHERE time_id = r_step.rlbp_target_time_id) END)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'SET_FK_IMM' THEN
-- set a foreign key immediate
          EXECUTE format('SET CONSTRAINTS %I.%I IMMEDIATE',
                         r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'ADD_FK' THEN
-- process a foreign key creation
          EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object_def);
        WHEN 'ENA_APP_TRG' THEN
-- process an application trigger enable
          EXECUTE format('ALTER TABLE %I.%I ENABLE %s TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object_def, r_step.rlbp_object);
        WHEN 'ENA_LOG_TRG' THEN
-- process a log trigger enable
          EXECUTE format('ALTER TABLE %I.%I ENABLE TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
      END CASE;
-- update the emaj_rlbk_plan table to set the step duration
-- NB: the computed duration does not include the time needed to update the emaj_rlbk_plan table
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_duration = ' || quote_literal(clock_timestamp()) || ' - rlbp_start_datetime';
      IF r_step.rlbp_step = 'RLBK_TABLE' OR r_step.rlbp_step = 'DELETE_LOG' THEN
--   and the effective number of processed rows for RLBK_TABLE and DELETE_LOG steps
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbRows;
      END IF;
      v_stmt = v_stmt ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#'||v_session, v_stmt, v_dblinkSchema);
    END LOOP;
-- update the emaj_rlbk_session table to set the timestamp representing the end of work for the session
    v_stmt = 'UPDATE emaj.emaj_rlbk_session SET rlbs_end_datetime = clock_timestamp()' ||
             ' WHERE rlbs_rlbk_id = ' || v_rlbkId || ' AND rlbs_session = ' || v_session ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#'||v_session, v_stmt, v_dblinkSchema);
-- close the dblink connection, if any, for session > 1
    IF v_isDblinkUsed AND v_session > 1 THEN
      PERFORM emaj._dblink_close_cnx('rlbk#'||v_session, v_dblinkSchema);
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
-- This is the last step of a rollback group processing. It:
--    - deletes the marks that are no longer available,
--    - deletes the recorded sequences values for these deleted marks
--    - copy data into the emaj_rlbk_stat table,
--    - rollbacks all sequences of the groups,
--    - set the end rollback mark if logged rollback,
--    - and finaly set the operation as COMPLETED or COMMITED.
-- It returns the execution report of the rollback operation (a set of rows).
  DECLARE
    v_stmt                   TEXT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_isAlterGroupAllowed    BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_ctrlDuration           INTERVAL;
    v_markTimeId             BIGINT;
    v_nbSeq                  INT;
    v_markName               TEXT;
    v_messages               TEXT;
    r_msg                    RECORD;
  BEGIN
-- get the rollback characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table,
           rlbk_dblink_schema, rlbk_is_dblink_used, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl,
           v_dblinkSchema, v_isDblinkUsed, v_rlbkDatetime
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_time_id = time_id AND  rlbk_id = v_rlbkId;
-- get the mark timestamp for the 1st group (they all share the same timestamp)
    SELECT mark_time_id INTO v_markTimeId FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- if "unlogged" rollback, delete all marks later than the now rolled back mark and the associated sequences
    IF NOT v_isLoggedRlbk THEN
-- get the highest mark time id of the mark used for rollback, for all groups
-- delete the marks that are suppressed by the rollback (the related sequences have been already deleted by rollback functions)
-- with a logging in the history
      WITH deleted AS (
        DELETE FROM emaj.emaj_mark
          WHERE mark_group = ANY (v_groupNames) AND mark_time_id > v_markTimeId
          RETURNING mark_time_id, mark_group, mark_name),
           sorted_deleted AS (                                       -- the sort is performed to produce stable results in regression tests
        SELECT mark_group, mark_name FROM deleted ORDER BY mark_time_id, mark_group)
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END,
               'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted' FROM sorted_deleted;
-- and reset the mark_log_rows_before_next column for the new last mark
      UPDATE emaj.emaj_mark SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND (mark_group, mark_time_id) IN                -- select only the last non deleted mark of each concerned group
              (SELECT mark_group, max(mark_time_id) FROM emaj.emaj_mark
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
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Prepare the CTRLxDBLINK pseudo step statistic by computing the global time spent between steps
    SELECT coalesce(sum(ctrl_duration),'0'::INTERVAL) INTO v_ctrlDuration FROM (
      SELECT rlbs_session, rlbs_end_datetime - min(rlbp_start_datetime) - sum(rlbp_duration) AS ctrl_duration
        FROM emaj.emaj_rlbk_session rlbs, emaj.emaj_rlbk_plan rlbp
        WHERE rlbp_rlbk_id = rlbs_rlbk_id AND rlbp_session = rlbs_session
          AND rlbs_rlbk_id = v_rlbkID
        GROUP BY rlbs_session, rlbs_end_datetime ) AS t;
-- report duration statistics into the emaj_rlbk_stat table
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_stat (rlbt_step, rlbt_schema, rlbt_table, rlbt_object,' ||
             '      rlbt_rlbk_id, rlbt_quantity, rlbt_duration)' ||
--   copy elementary steps for RLBK_TABLE, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_rlbk_id,' ||
             '      rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 6 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''DIS_APP_TRG'',''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_APP_TRG'',''ENA_LOG_TRG'') ' ||
             '    GROUP BY 1, 2, 3, 4, 5' ||
             '  UNION ALL ' ||
--   and the final CTRLxDBLINK pseudo step statistic
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      rlbp_estimated_quantity, ' || quote_literal(v_ctrlDuration) ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''CTRL+DBLINK'',''CTRL-DBLINK'') ' ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- rollback the application sequences belonging to the groups
-- warning, this operation is not transaction safe (that's why it is placed at the end of the operation)!
-- if the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time
    PERFORM emaj._rlbk_seq(t.*, greatest(v_markTimeId, lower(t.rel_time_range)))
      FROM (SELECT * FROM emaj.emaj_relation
              WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automatically set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_DONE';
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
        VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'NOTICE',
                'Rollback id ' || v_rlbkId, rlbk_message);
      v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
      IF v_isAlterGroupAllowed IS NOT NULL THEN
        RETURN NEXT;
      END IF;
    END IF;
-- then, for new style calling functions, return the WARNING messages for any elementary action from alter group operations that has not
-- been rolled back
    IF v_isAlterGroupAllowed IS NOT NULL THEN
      rlbk_severity = 'Warning';
      FOR r_msg IN
-- steps are splitted into 2 groups to filter them differently
        SELECT altr_time_id, altr_step, altr_schema, altr_tblseq,
               (CASE altr_step
                  WHEN 'ADD_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'ADD_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
            FROM (
-- suppress duplicate ADD_TBL / REMOVE_TBL or ADD_SEQ / REMOVE_SEQ for same table or sequence, by keeping the most recent step
              SELECT altr_schema, altr_tblseq, altr_time_id, altr_step FROM (
                SELECT altr_schema, altr_tblseq, altr_time_id, altr_step,
                       rank() OVER (PARTITION BY altr_schema, altr_tblseq ORDER BY altr_time_id DESC) AS altr_rank
                FROM emaj.emaj_alter_plan
                WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> '' AND altr_rlbk_id IS NULL AND
                      altr_step IN ('ADD_TBL','ADD_SEQ','REMOVE_TBL','REMOVE_SEQ','MOVE_TBL','MOVE_SEQ')
                ) AS t1
              WHERE altr_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE altr_time_id = time_id
        UNION
        SELECT altr_time_id, altr_step, altr_schema, altr_tblseq,
               (CASE altr_step
                  WHEN 'CHANGE_REL_PRIORITY' THEN
                    'Tables group change not rolled back: E-Maj priority for '
                    || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN
                    'Tables group change not rolled back: E-Maj log schema for '
                    || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN
                    'Tables group change not rolled back: E-Maj names prefix for '
                    || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
                    'Tables group change not rolled back: log data tablespace for '
                    || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
                    'Tables group change not rolled back: log index tablespace for '
                    || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  ELSE altr_step::TEXT || ' / ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  END)::TEXT AS message
            FROM (
-- suppress duplicates for other steps for each table or sequence
              SELECT altr_schema, altr_tblseq, altr_time_id, altr_step FROM (
                SELECT altr_schema, altr_tblseq, altr_time_id, altr_step,
                       rank() OVER (PARTITION BY altr_schema, altr_tblseq ORDER BY altr_time_id DESC) AS altr_rank
                FROM emaj.emaj_alter_plan
                WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> '' AND altr_rlbk_id IS NULL AND
                      altr_step NOT IN ('ADD_TBL','ADD_SEQ','REMOVE_TBL','REMOVE_SEQ','MOVE_TBL','MOVE_SEQ')
                ) AS t1
              WHERE altr_rank = 1
            ) AS t2
          ORDER BY altr_time_id, altr_step, altr_schema, altr_tblseq
        LOOP
          INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
            VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'WARNING', 'Rollback id ' || v_rlbkId,
                    r_msg.message);
          rlbk_message = r_msg.message;
          v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
          RETURN NEXT;
      END LOOP;
    END IF;
-- update the alter steps that have been covered by the rollback
    UPDATE emaj.emaj_alter_plan SET altr_rlbk_id = v_rlbkId
      WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_rlbk_id IS NULL;
-- update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the result message
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = '''
          || CASE WHEN v_isDblinkUsed THEN 'COMPLETED' ELSE 'COMMITTED' END
          || ''', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_messages || ']' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- close the dblink connection, if any
    IF v_isDblinkUsed THEN
      PERFORM emaj._dblink_close_cnx('rlbk#1', v_dblinkSchema);
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

CREATE OR REPLACE FUNCTION emaj._delete_between_marks_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT,
                                                            OUT v_nbMark INT, OUT v_nbTbl INT)
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
    v_firstMarkGlobalSeq     BIGINT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkGlobalSeq      BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_nbUpd                  BIGINT;
    r_rel                    RECORD;
  BEGIN
-- retrieve the timestamp and the emaj_gid value and the time stamp id of the first mark
    SELECT time_last_emaj_gid, mark_time_id INTO v_firstMarkGlobalSeq, v_firstMarkTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_firstMark;
-- retrieve the timestamp and the emaj_gid value and the time stamp id of the last mark
    SELECT time_last_emaj_gid, mark_time_id INTO v_lastMarkGlobalSeq, v_lastMarkTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_lastMark;
-- delete rows from all log tables (no need to try to delete if v_firstMarkGlobalSeq and v_lastMarkGlobalSeq are equal)
    v_nbTbl = 0;
    IF v_firstMarkGlobalSeq < v_lastMarkGlobalSeq THEN
-- loop on all tables that belonged to the group at the end of the period
      FOR r_rel IN
          SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
            WHERE rel_group = v_groupName AND rel_kind = 'r'
              AND rel_time_range @> v_lastMarkTimeId
            ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- delete log rows
        EXECUTE format('DELETE FROM %s WHERE emaj_gid > $1 AND emaj_gid <= $2',
                       r_rel.log_table_name)
          USING v_firstMarkGlobalSeq, v_lastMarkGlobalSeq;
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
      WHERE mark_group = v_groupName AND mark_time_id >= v_lastMarkTimeId
        AND mark_logged_rlbk_target_mark IN (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_time_id > v_firstMarkTimeId AND mark_time_id < v_lastMarkTimeId
            );
-- set the mark_log_rows_before_next of the first mark to 0
    UPDATE emaj.emaj_mark SET mark_log_rows_before_next = 0
      WHERE mark_group = v_groupName AND mark_name = v_firstMark;
-- and finaly delete all intermediate marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_time_id > v_firstMarkTimeId AND mark_time_id < v_lastMarkTimeId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
    RETURN;
  END;
$_delete_between_marks_group$;

CREATE OR REPLACE FUNCTION emaj._reset_groups(v_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_reset_groups$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences images.
-- It is called by emaj_reset_group(), emaj_start_group() and emaj_alter_group() functions.
-- Input: group names array
-- Output: number of processed tables and sequences
-- There is no check of the groups state (this is done by callers).
-- The function is defined as SECURITY DEFINER so that an emaj_adm role can truncate log tables.
  DECLARE
    v_eventTriggers          TEXT[];
    r_rel                    RECORD;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- delete all marks for the groups from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames);
-- delete emaj_sequence rows related to the tables of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence
        AND rel_group = ANY (v_groupNames) AND rel_kind = 'r';
-- delete all sequence holes for the tables of the groups
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND rel_group = ANY (v_groupNames) AND rel_kind = 'r';
-- delete emaj_sequence rows related to the sequences of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_schema = sequ_schema AND rel_tblseq = sequ_name AND
            rel_group = ANY (v_groupNames) AND rel_kind = 'S';
-- drop obsolete emaj objects for removed tables
    FOR r_rel IN
          SELECT DISTINCT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' AND NOT upper_inf(rel_time_range)
        EXCEPT
          SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r' AND upper_inf(rel_time_range)
          ORDER BY 1,2
        LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- keep a trace of the relation group ownership history
--   and finaly delete the old versions of emaj_relation rows (those with a not infinity upper bound)
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames) AND NOT upper_inf(rel_time_range)
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
        FROM deleted;
-- truncate remaining log tables for application tables
    FOR r_rel IN
        SELECT rel_log_schema, rel_log_table, rel_log_sequence FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
--   truncate the log table
      EXECUTE format('TRUNCATE %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN sum(group_nb_table)+sum(group_nb_sequence) FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames);
  END;
$_reset_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark.
-- For log tables, files contain all rows related to the time frame, sorted on emaj_gid.
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no end mark is specified.
--   They contain one row per sequence belonging to the group at the related time
--   (a sequence may belong to a group at the start mark time and not at the end mark time for instance).
-- To do its job, the function performs COPY TO statement, using the options provided by the caller.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability:
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
          WHERE rel_group = v_groupName AND rel_kind = 'r'
            AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
--   build names
      v_fileName = v_dir || '/' || translate(r_tblsq.rel_schema || '_' || r_tblsq.rel_log_table || '.snap', E' /\\$<>*', '_______');
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
             '   AND rel_time_range @> ' || v_firstMarkTimeId || '::BIGINT' ||
             '   AND sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
             ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
             coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
-- prepare the file for sequences state at end mark
-- generate the full file name and the COPY statement
    IF v_noSuppliedLastMark THEN
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_'
                || to_char(v_lastMarkTs,'HH24.MI.SS.MS'), E' /\\$<>*', '_______');
      v_stmt = 'SELECT seq.* FROM emaj.emaj_relation, LATERAL emaj._get_current_sequence_state(rel_schema, rel_tblseq, ' ||
                                                                                               v_lastMarkTimeId || ') AS seq' ||
               '  WHERE upper_inf(rel_time_range) AND rel_group = ' || quote_literal(v_groupName) || ' AND rel_kind = ''S''';
    ELSE
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || v_lastMark, E' /\\$<>*', '_______');
      v_stmt = 'SELECT emaj_sequence.*'
            || ' FROM emaj.emaj_sequence, emaj.emaj_relation'
            || ' WHERE sequ_time_id = ' || v_lastMarkTimeId
            || '   AND rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName)
            || '   AND (rel_time_range @> ' || v_lastMarkTimeId || '::BIGINT'
            || '        OR upper(rel_time_range) = ' || v_lastMarkTimeId || '::BIGINT)'
            || '   AND sequ_schema = rel_schema AND sequ_name = rel_tblseq'
            || ' ORDER BY sequ_schema, sequ_name';
    END IF;
-- and create the file
    EXECUTE format('COPY (%s) TO %L %s',
                   v_stmt, v_fileName, coalesce (v_copyOptions, ''));
-- create the _INFO file to keep general information about the snap operation
    EXECUTE format('COPY (SELECT %L) TO %L %s',
                  'E-Maj log tables snap of group ' || v_groupName || ' between marks ' || v_firstMark || ' and ' ||
                    CASE WHEN v_noSuppliedLastMark THEN 'current state' ELSE v_lastMark END || ' at ' || statement_timestamp(),
                  v_dir || '/_INFO', coalesce (v_copyOptions, ''));
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbFile || ' generated files');
    RETURN v_nbFile;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT,
                                                v_location TEXT, v_tblseqs TEXT[])
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET standard_conforming_strings = ON SET search_path = pg_catalog, pg_temp AS
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
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '')
      INTO v_groupNames;
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
            WHERE rel_time_range @> v_firstMarkTimeId AND rel_group = ANY (v_groupNames)    -- tables/sequences that belong to their group
                                                                                            -- at the start mark time
          ) AS t2;
        IF v_count > 0 THEN
          IF v_count = 1 THEN
            RAISE EXCEPTION '_gen_sql_groups: 1 table/sequence (%) did not belong to any of the selected tables groups at % mark time.',
              v_tblseqErr, v_firstMark;
          ELSE
            RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) did not belong to any of the selected tables groups at % mark time.',
              v_count, v_tblseqErr, v_firstMark;
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
        EXECUTE format('COPY (SELECT ''-- _gen_sql_groups() function in progress - started at %s'') TO %L',
                       statement_timestamp(), v_location);
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
-- for each application table referenced in the emaj_relation table, process the related log table, by calling the _gen_sql_tbl() function
      FOR r_rel IN
          SELECT * FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                               -- tables belonging to the groups
              AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))        -- filtered or not by the user
              AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId,                            -- only tables having updates to process
                                    least(v_lastMarkTimeId, upper(rel_time_range))) > 0
            ORDER BY rel_priority, rel_schema, rel_tblseq
          LOOP
        SELECT emaj._gen_sql_tbl(r_rel, v_firstEmajGid, v_lastEmajGid) INTO v_nbSQL;
        v_cumNbSQL = v_cumNbSQL + v_nbSQL;
      END LOOP;
-- process sequences
      v_nbSeq = 0;
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_time_range FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              AND rel_time_range @> v_firstMarkTimeId                                -- sequences belonging to the groups at the start mark
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))         -- filtered or not by the user
            ORDER BY rel_priority DESC, rel_schema DESC, rel_tblseq DESC
          LOOP
        v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- no supplied last mark and the sequence currently belongs to its group, so get current sequence characteritics
          IF emaj._pg_version_num() >= 100000 THEN
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                 || ''' || '' RESTART '' || CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END || '' '
                 || 'START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' '
                 || 'MINVALUE '' || min_value || '' CACHE '' || cache_size || '
                 || 'CASE WHEN NOT cycle THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                 || 'FROM ' || v_fullSeqName  || ' rel, pg_catalog.pg_sequences '
                 || ' WHERE schemaname = ' || quote_literal(r_tblsq.rel_schema) || ' AND sequencename = '
                 || quote_literal(r_tblsq.rel_tblseq) INTO v_rqSeq;
          ELSE
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                 || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' '
                 || 'START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' '
                 || 'MINVALUE '' || min_value || '' CACHE '' || cache_value || '
                 || 'CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                 || 'FROM ' || v_fullSeqName INTO v_rqSeq;
          END IF;
        ELSE
-- a last mark is supplied, or the sequence does not belong to its groupe anymore, so get sequence characteristics from the emaj_sequence
-- table
          v_endTimeId = CASE WHEN upper_inf(r_tblsq.rel_time_range) OR v_lastMarkTimeId < upper(r_tblsq.rel_time_range)
                               THEN v_lastMarkTimeId
                             ELSE upper(r_tblsq.rel_time_range) END;
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '
               || ''' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '
               || ''' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || '
               || 'CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_time_id = ' || v_endTimeId INTO v_rqSeq;
        END IF;
-- insert into temp table
        v_nbSeq = v_nbSeq + 1;
        EXECUTE 'INSERT INTO emaj_temp_script '
                '  SELECT NULL, -1 * $1, txid_current(), $2'
          USING v_nbSeq, v_rqSeq;
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
      INSERT INTO emaj_temp_script SELECT 0, 10, 0, 'SET standard_conforming_strings = OFF;';
      INSERT INTO emaj_temp_script SELECT 0, 11, 0, 'SET escape_string_warning = OFF;';
      INSERT INTO emaj_temp_script SELECT 0, 20, 0, 'BEGIN TRANSACTION;';
      INSERT INTO emaj_temp_script SELECT NULL, 1, txid_current(), 'COMMIT;';
      INSERT INTO emaj_temp_script SELECT NULL, 10, txid_current(), 'RESET standard_conforming_strings;';
      INSERT INTO emaj_temp_script SELECT NULL, 11, txid_current(), 'RESET escape_string_warning;';
-- write the SQL script on the external file
      EXECUTE format('COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid ) TO %L',
                     v_location);
-- drop the temporary table
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

CREATE OR REPLACE FUNCTION emaj._event_trigger_table_rewrite_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_table_rewrite_fnct$
-- This function is called by the emaj_table_rewrite_trg event trigger.
-- The function blocks any ddl operation that leads to a table rewrite for:
--   - an application table registered into an active (not stopped) E-Maj group,
--   - an E-Maj log table.
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when altering their tables.
  DECLARE
    v_tableSchema            TEXT;
    v_tableName              TEXT;
    v_groupName              TEXT;
  BEGIN
-- get the schema and table names of the altered table
    SELECT nspname, relname INTO v_tableSchema, v_tableName FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid AND pg_class.oid = pg_event_trigger_table_rewrite_oid();
-- look at the emaj_relation table to verify that the table being rewritten does not belong to any active (not stopped) group
    SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
      WHERE rel_schema = v_tableSchema AND rel_tblseq = v_tableName AND upper_inf(rel_time_range)
        AND group_name = rel_group AND group_is_logging;
    IF FOUND THEN
-- the table is an application table that belongs to a group, so raise an exception
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the application table "%.%" structure. But the table belongs to the'
                      ' active tables group "%".', v_tableSchema, v_tableName , v_groupName;
    END IF;
-- look at the emaj_relation table to verify that the table being rewritten is not a known log table
    SELECT rel_group INTO v_groupName FROM emaj.emaj_relation
      WHERE rel_log_schema = v_tableSchema AND rel_log_table = v_tableName;
    IF FOUND THEN
-- the table is an E-Maj log table, so raise an exception
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the log table "%.%" structure. But the table belongs to the tables'
                      ' group "%".', v_tableSchema, v_tableName , v_groupName;
    END IF;
  END;
$_event_trigger_table_rewrite_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_table_rewrite_fnct() IS
$$E-Maj extension: support of the emaj_table_rewrite_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj._disable_event_triggers()
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_disable_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_disable_event_triggers_protection() function.
-- Output: array of effectively disabled event trigger names. It can be reused as input when calling _enable_event_triggers().
  DECLARE
    v_eventTrigger           TEXT;
    v_eventTriggers          TEXT[] = ARRAY[]::TEXT[];
  BEGIN
-- build the event trigger names array from the pg_event_trigger table
-- (pg_event_trigger table doesn't exists in 9.2- postgres versions)
-- A single operation like emaj_alter_groups() may call the function several times. But this is not an issue as only enabled triggers are
-- disabled.
    SELECT coalesce(array_agg(evtname ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled <> 'D';
-- disable each event trigger
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I DISABLE',
                     v_eventTrigger);
    END LOOP;
    RETURN v_eventTriggers;
  END;
$_disable_event_triggers$;

CREATE OR REPLACE FUNCTION emaj._enable_event_triggers(v_eventTriggers TEXT[])
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_enable_event_triggers$
-- This function enables all event triggers supplied as parameter.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_enable_event_triggers_protection() function.
-- Input: array of event trigger names to enable.
-- Output: same array.
  DECLARE
    v_eventTrigger           TEXT;
  BEGIN
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I ENABLE',
                     v_eventTrigger);
    END LOOP;
    RETURN v_eventTriggers;
  END;
$_enable_event_triggers$;

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
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';

-- insert the upgrade end record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 3.1.0 completed');

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
