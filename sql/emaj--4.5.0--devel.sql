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
DROP FUNCTION IF EXISTS emaj._dblink_open_cnx(P_CNXNAME TEXT,OUT P_STATUS INT,OUT P_SCHEMA TEXT);
DROP FUNCTION IF EXISTS emaj._create_log_schema(P_SCHEMA TEXT,P_FUNCTION TEXT);
DROP FUNCTION IF EXISTS emaj._handle_trigger_fk_tbl(P_ACTION TEXT,P_FULLTABLENAME TEXT,P_OBJECTNAME TEXT,P_OBJECTDEF TEXT);
DROP FUNCTION IF EXISTS emaj._copy_from_file(P_DESTINATION_TABLE TEXT,P_LOCATION TEXT);
DROP FUNCTION IF EXISTS emaj._copy_to_file(P_SOURCE TEXT,P_LOCATION TEXT,P_COPYOPTIONS TEXT,P_REMOVEEMPTYFILE BOOLEAN,P_PSQLSCRIPT BOOLEAN);
DROP FUNCTION IF EXISTS emaj._get_current_sequence_state(P_SCHEMA TEXT,P_SEQUENCE TEXT,P_TIMEID BIGINT);
DROP FUNCTION IF EXISTS emaj.emaj_create_group(P_GROUPNAME TEXT,P_ISROLLBACKABLE BOOLEAN);
DROP FUNCTION IF EXISTS emaj.emaj_set_mark_group(P_GROUPNAME TEXT,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_set_mark_groups(P_GROUPNAMES TEXT[],P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._set_mark_groups(P_GROUPNAMES TEXT[],P_MARK TEXT,P_MULTIGROUP BOOLEAN,P_EVENTTORECORD BOOLEAN,P_LOGGEDRLBKTARGETMARK TEXT,P_TIMEID BIGINT,P_DBLINKSCHEMA TEXT);
DROP FUNCTION IF EXISTS emaj._gen_sql_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT,P_LOCATION TEXT,P_TBLSEQS TEXT[]);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(p_cnxName TEXT, p_callerRole TEXT, OUT p_status INT, OUT p_schema TEXT)
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
--         caller role to check for permissions
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
-- The function is defined as SECURITY DEFINER because reading the unix_socket_directories GUC needs to be at least a member
--   of pg_read_all_settings.
  DECLARE
    v_nbCnx                  INT;
    v_userPassword           TEXT;
    v_connectString          TEXT;
  BEGIN
-- Look for the schema holding the dblink functions.
--   (NULL if the dblink_connect_u function is not available, which should not happen)
    SELECT nspname INTO p_schema
      FROM pg_catalog.pg_proc
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
      WHERE proname = 'dblink_connect_u'
      LIMIT 1;
    IF NOT FOUND THEN
      p_status = -1;                      -- dblink is not installed
    ELSIF NOT has_function_privilege(p_callerRole, quote_ident(p_schema) || '.dblink_connect_u(text, text)', 'execute') THEN
      p_status = -3;                      -- current role has not the execute rights on dblink functions
    ELSIF (p_cnxName LIKE 'rlbk#%' OR p_cnxName = 'test') AND
          current_setting('transaction_isolation') <> 'read committed' THEN
      p_status = -4;                      -- 'rlbk#*' connection (used for rollbacks) must only come from a
                                          --   READ COMMITTED transaction
    ELSE
      EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                     p_cnxName, p_schema);
      GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
      IF v_nbCnx > 0 THEN
-- Dblink is usable, so search the requested connection name in dblink connections list.
        p_status = 0;                       -- the requested connection is already open
      ELSE
-- So, get the 'dblink_user_password' parameter if exists, from emaj_param.
        SELECT param_value_text INTO v_userPassword
          FROM emaj.emaj_param
          WHERE param_key = 'dblink_user_password';
        IF NOT FOUND THEN
          p_status = -5;                    -- no 'dblink_user_password' parameter is defined in the emaj_param table
        ELSE
-- Build the connect string.
          v_connectString = 'host='
                         || CASE
                              WHEN current_setting('listen_addresses') = ''
                                THEN coalesce(substring(current_setting('unix_socket_directories') from '(.*?)\s*,'),
                                              current_setting('unix_socket_directories'))
                              ELSE 'localhost'
                            END
                         || ' port=' || current_setting('port')
                         || ' dbname=' || current_database()
                         || ' ' || v_userPassword;
-- Try to connect.
          BEGIN
            EXECUTE format('SELECT %I.dblink_connect_u(%L ,%L)',
                           p_schema, p_cnxName, v_connectString);
            p_status = 1;                   -- the connection is successful
          EXCEPTION
            WHEN OTHERS THEN
              p_status = -6;                -- the connection attempt failed
          END;
        END IF;
      END IF;
    END IF;
-- For connections used for rollback operations, record the dblink connection attempt in the emaj_hist table.
    IF substring(p_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('DBLINK_OPEN_CNX', p_cnxName, 'Status = ' || p_status);
    END IF;
--
    RETURN;
  END;
$_dblink_open_cnx$;

CREATE OR REPLACE FUNCTION emaj._create_log_schemas(p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_schemas$
-- The function creates all log schemas that will be needed to create new log tables.
-- The function is called at tables groups configuration import.
-- Input: calling function to record into the emaj_hist table
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted the CREATE privilege on
--   the current database.
  DECLARE
    r_schema                 RECORD;
  BEGIN
    FOR r_schema IN
        SELECT DISTINCT 'emaj_' || tmp_schema AS log_schema
          FROM tmp_app_table
          WHERE NOT EXISTS                                                                -- minus those already created
                  (SELECT 0
                     FROM emaj.emaj_schema
                     WHERE sch_name = 'emaj_' || tmp_schema)
          ORDER BY 1
    LOOP
-- Check that the schema doesn't already exist.
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_namespace
              WHERE nspname = r_schema.log_schema
           ) THEN
        RAISE EXCEPTION '_create_log_schemas: The schema "%" should not exist. Drop it manually.',r_schema.log_schema;
      END IF;
-- Create the schema and give the appropriate rights.
      EXECUTE format('CREATE SCHEMA %I AUTHORIZATION emaj_adm',
                     r_schema.log_schema);
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                     r_schema.log_schema);
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
      INSERT INTO emaj.emaj_schema (sch_name)
        VALUES (r_schema.log_schema);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (p_function, 'LOG_SCHEMA CREATED', quote_ident(r_schema.log_schema));
    END LOOP;
--
    RETURN;
  END;
$_create_log_schemas$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(p_schema TEXT, p_tables TEXT[], p_group TEXT, p_properties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB p_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted the CREATE privilege on
--   the current database, needed to create log schemas.
  DECLARE
    v_function               TEXT;
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_priority               INT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_ignoredTriggers        TEXT[];
    v_ignoredTrgProfiles     TEXT[];
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_logSchema              TEXT;
    v_selectedIgnoredTrgs    TEXT[];
    v_selectConditions       TEXT;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_nbAssignedTbl          INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters.
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_group], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_group;
-- Check the supplied schema exists and is not an E-Maj schema.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" does not exist.', p_schema;
    END IF;
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_schema
            WHERE sch_name = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" is an E-Maj schema.', p_schema;
    END IF;
-- Check tables.
    IF NOT p_arrayFromRegex THEN
-- From the tables array supplied by the user, remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that application tables exist.
      WITH tables AS (
        SELECT unnest(p_tables) AS table_name
      )
      SELECT string_agg(quote_ident(table_name), ', ') INTO v_list
        FROM
          (SELECT table_name
             FROM tables
             WHERE NOT EXISTS
                     (SELECT 0
                        FROM pg_catalog.pg_class
                             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        WHERE nspname = p_schema
                          AND relname = table_name
                          AND relkind IN ('r','p')
                     )
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard partitioned application tables (only elementary partitions can be managed by E-Maj).
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'p';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are partitionned tables (only elementary partitions are supported'
                        ' by E-Maj).', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some partitionned tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
            (  SELECT unnest(p_tables)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_table);
      END IF;
    END IF;
-- Check or discard TEMP tables.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'r'
        AND relpersistence = 't';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are TEMP tables.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some TEMP tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
           (  SELECT unnest(p_tables)
            EXCEPT
              SELECT unnest(v_array)
           ) AS t(remaining_table);
      END IF;
    END IF;
-- If the group is ROLLBACKABLE, perform additional checks or filters (a PK, not UNLOGGED).
    IF v_groupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_assign_tables');
    END IF;
-- Check or discard tables already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', '), array_agg(rel_tblseq) INTO v_list, v_array
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_tables)
        AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) already belong to a group.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some tables already belonging to a group (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
            (  SELECT unnest(p_tables)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_table);
      END IF;
    END IF;
-- Check and extract the tables JSON properties.
    IF p_properties IS NOT NULL THEN
      SELECT * INTO v_priority, v_logDatTsp, v_logIdxTsp, v_ignoredTriggers, v_ignoredTrgProfiles
        FROM emaj._check_json_table_properties(p_properties);
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(array[p_group], p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_assign_tables: No table to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
--   Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--   vacuum operation.
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
--   And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Create new log schemas if needed.
      v_logSchema = 'emaj_' || p_schema;
      IF NOT EXISTS
           (SELECT 0
              FROM emaj.emaj_schema
              WHERE sch_name = v_logSchema
           ) THEN
-- Check that the schema doesn't already exist.
        IF EXISTS
             (SELECT 0
                FROM pg_catalog.pg_namespace
                WHERE nspname = v_logSchema
             ) THEN
          RAISE EXCEPTION '_assign_tables: The schema "%" should not exist. Drop it manually.',v_logSchema;
        END IF;
-- Create the schema and give the appropriate rights.
        EXECUTE format('CREATE SCHEMA %I AUTHORIZATION emaj_adm',
                       v_logSchema);
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                       v_logSchema);
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
        INSERT INTO emaj.emaj_schema (sch_name)
          VALUES (v_logSchema);
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES (CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END, 'LOG_SCHEMA CREATED', quote_ident(v_logSchema));
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively create the log components for each table.
--   Build the SQL conditions to use in order to build the array of "triggers to ignore at rollback time" for each table.
      IF v_ignoredTriggers IS NOT NULL OR v_ignoredTrgProfiles IS NOT NULL THEN
--   Build the condition on trigger names using the ignored_triggers parameters.
        IF v_ignoredTriggers IS NOT NULL THEN
          v_selectConditions = 'tgname = ANY (' || quote_literal(v_ignoredTriggers) || ') OR ';
        ELSE
          v_selectConditions = '';
        END IF;
--   Build the regexp conditions on trigger names using the ignored_triggers_profile parameters.
        IF v_ignoredTrgProfiles IS NOT NULL THEN
          SELECT v_selectConditions || string_agg('tgname ~ ' || quote_literal(profile), ' OR ')
            INTO v_selectConditions
            FROM unnest(v_ignoredTrgProfiles) AS profile;
        ELSE
          v_selectConditions = v_selectConditions || 'FALSE';
        END IF;
      END IF;
-- Process each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Check that the triggers listed in ignored_triggers property exists for the table.
        SELECT string_agg(quote_ident(trigger_name), ', ') INTO v_list
          FROM
            (  SELECT trigger_name
                 FROM unnest(v_ignoredTriggers) AS trigger_name
             EXCEPT
               SELECT tgname
                 FROM pg_catalog.pg_trigger
                      JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                      JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                 WHERE nspname = p_schema
                   AND relname = v_oneTable
                   AND tgconstraint = 0
                   AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
            ) AS t;
        IF v_list IS NOT NULL THEN
          RAISE EXCEPTION '_assign_tables: some triggers (%) have not been found in the table %.%.',
                          v_list, quote_ident(p_schema), quote_ident(v_oneTable);
        END IF;
-- Build the array of "triggers to ignore at rollback time".
        IF v_selectConditions IS NOT NULL THEN
          EXECUTE format(
            $$SELECT array_agg(tgname ORDER BY tgname)
                FROM pg_catalog.pg_trigger
                     JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                     JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                WHERE nspname = %L
                  AND relname = %L
                  AND tgconstraint = 0
                  AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
                  AND (%s)
            $$, p_schema, v_oneTable, v_selectConditions)
            INTO v_selectedIgnoredTrgs;
        END IF;
-- Create the table.
        PERFORM emaj._add_tbl(p_schema, v_oneTable, p_group, v_priority, v_logDatTsp, v_logIdxTsp, v_selectedIgnoredTrgs,
                              v_groupIsLogging, v_timeId, v_function);
        v_nbAssignedTbl = v_nbAssignedTbl + 1;
      END LOOP;
-- Enable previously disabled event triggers
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the group characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (
              SELECT count(*)
                FROM emaj.emaj_relation
                WHERE rel_group = group_name
                  AND upper_inf(rel_time_range)
                  AND rel_kind = 'r'
                             )
        WHERE group_name = p_group;
-- If the group is logging, check foreign keys with tables outside the groups (otherwise the check will be done at the group start time).
      IF v_groupIsLogging THEN
        PERFORM emaj._check_fk_groups(array[p_group]);
      END IF;
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbAssignedTbl || ' tables assigned to the group ' || p_group);
--
    RETURN v_nbAssignedTbl;
  END;
$_assign_tables$;

CREATE OR REPLACE FUNCTION emaj._remove_tables(p_schema TEXT, p_tables TEXT[], p_mark TEXT, p_multiTable BOOLEAN,
                                               p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_tables$
-- The function effectively removes tables from their tables group.
-- Inputs: schema, array of table names, mark to set if for logging groups,
--         boolean to indicate whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively removed to the tables group
  DECLARE
    v_function               TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_logSchema              TEXT;
    v_nbRemovedTbl           INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the tables list.
    IF NOT p_arrayFromRegex THEN
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', FALSE);
    END IF;
-- Get and lock the tables groups and logging groups holding these tables.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, NULL);
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_remove_tables: No table to process.';
    ELSE
      v_logSchema = 'emaj_' || p_schema;
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the log components for each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Get some characteristics of the group that holds the table.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneTable
            AND upper_inf(rel_time_range);
-- Drop this table.
        PERFORM emaj._remove_tbl(p_schema, v_oneTable, v_groupName, v_groupIsLogging, v_timeId, v_function);
        v_nbRemovedTbl = v_nbRemovedTbl + 1;
      END LOOP;
-- Drop the log schema if it is now useless.
      IF NOT EXISTS
           (SELECT 0
              FROM emaj.emaj_relation
              WHERE rel_log_schema = v_logSchema
           ) THEN
-- Drop the schema.
        EXECUTE format('DROP SCHEMA %I',
                       v_logSchema);
-- And record the schema drop into the emaj_schema and the emaj_hist tables.
        DELETE FROM emaj.emaj_schema
          WHERE sch_name = v_logSchema;
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES (CASE WHEN p_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END, 'LOG_SCHEMA DROPPED', quote_ident(v_logSchema));
      END IF;
-- Enable previously disabled event triggers.
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (
              SELECT count(*)
                FROM emaj.emaj_relation
                WHERE rel_group = group_name
                  AND upper_inf(rel_time_range)
                  AND rel_kind = 'r'
                             )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbRemovedTbl || ' tables removed from their groups');
--
    RETURN v_nbRemovedTbl;
  END;
$_remove_tables$;

CREATE OR REPLACE FUNCTION emaj._move_tables(p_schema TEXT, p_tables TEXT[], p_newGroup TEXT, p_mark TEXT, p_multiTable BOOLEAN,
                                             p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_move_tables$
-- The function effectively moves tables from their tables group to another tables group.
-- Inputs: schema, array of table names, new group name, mark to set if for logging groups,
--         boolean to indicate whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively moved to the tables group
  DECLARE
    v_function               TEXT;
    v_newGroupIsRollbackable BOOLEAN;
    v_newGroupIsLogging      BOOLEAN;
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_nbAuditOnlyGroups      INT;
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_oneTable               TEXT;
    v_nbMovedTbl             INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'MOVE_TABLES' ELSE 'MOVE_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_newGroup], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_rollbackable, group_is_logging INTO v_newGroupIsRollbackable, v_newGroupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_newGroup;
-- Check the tables list.
    IF NOT p_arrayFromRegex THEN
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', TRUE);
    END IF;
-- Remove tables that already belong to the new group.
    SELECT array_agg(rel_tblseq ORDER BY rel_tblseq) FILTER (WHERE rel_group <> p_newGroup),
           string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq) FILTER (WHERE rel_group = p_newGroup)
      INTO p_tables, v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_tables)
        AND upper_inf(rel_time_range);
-- Warn only if the tables list has been supplied by the user.
    IF v_list IS NOT NULL AND NOT p_arrayFromRegex THEN
      RAISE WARNING '_move_tables: In schema "%", some tables (%) already belong to the tables group "%".', p_schema, v_list, p_newGroup;
    END IF;
-- Get and lock the tables groups and logging groups holding these tables, and count the number of AUDIT_ONLY groups.
    SELECT p_groups, p_loggingGroups, p_nbAuditOnlyGroups INTO v_groups, v_loggingGroups, v_nbAuditOnlyGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, p_newGroup);
-- If at least 1 source tables group is of type AUDIT_ONLY and the target tables group is ROLLBACKABLE, add some checks on tables.
-- They may be incompatible with ROLLBACKABLE groups.
    IF v_nbAuditOnlyGroups > 0 AND v_newGroupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_move_tables');
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_move_tables: No table to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively move each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Get some characteristics of the group that holds the table before the move.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneTable
            AND upper_inf(rel_time_range);
-- Move this table.
        PERFORM emaj._move_tbl(p_schema, v_oneTable, v_groupName, v_groupIsLogging, p_newGroup, v_newGroupIsLogging, v_timeId, v_function);
        v_nbMovedTbl = v_nbMovedTbl + 1;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'r'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbMovedTbl || ' tables moved to the tables group ' || p_newGroup);
--
    RETURN v_nbMovedTbl;
  END;
$_move_tables$;

CREATE OR REPLACE FUNCTION emaj._modify_tables(p_schema TEXT, p_tables TEXT[], p_changedProperties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_modify_tables$
-- The function effectively modify the assignment properties of tables.
-- Inputs: schema, array of table names, properties as JSON structure
--         mark to set for logging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively modified
-- The JSONB v_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties can be set to NULL to delete a previously set value
  DECLARE
    v_function               TEXT;
    v_priorityChanged        BOOLEAN;
    v_logDatTspChanged       BOOLEAN;
    v_logIdxTspChanged       BOOLEAN;
    v_ignoredTrgChanged      BOOLEAN;
    v_newPriority            INT;
    v_newLogDatTsp           TEXT;
    v_newLogIdxTsp           TEXT;
    v_ignoredTriggers        TEXT[];
    v_ignoredTrgProfiles     TEXT[];
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_selectConditions       TEXT;
    v_isTableChanged         BOOLEAN;
    v_newIgnoredTriggers     TEXT[];
    v_nbChangedTbl           INT = 0;
    r_rel                    RECORD;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'MODIFY_TABLES' ELSE 'MODIFY_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters.
-- Check tables.
    IF NOT p_arrayFromRegex THEN
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', TRUE);
    END IF;
-- Determine which properties are listed in the json parameter.
    v_priorityChanged = p_changedProperties ? 'priority';
    v_logDatTspChanged = p_changedProperties ? 'log_data_tablespace';
    v_logIdxTspChanged = p_changedProperties ? 'log_index_tablespace';
    v_ignoredTrgChanged = p_changedProperties ? 'ignored_triggers' OR p_changedProperties ? 'ignored_triggers_profiles';
-- Check and extract the tables JSON properties.
    IF p_changedProperties IS NOT NULL THEN
      SELECT * INTO v_newPriority, v_newLogDatTsp, v_newLogIdxTsp, v_ignoredTriggers, v_ignoredTrgProfiles
        FROM emaj._check_json_table_properties(p_changedProperties);
    END IF;
-- Get and lock the tables groups and logging groups holding these tables.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, NULL);
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_modified_tables: No table to process.';
    ELSIF p_changedProperties IS NULL OR p_changedProperties = '{}' THEN
      RAISE WARNING '_modified_tables: No property change to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Build the SQL conditions to use in order to build the array of "triggers to ignore at rollback time" for each table.
      IF v_ignoredTriggers IS NOT NULL OR v_ignoredTrgProfiles IS NOT NULL THEN
--   Build the condition on trigger names using the ignored_triggers parameters.
        IF v_ignoredTriggers IS NOT NULL THEN
          v_selectConditions = 'tgname = ANY (' || quote_literal(v_ignoredTriggers) || ') OR ';
        ELSE
          v_selectConditions = '';
        END IF;
--   Build the regexp conditions on trigger names using the ignored_triggers_profile parameters.
        IF v_ignoredTrgProfiles IS NOT NULL THEN
          SELECT v_selectConditions || string_agg('tgname ~ ' || quote_literal(profile), ' OR ')
            INTO v_selectConditions
            FROM unnest(v_ignoredTrgProfiles) AS profile;
        ELSE
          v_selectConditions = v_selectConditions || 'FALSE';
        END IF;
      END IF;
-- Process the changes for each table, if any.
      FOR r_rel IN
        SELECT rel_tblseq, rel_time_range, rel_log_schema, rel_priority, rel_log_table, rel_log_index, rel_log_dat_tsp,
               rel_log_idx_tsp, rel_ignored_triggers, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        v_isTableChanged = FALSE;
-- Change the priority, if needed.
        IF v_priorityChanged AND
            (r_rel.rel_priority <> v_newPriority
            OR (r_rel.rel_priority IS NULL AND v_newPriority IS NOT NULL)
            OR (r_rel.rel_priority IS NOT NULL AND v_newPriority IS NULL)) THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_priority_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_priority, v_newPriority,
                                            v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the log data tablespace, if needed.
        IF v_logDatTspChanged AND coalesce(v_newLogDatTsp, '') <> coalesce(r_rel.rel_log_dat_tsp, '') THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_log_data_tsp_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_table,
                                                r_rel.rel_log_dat_tsp, v_newLogDatTsp, v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the log index tablespace, if needed.
        IF v_logIdxTspChanged AND coalesce(v_newLogIdxTsp, '') <> coalesce(r_rel.rel_log_idx_tsp, '') THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_log_index_tsp_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_index,
                                                 r_rel.rel_log_idx_tsp, v_newLogIdxTsp, v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the ignored_trigger array if needed.
        IF v_ignoredTrgChanged THEN
--   Compute the new list of "triggers to ignore at rollback time".
          IF v_selectConditions IS NOT NULL THEN
            EXECUTE format(
              $$SELECT array_agg(tgname ORDER BY tgname)
                  FROM pg_catalog.pg_trigger
                       JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                       JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                  WHERE nspname = %L
                    AND relname = %L
                    AND tgconstraint = 0
                    AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
                    AND (%s)
              $$, p_schema, r_rel.rel_tblseq, v_selectConditions)
              INTO v_newIgnoredTriggers;
          END IF;
          IF (r_rel.rel_ignored_triggers <> v_newIgnoredTriggers
             OR (r_rel.rel_ignored_triggers IS NULL AND v_newIgnoredTriggers IS NOT NULL)
             OR (r_rel.rel_ignored_triggers IS NOT NULL AND v_newIgnoredTriggers IS NULL)) THEN
            v_isTableChanged = TRUE;
--   If changes must be recorded, call the dedicated function.
            PERFORM emaj._change_ignored_triggers_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_ignored_triggers, v_newIgnoredTriggers,
                                                      v_timeId, r_rel.rel_group, v_function);
          END IF;
        END IF;
--
        IF v_isTableChanged THEN
          v_nbChangedTbl = v_nbChangedTbl + 1;
        END IF;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId
        WHERE group_name = ANY(v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbChangedTbl || ' tables effectively modified');
--
    RETURN v_nbChangedTbl;
  END;
$_modify_tables$;

CREATE OR REPLACE FUNCTION emaj._create_tbl(p_schema TEXT, p_tbl TEXT, p_groupName TEXT, p_priority INT, p_logDatTsp TEXT,
                                            p_logIdxTsp TEXT, p_ignoredTriggers TEXT[], p_timeId BIGINT, p_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table.
-- Input: the application table to process,
--        the group to add it into,
--        the table properties: priority, tablespaces attributes and triggers to ignore at rollback time
--        the time id of the operation,
--        a boolean indicating whether the group is currently in logging state.
-- The objects created in the log schema:
--    - the associated log table, with its own sequence,
--    - the function and trigger that log the tables updates.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on the
--   application table.
  DECLARE
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
    v_pkCols                 TEXT[];
    v_rlbkColList            TEXT;
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
-- The checks on the table properties are performed by the calling functions.
-- Build the prefix of all emaj object to create.
    IF length(p_tbl) <= 50 THEN
-- For not too long table name, the prefix is the table name itself.
      v_emajNamesPrefix = p_tbl;
    ELSE
-- For long table names (over 50 char long), compute the suffix to add to the first 50 characters (#1, #2, ...), by looking at the
-- existing names.
      SELECT substr(p_tbl, 1, 50) || '#' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_emajNamesPrefix
        FROM
          (SELECT (regexp_match(substr(rel_log_table, 51), '#(\d+)'))[1]::INT AS suffix
             FROM emaj.emaj_relation
             WHERE substr(rel_log_table, 1, 50) = substr(p_tbl, 1, 50)
          ) AS t;
    END IF;
-- Build the name of emaj components associated to the application table (non schema qualified and not quoted).
    v_baseLogTableName     = v_emajNamesPrefix || '_log';
    v_baseLogIdxName       = v_emajNamesPrefix || '_log_idx';
    v_baseLogFnctName      = v_emajNamesPrefix || '_log_fnct';
    v_baseSequenceName     = v_emajNamesPrefix || '_log_seq';
-- Build the different name for table, trigger, functions,...
    v_logSchema        = 'emaj_' || p_schema;
    v_fullTableName    = quote_ident(p_schema) || '.' || quote_ident(p_tbl);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogTableName);
    v_logIdxName       = quote_ident(v_baseLogIdxName);
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogFnctName);
    v_sequenceName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseSequenceName);
-- Prepare the TABLESPACE clauses for data and index
    v_dataTblSpace = coalesce('TABLESPACE ' || quote_ident(p_logDatTsp),'');
    v_idxTblSpace = coalesce('USING INDEX TABLESPACE ' || quote_ident(p_logIdxTsp),'');
-- Create the log table: it looks like the application table, with some additional technical columns.
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('CREATE TABLE %s (LIKE %s,'
                   '  emaj_verb      VARCHAR(3)  NOT NULL,'
                   '  emaj_tuple     VARCHAR(3)  NOT NULL,'
                   '  emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
                   '  emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
                   '  emaj_txid      BIGINT      DEFAULT txid_current(),'
                   '  emaj_user      VARCHAR(32) DEFAULT session_user,'
                   '  CONSTRAINT %s PRIMARY KEY (emaj_gid, emaj_tuple) %s'
                   '  ) %s',
                    v_logTableName, v_fullTableName, v_logIdxName, v_idxTblSpace, v_dataTblSpace);
-- Get the attnum of the emaj_verb column.
    SELECT attnum INTO STRICT v_attnum
      FROM pg_catalog.pg_attribute
           JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = v_logSchema
        AND relname = v_baseLogTableName
        AND attname = 'emaj_verb';
-- Adjust the log table structure with the alter_log_table parameter, if set.
    SELECT param_value_text INTO v_alter_log_table_param
      FROM emaj.emaj_param
      WHERE param_key = ('alter_log_table');
    IF v_alter_log_table_param IS NOT NULL AND v_alter_log_table_param <> '' THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_alter_log_table_param);
    END IF;
-- Set the index associated to the primary key as cluster index (It may be useful for CLUSTER command).
    EXECUTE format('ALTER TABLE ONLY %s CLUSTER ON %s',
                   v_logTableName, v_logIdxName);
-- Remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables.
    SELECT string_agg(action, ',') INTO v_stmt
      FROM
        (SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
           FROM pg_catalog.pg_attribute
                JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = v_logSchema
             AND relname = v_baseLogTableName
             AND attnum > 0
             AND attnum < v_attnum
             AND NOT attisdropped
             AND attnotnull
        ) AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_stmt);
    END IF;
-- Create the sequence associated to the log table.
    EXECUTE format('CREATE SEQUENCE %s',
                   v_sequenceName);
-- Create the log function.
-- The new row is logged for each INSERT, the old row is logged for each DELETE and the old and new rows are logged for each UPDATE.
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
-- Create the log and truncate triggers.
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                   p_schema, p_tbl);
    EXECUTE format('CREATE TRIGGER emaj_log_trg'
                   ' AFTER INSERT OR UPDATE OR DELETE ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE %s()',
                   p_schema, p_tbl, v_logFnctName);
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                   p_schema, p_tbl);
    EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                   '  BEFORE TRUNCATE ON %I.%I'
                   '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._truncate_trigger_fnct()',
                   p_schema, p_tbl);
    IF p_groupIsLogging THEN
-- If the group is in logging state, set the triggers as ALWAYS triggers, so that they can fire at rollback time.
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg, ENABLE ALWAYS TRIGGER emaj_log_trg',
                     p_schema, p_tbl);
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg, ENABLE ALWAYS TRIGGER emaj_trunc_trg',
                     p_schema, p_tbl);
    ELSE
-- If the group is idle, deactivate the triggers (they will be enabled at emaj_start_group time).
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                     p_schema, p_tbl);
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg',
                     p_schema, p_tbl);
    END IF;
-- Set emaj_adm as owner of log objects.
    EXECUTE format('ALTER TABLE %s OWNER TO emaj_adm',
                   v_logTableName);
    EXECUTE format('ALTER SEQUENCE %s OWNER TO emaj_adm',
                   v_sequenceName);
    EXECUTE format('ALTER FUNCTION %s () OWNER TO emaj_adm',
                   v_logFnctName);
-- Grant appropriate rights to the emaj_viewer role.
    EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                   v_logTableName);
    EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                   v_sequenceName);
-- Build the PK columns names array and some pieces of SQL statements that will be needed at table rollback and gen_sql times.
-- They are left NULL if the table has no pkey.
    SELECT * FROM emaj._build_sql_tbl(v_fullTableName)
      INTO v_pkCols, v_rlbkColList, v_genColList, v_genValList, v_genSetList, v_genPkConditions, v_nbGenAlwaysIdentCol;
-- Register the table into emaj_relation.
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function, rel_ignored_triggers, rel_pk_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns,
                rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions)
        VALUES (p_schema, p_tbl, int8range(p_timeId, NULL, '[)'), p_groupName, p_priority,
                v_logSchema, p_logDatTsp, p_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName, p_ignoredTriggers, v_pkCols,
                v_attnum, v_nbGenAlwaysIdentCol > 0, v_rlbkColList,
                v_genColList, v_genValList, v_genSetList, v_genPkConditions);
-- Check if the table has application (neither internal - ie. created for fk - nor previously created by emaj) triggers not already
-- declared as 'to be ignored at rollback time'.
    SELECT string_agg(tgname, ', ' ORDER BY tgname) INTO v_triggerList
      FROM
        (SELECT tgname
           FROM pg_catalog.pg_trigger
           WHERE tgrelid = v_fullTableName::regclass
             AND tgconstraint = 0
             AND tgname NOT LIKE E'emaj\\_%\\_trg'
             AND NOT tgname = ANY(coalesce(p_ignoredTriggers, '{}'))
        ) AS t;
-- If yes, issue a warning.
-- If a trigger updates another table in the same table group or outside, it could generate problem at rollback time.
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers that will be automatically disabled during E-Maj rollback operations (%).'
                    ' Use the emaj_modify_table() function to change this behaviour.', v_fullTableName, v_triggerList;
    END IF;
--
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._remove_tbl(p_schema TEXT, p_table TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                            p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_remove_tbl$
-- The function removes a table from a group. It is called during an alter group or a dynamic removal operation.
-- If the group is in idle state, it simply calls the _drop_tbl() function.
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group
-- time for instance).
-- Required inputs: schema and sequence to remove, related group name and logging state,
--                  time stamp id of the operation, main calling function.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_logSchema              TEXT;
    v_currentLogTable        TEXT;
    v_currentLogIndex        TEXT;
    v_logFunction            TEXT;
    v_logSequence            TEXT;
    v_logSequenceLastValue   BIGINT;
    v_namesSuffix            TEXT;
  BEGIN
    IF NOT p_groupIsLogging THEN
-- If the group is in idle state, drop the table immediately.
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*, p_timeId)
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
    ELSE
-- The group is in logging state.
-- Get the current relation characteristics.
      SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_function, rel_log_sequence
        INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logFunction, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- Get the current log sequence characteristics.
      SELECT tbl_log_seq_last_val INTO STRICT v_logSequenceLastValue
        FROM emaj.emaj_table
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND tbl_time_id = p_timeId;
-- Compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names.
      SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
        FROM
          (SELECT (regexp_match(rel_log_table,'_(\d+)$'))[1]::INT AS suffix
             FROM emaj.emaj_relation
             WHERE rel_schema = p_schema
               AND rel_tblseq = p_table
          ) AS t;
-- Rename the log table and its index (they may have been dropped).
      EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogTable, v_currentLogTable || v_namesSuffix);
      EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogIndex, v_currentLogIndex || v_namesSuffix);
-- Drop the log and truncate triggers.
-- (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              WHERE nspname = p_schema
                AND relname = p_table
                AND relkind = 'r'
           ) THEN
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                       p_schema, p_table);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                       p_schema, p_table);
      END IF;
-- Drop the log function and the log sequence.
-- (but we keep the sequence related data in the emaj_table and the emaj_seq_hole tables)
      EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                     v_logSchema, v_logFunction);
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     v_logSchema, v_logSequence);
-- Register the end of the relation time frame, the last value of the log sequence, the log table and index names change.
-- Reflect the changes into the emaj_relation rows:
--   - for all timeranges pointing to this log table and index
--     (do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
      UPDATE emaj.emaj_relation
        SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix,
            rel_log_function = NULL, rel_sql_rlbk_columns = NULL,
            rel_log_seq_last_value = v_logSequenceLastValue
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND rel_log_table = v_currentLogTable;
--   - and close the last timerange.
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range), p_timeId, '[)')
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_table, 'REMOVE_TABLE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TABLE REMOVED', quote_ident(p_schema) || '.' || quote_ident(p_table),
              'From the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
--
    RETURN;
  END;
$_remove_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_tbl$
-- The function deletes a timerange for a table. This centralizes the deletion of all what has been created by _create_tbl() function.
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess, time id.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  BEGIN
-- If the table is currently linked to a group, drop the log trigger, function and sequence.
    IF upper_inf(r_rel.rel_time_range) THEN
-- Check the table exists before dropping its triggers.
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              WHERE nspname = r_rel.rel_schema
                AND relname = r_rel.rel_tblseq
                AND relkind = 'r'
           ) THEN
-- Drop the log and truncate triggers on the application table.
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                       r_rel.rel_schema, r_rel.rel_tblseq);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                       r_rel.rel_schema, r_rel.rel_tblseq);
      END IF;
-- Drop the log function.
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                       r_rel.rel_log_schema, r_rel.rel_log_function);
      END IF;
-- Drop the sequence associated to the log table.
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_sequence);
    END IF;
-- Drop the log table if it is not referenced on other timeranges (for potentially other groups).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_relation
            WHERE rel_log_schema = r_rel.rel_log_schema
              AND rel_log_table = r_rel.rel_log_table
              AND rel_time_range <> r_rel.rel_time_range
         ) THEN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END IF;
-- Process log sequence information if the sequence is not referenced in other timerange (for potentially other groups).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_relation
            WHERE rel_log_schema = r_rel.rel_log_schema
              AND rel_log_sequence = r_rel.rel_log_sequence
              AND rel_time_range <> r_rel.rel_time_range
         ) THEN
-- Delete rows related to the log sequence from emaj_table
-- (it may delete rows for other already processed time_ranges for the same table).
      DELETE FROM emaj.emaj_table
        WHERE tbl_schema = r_rel.rel_schema
          AND tbl_name = r_rel.rel_tblseq;
-- Delete rows related to the table from emaj_seq_hole table
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks).
      DELETE FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema
          AND sqhl_table = r_rel.rel_tblseq;
    END IF;
-- Keep a trace of the table group ownership history and finaly delete the table reference from the emaj_relation table.
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_schema = r_rel.rel_schema
          AND rel_tblseq = r_rel.rel_tblseq
          AND rel_time_range = r_rel.rel_time_range
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq,
             CASE WHEN upper_inf(rel_time_range) THEN int8range(lower(rel_time_range), p_timeId, '[)') ELSE rel_time_range END,
             rel_group, rel_kind
        FROM deleted;
--
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj._assign_sequences(p_schema TEXT, p_sequences TEXT[], p_group TEXT, p_mark TEXT,
                                                  p_multiSequence BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_sequences$
-- The function effectively assigns sequences into a tables group.
-- Inputs: schema, array of sequence names, group name,
--         mark to set for lonnging groups, a boolean indicating whether several sequences need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of sequences effectively assigned to the tables group
-- The JSONB v_properties parameter has currenlty only one field '{"priority":...}' the properties being NULL by default
  DECLARE
    v_function               TEXT;
    v_groupIsLogging         BOOLEAN;
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_oneSequence            TEXT;
    v_nbAssignedSeq          INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'ASSIGN_SEQUENCES' ELSE 'ASSIGN_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_group], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_group;
-- Check the supplied schema exists and is not an E-Maj schema.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" does not exist.', p_schema;
    END IF;
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_schema
            WHERE sch_name = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" is an E-Maj schema.', p_schema;
    END IF;
-- Check sequences.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the sequence names array supplied by the user.
      SELECT array_agg(DISTINCT sequence_name) INTO p_sequences
        FROM unnest(p_sequences) AS sequence_name
        WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- Check that application sequences exist.
      WITH sequences AS (
        SELECT unnest(p_sequences) AS sequence_name)
      SELECT string_agg(quote_ident(sequence_name), ', ') INTO v_list
        FROM
          (SELECT sequence_name
             FROM sequences
             WHERE NOT EXISTS
                    (SELECT 0
                       FROM pg_catalog.pg_class
                            JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                       WHERE nspname = p_schema
                         AND relname = sequence_name
                         AND relkind = 'S')
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_sequences: In schema %, some sequences (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard sequences already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', '), array_agg(rel_tblseq) INTO v_list, v_array
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_sequences)
        AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_sequences: In schema %, some sequences (%) already belong to a group.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_sequences: Some sequences already belonging to a group (%) are not selected.', v_list;
        -- remove these sequences from the sequences to process
        SELECT array_agg(remaining_sequence) INTO p_sequences
          FROM
            (  SELECT unnest(p_sequences)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_sequence);
      END IF;
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(array[p_group], p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL OR p_sequences = '{}' THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_assign_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively create the log components for each table.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
        PERFORM emaj._add_seq(p_schema, v_oneSequence, p_group, v_groupIsLogging, v_timeId, v_function);
        v_nbAssignedSeq = v_nbAssignedSeq + 1;
      END LOOP;
-- Adjust the group characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = p_group;
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbAssignedSeq || ' sequences assigned to the group ' || p_group);
--
    RETURN v_nbAssignedSeq;
  END;
$_assign_sequences$;

CREATE OR REPLACE FUNCTION emaj._remove_sequences(p_schema TEXT, p_sequences TEXT[], p_mark TEXT, p_multiSequence BOOLEAN,
                                                  p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_sequences$
-- The function effectively removes sequences from their sequences group.
-- Inputs: schema, array of sequence names, mark to set if for logging groups,
--         a boolean to indicate whether several sequences need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of sequences effectively assigned to the sequences group
  DECLARE
    v_function               TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_oneSequence            TEXT;
    v_nbRemovedSeq           INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'REMOVE_SEQUENCES' ELSE 'REMOVE_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the sequences array.
    IF NOT p_arrayFromRegex THEN
      p_sequences = emaj._check_tblseqs_array(p_schema, p_sequences, 'S', FALSE);
    END IF;
-- Get and lock the tables groups and logging groups holding these sequences.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_sequences, NULL);
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_remove_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the log components for each sequence.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
-- Get some characteristics of the group that holds the sequence.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneSequence
            AND upper_inf(rel_time_range);
-- Drop this sequence from its group.
        PERFORM emaj._remove_seq(p_schema, v_oneSequence, v_groupName, v_groupIsLogging, v_timeId, v_function);
        v_nbRemovedSeq = v_nbRemovedSeq + 1;
      END LOOP;
-- Enable previously disabled event triggers.
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbRemovedSeq || ' sequences removed from their groups');
--
    RETURN v_nbRemovedSeq;
  END;
$_remove_sequences$;

CREATE OR REPLACE FUNCTION emaj._move_sequences(p_schema TEXT, p_sequences TEXT[], p_newGroup TEXT, p_mark TEXT, p_multiSequence BOOLEAN,
                                             p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_move_sequences$
-- The function effectively moves sequences from their tables group to another tables group.
-- Inputs: schema, array of sequence names, new group name, mark to set if for logging groups,
--         boolean to indicate whether several sequences need to be processed,
--         a boolean indicating whether the sequences array has been built from regex filters
-- Outputs: number of sequences effectively moved to the tables group
  DECLARE
    v_function               TEXT;
    v_newGroupIsLogging      BOOLEAN;
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_oneSequence            TEXT;
    v_nbMovedSeq             INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'MOVE_SEQUENCES' ELSE 'MOVE_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_newGroup], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_logging INTO v_newGroupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_newGroup;
-- Check the sequences list.
    IF NOT p_arrayFromRegex THEN
      p_sequences = emaj._check_tblseqs_array(p_schema, p_sequences, 'S', TRUE);
    END IF;
-- Remove sequences that already belong to the new group.
    SELECT array_agg(rel_tblseq ORDER BY rel_tblseq) FILTER (WHERE rel_group <> p_newGroup),
           string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq) FILTER (WHERE rel_group = p_newGroup)
      INTO p_sequences, v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_sequences)
        AND upper_inf(rel_time_range);
-- Warn only if the sequences list has been supplied by the user.
    IF v_list IS NOT NULL AND NOT p_arrayFromRegex THEN
      RAISE WARNING '_move_sequences: In schema "%", some sequences (%) already belong to the tables group "%".',
                    p_schema, v_list, p_newGroup;
    END IF;
-- Get and lock the tables groups and logging groups holding these sequences, and count the number of AUDIT_ONLY groups.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_sequences, p_newGroup);
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_move_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively move each sequence.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
-- Get some characteristics of the group that holds the sequence before the move.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneSequence
            AND upper_inf(rel_time_range);
-- Move this sequence.
        PERFORM emaj._move_seq(p_schema, v_oneSequence, v_groupName, v_groupIsLogging, p_newGroup, v_newGroupIsLogging, v_timeId,
                               v_function);
        v_nbMovedSeq = v_nbMovedSeq + 1;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbMovedSeq || ' sequences moved to the tables group ' || p_newGroup);
--
    RETURN v_nbMovedSeq;
  END;
$_move_sequences$;

CREATE OR REPLACE FUNCTION emaj._add_seq(p_schema TEXT, p_sequence TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                         p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_add_seq$
-- The function adds a sequence to a group. It is called during an alter group or a dynamic assignment operation.
-- If the group is in idle state, it simply calls the _create_seq() function.
-- Otherwise, it calls the _create_seql() function, and records the current state of the sequence.
-- Required inputs: schema and sequence to add, group name, priority, the group's logging state,
--                  the time stamp id of the operation, main calling function.
-- The function is defined as SECURITY DEFINER so that emaj_adm roles can use it even without SELECT right on the sequence.
  BEGIN
-- Create the sequence.
    PERFORM emaj._create_seq(p_schema, p_sequence, p_group, p_timeId);
-- If the group is in logging state, perform additional tasks.
    IF p_groupIsLogging THEN
-- Record the new sequence state in the emaj_sequence table for the current alter_group mark.
      EXECUTE format('INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,'
                     '            sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)'
                     '  SELECT nspname, relname, %s, sq.last_value, seqstart,'
                     '         seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '    FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '    WHERE nspname = %L AND relname = %L',
                     p_timeId, p_schema, p_sequence, p_schema, p_sequence);
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_sequence, 'ADD_SEQUENCE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'SEQUENCE ADDED', quote_ident(p_schema) || '.' || quote_ident(p_sequence),
              'To the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
--
    RETURN;
  END;
$_add_seq$;

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

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark.
-- Input: the emaj_relation row related to the application sequence to process, time id of the mark to rollback to.
-- Ouput: 0 if no change have to be applied, otherwise 1.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it is not the owner of the application sequence.
  DECLARE
    v_stmt                   TEXT;
    v_fullSeqName            TEXT;
    mark_seq_rec             emaj.emaj_sequence%ROWTYPE;
    curr_seq_rec             emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Read sequence's characteristics at mark time.
    SELECT *
      INTO mark_seq_rec
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_timeId;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".',
        p_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END IF;
-- Read the current sequence's characteristics.
    EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                   '  FROM %I.%I sq,'
                   '       pg_catalog.pg_sequence s'
                   '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                   '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                   '  WHERE nspname = %L AND relname = %L',
                   r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
      INTO STRICT curr_seq_rec;
-- Build the ALTER SEQUENCE statement, depending on the differences between the current sequence state and its characteristics
-- at the requested mark time.
    SELECT emaj._build_alter_seq(curr_seq_rec, mark_seq_rec) INTO v_stmt;
-- If there is no change to apply, return with 0.
    IF v_stmt = '' THEN
      RETURN 0;
    END IF;
-- Otherwise, execute the statement, report the event into the history and return 1.
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    EXECUTE format('ALTER SEQUENCE %s %s',
                   v_fullSeqName, v_stmt);
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
--
    RETURN 1;
  END;
$_rlbk_seq$;

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

CREATE OR REPLACE FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                   OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_sequence_stat_seq$
-- This function compares the state of a single sequence between 2 time stamps or between a time stamp and the current state.
-- It is called by the _sequence_stat_group() function.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time stamp ids defining the time range to examine (a end time stamp id set to NULL indicates the current state).
-- Output: number of sequence increments between both time stamps for the sequence
--         a boolean indicating whether any structure property has been modified between both time stamps.
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
  DECLARE
    r_beginSeq               emaj.emaj_sequence%ROWTYPE;
    r_endSeq                 emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence characteristics at begin time id.
    SELECT *
      INTO r_beginSeq
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_beginTimeId;
-- Get the sequence characteristics at end time id.
    IF p_endTimeId IS NOT NULL THEN
      SELECT *
        INTO r_endSeq
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq
          AND sequ_time_id = p_endTimeId;
    ELSE
      EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '  FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '  WHERE nspname = %L AND relname = %L',
                     r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
        INTO STRICT r_endSeq;
    END IF;
-- Compute the statistics
    p_increments = (r_endSeq.sequ_last_val - r_beginSeq.sequ_last_val) / r_beginSeq.sequ_increment
                   - CASE WHEN r_endSeq.sequ_is_called THEN 0 ELSE 1 END
                   + CASE WHEN r_beginSeq.sequ_is_called THEN 0 ELSE 1 END;
    p_hasStructureChanged = r_beginSeq.sequ_start_val <> r_endSeq.sequ_start_val
                         OR r_beginSeq.sequ_increment <> r_endSeq.sequ_increment
                         OR r_beginSeq.sequ_max_val <> r_endSeq.sequ_max_val
                         OR r_beginSeq.sequ_min_val <> r_endSeq.sequ_min_val
                         OR r_beginSeq.sequ_is_cycled <> r_endSeq.sequ_is_cycled;
    RETURN;
  END;
$_sequence_stat_seq$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_seq(r_rel emaj.emaj_relation, p_firstMarkTimeId BIGINT, p_lastMarkTimeId BIGINT, p_nbSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_gen_sql_seq$
-- This function generates a SQL statement to set the final characteristics of a sequence.
-- The statement is stored into a temporary table created by the _gen_sql_groups() calling function.
-- If the sequence has not been changed between both marks, no statement is generated.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time id at requested start and end marks,
--        the number of already processed sequences
-- Output: number of generated SQL statements (0 or 1)
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
  DECLARE
    v_endTimeId              BIGINT;
    v_rqSeq                  TEXT;
    ref_seq_rec              emaj.emaj_sequence%ROWTYPE;
    trg_seq_rec              emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence characteristics at start mark.
    SELECT *
      INTO ref_seq_rec
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_firstMarkTimeId;
-- Get the sequence characteristics at end mark or the current state.
    IF p_lastMarkTimeId IS NULL AND upper_inf(r_rel.rel_time_range) THEN
-- No supplied last mark and the sequence currently belongs to its group, so get the current sequence characteritics.
      EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '  FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '  WHERE nspname = %L AND relname = %L',
                     r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
        INTO STRICT trg_seq_rec;
    ELSE
-- A last mark is supplied, or the sequence does not belong to its group anymore, so get the sequence characteristics
-- from the emaj_sequence table.
      v_endTimeId = CASE WHEN upper_inf(r_rel.rel_time_range) OR p_lastMarkTimeId < upper(r_rel.rel_time_range)
                           THEN p_lastMarkTimeId
                         ELSE upper(r_rel.rel_time_range) END;
      SELECT *
        INTO trg_seq_rec
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq
          AND sequ_time_id = v_endTimeId;
    END IF;
-- Build the ALTER SEQUENCE clause.
    SELECT emaj._build_alter_seq(ref_seq_rec, trg_seq_rec) INTO v_rqSeq;
-- Insert into the temp table and return 1 if at least 1 characteristic needs to be changed.
    IF v_rqSeq <> '' THEN
      v_rqSeq = 'ALTER SEQUENCE ' || quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq) || ' ' || v_rqSeq || ';';
      EXECUTE 'INSERT INTO emaj_temp_script '
              '  SELECT NULL, -1 * $1, txid_current(), $2'
        USING p_nbSeq + 1, v_rqSeq;
      RETURN 1;
    END IF;
-- Otherwise return 0.
    RETURN 0;
  END;
$_gen_sql_seq$;

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
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_get_app_sequence_last_value$
-- The function returns the last value state of a single application sequence.
-- If the sequence has not been called, it returns the previous value defined as (last_value - increment).
-- It first calls the undocumented but very efficient pg_sequence_last_value(oid) function.
-- If this pg_sequence_last_value() function returns NULL, meaning the is_called attribute is FALSE which is not a frequent case,
--   select the sequence itself.
-- Input: schema and sequence name
-- Output: last_value
-- The function is defined as SECURITY DEFINER so that any emaj role can use it even if he is not the application sequence owner.
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

CREATE OR REPLACE FUNCTION emaj._lock_groups(p_groupNames TEXT[], p_lockMode TEXT, p_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_lock_groups$
-- This function locks all tables of a groups array.
-- The lock mode is provided by the calling function.
-- It only locks existing tables. It is calling function's responsability to handle cases when application tables are missing.
-- Input: array of group names, lock mode, flag indicating whether the function is called to processed several groups.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on tables.
  DECLARE
    v_nbRetry                SMALLINT = 0;
    v_nbTbl                  INT;
    v_ok                     BOOLEAN = FALSE;
    v_fullTableName          TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN p_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END,'BEGIN', array_to_string(p_groupNames,','));
-- Acquire lock on all tables.
-- In case of deadlock, retry up to 5 times.
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- Process each table currently belonging to the groups.
        v_nbTbl = 0;
        FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq
            FROM emaj.emaj_relation
                 JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                 JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
            WHERE rel_group = ANY (p_groupNames)
              AND rel_kind = 'r'
              AND upper_inf(rel_time_range)
            ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- Lock the table.
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE format('LOCK TABLE %s IN %s MODE',
                         v_fullTableName, p_lockMode);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- Ok, all tables are locked.
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_lock_groups: A deadlock has been trapped while locking tables of group "%".', p_groupNames;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_lock_groups: Too many (5) deadlocks encountered while locking tables of group "%".',p_groupNames;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END, 'END',
              array_to_string(p_groupNames,','), v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
--
    RETURN;
  END;
$_lock_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(p_groupName TEXT, p_isRollbackable BOOLEAN DEFAULT TRUE, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates a group, for the moment empty.
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
--        optional comment.
-- Output: 1 = number of created groups
  DECLARE
    v_function               TEXT = 'CREATE_GROUP';
    v_timeId                 BIGINT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'BEGIN', p_groupName, CASE WHEN p_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- Check that the group name is valid.
    IF p_groupName IS NULL OR p_groupName = '' THEN
      RAISE EXCEPTION 'emaj_create_group: The group name can''t be NULL or empty.';
    END IF;
-- Check that the group is not yet recorded in emaj_group table
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_group
            WHERE group_name = p_groupName
         ) THEN
      RAISE EXCEPTION 'emaj_create_group: The group "%" already exists.', p_groupName;
    END IF;
-- OK
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'C') INTO v_timeId;
-- Insert the row describing the group into the emaj_group and emaj_group_hist tables
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable).
    INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_is_logging, group_is_rlbk_protected,
                                 group_nb_table, group_nb_sequence, group_comment)
      VALUES (p_groupName, p_isRollbackable, FALSE, NOT p_isRollbackable, 0, 0, p_comment);
    INSERT INTO emaj.emaj_group_hist (grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
      VALUES (p_groupName, int8range(v_timeId, NULL, '[]'), p_isRollbackable, 0);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'END', p_groupName);
    RETURN 1;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN,TEXT) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_groups_configuration$
-- This function stores some or all configured tables groups configuration into a file on the server.
-- The JSON structure is built by the _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the number of tables groups recorded in the file.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_groupsJson             JSON;
  BEGIN
-- Get the json structure.
    SELECT emaj._export_groups_conf(p_groups) INTO v_groupsJson;
-- Store the structure into the provided file name.
    CREATE TEMP TABLE t (groups TEXT);
    INSERT INTO t
      SELECT line
        FROM regexp_split_to_table(v_groupsJson::TEXT, '\n') AS line;
    EXECUTE format ('COPY t TO %L',
                    p_location);
    DROP TABLE t;
-- Return the number of recorded tables groups.
    RETURN json_array_length(v_groupsJson->'tables_groups');
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT, TEXT[]) IS
$$Generates and stores in a file a json structure describing configured tables groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%')
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_groups_configuration$
-- This function imports a file containing a JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - input file location
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--        - an optional mark name to set for tables groups in logging state (IMPORT_% by default)
-- Output: the number of created or altered tables groups
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_groupsText             TEXT;
    v_json                   JSON;
  BEGIN
-- Read the input file and put its content into a temporary table.
    CREATE TEMP TABLE t (groups TEXT);
    EXECUTE format ('COPY t FROM %L',
                    p_location);
-- Aggregate the lines into a single text variable.
    SELECT string_agg(groups, E'\n') INTO v_groupsText
      FROM t;
    DROP TABLE t;
-- Verify that the file content is a valid json structure.
    BEGIN
      v_json = v_groupsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_groups_configuration: The file content is not a valid JSON content.';
    END;
-- Proccess the tables groups and return the result.
    RETURN emaj._import_groups_conf(v_json, p_groups, p_allowGroupsUpdate, p_location, p_mark);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(TEXT,TEXT[],BOOLEAN, TEXT) IS
$$Create or alter tables groups configuration from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_check(p_groupNames TEXT[])
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_check$
-- This function verifies that the content of tables group as defined into the tmp_app_table table is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them, depending on the tables group type.
-- It is called by the _import_groups_conf_prepare() function.
-- This function checks that the referenced application tables and sequences:
--  - exist,
--  - are not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
-- It also checks that:
--  - tables are not TEMPORARY
--  - for rollbackable groups, tables are not UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for tables, configured tablespaces exist
-- Input: name array of the tables groups to check
-- Output: _report_message_type records representing diagnostic messages
--         the rpt_severity is set to 1 if the error blocks any type group creation or alter,
--                                 or 2 if the error only blocks ROLLBACKABLE groups creation
  BEGIN
-- Check that all application tables listed for the group really exist.
    RETURN QUERY
      SELECT 1, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE tmp_schema = nspname AND tmp_tbl_name = relname
                     AND relkind IN ('r','p')
                );
-- Check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj).
    RETURN QUERY
      SELECT 2, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'p';
-- Check no application schema listed for the group in the tmp_app_table table is an E-Maj schema.
    RETURN QUERY
      SELECT 3, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table or sequence %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
-- Check that no table of the checked groups already belongs to other created groups.
    RETURN QUERY
      SELECT 4, 1, tmp_group, tmp_schema, tmp_tbl_name, rel_group, NULL::INT,
             format('in the group %s, the table %s.%s already belongs to the group %s.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(rel_group))
        FROM tmp_app_table
             JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_tbl_name AND upper_inf(rel_time_range))
        WHERE NOT rel_group = ANY (p_groupNames);
-- Check no table is a TEMP table.
    RETURN QUERY
      SELECT 5, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s is a TEMPORARY table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r' AND relpersistence = 't';
-- Check that the log data tablespaces for tables exist.
    RETURN QUERY
      SELECT 12, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_dat_tsp, NULL::INT,
             format('in the group %s, for the table %s.%s, the data log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_dat_tsp))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND tmp_log_dat_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_dat_tsp
                );
-- Check that the log index tablespaces for tables exist.
    RETURN QUERY
      SELECT 13, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_idx_tsp, NULL::INT,
             format('in the group %s, for the table %s.%s, the index log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_idx_tsp))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND tmp_log_idx_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_idx_tsp
                );
-- Check that all listed triggers exist,
    RETURN QUERY
      SELECT 15, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_trigger, NULL::INT,
             format('In tables group "%s" and for the table %I.%I, the trigger %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_trigger))
        FROM
          (SELECT tmp_group, tmp_schema, tmp_tbl_name, unnest(tmp_ignored_triggers) AS tmp_trigger
             FROM tmp_app_table
          ) AS t
        WHERE NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
                   WHERE nspname = tmp_schema AND relname = tmp_tbl_name AND tgname = tmp_trigger
                     AND NOT tgisinternal
                );
-- ... and are not emaj triggers.
    RETURN QUERY
      SELECT 16, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_trigger, NULL::INT,
             format('In tables group "%s" and for the table %I.%I, the trigger %I is an E-Maj trigger.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_trigger))
        FROM
          (SELECT tmp_group, tmp_schema, tmp_tbl_name, unnest(tmp_ignored_triggers) AS tmp_trigger
             FROM tmp_app_table
          ) AS t
        WHERE tmp_trigger IN ('emaj_trunc_trg', 'emaj_log_trg');
-- Check that no table is an unlogged table (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 20, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s is an UNLOGGED table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND relpersistence = 'u';
-- With PG11- check no table is a WITH OIDS table (blocking rollbackable groups only).
    IF emaj._pg_version_num() < 120000 THEN
      RETURN QUERY
        SELECT 21, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
               format('in the group %s, the table %s.%s is declared WITH OIDS.',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
          FROM tmp_app_table
               JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
          WHERE relkind = 'r' AND relhasoids;
    END IF;
-- Check every table has a primary key (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 22, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s has no PRIMARY KEY.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                   WHERE contype = 'p' AND nspname = tmp_schema AND relname = tmp_tbl_name
                );
-- Check that all application sequences listed for the group really exist.
    RETURN QUERY
      SELECT 31, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the sequence %s.%s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM tmp_app_sequence
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE tmp_schema = nspname
                     AND tmp_seq_name = relname
                     AND relkind = 'S'
                );
-- Check that no sequence of the checked groups already belongs to other created groups.
    RETURN QUERY
      SELECT 32, 1, tmp_group, tmp_schema, tmp_seq_name, rel_group, NULL::INT,
             format('in the group %s, the sequence %s.%s already belongs to the group %s.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name), quote_ident(rel_group))
        FROM tmp_app_sequence
             JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_seq_name AND upper_inf(rel_time_range))
        WHERE NOT rel_group = ANY (p_groupNames);
--
    RETURN;
  END;
$_import_groups_conf_check$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_exec(p_json JSON, p_groups TEXT[], p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf_exec$
-- This function completes a tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- Non existing groups are created empty.
-- The _import_groups_conf_alter() function is used to process the assignement, the move, the removal or the attributes change for tables
-- and sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process
--        - a boolean indicating whether tables groups to import may already exist
--        - the mark name to set for tables groups in logging state
-- Output: the number of created or altered tables groups
  DECLARE
    v_function               TEXT = 'IMPORT_GROUPS';
    v_timeId                 BIGINT;
    v_groupsJson             JSON;
    v_nbGroup                INT;
    v_comment                TEXT;
    v_isRollbackable         BOOLEAN;
    v_loggingGroups          TEXT[];
    v_markName               TEXT;
    r_group                  RECORD;
  BEGIN
-- Get a time stamp id of type 'I' for the operation.
    SELECT emaj._set_time_stamp(v_function, 'I') INTO v_timeId;
-- Extract the "tables_groups" json path.
    v_groupsJson = p_json #> '{"tables_groups"}';
-- In a third pass over the JSON structure:
--   - create empty groups for those which does not exist yet,
--   - adjust the comment on the groups, if needed.
    v_nbGroup = 0;
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(v_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
      v_nbGroup = v_nbGroup + 1;
-- Create the tables group if it does not exist yet.
      SELECT group_comment INTO v_comment
        FROM emaj.emaj_group
        WHERE group_name = r_group.groupJson ->> 'group';
      IF NOT FOUND THEN
        v_isRollbackable = coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE);
        INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable,
                                     group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment)
          VALUES (r_group.groupJson ->> 'group', v_isRollbackable,
                                     FALSE, NOT v_isRollbackable, 0, 0, r_group.groupJson ->> 'comment');
        INSERT INTO emaj.emaj_group_hist (grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
          VALUES (r_group.groupJson ->> 'group', int8range(v_timeId, NULL, '[]'), v_isRollbackable, 0);
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
          VALUES (v_function, 'GROUP CREATED', r_group.groupJson ->> 'group',
                  CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
      ELSE
-- If the group exists, adjust the comment if needed.
        IF coalesce(v_comment, '') <> coalesce(r_group.groupJson ->> 'comment', '') THEN
          UPDATE emaj.emaj_group
            SET group_comment = r_group.groupJson ->> 'comment'
            WHERE group_name = r_group.groupJson ->> 'group';
        END IF;
      END IF;
    END LOOP;
-- Lock the group names to avoid concurrent operation on these groups.
    PERFORM 0
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groups)
      FOR UPDATE;
-- Build the list of groups that are in logging state.
    SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groups)
        AND group_is_logging;
-- If some groups are in logging state, check and set the supplied mark name and lock the groups.
    IF v_loggingGroups IS NOT NULL THEN
      SELECT emaj._check_new_mark(p_groups, p_mark) INTO v_markName;
-- Lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
      PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', TRUE);
-- And set the mark, using the same time identifier.
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
    END IF;
-- Process the tmp_app_table and tmp_app_sequence content change.
    PERFORM emaj._import_groups_conf_alter(p_groups, p_mark, v_timeId);
-- Check foreign keys with tables outside the groups in logging state.
    PERFORM emaj._check_fk_groups(v_loggingGroups);
-- The temporary tables are not needed anymore. So drop them.
    DROP TABLE tmp_app_table;
    DROP TABLE tmp_app_sequence;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbGroup || ' created or altered tables groups');
--
    RETURN v_nbGroup;
  END;
$_import_groups_conf_exec$;

CREATE OR REPLACE FUNCTION emaj._start_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark.
-- It also delete oldest rows in emaj_hist table.
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function,
--        boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_function               TEXT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_eventTriggers          TEXT[];
    r_tblsq                  RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','),
              CASE WHEN p_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE, p_checkIdle := TRUE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- If there is at least 1 group to process, go on.
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Get a time stamp id of type 'S' for the operation.
      SELECT emaj._set_time_stamp(v_function, 'S') INTO v_timeId;
-- Check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(p_groupNames);
-- If requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs.
      IF p_resetLog THEN
        PERFORM emaj._reset_groups(p_groupNames);
-- Drop the log schemas that would have been emptied by the _reset_groups() call.
        SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
        PERFORM emaj._drop_log_schemas(CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, FALSE);
        PERFORM emaj._enable_event_triggers(v_eventTriggers);
      END IF;
-- Check the supplied mark name (the check must be performed after the _reset_groups() call to allow to reuse an old mark name that is
-- being deleted.
      IF p_mark IS NULL OR p_mark = '' THEN
        p_mark = 'START_%';
      END IF;
      SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
--   risk of deadlock.
      PERFORM emaj._lock_groups(p_groupNames,'SHARE ROW EXCLUSIVE',p_multiGroup);
-- Enable all log triggers for the groups.
-- For each relation currently belonging to the groups,
      FOR r_tblsq IN
        SELECT rel_kind, rel_schema, rel_tblseq
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, enable the emaj log and truncate triggers.
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_log_trg',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq);
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_trunc_trg',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq);
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
-- Update the state of the group row from the emaj_group table.
      UPDATE emaj.emaj_group
        SET group_is_logging = TRUE
        WHERE group_name = ANY (p_groupNames);
-- Insert log sessions start into emaj_log_session...
--   lses_marks is already set to 1 as it will not be incremented at the first mark set.
      INSERT INTO emaj.emaj_log_session
        SELECT group_name, int8range(v_timeId, NULL, '[]'), 1, 0
          FROM emaj.emaj_group
          WHERE group_name = ANY (p_groupNames);
-- ... and update the last group history row to increment the number of log sessions
      UPDATE emaj.emaj_group_hist
        SET grph_log_sessions = grph_log_sessions + 1
        WHERE grph_group = ANY (p_groupNames)
          AND upper_inf(grph_time_range);
-- Set the first mark for each group.
      PERFORM emaj._set_mark_groups(p_groupNames, v_markName, NULL, p_multiGroup, TRUE, NULL, v_timeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_function               TEXT;
    v_groupList              TEXT;
    v_count                  INT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_group                  TEXT;
    v_lsesTimeRange          INT8RANGE;
    r_schema                 RECORD;
    r_tblsq                  RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','));
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE)
      INTO p_groupNames;
-- For all already IDLE groups, generate a warning message and remove them from the list of the groups to process.
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*)
      INTO v_groupList, v_count
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
       AND NOT group_is_logging;
    IF v_count = 1 THEN
      RAISE WARNING '_stop_groups: The group "%" is already in IDLE state.', v_groupList;
    END IF;
    IF v_count > 1 THEN
      RAISE WARNING '_stop_groups: The groups "%" are already in IDLE state.', v_groupList;
    END IF;
-- Process the LOGGING groups.
    SELECT array_agg(DISTINCT group_name)
      INTO p_groupNames
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
        AND group_is_logging;
    IF p_groupNames IS NOT NULL THEN
-- Check and process the supplied mark name (except if the function is called by emaj_force_stop_group()).
      IF NOT p_isForced THEN
        IF p_mark IS NULL OR p_mark = '' THEN
          p_mark = 'STOP_%';
        END IF;
        SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
      END IF;
-- OK (no error detected and at least one group in logging state)
-- Get a time stamp id of type 'X' for the operation.
      SELECT emaj._set_time_stamp(v_function, 'X') INTO v_timeId;
-- Lock all tables to get a stable point.
-- One sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
-- risk of deadlock.
      PERFORM emaj._lock_groups(p_groupNames,'SHARE ROW EXCLUSIVE',p_multiGroup);
-- Verify that all application schemas for the groups still exists.
      FOR r_schema IN
        SELECT DISTINCT rel_schema
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND NOT EXISTS
                 (SELECT nspname
                    FROM pg_catalog.pg_namespace
                    WHERE nspname = rel_schema
                 )
          ORDER BY rel_schema
      LOOP
        IF p_isForced THEN
          RAISE WARNING '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        END IF;
      END LOOP;
-- For each relation currently belonging to the groups to process...
      FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- If it is a table, check the table still exists,
          IF NOT EXISTS
               (SELECT 0
                  FROM pg_catalog.pg_class
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                  WHERE nspname = r_tblsq.rel_schema
                    AND relname = r_tblsq.rel_tblseq
               ) THEN
            IF p_isForced THEN
              RAISE WARNING '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            ELSE
              RAISE EXCEPTION '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            END IF;
          ELSE
-- ... and disable the emaj log and truncate triggers.
-- Errors are captured so that emaj_force_stop_group() can be silently executed.
            BEGIN
              EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                             r_tblsq.rel_schema, r_tblsq.rel_tblseq);
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
            BEGIN
              EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg',
                             r_tblsq.rel_schema, r_tblsq.rel_tblseq);
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
          END IF;
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
      IF NOT p_isForced THEN
-- If the function is not called by emaj_force_stop_group(), set the stop mark for each group,
        PERFORM emaj._set_mark_groups(p_groupNames, v_markName, NULL, p_multiGroup, TRUE, NULL, v_timeId);
-- and set the number of log rows to 0 for these marks.
        UPDATE emaj.emaj_mark m
          SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (p_groupNames)
            AND mark_time_id = v_timeId;
      END IF;
-- Process each tables group separately to ...
      FOREACH v_group IN ARRAY p_groupNames
      LOOP
-- Get the latest log session of the tables group.
        SELECT lses_time_range
          INTO v_lsesTimeRange
          FROM emaj.emaj_log_session
          WHERE lses_group = v_group
          ORDER BY lses_time_range DESC
          LIMIT 1;
-- Set all marks as 'DELETED' to avoid any further rollback and remove marks protection against rollback, if any.
        UPDATE emaj.emaj_mark
          SET mark_is_rlbk_protected = FALSE
          WHERE mark_group = v_group
            AND mark_time_id >= lower(v_lsesTimeRange);
-- Update the log session to set the time range upper bound
        UPDATE emaj.emaj_log_session
          SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[]')
          WHERE lses_group = v_group
            AND lses_time_range = v_lsesTimeRange;
      END LOOP;
-- Update the emaj_group table to set the groups state and the rollback protections.
      UPDATE emaj.emaj_group
        SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (p_groupNames);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_group(p_groupName TEXT, p_mark TEXT DEFAULT NULL, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group.
-- Input: group name, mark to set, optional comment.
-- Output: number of processed tables and sequences
-- '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
-- if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp,
  DECLARE
    v_markName               TEXT;
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'BEGIN', p_groupName, v_markName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkLogging := TRUE);
-- Check if the emaj group is OK.
    PERFORM 0
      FROM emaj._verify_groups(array[p_groupName], TRUE);
-- Check and process the supplied mark name.
    SELECT emaj._check_new_mark(array[p_groupName], p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
    PERFORM emaj._lock_groups(array[p_groupName],'ROW EXCLUSIVE',FALSE);
-- Effectively set the mark using the internal _set_mark_groups() function.
    SELECT emaj._set_mark_groups(array[p_groupName], v_markName, p_comment, FALSE, FALSE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'END', p_groupName, v_markName);
--
    RETURN v_nbRel;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_groups(p_groupNames TEXT[], p_mark TEXT DEFAULT NULL, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time.
-- Input: array of group names, mark to set, optional comment.
-- Output: number of processed tables and sequences
-- '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
-- if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
  DECLARE
    v_markName               TEXT;
    v_nbTblseq               INT = 0;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'BEGIN', array_to_string(p_groupNames,','), p_mark);
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := TRUE, p_lockGroups := TRUE, p_checkLogging := TRUE)
      INTO p_groupNames;
-- Process the groups.
    IF p_groupNames IS NOT NULL THEN
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Check and process the supplied mark name.
      SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
      PERFORM emaj._lock_groups(p_groupNames,'ROW EXCLUSIVE',TRUE);
-- Effectively set the mark using the internal _set_mark_groups() function.
      SELECT emaj._set_mark_groups(p_groupNames, v_markName, p_comment, TRUE, FALSE) INTO v_nbTblseq;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(p_groupNames,','), p_mark);
--
    RETURN v_nbTblseq;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT,TEXT) IS
$$Sets a mark on several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_comment TEXT, p_multiGroup BOOLEAN,
                                                 p_eventToRecord BOOLEAN, p_loggedRlbkTargetMark TEXT DEFAULT NULL,
                                                 p_timeId BIGINT DEFAULT NULL, p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates 1) the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into
-- all log tables between this previous mark and the new mark and 2) the current log session.
-- The function is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks,
-- like functions that start, stop or rollback groups.
-- Input: group names array, mark to set, comment,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
--        name of the rollback target mark when this mark is created by the logged_rollback functions (NULL by default)
--        time stamp identifier to reuse (NULL by default) (this parameter is set when the mark is a rollback start mark)
--        dblink schema when the mark is set by a rollback operation and dblink connection are used (NULL by default)
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
-- The function is defined as SECURITY DEFINER so that emaj_adm roles can use it even without SELECT right on the sequence.
  DECLARE
    v_function               TEXT;
    v_nbSeq                  INT;
    v_group                  TEXT;
    v_lsesTimeRange          INT8RANGE;
    v_latestMarkTimeId       BIGINT;
    v_nbChanges              BIGINT;
    v_nbTbl                  INT;
    v_stmt                   TEXT;
    r_seq                    RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END;
-- If requested by the calling function, record the set mark begin in emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','), p_mark);
    END IF;
-- Get the time stamp of the operation, if not supplied as input parameter.
    IF p_timeId IS NULL THEN
      SELECT emaj._set_time_stamp(v_function, 'M') INTO p_timeId;
    END IF;
-- Record sequences state as early as possible (no lock protects them from other transactions activity).
-- The join on pg_namespace and pg_class filters the potentially dropped application sequences.
    v_nbSeq = 0;
    FOR r_seq IN
      SELECT rel_schema, rel_tblseq
        FROM emaj.emaj_relation
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'S'
          AND rel_group = ANY (p_groupNames)
    LOOP
      EXECUTE format('INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,'
                     '            sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)'
                     '  SELECT nspname, relname, %s, sq.last_value, seqstart,'
                     '         seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '    FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '    WHERE nspname = %L AND relname = %L',
                     p_timeId, r_seq.rel_schema, r_seq.rel_tblseq, r_seq.rel_schema, r_seq.rel_tblseq);
      v_nbSeq = v_nbSeq + 1;
    END LOOP;
-- Record the number of log rows for the previous last mark of each selected group.
    FOREACH v_group IN ARRAY p_groupNames
    LOOP
-- Get the latest log session of the tables group.
      SELECT lses_time_range
        INTO v_lsesTimeRange
        FROM emaj.emaj_log_session
        WHERE lses_group = v_group
        ORDER BY lses_time_range DESC
        LIMIT 1;
      IF p_timeId > lower(v_lsesTimeRange) OR lower(v_lsesTimeRange) IS NULL THEN
-- This condition excludes marks set at start_group time, for which there is nothing to do.
--   The lower bound may be null when the log session has been created by the emaj version upgrade processing and the last start_group
--   call has not been found into the history.
-- Get the latest mark for the tables group.
        SELECT mark_time_id
          INTO v_latestMarkTimeId
          FROM emaj.emaj_mark
          WHERE mark_group = v_group
          ORDER BY mark_time_id DESC
          LIMIT 1;
-- Compute the number of changes for tables since this latest mark
        SELECT coalesce(sum(emaj._log_stat_tbl(emaj_relation, greatest(v_latestMarkTimeId, lower(rel_time_range)),NULL)), 0)
          INTO v_nbChanges
          FROM emaj.emaj_relation
          WHERE rel_group = v_group
            AND rel_kind = 'r'
            AND upper_inf(rel_time_range);
-- Update the latest mark statistics.
        UPDATE emaj.emaj_mark
          SET mark_log_rows_before_next = v_nbChanges
          WHERE mark_group = v_group
            AND mark_time_id = v_latestMarkTimeId;
-- Update the current log session statistics.
        UPDATE emaj.emaj_log_session
          SET lses_marks = lses_marks + 1,
              lses_log_rows = lses_log_rows + v_nbChanges
          WHERE lses_group = v_group
            AND lses_time_range = v_lsesTimeRange;
      END IF;
    END LOOP;
-- For tables currently belonging to the groups, record their state and their log sequence last_value.
    INSERT INTO emaj.emaj_table (tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val)
      SELECT rel_schema, rel_tblseq, p_timeId, reltuples, relpages, last_value
        FROM emaj.emaj_relation
             LEFT OUTER JOIN pg_catalog.pg_namespace ON (nspname = rel_schema)
             LEFT OUTER JOIN pg_catalog.pg_class ON (relname = rel_tblseq AND relnamespace = pg_namespace.oid),
             LATERAL emaj._get_log_sequence_last_value(rel_log_schema, rel_log_sequence) AS last_value
        WHERE upper_inf(rel_time_range)
          AND rel_group = ANY (p_groupNames)
          AND rel_kind = 'r';
    GET DIAGNOSTICS v_nbTbl = ROW_COUNT;
-- Record the mark for each group into the emaj_mark table.
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_rlbk_protected, mark_comment, mark_logged_rlbk_target_mark)
      SELECT group_name, p_mark, p_timeId, FALSE, p_comment, p_loggedRlbkTargetMark
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
        ORDER BY group_name;
-- Before exiting, cleanup the state of the pending rollback events from the emaj_rlbk table.
-- It uses a dblink connection when the mark to set comes from a rollback operation that uses dblink connections.
    v_stmt = 'SELECT emaj._cleanup_rollback_state()';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, p_dblinkSchema);
-- If requested by the calling function, record the set mark end into emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (v_function, 'END', array_to_string(p_groupNames,','), p_mark);
    END IF;
--
    RETURN v_nbSeq + v_nbTbl;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj._rlbk_async(p_rlbkId INT, p_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_async$
-- The function calls the main rollback functions following the initialisation phase.
-- It is only called by the Emaj_web client, in an asynchronous way, so that the rollback can be then monitored by the client.
-- Input: rollback identifier, and a boolean saying if the rollback is a logged rollback
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dbLinkCnxStatus        INT;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_is_dblink_used INTO v_isDblinkUsed
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- If dblink is used (which should always be true), try to open the first session connection (no error is issued if it is already opened).
    IF v_isDblinkUsed THEN
      SELECT p_status INTO v_dbLinkCnxStatus
        FROM emaj._dblink_open_cnx('rlbk#1', current_role);
      IF v_dbLinkCnxStatus < 0 THEN
        RAISE EXCEPTION '_rlbk_async: Error while opening the dblink session #1 (Status of the dblink connection attempt = %'
                        ' - see E-Maj documentation).',
          v_dbLinkCnxStatus;
      END IF;
    ELSE
      RAISE EXCEPTION '_rlbk_async: The function is called but dblink cannot be used. This is an error from the client side.';
    END IF;
-- Simply chain the internal functions.
    PERFORM emaj._rlbk_session_lock(p_rlbkId, 1);
    PERFORM emaj._rlbk_start_mark(p_rlbkId, p_multiGroup);
    PERFORM emaj._rlbk_session_exec(p_rlbkId, 1);
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_end(p_rlbkId, p_multiGroup);
  END;
$_rlbk_async$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN, p_nbSession INT, p_multiGroup BOOLEAN,
                                           p_isAlterGroupAllowed BOOLEAN, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps (or NULL if there are some NULL input).
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_startTs                TIMESTAMPTZ;
    v_markName               TEXT;
    v_markTimeId             BIGINT;
    v_markTimestamp          TIMESTAMPTZ;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsed           BOOLEAN;
    v_dbLinkSchema           TEXT;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_stmt                   TEXT;
    v_rlbkId                 INT;
  BEGIN
    v_startTs = clock_timestamp();
-- Check supplied group names and mark parameters.
    SELECT emaj._rlbk_check(p_groupNames, p_mark, p_isAlterGroupAllowed, FALSE) INTO v_markName;
    IF v_markName IS NOT NULL THEN
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Get the time stamp id and its clock timestamp for the first group (as we know this time stamp is the same for all groups of the array).
      SELECT time_id, time_clock_timestamp INTO v_markTimeId, v_markTimestamp
        FROM emaj.emaj_mark
             JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
        WHERE mark_group = p_groupNames[1]
          AND mark_name = v_markName;
-- Insert a BEGIN event into the history.
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
                array_to_string(p_groupNames,','),
                CASE WHEN p_isLoggedRlbk THEN 'Logged' ELSE 'Unlogged' END || ' rollback to mark ' || v_markName
                || ' [' || v_markTimestamp || ']'
               )
        RETURNING hist_id INTO v_histId;
-- Get the total number of tables and sequences for these groups.
      SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
        FROM emaj.emaj_group
        WHERE group_name = ANY (p_groupNames) ;
-- First try to open a dblink connection.
      SELECT p_status, (p_status >= 0), CASE WHEN p_status >= 0 THEN p_schema ELSE NULL END
        INTO v_dbLinkCnxStatus, v_isDblinkUsed, v_dbLinkSchema
        FROM emaj._dblink_open_cnx('rlbk#1', current_role);
-- For parallel rollback (i.e. when nb sessions > 1), the dblink connection must be ok.
      IF p_nbSession > 1 AND NOT v_isDblinkUsed THEN
        RAISE EXCEPTION '_rlbk_init: Cannot use several sessions without dblink connection capability. (Status of the dblink'
                        ' connection attempt = % - see E-Maj documentation)',
          v_dbLinkCnxStatus;
      END IF;
-- Create the row representing the rollback event in the emaj_rlbk table and get the rollback id back.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, ' ||
               'rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, ' ||
               'rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
               'rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_start_datetime) ' ||
               'VALUES (' || quote_literal(p_groupNames) || ',' || quote_literal(v_markName) || ',' ||
               v_markTimeId || ',' || p_isLoggedRlbk || ',' || quote_nullable(p_isAlterGroupAllowed) || ',' ||
               quote_nullable(p_comment) || ',' || p_nbSession || ',' || v_nbTblInGroups || ',' || v_nbSeqInGroups || ',' ||
               CASE WHEN v_nbSeqInGroups = 0 THEN '0' ELSE 'NULL' END || ',''PLANNING'',' || v_histId || ',' ||
               quote_nullable(v_dbLinkSchema) || ',' || v_isDblinkUsed || ',' || quote_literal(v_startTs) || ') RETURNING rlbk_id';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_rlbkId;
-- Create the session row the emaj_rlbk_session table.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
               'VALUES (' || v_rlbkId || ', 1, ' || txid_current() || ',' ||
                quote_literal(clock_timestamp()) || ') RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Call the rollback planning function to define all the elementary steps to perform, compute their estimated duration
-- and spread the elementary steps among sessions.
      v_stmt = 'SELECT emaj._rlbk_planning(' || v_rlbkId || ')';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_effNbTable;
-- Update the emaj_rlbk table to set the real number of tables to process and adjust the rollback status.
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_table = ' || v_effNbTable ||
               ', rlbk_status = ''LOCKING'', rlbk_end_planning_datetime = ''' || clock_timestamp() || '''' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
    END IF;
--
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(p_rlbkId INT, p_session INT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_lock$
-- It opens the session if needed, creates the session row in the emaj_rlbk_session table
-- and then locks all the application tables for the session.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on tables.
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
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema, rlbk_groups
      INTO v_isDblinkUsed, v_dblinkSchema, v_groupNames
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- For dblink session > 1, open the connection (the session 1 is already opened).
    IF p_session > 1 THEN
      SELECT p_status INTO v_dbLinkCnxStatus
        FROM emaj._dblink_open_cnx('rlbk#' || p_session, current_role);
      IF v_dbLinkCnxStatus < 0 THEN
        RAISE EXCEPTION '_rlbk_session_lock: Error while opening the dblink session #% (Status of the dblink connection attempt = %'
                        ' - see E-Maj documentation).',
          p_session, v_dbLinkCnxStatus;
      END IF;
-- ... and create the session row the emaj_rlbk_session table.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
               'VALUES (' || p_rlbkId || ',' || p_session || ',' || txid_current() || ',' ||
                quote_literal(clock_timestamp()) || ') RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
    END IF;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'BEGIN', array_to_string(v_groupNames,','), 'Rollback session #' || p_session);
--
-- Acquire locks on tables.
--
-- In case of deadlock, retry up to 5 times.
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- Scan all tables of the session, in priority ascending order.
        FOR r_tbl IN
          SELECT quote_ident(rlbp_schema) || '.' || quote_ident(rlbp_table) AS fullName,
                 EXISTS
                   (SELECT 1
                      FROM emaj.emaj_rlbk_plan rlbp2
                      WHERE rlbp2.rlbp_rlbk_id = p_rlbkId
                        AND rlbp2.rlbp_session = p_session
                        AND rlbp2.rlbp_schema = rlbp1.rlbp_schema
                        AND rlbp2.rlbp_table = rlbp1.rlbp_table
                        AND rlbp2.rlbp_step = 'DIS_LOG_TRG'
                   ) AS disLogTrg
            FROM emaj.emaj_rlbk_plan rlbp1
                 JOIN emaj.emaj_relation ON (rel_schema = rlbp_schema AND rel_tblseq = rlbp_table AND upper_inf(rel_time_range))
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'LOCK_TABLE'
              AND rlbp_session = p_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- Lock each table.
-- The locking level is EXCLUSIVE mode.
-- This blocks all concurrent update capabilities of all tables of the groups (including tables with no logged update to rollback),
-- in order to ensure a stable state of the group at the end of the rollback operation).
-- But these tables can be accessed by SELECT statements during the E-Maj rollback.
          EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE',
                         r_tbl.fullName);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- OK, all tables locked.
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_session_lock: A deadlock has been trapped while locking tables for groups "%".',
            array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      PERFORM emaj._rlbk_error(p_rlbkId, '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables',
                               'rlbk#' || p_session);
      RAISE EXCEPTION '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables for groups "%".',
        array_to_string(v_groupNames,',');
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'END', array_to_string(v_groupNames,','),
              'Rollback session #' || p_session || ': ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_session_lock() for session ' || p_session || ': ' || SQLERRM, 'rlbk#' || p_session);
      RAISE;
  END;
$_rlbk_session_lock$;

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(p_rlbkId INT, p_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_start_mark$
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
-- Before setting the mark, it checks no update has been recorded between the planning step and the locks set
-- for tables for which no rollback was needed at planning time.
-- It also sets the rollback status to EXECUTING.
  DECLARE
    v_function               TEXT;
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
    v_markComment            TEXT;
    v_errorMsg               TEXT;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END;
-- Get the dblink usage characteristics for the current rollback.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema INTO v_isDblinkUsed, v_dblinkSchema
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get a time stamp for the rollback operation and record it into emaj_hist
--   (the _set_time_stamp() function doesn't trace events of type 'R' in emaj_hist for visibility reason.)
    v_stmt = 'SELECT emaj._set_time_stamp(''' || v_function || ''', ''R'')';
    SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'TIME STAMP SET', v_timeId::TEXT);
-- Update the emaj_rlbk table to record the time stamp and adjust the rollback status.
    v_stmt = 'UPDATE emaj.emaj_rlbk' ||
             ' SET rlbk_time_id = ' || v_timeId || ', rlbk_end_locking_datetime = time_clock_timestamp, rlbk_status = ''EXECUTING''' ||
             ' FROM emaj.emaj_time_stamp' ||
             ' WHERE time_id = ' || v_timeId || ' AND rlbk_id = ' || p_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_end_locking_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_rlbkDatetime
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get some mark attributes from emaj_mark.
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Check that no update has been recorded between planning time and lock time for tables that did not need to
-- be rolled back at planning time.
-- This may occur and cannot be avoided because tables cannot be locked before processing the rollback planning.
-- Sessions must lock the tables they will rollback and the planning processing distribute those tables to sessions.
    IF EXISTS
         (SELECT 0
            FROM
              (SELECT *
                 FROM emaj.emaj_relation
                 WHERE upper_inf(rel_time_range)
                   AND rel_group = ANY (v_groupNames)
                   AND rel_kind = 'r'
                   AND NOT EXISTS
                         (SELECT NULL
                            FROM emaj.emaj_rlbk_plan
                            WHERE rlbp_schema = rel_schema
                              AND rlbp_table = rel_tblseq
                              AND rlbp_rlbk_id = p_rlbkId
                              AND rlbp_step = 'RLBK_TABLE'
                         )
              ) AS t
            WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0
         ) THEN
      v_errorMsg = 'the rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables'
                   ' to process.';
      PERFORM emaj._rlbk_error(p_rlbkId, v_errorMsg, 'rlbk#1');
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If the rollback is "logged", set a mark named with the pattern: 'RLBK_<rollback_id>_START'.
      v_markName = 'RLBK_' || p_rlbkId::text || '_START';
      v_markComment = 'Automatically set at rollback to mark ' || v_mark || ' start';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_markComment, p_multiGroup, TRUE, NULL, v_timeId, v_dblinkSchema);
    END IF;
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_start_mark(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_start_mark$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(p_rlbkId INT, p_session INT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_rlbkTimeId             BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_nbSequence             INT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_maxGlobalSeq           BIGINT;
    v_rlbkMarkTimeId         BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_effNbSequence          INT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_nb_session, rlbk_nb_sequence,
           rlbk_dblink_schema, rlbk_is_dblink_used, time_last_emaj_gid
      INTO v_groupNames, v_mark, v_rlbkTimeId, v_isLoggedRlbk, v_nbSession, v_nbSequence,
           v_dblinkSchema, v_isDblinkUsed, v_maxGlobalSeq
      FROM emaj.emaj_rlbk
           JOIN emaj.emaj_time_stamp ON (time_id = rlbk_time_id)
      WHERE rlbk_id = p_rlbkId;
-- Fetch the mark_time_id, the last global sequence at set_mark time for the first group of the groups array.
-- They all share the same values.
    SELECT mark_time_id, time_last_emaj_gid INTO v_rlbkMarkTimeId, v_lastGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Scan emaj_rlbp_plan to get all steps to process that have been assigned to this session, in batch_number and step order.
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_object_def, rlbp_app_trg_type,
             rlbp_is_repl_role_replica, rlbp_target_time_id
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step NOT IN ('LOCK_TABLE','CTRL-DBLINK','CTRL+DBLINK')
          AND rlbp_session = p_session
        ORDER BY rlbp_batch_number, rlbp_step, rlbp_table, rlbp_object
    LOOP
-- Update the emaj_rlbk_plan table to set the step start time.
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = clock_timestamp() ' ||
               ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
-- Process the step depending on its type.
      CASE r_step.rlbp_step
        WHEN 'RLBK_SEQUENCES' THEN
-- Rollback all sequences at once
-- If the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time.
          SELECT sum(emaj._rlbk_seq(t.*, greatest(v_rlbkMarkTimeId, lower(t.rel_time_range)))) INTO v_effNbSequence
            FROM
              (SELECT *
                 FROM emaj.emaj_relation
                 WHERE upper_inf(rel_time_range)
                   AND rel_group = ANY (v_groupNames)
                   AND rel_kind = 'S'
                 ORDER BY rel_schema, rel_tblseq
              ) as t;
-- Record into emaj_rlbk the number of effectively rolled back sequences
          v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_sequence = ' || coalesce(v_effNbSequence, 0) ||
                   ' WHERE rlbk_id = ' || p_rlbkId || ' RETURNING 1';
          PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
        WHEN 'DIS_APP_TRG' THEN
-- Disable an application trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'SET_ALWAYS_APP_TRG' THEN
-- Set an application trigger as an ALWAYS trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I, ENABLE ALWAYS TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object);
        WHEN 'DIS_LOG_TRG' THEN
-- Disable a log trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
        WHEN 'DROP_FK' THEN
-- Delete a foreign key.
          EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'SET_FK_DEF' THEN
-- Set a foreign key deferred.
          EXECUTE format('SET CONSTRAINTS %I.%I DEFERRED',
                             r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'RLBK_TABLE' THEN
-- Process a table rollback.
-- For tables added to the group after the rollback target mark, get the last sequence value specific to each table.
          SELECT emaj._rlbk_tbl(emaj_relation.*,
                                CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                     ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id)
                                END,
                                v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk, r_step.rlbp_is_repl_role_replica) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema
              AND rel_tblseq = r_step.rlbp_table
              AND upper_inf(rel_time_range);
        WHEN 'DELETE_LOG' THEN
-- Process the deletion of log rows.
-- For tables added to the group after the rollback target mark, get the last sequence value specific to each table.
          SELECT emaj._delete_log_tbl(emaj_relation.*, r_step.rlbp_target_time_id, v_rlbkTimeId,
                                      CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                           ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp
                                                   WHERE time_id = r_step.rlbp_target_time_id) END)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema
              AND rel_tblseq = r_step.rlbp_table
              AND upper_inf(rel_time_range);
        WHEN 'SET_FK_IMM' THEN
-- Set a foreign key immediate.
          EXECUTE format('SET CONSTRAINTS %I.%I IMMEDIATE',
                             r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'ADD_FK' THEN
-- Re-create a foreign key.
          EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object_def);
        WHEN 'ENA_APP_TRG' THEN
-- Enable an application trigger.
          EXECUTE format('ALTER TABLE %I.%I ENABLE %s TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_app_trg_type, r_step.rlbp_object);
        WHEN 'SET_LOCAL_APP_TRG' THEN
-- Reset an application trigger to its common type.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I, ENABLE TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object);
        WHEN 'ENA_LOG_TRG' THEN
-- Enable a log trigger.
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
      END CASE;
-- Update the emaj_rlbk_plan table to set the step duration as well as the quantity when it is relevant.
-- The computed duration does not include the time needed to update the emaj_rlbk_plan table,
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_duration = ' || quote_literal(clock_timestamp()) || ' - rlbp_start_datetime';
      IF r_step.rlbp_step = 'RLBK_TABLE' OR r_step.rlbp_step = 'DELETE_LOG' THEN
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbRows;
      END IF;
      IF r_step.rlbp_step = 'RLBK_SEQUENCES' THEN
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbSequence;
      END IF;
      v_stmt = v_stmt ||
               ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
    END LOOP;
-- Update the emaj_rlbk_session table to set the timestamp representing the end of work for the session.
    v_stmt = 'UPDATE emaj.emaj_rlbk_session SET rlbs_end_datetime = clock_timestamp()' ||
             ' WHERE rlbs_rlbk_id = ' || p_rlbkId || ' AND rlbs_session = ' || p_session ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
-- Close the dblink connection, if any, for session > 1.
    IF v_isDblinkUsed AND p_session > 1 THEN
      PERFORM emaj._dblink_close_cnx('rlbk#' || p_session, v_dblinkSchema);
    END IF;
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_session_exec() for session ' || p_session || ': ' || SQLERRM, 'rlbk#' || p_session);
      RAISE;
  END;
$_rlbk_session_exec$;

CREATE OR REPLACE FUNCTION emaj._rlbk_end(p_rlbkId INT, p_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
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
    v_function               TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_isAlterGroupAllowed    BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_nbSeq                  INT;
    v_effNbSeq               INT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_markTimeId             BIGINT;
    v_stmt                   TEXT;
    v_ctrlDuration           INTERVAL;
    v_messages               TEXT[] = ARRAY[]::TEXT[];
    v_markName               TEXT;
    v_markComment            TEXT;
    v_msg                    TEXT;
    v_msgList                TEXT;
    r_msg                    RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END;
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table, rlbk_nb_sequence,
           rlbk_eff_nb_sequence, rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_end_locking_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl, v_nbSeq,
           v_effNbSeq, v_dblinkSchema, v_isDblinkUsed, v_rlbkDatetime
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get the mark timestamp for the 1st group (they all share the same timestamp).
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- If "unlogged" rollback, delete all marks later than the now rolled back mark and the associated sequences.
    IF NOT v_isLoggedRlbk THEN
-- Get the highest mark time id of the mark used for rollback, for all groups.
-- Delete the marks that are suppressed by the rollback (the related sequences have been already deleted), with a trace in the history,
      WITH deleted AS
        (DELETE FROM emaj.emaj_mark
           WHERE mark_group = ANY (v_groupNames)
             AND mark_time_id > v_markTimeId
           RETURNING mark_time_id, mark_group, mark_name
        ),
           sorted_deleted AS                                        -- the sort is performed to produce stable results in regression tests
        (SELECT mark_group, mark_name
           FROM deleted
           ORDER BY mark_time_id, mark_group
        )
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT v_function, 'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted'
        FROM sorted_deleted;
-- ... and reset the mark_log_rows_before_next column for the new groups latest marks.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND mark_time_id = v_markTimeId;
-- The sequences related to the deleted marks can be also suppressed.
-- Delete first application sequences related data for the groups.
      DELETE FROM emaj.emaj_sequence
        USING emaj.emaj_relation
        WHERE sequ_schema = rel_schema
          AND sequ_name = rel_tblseq
          AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND rel_kind = 'S'
          AND sequ_time_id > v_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
-- Delete then tables related data for the groups.
      DELETE FROM emaj.emaj_table
        USING emaj.emaj_relation
        WHERE tbl_schema = rel_schema
          AND tbl_name = rel_tblseq
          AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND tbl_time_id > v_markTimeId
          AND tbl_time_id <@ rel_time_range
          AND tbl_time_id <> lower(rel_time_range);
    END IF;
-- Delete the now useless 'LOCK TABLE' steps from the emaj_rlbk_plan table.
    v_stmt = 'DELETE FROM emaj.emaj_rlbk_plan ' ||
             ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ''LOCK_TABLE'' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Prepare the CTRLxDBLINK pseudo step statistic by computing the global time spent between steps.
    SELECT coalesce(sum(ctrl_duration),'0'::INTERVAL) INTO v_ctrlDuration
      FROM
        (SELECT rlbs_session, rlbs_end_datetime - min(rlbp_start_datetime) - sum(rlbp_duration) AS ctrl_duration
           FROM emaj.emaj_rlbk_session rlbs
                JOIN emaj.emaj_rlbk_plan rlbp ON (rlbp_rlbk_id = rlbs_rlbk_id AND rlbp_session = rlbs_session)
           WHERE rlbs_rlbk_id = p_rlbkId
           GROUP BY rlbs_session, rlbs_end_datetime
        ) AS t;
-- Report duration statistics into the emaj_rlbk_stat table.
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_stat (rlbt_step, rlbt_schema, rlbt_table, rlbt_object,' ||
             '      rlbt_rlbk_id, rlbt_quantity, rlbt_duration)' ||
--   copy elementary steps for RLBK_TABLE, RLBK_SEQUENCES, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_rlbk_id,' ||
             '      rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''RLBK_SEQUENCES'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 6 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''DIS_APP_TRG'',''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_APP_TRG'',''ENA_LOG_TRG'') ' ||
             '    GROUP BY 1, 2, 3, 4, 5' ||
             '  UNION ALL ' ||
--   and the final CTRLxDBLINK pseudo step statistic
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      rlbp_estimated_quantity, ' || quote_literal(v_ctrlDuration) ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''CTRL+DBLINK'',''CTRL-DBLINK'') ' ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Build the execution report.
-- Start with the NOTICE messages.
    v_messages = array_append(v_messages,
                              'Notice: ' || format ('Rollback id = %s.', p_rlbkId::TEXT));
    IF v_nbTbl > 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: ' || format ('%s / %s tables effectively processed.', v_effNbTbl::TEXT, v_nbTbl::TEXT));
    END IF;
    IF v_nbSeq > 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: ' || format ('%s / %s sequences effectively processed.', v_effNbSeq::TEXT, v_nbSeq::TEXT));
    END IF;
    IF v_nbTbl = 0 AND v_nbSeq = 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: no table and sequence to process');
    END IF;
-- And then the WARNING messages for any elementary action from group structure change that has not been rolled back.
    FOR r_msg IN
-- Steps are splitted into 2 groups to filter them differently.
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               (CASE
                  WHEN rlchg_change_kind = 'ADD_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND NOT new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'ADD_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND NOT new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
          FROM
-- Suppress duplicate ADD_TABLE / MOVE_TABLE / REMOVE_TABLE or ADD_SEQUENCE / MOVE_SEQUENCE / REMOVE_SEQUENCE for same table or sequence,
-- by keeping the most recent changes.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind, new_group_is_rolledback
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         (rlchg_new_group = ANY (v_groupNames)) AS new_group_is_rolledback,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND (rlchg_group = ANY (v_groupNames) OR rlchg_new_group = ANY (v_groupNames))
                      AND rlchg_tblseq <> ''
                      AND rlchg_change_kind IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                  ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE rlchg_time_id = time_id
      UNION
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               'Tables group change not rolled back: ' ||
               (CASE rlchg_change_kind
                  WHEN 'CHANGE_PRIORITY' THEN
                    'E-Maj priority for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_DATA_TABLESPACE' THEN
                    'log data tablespace for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_INDEX_TABLESPACE' THEN
                    'log index tablespace for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_IGNORED_TRIGGERS' THEN
                    'ignored triggers list for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  ELSE rlchg_change_kind::TEXT || ' / ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  END)::TEXT AS message
          FROM
-- Suppress duplicates for other change kind for each table or sequence.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND rlchg_group = ANY (v_groupNames)
                      AND rlchg_tblseq <> ''
                      AND rlchg_change_kind NOT IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                 ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2
        ORDER BY rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq
    LOOP
      v_messages = array_append(v_messages, 'Warning: ' || r_msg.message);
    END LOOP;
    IF v_isLoggedRlbk THEN
-- If the rollback is "logged", set a mark named with the pattern: 'RLBK_<rollback_id>_DONE'.
      v_markName = 'RLBK_' || p_rlbkId::text || '_DONE';
      v_markComment = 'Automatically set at rollback to mark ' || v_mark || ' end';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_markComment, p_multiGroup, TRUE, v_mark);
    END IF;
-- Return and trace the execution report
    FOREACH v_msg IN ARRAY v_messages
    LOOP
      SELECT substring(v_msg FROM '^(Notice|Warning): '), substring(v_msg, '^(?:Notice|Warning): (.*)') INTO rlbk_severity, rlbk_message;
      RETURN NEXT;
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, UPPER(rlbk_severity), 'Rollback id ' || p_rlbkId,
                rlbk_message);
    END LOOP;
-- Update the emaj_rlbk table to adjust the rollback status and set the output messages.
    SELECT string_agg(quote_literal(msg), ',') FROM unnest(v_messages) AS msg INTO v_msgList;
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = '''
          || CASE WHEN v_isDblinkUsed THEN 'COMPLETED' ELSE 'COMMITTED' END
          || ''', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_msgList || ']' ||
               ' WHERE rlbk_id = ' || p_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Close the dblink connection, if any.
    IF v_isDblinkUsed THEN
      PERFORM emaj._dblink_close_cnx('rlbk#1', v_dblinkSchema);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(v_groupNames,','), 'Rollback_id ' || p_rlbkId);
-- Final return.
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_end(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_end$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                                p_optionsList TEXT, p_tblseqs TEXT[], p_scriptLocation TEXT)
RETURNS TEXT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- The SQL statements are stored into a flat file with a COPY TO statement, using a location provided by the caller.
-- Some options may be set to customize the SQL generation (here in alphabetic order):
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - ORDER_BY=PK|TIME defines the data sort criteria in the output for tables (default depends on the consolidation level)
--   - SEQUENCES_ONLY filters only sequences
--   - PSQL_COPY_DIR generates a psql \copy meta-command for each statement, using the directory name given by the option
--   - PSQL_COPY_OPTIONS defines the options to use for the psql \copy meta-command
--   - SQL_FORMAT=RAW|PRETTY defines how the generated SQL will be formatted
--   - TABLES_ONLY filters only tables
-- Complex options such as lists or directory names must be set between ().
-- It's users responsability to create the directory containing the output file before the function call (with proper permissions
--   allowing the cluster to write into).
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        the absolute pathname of the file that will hold the result (NULL to get the result from a temporary table).
-- Output: Message with the number of generated SQL statements.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_nbStmt                 INT;
    v_isPsqlCopy             BOOLEAN;
  BEGIN
-- Insert the BEGIN event into the history, but only if an external will be produced.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_DUMP_CHANGES_GROUP', 'BEGIN', p_groupName,
       'From mark ' || coalesce(p_firstMark, '') || ' to ' || coalesce(p_lastMark, '') ||
       ' towards ' || p_scriptLocation);
-- Check the script location is not null.
    IF p_scriptLocation IS NULL THEN
      RAISE EXCEPTION 'emaj_gen_sql_dump_changes_group: The output script location parameter cannot be NULL.';
    END IF;
-- Call the _gen_sql_dump_changes_group() function to proccess options and build the SQL statements.
    SELECT p_nbStmt, p_isPsqlCopy
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, TRUE)
      INTO v_nbStmt, v_isPsqlCopy;
-- Process the emaj_temp_sql temporary table.
-- An output file is supplied. So write the SQL script into the external file and drop the temporary table.
    IF v_isPsqlCopy THEN
-- If there are psql \copy meta-commands, remove the doubled antislash characters.
      BEGIN
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number)' ||
                      ' TO PROGRAM ''sed "s/\\\\\\\\/\\\\/g" >%s'' ',
                      p_scriptLocation);
      EXCEPTION
        WHEN OTHERS THEN
--   If it fails (typically because the sed command is not available), write the script as is, and warn the user about the doubled
--     antislashes he has to remove.
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number) TO %L',
                      p_scriptLocation);
          RAISE WARNING 'emaj_gen_sql_dump_changes_group: the shell sed command does not seem to exist.'
                        ' Generated doubled antislash characters will need to be removed manually.';
      END;
    ELSE
-- There is no psql meta-command so no antislashes to remove.
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number) TO %L',
                      p_scriptLocation);
    END IF;
    DROP TABLE IF EXISTS emaj_temp_sql;
-- Insert a END event into the history if a file has been generated.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_DUMP_CHANGES_GROUP', 'END', p_groupName, v_nbStmt || ' generated SQL statements');
-- Return a formatted message.
    RETURN format('%s SQL statements have been written into the "%s" file', v_nbStmt, p_scriptLocation);
  END;
$emaj_gen_sql_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[],TEXT) IS
$$Generate SQL statements into a file to dump recorded changes between two marks for application tables and sequences of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_optionsList TEXT,
                                                        p_tblseqs TEXT[], p_dir TEXT)
RETURNS TEXT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_dump_changes_group$
-- This function reads log tables and sequences states to export into files the data changes recorded between 2 marks for a group.
-- The function performs COPY TO statements, using the options provided by the caller.
-- Some options may be set to customize the changes dump:
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - COPY_OPTIONS=(options) sets the options to use for COPY TO statements
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - NO_EMPTY_FILES ... removes empty files
--   - ORDER_BY=PK|TIME defines the data sort criteria in the output for tables (default depends on the consolidation level)
--   - SEQUENCES_ONLY filters only sequences
--   - TABLES_ONLY filters only tables
-- Complex options such as lists or directory names must be set between ().
-- It's users responsability to create the directory  before the function call (with proper permissions allowing the cluster to
--   write into).
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        the absolute pathname of the directory where the files are to be created.
-- Output: Message with the number of generated files (for tables and sequences, including the _INFO file).
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_copyOptions            TEXT;
    v_noEmptyFiles           BOOLEAN;
    v_nbFile                 INT = 1;
    v_pathName               TEXT;
    v_copyResult             INT;
    v_nbRows                 INT;
    v_stmt                   TEXT;
    r_sql                    RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DUMP_CHANGES_GROUP', 'BEGIN', p_groupName,
       'From mark ' || coalesce(p_firstMark, '') || ' to ' || coalesce(p_lastMark, '') ||
       coalesce(' towards ' || p_dir, ''));
-- Call the _gen_sql_dump_changes_group() function to proccess options and get the SQL statements.
    SELECT p_copyOptions, p_noEmptyFiles, g.p_lastMark
      INTO v_copyOptions, v_noEmptyFiles, p_lastMark
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, FALSE) g;
-- Test the supplied output directory and copy options.
    IF p_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_dump_changes_group: The directory parameter cannot be NULL.';
    END IF;
    EXECUTE format ('COPY (SELECT '''') TO %L %s',
                    p_dir || '/_INFO', coalesce(v_copyOptions, ''));
-- Execute each generated SQL statement.
    FOR r_sql IN
      SELECT sql_stmt_number, sql_schema, sql_tblseq, sql_file_name_suffix, sql_text
        FROM emaj_temp_sql
        WHERE sql_line_number = 1
        ORDER BY sql_stmt_number
      LOOP
        IF r_sql.sql_text ~ '^(SET|RESET)' THEN
-- The SET or RESET statements are executed as is.
          EXECUTE r_sql.sql_text;
        ELSE
-- Otherwise, dump the log table or the sequence states.
          v_pathName = emaj._build_path_name(p_dir, r_sql.sql_schema || '_' || r_sql.sql_tblseq || r_sql.sql_file_name_suffix);
          EXECUTE format ('COPY (%s) TO %L %s',
                          r_sql.sql_text, v_pathName, coalesce(v_copyOptions, ''));
          v_copyResult = 1;
-- If the output file is empty, remove it, if requested.
          IF v_noEmptyFiles THEN
            GET DIAGNOSTICS v_nbRows = ROW_COUNT;
            IF v_nbRows = 0 THEN
-- The file is removed by calling a rm shell command through a COPY TO PROGRAM statement.
              EXECUTE format ('COPY (SELECT NULL) TO PROGRAM ''rm %s''',
                              v_pathName);
              v_copyResult = 0;
            END IF;
          END IF;
          v_nbFile = v_nbFile + v_copyResult;
-- Keep a trace of the dump execution.
          UPDATE emaj_temp_sql
            SET sql_result = v_copyResult
            WHERE sql_stmt_number = r_sql.sql_stmt_number AND sql_line_number = 1;
        END IF;
    END LOOP;
-- Create the _INFO file to keep information about the operation.
-- It contains 3 first rows with general information and then 1 row per effectively written file, describing the file content.
    v_stmt = '(SELECT ' || quote_literal('Dump logged changes for the group "' || p_groupName || '" between mark "' || p_firstMark ||
                                         '" and mark "' || p_lastMark || '"') ||
             ' UNION ALL' ||
             ' SELECT ' || quote_literal(coalesce('  using options "' || p_optionsList || '"', ' without option')) ||
             ' UNION ALL' ||
             ' SELECT ' || quote_literal('  started at ' || statement_timestamp()) ||
             ' UNION ALL' ||
             ' SELECT ''File '' || '
                      'translate(sql_schema || ''_'' || sql_tblseq || sql_file_name_suffix, E'' /\\$<>*'', ''_______'')'
                      ' || '' covers '' || sql_rel_kind || '' "'' || sql_schema || ''.'' || sql_tblseq || ''" from mark "'''
                      ' || sql_first_mark || ''" to mark "'' || sql_last_mark || ''"'''
                'FROM emaj_temp_sql WHERE sql_line_number = 1 AND sql_result = 1)';
    EXECUTE format ('COPY %s TO %L',
                    v_stmt, p_dir || '/_INFO');
-- Drop the temporary table.
    DROP TABLE IF EXISTS emaj_temp_sql;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DUMP_CHANGES_GROUP', 'END', p_groupName, v_nbFile || ' generated files');
-- Return a formated message.
    RETURN format('%s files have been created in %s', v_nbFile, p_dir);
  END;
$emaj_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[],TEXT) IS
$$Dump recorded changes between two marks for application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(p_groupName TEXT, p_dir TEXT, p_copyOptions TEXT)
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
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_nbRel                  INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_relOid                 OID;
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
          SELECT pg_class.oid INTO v_relOid
            FROM pg_class
                 JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
            WHERE nspname = r_tblsq.rel_schema
              AND relname = r_tblsq.rel_tblseq;
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
                     AND indrelid = v_relOid
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
                   WHERE attrelid = v_relOid
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          END IF;
--   Dump the table
          v_stmt = format('(SELECT * FROM %s ORDER BY %s)', v_fullTableName, v_colList);
          EXECUTE format ('COPY %s TO %L %s',
                          v_stmt, v_pathName, coalesce(p_copyOptions, ''));
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
          EXECUTE format ('COPY %s TO %L %s',
                          v_stmt, v_pathName, coalesce(p_copyOptions, ''));
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || transaction_timestamp()) || ')';
    EXECUTE format ('COPY %s TO %L',
                    v_stmt, p_dir || '/_INFO');
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_location TEXT,
                                                   p_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SET standard_conforming_strings = ON AS
$emaj_gen_sql_group$
-- This function generates a SQL script representing all updates performed on a tables group between 2 marks.
-- or beetween a mark and the current state. The result is stored into an external file.
-- It calls the _gen_sql_groups() function to effetively process the request.
-- Input: - tables group
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- Call the _gen_sql_groups() function that effectively processes the request.
    RETURN emaj._gen_sql_groups(array[p_groupName], FALSE, p_firstMark, p_lastMark, p_location, p_tblseqs, current_user);
  END;
$emaj_gen_sql_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_group(TEXT,TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a tables group between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT, p_location TEXT,
                                                    p_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SET standard_conforming_strings = ON AS
$emaj_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a set of tables groups between 2 marks
-- or beetween a mark and the current state. The result is stored into an external file.
-- It calls the _gen_sql_groups() function to effetively process the request.
-- Input: - tables groups array
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- Call the _gen_sql_groups() function that effectively processes the request.
    RETURN emaj._gen_sql_groups(p_groupNames, TRUE, p_firstMark, p_lastMark, p_location, p_tblseqs, current_user);
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(TEXT[],TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script replaying all updates performed on a tables groups set between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT,
                                                p_location TEXT, p_tblseqs TEXT[], p_currentUser TEXT)
RETURNS BIGINT LANGUAGE plpgsql
SET DateStyle = 'ISO, YMD' SET standard_conforming_strings = ON
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a tables groups array between 2 marks
-- or beetween a mark and the current state. The result is stored into an external file.
-- The function can process groups that are in LOGGING state or not.
-- The sql statements are placed between a BEGIN TRANSACTION and a COMMIT statements.
-- The output file can be reused as input file to a psql command to replay the updates scenario. Just '\\'
-- character strings (double antislash), if any, must be replaced by '\' (single antislash) before feeding
-- the psql command.
-- Input: - tables groups array
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - optional array of schema qualified table and sequence names to only process those tables and sequences
--        - the current user of the calling function
-- Output: number of generated SQL statements (non counting comments and transaction management)
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_lastEmajGid            BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_tblseqErr              TEXT;
    v_count                  INT;
    v_nbSQL                  BIGINT;
    v_nbSeq                  INT;
    v_cumNbSQL               BIGINT = 0;
    v_endComment             TEXT;
    v_dateStyle              TEXT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','),
       'From mark ' || coalesce(p_firstMark, '') ||
       CASE WHEN p_lastMark IS NULL OR p_lastMark = '' THEN ' to current state' ELSE ' to mark ' || p_lastMark END ||
       CASE WHEN p_tblseqs IS NOT NULL THEN ' with tables/sequences filtering' ELSE '' END );
-- Check the group name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If there is at least 1 group to process, go on.
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT *
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark);
-- If table/sequence names are supplied, check them.
      IF p_tblseqs IS NOT NULL THEN
        SELECT emaj._check_tblseqs_filter(p_tblseqs, p_groupNames, v_firstMarkTimeId, v_lastMarkTimeId, TRUE)
          INTO p_tblseqs;
      END IF;
-- Check that all tables had pk at start mark time, by verifying the emaj_relation.rel_sql_gen_pk_conditions column.
      SELECT string_agg(rel_schema || '.' || rel_tblseq, ', ' ORDER BY rel_schema, rel_tblseq), count(*)
        INTO v_tblseqErr, v_count
        FROM
          (SELECT *
             FROM emaj.emaj_relation
             WHERE rel_group = ANY (p_groupNames)
               AND rel_kind = 'r'                                                                  -- tables belonging to the groups
               AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
               AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))        -- filtered or not by the user
               AND rel_sql_gen_pk_conditions IS NULL                                               -- no pk at assignment time
          ) as t;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) had no pkey at % mark time.',
          v_count, v_tblseqErr, p_firstMark;
      END IF;
-- Create a temporary table to hold the generated script.
      DROP TABLE IF EXISTS emaj_temp_script CASCADE;
      CREATE TEMP TABLE emaj_temp_script (
        scr_emaj_gid           BIGINT,              -- the emaj_gid of the corresponding log row,
                                                    --   0 for initial technical statements,
                                                    --   NULL for final technical statements
        scr_subid              INT,                 -- used to distinguish several generated sql per log row
        scr_emaj_txid          BIGINT,              -- for future use, to insert commit statement at each txid change
        scr_sql                TEXT                 -- the generated sql text
      );
-- Test the supplied output file to avoid to discover a bad file name after having spent a lot of time to build the script.
      IF p_location IS NOT NULL THEN
        EXECUTE format ('COPY (SELECT 0) TO %L',
                        p_location);
      END IF;
-- End of checks.
-- Insert initial comments, some session parameters setting:
--    - the standard_conforming_strings option to properly handle special characters,
--    - the DateStyle mode used at export time,
-- and a transaction start.
      IF v_lastMarkTimeId IS NOT NULL THEN
        v_endComment = ' and mark ' || p_lastMark;
      ELSE
        v_endComment = ' and the current state';
      END IF;
      SELECT setting INTO v_dateStyle
        FROM pg_settings
        WHERE name = 'DateStyle';
      INSERT INTO emaj_temp_script VALUES
        (0, 1, 0, '-- SQL script generated by E-Maj at ' || statement_timestamp()),
        (0, 2, 0, '--    for tables group(s): ' || array_to_string(p_groupNames,',')),
        (0, 3, 0, '--    processing logs between mark ' || p_firstMark || v_endComment);
      IF p_tblseqs IS NOT NULL THEN
        INSERT INTO emaj_temp_script VALUES
          (0, 4, 0, '--    only for the following tables/sequences: ' || array_to_string(p_tblseqs,','));
      END IF;
      INSERT INTO emaj_temp_script VALUES
        (0, 10, 0, 'SET standard_conforming_strings = OFF;'),
        (0, 11, 0, 'SET escape_string_warning = OFF;'),
        (0, 12, 0, 'SET datestyle = ' || quote_literal(v_dateStyle) || ';'),
        (0, 20, 0, 'BEGIN TRANSACTION;');
-- Process tables.
      FOR r_rel IN
        SELECT *
          FROM emaj.emaj_relation
          WHERE rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'                                                                  -- tables belonging to the groups
            AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
            AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))        -- filtered or not by the user
            AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId,                            -- only tables having updates to process
                                  least(v_lastMarkTimeId, upper(rel_time_range))) > 0
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- For each application table referenced in the emaj_relation table, process the related log table, by calling the _gen_sql_tbl() function.
        SELECT emaj._gen_sql_tbl(r_rel, v_firstEmajGid, v_lastEmajGid) INTO v_nbSQL;
        v_cumNbSQL = v_cumNbSQL + v_nbSQL;
      END LOOP;
-- Process sequences.
      v_nbSeq = 0;
      FOR r_rel IN
        SELECT *
          FROM emaj.emaj_relation
          WHERE rel_group = ANY (p_groupNames)
            AND rel_kind = 'S'
            AND rel_time_range @> v_firstMarkTimeId                                -- sequences belonging to the groups at the start mark
            AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))         -- filtered or not by the user
          ORDER BY rel_schema DESC, rel_tblseq DESC
      LOOP
-- Process each sequence and increment the sequence counter.
        v_nbSeq = v_nbSeq + emaj._gen_sql_seq(r_rel, v_firstMarkTimeId, v_lastMarkTimeId, v_nbSeq);
      END LOOP;
-- Add command to commit the transaction and reset the modified session parameters.
      INSERT INTO emaj_temp_script VALUES
        (NULL, 1, txid_current(), 'COMMIT;'),
        (NULL, 10, txid_current(), 'RESET standard_conforming_strings;'),
        (NULL, 11, txid_current(), 'RESET escape_string_warning;'),
        (NULL, 11, txid_current(), 'RESET datestyle;');
-- If an output file is supplied, write the SQL script on the external file and drop the temporary table.
      IF p_location IS NOT NULL THEN
        EXECUTE format ('COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid) TO %L',
                        p_location);
        DROP TABLE IF EXISTS emaj_temp_script;
      ELSE
-- Otherwise create a view to ease the generation script export..;
        CREATE TEMPORARY VIEW emaj_sql_script AS
          SELECT scr_sql
            FROM emaj_temp_script
            ORDER BY scr_emaj_gid NULLS LAST, scr_subid;
-- ... and grant SELECT privileges on the temporary view and the underneath temporary table to the current user.
        EXECUTE format('GRANT SELECT ON emaj_sql_script, emaj_temp_script TO %I',
                       p_currentUser);
      END IF;
-- Return the number of sql verbs generated into the output file.
      v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    END IF;
-- Insert end in the history and return.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
              array_to_string(p_groupNames,','), v_cumNbSQL || ' generated statements' ||
                CASE WHEN p_location IS NOT NULL THEN ' - script exported into ' || p_location ELSE ' - script not exported' END );
--
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration(p_location TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_parameters_configuration$
-- This function stores the parameters configuration into a file on the server.
-- The JSON structure is built by the _export_param_conf() function.
-- Output: the number of parameters of the recorded JSON structure.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_paramsJson             JSON;
  BEGIN
-- Get the json structure.
    SELECT emaj._export_param_conf() INTO v_paramsJson;
-- Store the structure into the provided file name.
    CREATE TEMP TABLE t (params TEXT);
    INSERT INTO t
      SELECT line
        FROM regexp_split_to_table(v_paramsJson::TEXT, '\n') AS line;
    EXECUTE format ('COPY t TO %L',
                    p_location);
    DROP TABLE t;
-- Return the number of recorded parameters.
    RETURN json_array_length(v_paramsJson->'parameters');
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration(TEXT) IS
$$Generates and stores in a file a json structure describing the E-Maj parameters.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(p_location TEXT, p_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_parameters_configuration$
-- This function imports a file containing a JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - input file location
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_paramsText             TEXT;
    v_paramsJson             JSON;
    v_nbParam                INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN', 'Input file: ' || quote_literal(p_location));
-- Read the input file and put its content into a temporary table.
    CREATE TEMP TABLE t (params TEXT);
    EXECUTE format ('COPY t FROM %L',
                    p_location);
-- Aggregate the lines into a single text variable.
    SELECT string_agg(params, E'\n') INTO v_paramsText
      FROM t;
    DROP TABLE t;
-- Verify that the file content is a valid json structure.
    BEGIN
      v_paramsJson = v_paramsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_parameters_configuration: The file content is not a valid JSON content.';
    END;
-- Load the parameters.
    SELECT emaj._import_param_conf(v_paramsJson, p_deleteCurrentConf) INTO v_nbParam;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
--
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(TEXT,BOOLEAN) IS
$$Import E-Maj parameters from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of error or warning messages for discovered discrepancies.
-- If no error is detected, no row is returned.
  BEGIN
--
-- Errors detection.
--
-- Check that all application schemas referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: The application schema "' || rel_schema || '" does not exist any more.' AS msg
        FROM
          (  SELECT DISTINCT rel_schema
               FROM emaj.emaj_relation
               WHERE upper_inf(rel_time_range)
           EXCEPT
              SELECT nspname
                FROM pg_catalog.pg_namespace
          ) AS t
        ORDER BY msg;
-- Check that all application relations referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM                                          -- all expected application relations
          (  SELECT rel_schema, rel_tblseq, rel_kind
               FROM emaj.emaj_relation
               WHERE upper_inf(rel_time_range)
           EXCEPT                                    -- minus relations known by postgres
             SELECT nspname, relname, relkind::TEXT
               FROM pg_catalog.pg_class
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
               WHERE relkind IN ('r','S')
          ) AS t
          JOIN emaj.emaj_relation r ON (t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range))
        ORDER BY t.rel_schema, t.rel_tblseq, 1;
-- Check that the log table for all tables referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_log_schema
                     AND relname = rel_log_table
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that the log sequence for all tables referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log sequence "' ||
               rel_log_schema || '"."' || rel_log_sequence || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                 WHERE nspname = rel_log_schema AND relname = rel_log_sequence
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the log function for each table referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log function "' ||
               rel_log_schema || '"."' || rel_log_function || '" is not found.'
             AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_proc
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                 WHERE nspname = rel_log_schema
                   AND proname = rel_log_function
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check log and truncate triggers for all tables referenced in the emaj_relation table still exist.
-- Start with log triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_log_trg'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Then truncate triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_trunc_trg'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that all log tables have a structure consistent with the application tables they reference
-- (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM
        (WITH cte_app_tables_columns AS                -- application table's columns
           (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation
                   JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                   JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
              WHERE attnum > 0
                AND attisdropped = FALSE
                AND upper_inf(rel_time_range)
                AND rel_kind = 'r'
           ),
              cte_log_tables_columns AS                 -- log table's columns
           (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation
                   JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                   JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
              WHERE attnum > 0
                AND attisdropped = FALSE
                AND attnum < rel_emaj_verb_attnum
                AND upper_inf(rel_time_range)
                AND rel_kind = 'r'
           )
        SELECT DISTINCT rel_schema, rel_tblseq,
               'Error: In tables group "' || rel_group || '", the structure of the application table "' ||
                 rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
               rel_log_schema || '"."' || rel_log_table || '").' AS msg
          FROM
            (                                              -- application table's columns
              (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_app_tables_columns
               EXCEPT                                      -- minus log table's columns
                 SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_log_tables_columns
              )
            UNION                                          -- log table's columns
              (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_log_tables_columns
               EXCEPT                                      --  minus application table's columns
                 SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_app_tables_columns
              )
            ) AS t
                           -- do not issue a row if the log or application table does not exist,
                           -- these cases have been already detected
        WHERE (rel_log_schema, rel_log_table) IN
              (SELECT nspname, relname
                 FROM pg_catalog.pg_class
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              )
          AND (rel_schema, rel_tblseq) IN
              (SELECT nspname, relname
                 FROM pg_catalog.pg_class
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              )
        ORDER BY 1,2,3
        ) AS t;
-- Check that all tables of rollbackable groups have their primary key.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND group_is_rollbackable
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND contype = 'p'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- For rollbackable groups, check that no table has been altered as UNLOGGED or dropped and recreated as TEMP table after
-- tables groups creation.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY rel_schema, rel_tblseq, 1;
-- With PG 11-, check that all tables are WITHOUT OIDS (i.e. have not been altered as WITH OIDS after their tables group creation).
    IF emaj._pg_version_num() < 120000 THEN
      RETURN QUERY
        SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
               rel_schema || '"."' || rel_tblseq || '" is WITH OIDS.' AS msg
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
               JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND group_is_rollbackable
            AND relhasoids
          ORDER BY rel_schema, rel_tblseq, 1;
    END IF;
-- Check the primary key structure of all tables belonging to rollbackable groups is unchanged.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  array_to_string(rel_pk_cols, ',') AS registered_pk_columns,
                  string_agg(attname, ',' ORDER BY attnum) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_index ON (indrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_index.indrelid)
             WHERE rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND attnum = ANY (indkey)
               AND indisprimary
               AND attnum > 0
               AND attisdropped = FALSE
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_pk_columns <> current_pk_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the array of triggers to ignore at rollback time only contains existing triggers.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the trigger "' || trg_name || '" for table "'
          || rel_schema || '"."' || rel_tblseq || '" is missing. '
          || 'Use the emaj_modify_table() function to adjust the list of application triggers that should not be'
          || ' automatically disabled at rollback time.'
             AS msg
        FROM
          (SELECT rel_group, rel_schema, rel_tblseq, unnest(rel_ignored_triggers) AS trg_name
             FROM emaj.emaj_relation
             WHERE upper_inf(rel_time_range)
               AND rel_ignored_triggers IS NOT NULL
          ) AS t
        WHERE NOT EXISTS
                 (SELECT NULL
                    FROM pg_catalog.pg_trigger
                         JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                    WHERE nspname = rel_schema
                      AND relname = rel_tblseq
                      AND tgname = trg_name
                 )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check all log tables have the 6 required technical columns.
    RETURN QUERY
      SELECT msg FROM
        (SELECT DISTINCT rel_schema, rel_tblseq,
                'Error: In tables group "' || rel_group || '", the log table "' ||
                rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
                string_agg(attname,', ') || ').' AS msg
           FROM
             (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                  FROM emaj.emaj_relation,
                       (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
                  WHERE rel_kind = 'r'
                    AND upper_inf(rel_time_range)
                    AND EXISTS
                          (SELECT NULL
                             FROM pg_catalog.pg_namespace
                                  JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
                             WHERE nspname = rel_log_schema
                               AND relname = rel_log_table
                          )
              EXCEPT
                SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                  FROM emaj.emaj_relation
                       JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                       JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
                  WHERE attnum > 0
                    AND attisdropped = FALSE
                    AND attname IN ('emaj_verb', 'emaj_tuple', 'emaj_gid', 'emaj_changed', 'emaj_txid', 'emaj_user')
                    AND rel_kind = 'r'
                    AND upper_inf(rel_time_range)
             ) AS t2
           GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
           ORDER BY 1,2,3
         ) AS t;
--
-- Warnings detection.
--
-- Detect all sequences associated to a serial or a "generated as identity" column have their related table in the same group.
    RETURN QUERY
      SELECT msg FROM
        (WITH serial_dependencies AS
           (SELECT rs.rel_group AS seq_group, rs.rel_schema AS seq_schema, rs.rel_tblseq AS seq_name,
                   rt.rel_group AS tbl_group, nt.nspname AS tbl_schema, ct.relname AS tbl_name
              FROM emaj.emaj_relation rs
                   JOIN pg_catalog.pg_class cs ON (cs.relname = rel_tblseq)
                   JOIN pg_catalog.pg_namespace ns ON (ns.oid = cs.relnamespace AND ns.nspname = rel_schema)
                   JOIN pg_catalog.pg_depend ON (pg_depend.objid = cs.oid)
                   JOIN pg_catalog.pg_class ct ON (ct.oid = pg_depend.refobjid)
                   JOIN pg_catalog.pg_namespace nt ON (nt.oid = ct.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation rt ON (rt.rel_schema = nt.nspname AND rt.rel_tblseq = ct.relname
                                                             AND (rt.rel_time_range IS NULL OR upper_inf(rt.rel_time_range)))
              WHERE rs.rel_kind = 'S'
                AND upper_inf(rs.rel_time_range)
                AND pg_depend.classid = pg_depend.refclassid             -- the classid et refclassid must be 'pg_class'
                AND pg_depend.classid =
                      (SELECT oid
                         FROM pg_catalog.pg_class
                         WHERE relname = 'pg_class'
                      )
           )
           SELECT DISTINCT seq_schema, seq_name,
                  'Warning: In tables group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
                  '" is linked to the table "' || tbl_schema || '"."' || tbl_name ||
                  '" but this table does not belong to any tables group.' AS msg
             FROM serial_dependencies
             WHERE tbl_group IS NULL
         UNION ALL
           SELECT DISTINCT seq_schema, seq_name,
                  'Warning: In tables group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
                  '" is linked to the table "' || tbl_schema || '"."' || tbl_name ||
                  '" but this table belongs to another tables group (' || tbl_group || ').' AS msg
             FROM serial_dependencies
             WHERE tbl_group <> seq_group
           ORDER BY 1,2,3
        ) AS t;
-- Detect tables linked by a foreign key but not belonging to the same tables group.
    RETURN QUERY
      SELECT msg FROM
        (WITH fk_dependencies AS             -- all foreign keys that link 2 tables at least one of both belongs to a tables group
           (SELECT n.nspname AS tbl_schema, t.relname AS tbl_name, c.conname, nf.nspname AS reftbl_schema, tf.relname AS reftbl_name,
                 r.rel_group AS tbl_group, g.group_is_rollbackable AS tbl_group_is_rollbackable,
                 rf.rel_group AS reftbl_group, gf.group_is_rollbackable AS reftbl_group_is_rollbackable
              FROM pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t      ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n  ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf     ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation r ON (r.rel_schema = n.nspname AND r.rel_tblseq = t.relname
                                                       AND upper_inf(r.rel_time_range))
                   LEFT OUTER JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
                   LEFT OUTER JOIN emaj.emaj_relation rf ON (rf.rel_schema = nf.nspname AND rf.rel_tblseq = tf.relname
                                                       AND upper_inf(rf.rel_time_range))
                   LEFT OUTER JOIN emaj.emaj_group gf ON (gf.group_name = rf.rel_group)
              WHERE contype = 'f'                                         -- FK constraints only
                AND (r.rel_group IS NOT NULL OR rf.rel_group IS NOT NULL) -- at least the table or the referenced table belongs to
                                                                          -- a tables group
                AND t.relkind = 'r'                                       -- only constraint linking true tables, ie. excluding
                AND tf.relkind = 'r'                                      --   partitionned tables
           )
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on the table "' || tbl_schema || '"."' || tbl_name ||
                  '" references the table "' || reftbl_schema || '"."' || reftbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND tbl_group_is_rollbackable
               AND reftbl_group IS NULL
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || reftbl_group || '", the table "' || reftbl_schema || '"."' || reftbl_name ||
                  '" is referenced by the foreign key "' || conname ||
                  '" of the table "' || tbl_schema || '"."' || tbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE reftbl_group IS NOT NULL
               AND reftbl_group_is_rollbackable
               AND tbl_group IS NULL
        UNION ALL
          SELECT tbl_schema, tbl_name,
                 'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                 '" on the table "' || tbl_schema || '"."' || tbl_name ||
                 '" references the table "' || reftbl_schema || '"."' || reftbl_name || '" that belongs to another group ("' ||
                 reftbl_group || '")' AS msg
            FROM fk_dependencies
            WHERE tbl_group IS NOT NULL
              AND reftbl_group IS NOT NULL
              AND tbl_group <> reftbl_group
              AND (tbl_group_is_rollbackable OR reftbl_group_is_rollbackable)
          ORDER BY 1,2,3
        ) AS t;
--
    RETURN;
  END;
$_verify_all_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_schemas()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_schemas$
-- The function verifies that all E-Maj schemas only contains E-Maj objects.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  BEGIN
-- Verify that the expected E-Maj schemas still exist.
    RETURN QUERY
      SELECT DISTINCT 'Error: The E-Maj schema "' || sch_name || '" does not exist any more.' AS msg
        FROM emaj.emaj_schema
        WHERE NOT EXISTS
               (SELECT NULL
                  FROM pg_catalog.pg_namespace
                  WHERE nspname = sch_name
               )
        ORDER BY msg;
-- Detect all objects that are not directly linked to a known table groups in all E-Maj schemas, by scanning the catalog
-- (pg_class, pg_proc, pg_type, pg_conversion, pg_operator, pg_opclass).
    RETURN QUERY
      SELECT msg FROM
-- Look for unexpected tables.
        (  SELECT nspname, 1, 'Error: In schema "' || nspname ||
                 '", the table "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'r'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal tables
                AND NOT EXISTS                                                   -- exclude emaj log tables
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_table = relname
                     )
         UNION ALL
-- Look for unexpected sequences.
           SELECT nspname, 2, 'Error: In schema "' || nspname ||
                  '", the sequence "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'S'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal sequences
                AND NOT EXISTS                                                   -- exclude emaj log table sequences
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_sequence = relname
                     )
         UNION ALL
-- Look for unexpected functions.
           SELECT nspname, 3, 'Error: In schema "' || nspname ||
                  '", the function "' || nspname || '"."' || proname  || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_proc
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE (nspname <> 'emaj' OR (proname NOT LIKE E'emaj\\_%' AND proname NOT LIKE E'\\_%'))
                                                                                 -- exclude emaj internal functions
                AND NOT EXISTS                                                   -- exclude emaj log functions
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_function = proname
                     )
         UNION ALL
-- Look for unexpected composite types.
           SELECT nspname, 4, 'Error: In schema "' || nspname ||
                  '", the type "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'c'
                AND (nspname <> 'emaj' OR (relname NOT LIKE E'emaj\\_%' AND relname NOT LIKE E'\\_%'))
                                                                                 -- exclude emaj internal types
         UNION ALL
-- Look for unexpected views.
           SELECT nspname, 5, 'Error: In schema "' || nspname ||
                  '", the view "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'v'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal views
         UNION ALL
-- Look for unexpected foreign tables.
           SELECT nspname, 6, 'Error: In schema "' || nspname ||
                  '", the foreign table "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'f'
         UNION ALL
-- Look for unexpected domains.
           SELECT nspname, 7, 'Error: In schema "' || nspname ||
                  '", the domain "' || nspname || '"."' || typname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_type
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = typnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE typisdefined
                AND typtype = 'd'
         UNION ALL
-- Look for unexpected conversions.
         SELECT nspname, 8, 'Error: In schema "' || nspname ||
                '", the conversion "' || nspname || '"."' || conname || '" is not an E-Maj component.' AS msg
            FROM pg_catalog.pg_conversion
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = connamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
           UNION ALL
-- Look for unexpected operators.
           SELECT nspname, 9, 'Error: In schema "' || nspname ||
                  '", the operator "' || nspname || '"."' || oprname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_operator
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = oprnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
         UNION ALL
-- Look for unexpected operator classes.
           SELECT nspname, 10, 'Error: In schema "' || nspname ||
                  '", the operator class "' || nspname || '"."' || opcname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_opclass
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = opcnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
           ORDER BY 1, 2, 3
        ) AS t;
--
    RETURN;
  END;
$_verify_all_schemas$;

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in the emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_errorFound             BOOLEAN = FALSE;
    v_status                 INT;
    v_schema                 TEXT;
    r_object                 RECORD;
  BEGIN
-- Global checks.
-- Detect if the current postgres version is at least 11.
    IF emaj._pg_version_num() < 110000 THEN
      RETURN NEXT 'Error: The current postgres version (' || version()
               || ') is not compatible with this E-Maj version. It should be at least 11';
      v_errorFound = TRUE;
    END IF;
-- Check all E-Maj schemas.
    FOR r_object IN
      SELECT msg
        FROM emaj._verify_all_schemas() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- Check all groups components.
    FOR r_object IN
      SELECT msg
        FROM emaj._verify_all_groups() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- Report a warning if dblink connections are not operational
    IF has_function_privilege('emaj._dblink_open_cnx(text,text)', 'execute') THEN
      SELECT p_status, p_schema INTO v_status, v_schema
        FROM emaj._dblink_open_cnx('test', current_role);
      CASE v_status
        WHEN 0, 1 THEN
          PERFORM emaj._dblink_close_cnx('test', v_schema);
        WHEN -1 THEN
          RETURN NEXT 'Warning: The dblink extension is not installed.';
        WHEN -3 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the current role is not granted to execute dblink_connect_u().';
        WHEN -4 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the transaction isolation level is not READ COMMITTED.';
        WHEN -5 THEN
          RETURN NEXT 'Warning: The ''dblink_user_password'' parameter value is not set in the emaj_param table.';
        WHEN -6 THEN
          RETURN NEXT 'Warning: The dblink connection test failed. The ''dblink_user_password'' parameter value is probably incorrect.';
        ELSE
          RETURN NEXT format('Warning: The dblink connection test failed for an unknown reason (status = %s).',
                             v_status::TEXT);
      END CASE;
    ELSE
      RETURN NEXT 'Warning: The dblink connection has not been tested (the current role is not granted emaj_adm).';
    END If;
-- Report a warning if the max_prepared_transaction GUC setting is not appropriate for parallel rollbacks
    IF current_setting('max_prepared_transactions')::INT <= 1 THEN
      RETURN NEXT format('Warning: The max_prepared_transactions parameter value (%s) on this cluster is too low to launch parallel '
                         'rollback.',
                         current_setting('max_prepared_transactions'));
    END IF;
-- Report a warning if the emaj_protection_trg event triggers is missing.
-- The other event triggers are protected by the emaj extension they belong to.
    PERFORM 0
      FROM pg_catalog.pg_event_trigger
      WHERE evtname = 'emaj_protection_trg';
    IF NOT FOUND THEN
      RETURN NEXT 'Warning: The "emaj_protection_trg" event triggers is missing. It can be recreated using the '
                  'emaj_enable_protection_by_event_triggers() function.';
    END IF;
-- Report a warning if some E-Maj event triggers exist but are not enabled.
    IF EXISTS
         (SELECT 0
            FROM pg_catalog.pg_event_trigger
              WHERE evtname LIKE 'emaj%'
                AND evtenabled = 'D'
         ) THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers are disabled. You may enable them using the '
                  'emaj_enable_protection_by_event_triggers() function.';
    END IF;
-- Final message if no error has been yet detected.
    IF NOT v_errorFound THEN
      RETURN NEXT 'No error detected';
    END IF;
--
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

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
