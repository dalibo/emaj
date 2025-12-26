--
-- E-Maj: migration from 4.7.1 to <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- Complain if this script is executed in psql, rather than via an ALTER EXTENSION statement.
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

----------------------------------------------------------------
--                                                            --
--                           Checks                           --
--                                                            --
----------------------------------------------------------------

-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_txid                   TEXT;
    v_nbNoError              INT;
    v_nbWarning              INT;
  BEGIN
-- The current emaj version must be '4.7.1'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.7.1' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.7.1',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 12.
    IF current_setting('server_version_num')::int < 120000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 12.', current_setting('server_version');
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

----------------------------------------------------------------
--                                                            --
--                       Upgrade start                        --
--                                                            --
----------------------------------------------------------------

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.7.1 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------------------------
--                                                            --
--       Enumerated types, tables, sequences and views        --
--                                                            --
----------------------------------------------------------------

-- Table emaj_install_conf

-- Table containing the install configuration characteristics of this emaj instance.
-- It only contains a single row.
-- Its content is only set at emaj install time.
-- It is created and populated early in this script to let the install process know what to setup.
CREATE TABLE emaj.emaj_install_conf (
  inst_as_extension            BOOLEAN     NOT NULL,       -- boolean indicating whether this emaj instance has been created as EXTENSION
  inst_by_superuser            BOOLEAN     NOT NULL,       -- boolean indicating whether this emaj instance has been installed by a
                                                           --   SUPERUSER
  inst_installer_role          TEXT        NOT NULL,       -- role who installed the extension
  inst_with_emaj_adm           BOOLEAN     NOT NULL,       -- boolean indicating whether this emaj instance supports the emaj_adm role
  inst_with_emaj_viewer        BOOLEAN     NOT NULL,       -- boolean indicating whether this emaj instance supports the emaj_viewer role
  inst_with_event_triggers     BOOLEAN     NOT NULL        -- boolean indicating whether this emaj instance supports event triggers
  );
COMMENT ON TABLE emaj.emaj_install_conf IS
$$Contains the install configuration characteristics of this emaj instance.$$;

-- All boolean columns are set to TRUE because the initial installation was necessary done by a superuser
--   (no script being available for an upgrade by a non superuser).
INSERT INTO emaj.emaj_install_conf (inst_as_extension, inst_by_superuser, inst_installer_role,
                                    inst_with_emaj_adm, inst_with_emaj_viewer, inst_with_event_triggers)
  VALUES (TRUE, TRUE, current_user, TRUE, TRUE, TRUE);
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE emaj.emaj_install_conf FROM current_user;

--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--

----------------------------------------------------------------
--                                                            --
--                      Composite types                       --
--                                                            --
----------------------------------------------------------------

----------------------------------------------------------------
--                                                            --
--                         Functions                          --
--                                                            --
----------------------------------------------------------------
-- Recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script.


--<begin_functions>                              pattern used by the tool that extracts and inserts the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------
DROP FUNCTION IF EXISTS emaj._dblink_open_cnx(P_CNXNAME TEXT,P_CALLERROLE TEXT,OUT P_STATUS INT,OUT P_SCHEMA TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_import_groups_configuration(P_JSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_import_groups_configuration(P_LOCATION TEXT,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf(P_JSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_LOCATION TEXT,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_prepare(P_GROUPSJSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_LOCATION TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_check(P_GROUPNAMES TEXT[]);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_exec(P_JSON JSON,P_GROUPS TEXT[],P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_alter(P_GROUPNAMES TEXT[],P_MARK TEXT,P_TIMEID BIGINT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._clean_array(p_array TEXT[])
RETURNS TEXT[] LANGUAGE SQL IMMUTABLE AS
$$
-- This function cleans up a text array by removing duplicates, NULL and empty strings.
  SELECT array_agg(DISTINCT element)
    FROM unnest(p_array) AS element
    WHERE element IS NOT NULL AND element <> '';
$$;

CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(p_cnxName TEXT, OUT p_status INT, OUT p_schema TEXT)
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_dblink_open_cnx$
-- This function tries to open a named dblink connection.
-- It uses as target: the current cluster (port), the current database and a role defined in the emaj_param table.
-- This connection role must be defined in the emaj_param table with a row having:
--   - param_key = 'dblink_user_password',
--   - param_value_text = 'user=<user> password=<password>' with the rules that apply to usual libPQ connect strings.
-- The password can be omited if the connection doesn't require it.
-- The connection is performed by calling dblink_connect() when the installer role is superuser, or dblink_connect_u() in other cases.
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
--          -7 the dblink user/password from emaj_param has not emaj_adm rights
--          -8 the installer role has not pg_read_all_settings privileges in an instance only accessible by sockets
--         name of the schema that holds the dblink extension (used later to schema qualify all calls to dblink functions)
-- The function is defined as SECURITY DEFINER because reading the unix_socket_directories GUC needs to be at least a member
--   of pg_read_all_settings.
  DECLARE
    v_nbCnx                  INT;
    v_installedBySuperuser   BOOLEAN;
    v_connectFunction        TEXT;
    v_userPassword           TEXT;
    v_connectString          TEXT;
    v_stmt                   TEXT;
    v_isEmajAdmin            BOOLEAN;
    v_sqlstate               TEXT;
  BEGIN
-- Look for the schema holding the dblink functions.
    SELECT nspname INTO p_schema
      FROM pg_catalog.pg_proc
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
      WHERE proname = 'dblink_connect'
      LIMIT 1;
    IF NOT FOUND THEN
      p_status = -1;                      -- dblink is not installed
    END IF;
    IF p_status IS NULL THEN
-- dblink is usable, so search the requested connection name in dblink connections list.
      EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                     p_cnxName, p_schema);
      GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
      IF v_nbCnx > 0 THEN
        p_status = 0;                     -- the requested connection is already opened
      END IF;
    END IF;
    IF p_status IS NULL THEN
-- Determine the function to execute to open the dblink connection:
--   dblink_connect() if the installer is superuser, otherwise dblink_connect_u()
      SELECT inst_by_superuser, CASE WHEN inst_by_superuser THEN 'dblink_connect' ELSE 'dblink_connect_u' END
        INTO v_installedBySuperuser, v_connectFunction
        FROM emaj.emaj_install_conf;
-- Verify that a non superuser installer role can execute the connection function.
      IF NOT v_installedBySuperuser AND
          NOT pg_catalog.has_function_privilege(quote_ident(p_schema) || '.dblink_connect_u(text, text)', 'EXECUTE') THEN
        p_status = -3;                    -- installer role has not the EXECUTE privileges on the dblink connection function
      END IF;
    END IF;
    IF p_status IS NULL AND
-- Verify the proper transaction isolation level for E-Maj rollback operations.
       (p_cnxName LIKE 'rlbk#%' OR p_cnxName = 'emaj_verify_all') AND
       pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
      p_status = -4;                      -- 'rlbk#*' connections (used for rollbacks) must only come from a
                                          --   READ COMMITTED transaction
    END IF;
    IF p_status IS NULL THEN
-- Read and verify the dblink_user_password parameter exists in emaj_param.
      SELECT param_value_text
        INTO v_userPassword
        FROM emaj.emaj_param
        WHERE param_key = 'dblink_user_password';
      IF NOT FOUND THEN
        p_status = -5;                    -- no 'dblink_user_password' parameter is set in the emaj_param table
      END IF;
    END IF;
    IF p_status IS NULL THEN
-- Build the connection string.
      IF pg_catalog.current_setting('listen_addresses') <> '' THEN
        v_connectString = 'host=localhost';
      ELSE
        IF pg_catalog.pg_has_role('pg_read_all_settings', 'usage') THEN
          v_connectString = 'host=' ||
                             coalesce(substring(pg_catalog.current_setting('unix_socket_directories') from '(.*?)\s*,'),
                                      pg_catalog.current_setting('unix_socket_directories'));
        ELSE
          p_status = -8;                   -- missing "pg_read_all_settings" privilege to examine the needed "unix_socket_directories" GUC
        END IF;
      END IF;
      v_connectString = v_connectString
                     || ' port=' || pg_catalog.current_setting('port')
                     || ' dbname=' || pg_catalog.current_database()
                     || ' ' || v_userPassword;
    END IF;
    IF p_status IS NULL THEN
-- Try to connect.
      BEGIN
        EXECUTE format('SELECT %I.%I(%L ,%L)',
                       p_schema, v_connectFunction, p_cnxName, v_connectString);
        p_status = 1;                   -- the connection is successful
      EXCEPTION
        WHEN OTHERS THEN
          p_status = -6;                -- the connection attempt failed
          v_sqlstate = SQLSTATE;
      END;
    END IF;
-- The connection succeeds.
    IF p_status = 1 THEN
-- For E-Maj rollback first connections and test connections, check the role from emaj_param is an E-Maj administrator.
--   i.e. the role is either the installer or a member of emaj_adm.
--   Error on the statement is trapped because the emaj_adm role may not exist or be unused in this emaj instance.
      IF (p_cnxName LIKE 'rlbk#1' OR p_cnxName = 'emaj_verify_all') THEN
        v_stmt = 'SELECT '
                    '(SELECT inst_installer_role = current_user FROM emaj.emaj_install_conf) '
                 'OR '
                    '(SELECT pg_catalog.pg_has_role(''emaj_adm'', ''MEMBER'')) '
                 'AS is_emaj_admin';
        BEGIN
          EXECUTE format('SELECT is_emaj_admin FROM %I.dblink(%L, %L) AS (is_emaj_admin BOOLEAN)',
                         p_schema, p_cnxName, v_stmt)
            INTO v_isEmajAdmin;
          IF NOT v_isEmajAdmin THEN
            p_status = -7;              -- the dblink user from emaj_param is not an E-Maj administrator
          END IF;
        EXCEPTION
          WHEN OTHERS THEN
            p_status = -7;              -- the emaj_adm role doesn't exist or the current role is not allowed to access the emaj schema
        END;
      END IF;
      IF p_status < 0 THEN
-- Disconnect an opened connection if an error has been detected.
        EXECUTE format('SELECT %I.dblink_disconnect(%L) WHERE %L = ANY (%I.dblink_get_connections())',
                       p_schema, p_cnxName, p_cnxName, p_schema);
      END IF;
    END IF;
-- For connections used for E-Maj rollback operations, record the dblink connection attempt into the emaj_hist table.
    IF substring(p_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('DBLINK_OPEN_CNX', p_cnxName, 'Status = ' || p_status ||
          CASE p_status
            WHEN -1 THEN ' (dblink extension not installed)'
            WHEN -3 THEN ' (installer role not allowed to EXECUTE dblink_connect_u())'
            WHEN -4 THEN ' (transaction isolation level not READ COMMITTED)'
            WHEN -5 THEN ' (missing ''dblink_user_password'' parameter in emaj_param table)'
            WHEN -6 THEN ' (dblink connection test failed - SQLSTATE ' || v_sqlstate || ')'
            WHEN -7 THEN ' (role set in ''dblink_user_password'' parameter has not emaj administration rights)'
            WHEN -8 THEN ' (installer role has not pg_read_all_settings privileges)'
            ELSE ''
          END);
    END IF;
--
    RETURN;
  END;
$_dblink_open_cnx$;

CREATE OR REPLACE FUNCTION emaj._build_tblseqs_array_from_regexp(p_schema TEXT, p_relkind TEXT, p_includeFilter TEXT, p_excludeFilter TEXT,
                                                                 p_exceptionIfMissing BOOLEAN)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_build_tblseqs_array_from_regexp$
-- The function builds the names array of tables or sequences belonging to any tables groups, based on include and exclude regexp filters.
-- Depending on the p_exceptionIfMissing parameter, it warns or raises an error if the schema doesn't exist.
-- (WARNING only is used by removal functions so that it is possible to remove from its group relations of a dropped or renamed schema)
-- Inputs: schema,
--         relation kind ('r' or 'S'),
--         2 patterns to filter table names (one to include and another to exclude),
--         boolean indicating whether a missing schema must raise an exception or just a warning.
-- Outputs: tables or sequences names array
  BEGIN
-- Check that the schema exists.
    PERFORM emaj._check_schema(p_schema, p_exceptionIfMissing, FALSE);
-- Build and return the list of relations names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    RETURN array_agg(rel_tblseq)
      FROM (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND (p_includeFilter IS NOT NULL AND p_includeFilter <> '' AND rel_tblseq ~ p_includeFilter)
            AND (p_excludeFilter IS NULL OR p_excludeFilter = '' OR rel_tblseq !~ p_excludeFilter)
            AND rel_kind = p_relkind
            AND upper_inf(rel_time_range)
          ORDER BY rel_tblseq
           ) AS t;
  END;
$_build_tblseqs_array_from_regexp$;

CREATE OR REPLACE FUNCTION emaj._check_can_copy_from()
RETURNS VOID LANGUAGE plpgsql AS
$_check_can_copy_from$
-- This function checks that COPY FROM are allowed on this emaj instance for the installer role.
-- This capability is evaluated by checking the pg_read_server_files rights.
--   It may be false if the installer role was not a superuser and has not been granted the proper rights.
  BEGIN
    IF NOT pg_catalog.pg_has_role('pg_read_server_files', 'usage') THEN
      RAISE EXCEPTION '_check_can_copy_from: Reading from an external file is not allowed on this emaj instance'
                      ' (the installer role had not SUPERUSER rights at install time and has not been granted pg_read_server_files).';
    END IF;
  END;
$_check_can_copy_from$;

CREATE OR REPLACE FUNCTION emaj._check_can_copy_to()
RETURNS VOID LANGUAGE plpgsql AS
$_check_can_copy_to$
-- This function checks that COPY TO are allowed on this emaj instance for the installer role.
-- This capability is evaluated by checking the pg_write_server_files rights.
--   It may be false if the installer role was not a superuser and has not been granted the proper rights.
  BEGIN
    IF NOT pg_catalog.pg_has_role('pg_write_server_files', 'usage') THEN
      RAISE EXCEPTION '_check_can_copy_to: Writing into an external file is not allowed on this emaj instance'
                      ' (the installer role had not SUPERUSER rights at install time and has not been granted pg_write_server_files).';
    END IF;
  END;
$_check_can_copy_to$;

CREATE OR REPLACE FUNCTION emaj._check_can_exec_program()
RETURNS VOID LANGUAGE plpgsql AS
$_check_can_exec_program$
-- This function checks that COPY TO PROGRAM are allowed on this emaj instance for the installer role.
-- This capability is evaluated by checking the pg_execute_server_program rights.
--   It may be false if the installer role was not a superuser and has not been granted the proper rights.
  BEGIN
    IF NOT pg_catalog.pg_has_role('pg_execute_server_program', 'usage') THEN
      RAISE EXCEPTION '_check_can_exec_program: Executing an external program is not allowed on this emaj instance'
                      ' (the installer role had not SUPERUSER rights at install time and has not been granted pg_execute_server_program).';
    END IF;
  END;
$_check_can_exec_program$;

CREATE OR REPLACE FUNCTION emaj._create_log_schemas(p_function TEXT, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_schemas$
-- The function creates all log schemas that will be needed to create new log tables.
-- The function is called at tables groups configuration import.
-- Input: calling function to record into the emaj_hist table and time_id
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted the CREATE privilege on
--   the current database.
  DECLARE
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
    r_schema                 RECORD;
  BEGIN
-- Create schemas.
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
-- Create the schema.
      EXECUTE format('CREATE SCHEMA %I',
                     r_schema.log_schema);
-- And give the appropriate rights, when possible.
      SELECT inst_with_emaj_adm, inst_with_emaj_viewer
        INTO STRICT v_supportEmajAdm, v_supportEmajViewer
        FROM emaj.emaj_install_conf;
      IF v_supportEmajAdm THEN
        EXECUTE format('ALTER SCHEMA %I OWNER TO emaj_adm',
                       r_schema.log_schema);
      END IF;
      IF v_supportEmajViewer THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                       r_schema.log_schema);
      END IF;
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
      INSERT INTO emaj.emaj_schema (sch_name, sch_time_id)
        VALUES (r_schema.log_schema, p_timeId);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (p_function, 'LOG_SCHEMA CREATED', quote_ident(r_schema.log_schema));
    END LOOP;
--
    RETURN;
  END;
$_create_log_schemas$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_tables(p_schema TEXT, p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                    p_group TEXT, p_properties JSONB DEFAULT NULL, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_tables$
-- The function assigns tables on name regexp pattern into a tables group.
-- Inputs: schema name, 2 patterns to filter table names (one to include and another to exclude) , assignment group name,
--         assignment properties (optional), mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group
  DECLARE
    v_tables                 TEXT[];
  BEGIN
-- Build the list of tables names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    SELECT array_agg(relname) INTO v_tables
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND (p_tablesIncludeFilter IS NOT NULL AND p_tablesIncludeFilter <> '' AND relname ~ p_tablesIncludeFilter)
             AND (p_tablesExcludeFilter IS NULL OR p_tablesExcludeFilter = '' OR relname !~ p_tablesExcludeFilter)
             AND relkind IN ('r', 'p')
           ORDER BY relname
        ) AS t;
-- Call the _assign_tables() function for execution.
    RETURN emaj._assign_tables(p_schema, v_tables, p_group, p_properties, p_mark, TRUE, TRUE);
  END;
$emaj_assign_tables$;
COMMENT ON FUNCTION emaj.emaj_assign_tables(TEXT,TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign tables on name patterns into a tables group.$$;

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
    v_installedBySuperuser   BOOLEAN;
    v_installerRole          TEXT;
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_logSchema              TEXT;
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
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
-- Check the group name and, if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_group], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_rollbackable, group_is_logging
      INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_group;
-- Check the supplied schema exists and is not an E-Maj schema.
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check tables.
    IF NOT p_arrayFromRegex THEN
-- From the tables array supplied by the user, remove duplicates values, NULL and empty strings from the supplied table names array.
      p_tables = emaj._clean_array(p_tables);
-- Check that application tables exist.
      SELECT string_agg(quote_ident(table_name), ', ' ORDER BY table_name)
        INTO v_list
        FROM unnest(p_tables) AS table_name
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = p_schema
                     AND relname = table_name
                     AND relkind IN ('r','p')
                );
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard partitioned application tables (only elementary partitions can be managed by E-Maj).
    SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
      INTO v_list, v_array
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
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- Check or discard TEMP tables.
    SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
      INTO v_list, v_array
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
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- If the group is ROLLBACKABLE, perform additional checks or filters (a PK, not UNLOGGED).
    IF v_groupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex);
    END IF;
-- Check or discard tables already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
      INTO v_list, v_array
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
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- Check and extract the tables JSON properties.
    IF p_properties IS NOT NULL THEN
      SELECT * INTO v_priority, v_logDatTsp, v_logIdxTsp, v_ignoredTriggers, v_ignoredTrgProfiles
        FROM emaj._check_json_table_properties(p_properties);
    END IF;
-- In non superuser install, check or discard tables whose owner is not the installer role.
    SELECT inst_by_superuser, inst_installer_role
      INTO v_installedBySuperuser, v_installerRole
      FROM emaj.emaj_install_conf;
    IF NOT v_installedBySuperuser THEN
      SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
        INTO v_list, v_array
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
        WHERE nspname = p_schema
          AND relname = ANY(p_tables)
          AND relowner::regrole::text <> current_user;
      IF v_list IS NOT NULL THEN
        IF NOT p_arrayFromRegex THEN
          RAISE EXCEPTION '_assign_tables: In schema %, the owner of some tables (%) is not the emaj installer role.',
                          quote_ident(p_schema), v_list;
        ELSE
          RAISE WARNING '_assign_tables: Some tables whose owner is not the emaj installer role (%) are not selected.', v_list;
          -- remove these tables from the tables to process
          p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
        END IF;
      END IF;
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
-- Create the new log schema, if it doesn't exist yet.
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
-- Create the schema.
        EXECUTE format('CREATE SCHEMA %I',
                       v_logSchema);
-- And give the appropriate rights, when possible.
        SELECT inst_with_emaj_adm, inst_with_emaj_viewer
          INTO STRICT v_supportEmajAdm, v_supportEmajViewer
          FROM emaj.emaj_install_conf;
        IF v_supportEmajAdm THEN
          EXECUTE format('ALTER SCHEMA %I OWNER TO emaj_adm',
                         v_logSchema);
        END IF;
        IF v_supportEmajViewer THEN
          EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                         v_logSchema);
        END IF;
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
        INSERT INTO emaj.emaj_schema (sch_name, sch_time_id)
          VALUES (v_logSchema, v_timeId);
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
        SELECT string_agg(quote_ident(trigger_name), ', ' ORDER BY trigger_name)
          INTO v_list
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
          EXECUTE format('SELECT array_agg(tgname ORDER BY tgname)'
                         '  FROM pg_catalog.pg_trigger'
                         '       JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)'
                         '       JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)'
                         '  WHERE nspname = %L'
                         '    AND relname = %L'
                         '    AND tgconstraint = 0'
                         '    AND tgname NOT IN (''emaj_log_trg'',''emaj_trunc_trg'')'
                         '    AND (%s)',
                         p_schema, v_oneTable, v_selectConditions)
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

CREATE OR REPLACE FUNCTION emaj.emaj_get_assigned_group_table(p_schema TEXT, p_table TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_assigned_group_table$
-- The function reports the tables group a table is assigned to.
-- It returns NULL if the table is not currently assigned to a group.
-- Inputs: schema name, table name.
-- Outputs: tables group currently owning the table.
  DECLARE
    v_group                  TEXT;
  BEGIN
-- Check the supplied schema exists and is not an E-Maj schema.
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check that application table exist.
    PERFORM 0
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = p_table
        AND relkind IN ('r','p');
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_assigned_group_table: In schema %, the table % does not exist.',
                      quote_ident(p_schema), quote_ident(p_table);
    END IF;
-- Get the tables group the table is assignned to.
    SELECT rel_group INTO v_group
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND rel_kind = 'r'
        AND upper_inf(rel_time_range);
--
    RETURN v_group;
  END;
$emaj_get_assigned_group_table$;
COMMENT ON FUNCTION emaj.emaj_get_assigned_group_table(TEXT,TEXT) IS
$$Returns the tables group a table is assigned to.$$;

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
    v_prevCMM                TEXT;
    v_pkCols                 TEXT[];
    v_genExprCols            TEXT[];
    v_rlbkColList            TEXT;
    v_genColList             TEXT;
    v_genValList             TEXT;
    v_genSetList             TEXT;
    v_genPkConditions        TEXT;
    v_nbGenAlwaysIdentCol    INTEGER;
    v_attnum                 SMALLINT;
    v_alter_log_table_param  TEXT;
    v_stmt                   TEXT;
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
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
-- Drop a potential log table or E-Maj trigger leftover.
    v_prevCMM = pg_catalog.current_setting('client_min_messages');
    SET client_min_messages TO WARNING;
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                   p_schema, p_tbl);
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                   p_schema, p_tbl);
    PERFORM pg_catalog.set_config ('client_min_messages', v_prevCMM, FALSE);
-- Create the log table: it looks like the application table, with some additional technical columns.
    EXECUTE format('CREATE TABLE %s (LIKE %s,'
                   '  emaj_verb      VARCHAR(3)  NOT NULL,'
                   '  emaj_tuple     VARCHAR(3)  NOT NULL,'
                   '  emaj_gid       BIGINT      NOT NULL DEFAULT nextval(''emaj.emaj_global_seq''),'
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
    EXECUTE format('CREATE TRIGGER emaj_log_trg'
                   ' AFTER INSERT OR UPDATE OR DELETE ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE %s()',
                   p_schema, p_tbl, v_logFnctName);
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
-- Set emaj_adm as owner of log objects, when supported.
    SELECT inst_with_emaj_adm, inst_with_emaj_viewer
      INTO STRICT v_supportEmajAdm, v_supportEmajViewer
      FROM emaj.emaj_install_conf;
    IF v_supportEmajAdm THEN
      EXECUTE format('ALTER TABLE %s OWNER TO emaj_adm',
                     v_logTableName);
      EXECUTE format('ALTER SEQUENCE %s OWNER TO emaj_adm',
                     v_sequenceName);
      EXECUTE format('ALTER FUNCTION %s () OWNER TO emaj_adm',
                     v_logFnctName);
    END IF;
-- Grant appropriate rights to the emaj_viewer role, whenn supported.
    IF v_supportEmajViewer THEN
      EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                     v_logTableName);
      EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                     v_sequenceName);
    END IF;
-- Build the PK columns names array and some pieces of SQL statements that will be needed at table rollback and gen_sql times.
-- They are left NULL if the table has no pkey.
    SELECT * FROM emaj._build_sql_tbl(v_fullTableName)
      INTO v_pkCols, v_genExprCols, v_rlbkColList, v_genColList, v_genValList, v_genSetList, v_genPkConditions, v_nbGenAlwaysIdentCol;
-- Register the table into emaj_relation.
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function, rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns,
                rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions)
        VALUES (p_schema, p_tbl, int8range(p_timeId, NULL, '[)'), p_groupName, p_priority,
                v_logSchema, p_logDatTsp, p_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName, p_ignoredTriggers, v_pkCols, v_genExprCols,
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

CREATE OR REPLACE FUNCTION emaj._move_tbl(p_schema TEXT, p_table TEXT, p_oldGroup TEXT, p_oldGroupIsLogging BOOLEAN, p_newGroup TEXT,
                                          p_newGroupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_tbl$
-- The function changes the group ownership of a table. It is called during an alter group or a dynamic assignment operation.
-- Required inputs: schema and table to move, old and new group names and their logging state,
--                  time stamp id of the operation, main calling function.
  DECLARE
    v_logSchema              TEXT;
    v_logSequence            TEXT;
    v_currentLogTable        TEXT;
    v_currentLogIndex        TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_namesSuffix            TEXT;
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
  BEGIN
-- Get the current relation characteristics.
    SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_sequence,
           coalesce('TABLESPACE ' || quote_ident(rel_log_dat_tsp),''),
           coalesce('USING INDEX TABLESPACE ' || quote_ident(rel_log_idx_tsp),'')
      INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logSequence,
           v_dataTblSpace,
           v_idxTblSpace
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
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
-- Update emaj_relation to reflect the log table and index rename for all concerned rows.
    UPDATE emaj.emaj_relation
      SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND rel_log_table = v_currentLogTable;
-- Create the new log table, by copying the just renamed table structure.
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS) %s',
                    v_logSchema, v_currentLogTable, v_logSchema, v_currentLogTable || v_namesSuffix, v_dataTblSpace);
-- Add the primary key.
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAiNT %I PRIMARY KEY (emaj_gid, emaj_tuple) %s',
                    v_logSchema, v_currentLogTable, v_currentLogIndex, v_idxTblSpace);
-- Set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE format('ALTER TABLE ONLY %I.%I CLUSTER ON %I',
                   v_logSchema, v_currentLogTable, v_currentLogIndex);
-- Grant appropriate rights to both emaj roles, when possible.
    SELECT inst_with_emaj_adm, inst_with_emaj_viewer
      INTO STRICT v_supportEmajAdm, v_supportEmajViewer
      FROM emaj.emaj_install_conf;
    IF v_supportEmajAdm THEN
      EXECUTE format('ALTER TABLE %I.%I OWNER TO emaj_adm',
                     v_logSchema, v_currentLogTable);
    END IF;
    IF v_supportEmajViewer THEN
      EXECUTE format('GRANT SELECT ON TABLE %I.%I TO emaj_viewer',
                     v_logSchema, v_currentLogTable);
    END IF;
-- Register the end of the previous relation time frame and create a new relation time frame with the new group.
    UPDATE emaj.emaj_relation
      SET rel_time_range = int8range(lower(rel_time_range),p_timeId,'[)')
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema,
                                    rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
                                    rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
                                    rel_sql_rlbk_columns,
                                    rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
                                    rel_log_seq_last_value)
      SELECT rel_schema, rel_tblseq, int8range(p_timeId, NULL, '[)'), p_newGroup, rel_kind, rel_priority, rel_log_schema,
             v_currentLogTable, rel_log_dat_tsp, v_currentLogIndex, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
             rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
             rel_sql_rlbk_columns,
             rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
             rel_log_seq_last_value
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper(rel_time_range) = p_timeId;
-- If the table is moved from an idle group to a group in logging state,
    IF NOT p_oldGroupIsLogging AND p_newGroupIsLogging THEN
-- ... get the log schema and sequence for the new relation,
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- ... and record the new log sequence state in the emaj_table table for the current operation mark.
      INSERT INTO emaj.emaj_table (tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val)
        SELECT p_schema, p_table, p_timeId, reltuples, relpages, last_value
          FROM pg_catalog.pg_class
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace),
               LATERAL emaj._get_log_sequence_last_value(v_logSchema, v_logSequence) AS last_value
          WHERE nspname = p_schema
            AND relname = p_table;
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group, rlchg_new_group)
      VALUES (p_timeId, p_schema, p_table, 'MOVE_TABLE', p_oldGroup, p_newGroup);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TABLE MOVED', quote_ident(p_schema) || '.' || quote_ident(p_table),
              'From the ' || CASE WHEN p_oldGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_oldGroup ||
              ' to the ' || CASE WHEN p_newGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_newGroup);
--
    RETURN;
  END;
$_move_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequences(p_schema TEXT, p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                      p_group TEXT, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequences$
-- The function assigns sequences on name regexp pattern into a tables group.
-- Inputs: schema name, 2 patterns to filter sequence names (one to include and another to exclude), assignment group name,
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group
  DECLARE
    v_sequences              TEXT[];
  BEGIN
-- Build the list of sequences names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    SELECT array_agg(relname) INTO v_sequences
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND (p_sequencesIncludeFilter IS NOT NULL AND p_sequencesIncludeFilter <> '' AND relname ~ p_sequencesIncludeFilter)
             AND (p_sequencesExcludeFilter IS NULL OR p_sequencesExcludeFilter = '' OR relname !~ p_sequencesExcludeFilter)
             AND relkind = 'S'
           ORDER BY relname
        ) AS t;
-- OK, call the _assign_sequences() function for execution.
    RETURN emaj._assign_sequences(p_schema, v_sequences, p_group, p_mark, TRUE, TRUE);
  END;
$emaj_assign_sequences$;
COMMENT ON FUNCTION emaj.emaj_assign_sequences(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Assign sequences on name patterns into a tables group.$$;

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
    v_installedBySuperuser   BOOLEAN;
    v_installerRole          TEXT;
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
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check sequences.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the sequence names array supplied by the user.
      p_sequences = emaj._clean_array(p_sequences);
-- Check that application sequences exist.
      SELECT string_agg(quote_ident(sequence_name), ', ' ORDER BY sequence_name)
        INTO v_list
        FROM unnest(p_sequences) AS sequence_name
        WHERE NOT EXISTS
               (SELECT 0
                  FROM pg_catalog.pg_class
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                  WHERE nspname = p_schema
                    AND relname = sequence_name
                    AND relkind = 'S');
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_sequences: In schema %, some sequences (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard sequences already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
      INTO v_list, v_array
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
        p_sequences = array(SELECT unnest(p_sequences) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- In non superuser install, check or discard sequences whose owner is not the installer role.
    SELECT inst_by_superuser, inst_installer_role
      INTO v_installedBySuperuser, v_installerRole
      FROM emaj.emaj_install_conf;
    IF NOT v_installedBySuperuser THEN
      SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
        INTO v_list, v_array
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
        WHERE nspname = p_schema
          AND relname = ANY(p_sequences)
          AND relowner::regrole::text <> current_user;
      IF v_list IS NOT NULL THEN
        IF NOT p_arrayFromRegex THEN
          RAISE EXCEPTION '_assign_sequences: In schema %, the owner of some sequences (%) is not the emaj installer role.',
                          quote_ident(p_schema), v_list;
        ELSE
          RAISE WARNING '_assign_sequences: Some sequences whose owner is not the emaj installer role (%) are not selected.', v_list;
          -- remove these sequences from the sequences to process
          p_sequences = array(SELECT unnest(p_sequences) EXCEPT SELECT unnest(v_array));
        END IF;
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
-- Effectively process each sequence.
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

CREATE OR REPLACE FUNCTION emaj.emaj_get_assigned_group_sequence(p_schema TEXT, p_sequence TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_assigned_group_sequence$
-- The function reports the tables group a sequence is assigned to.
-- It returns NULL if the sequence is not currently assigned to a group.
-- Inputs: schema name, sequence name.
-- Outputs: tables group currently owning the sequence.
  DECLARE
    v_group                  TEXT;
  BEGIN
-- Check the supplied schema exists and is not an E-Maj schema.
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check that application sequence exist.
    PERFORM 0
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = p_sequence
        AND relkind = 'S';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_assigned_group_sequence: In schema %, the sequence % does not exist.',
                      quote_ident(p_schema), quote_ident(p_sequence);
    END IF;
-- Get the tables group the sequence is assignned to.
    SELECT rel_group INTO v_group
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_sequence
        AND rel_kind = 'S'
        AND upper_inf(rel_time_range);
--
    RETURN v_group;
  END;
$emaj_get_assigned_group_sequence$;
COMMENT ON FUNCTION emaj.emaj_get_assigned_group_sequence(TEXT,TEXT) IS
$$Returns the tables group a sequence is assigned to.$$;

CREATE OR REPLACE FUNCTION emaj._get_current_seq(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_get_current_seq$
-- This function reads the current characteristics of a sequence and returns it in an emaj_sequence format.
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
  DECLARE
    r_seq                    emaj.emaj_sequence%ROWTYPE;
  BEGIN
    EXECUTE format(
        'SELECT nspname, relname, %s, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
        '  FROM %I.%I sq,'
        '     pg_catalog.pg_sequence s'
        '     JOIN pg_catalog.pg_class c ON (c.oid = s.seqrelid)'
        '     JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)'
        '  WHERE nspname = %L AND relname = %L',
        p_timeId, p_schema, p_sequence, p_schema, p_sequence)
      INTO STRICT r_seq;
    RETURN r_seq;
  END;
$_get_current_seq$;

CREATE OR REPLACE FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                   OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN)
LANGUAGE plpgsql AS
$_sequence_stat_seq$
-- This function compares the state of a single sequence between 2 time stamps or between a time stamp and the current state.
-- It is called by the _sequence_stat_group() function.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time stamp ids defining the time range to examine (a end time stamp id set to NULL indicates the current state).
-- Output: number of sequence increments between both time stamps for the sequence
--         a boolean indicating whether any structure property has been modified between both time stamps.
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
      r_endSeq = emaj._get_current_seq(r_rel.rel_schema, r_rel.rel_tblseq, 0);
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
RETURNS BIGINT LANGUAGE plpgsql AS
$_gen_sql_seq$
-- This function generates a SQL statement to set the final characteristics of a sequence.
-- The statement is stored into a temporary table created by the _gen_sql_groups() calling function.
-- If the sequence has not been changed between both marks, no statement is generated.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time id at requested start and end marks,
--        the number of already processed sequences
-- Output: number of generated SQL statements (0 or 1)
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
      trg_seq_rec = emaj._get_current_seq(r_rel.rel_schema, r_rel.rel_tblseq, 0);
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

CREATE OR REPLACE FUNCTION emaj._verify_groups(p_groups TEXT[], p_onErrorStop BOOLEAN)
RETURNS SETOF emaj._verify_groups_type LANGUAGE plpgsql AS
$_verify_groups$
-- The function verifies the consistency of a tables groups array.
-- Input: - tables groups array,
--        - a boolean indicating whether the function has to raise an exception in case of detected unconsistency.
-- If onErrorStop boolean is false, it returns a set of _verify_groups_type records, one row per detected unconsistency, including
-- the faulting schema and table or sequence names and a detailed message.
-- If no error is detected, no row is returned.
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_hint                   CONSTANT TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected.
-- Look for groups unconsistency.
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist.
-- Check that all application relations referenced in the emaj_relation table still exist.
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq, r.rel_group,
             'In tables group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist anymore.' AS msg
        FROM
          (  SELECT rel_schema, rel_tblseq, rel_kind
               FROM emaj.emaj_relation
               WHERE rel_group = ANY (p_groups)
                 AND upper_inf(rel_time_range)
           EXCEPT                                -- all relations known by postgres
             SELECT nspname, relname, relkind::TEXT
               FROM pg_catalog.pg_class
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
               WHERE relkind IN ('r','S')
          ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema
          AND t.rel_tblseq = r.rel_tblseq
          AND upper_inf(r.rel_time_range)
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (1): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log table for all tables referenced in the emaj_relation table still exist.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_log_schema
                     AND relname = rel_log_table
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (2): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log function for each table referenced in the emaj_relation table still exists.
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function ||
             '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_proc
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                   WHERE nspname = rel_log_schema
                     AND proname = rel_log_function
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (3): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that log and truncate triggers for all tables referenced in the emaj_relation table still exist.
--   Start with the log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_log_trg'
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (4): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   Then the truncate trigger.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_trunc_trg'
                )
      ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (5): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have a structure consistent with the application tables they reference
-- (same columns and same formats). It only returns one row per faulting table.
    FOR r_object IN
      WITH cte_app_tables_columns AS                  -- application table's columns
        (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
           FROM emaj.emaj_relation
                JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
           WHERE attnum > 0
             AND attisdropped = FALSE
             AND rel_group = ANY (p_groups)
             AND rel_kind = 'r'
             AND upper_inf(rel_time_range)
        ),
           cte_log_tables_columns AS                  -- log table's columns
        (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
           FROM emaj.emaj_relation
                JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
            WHERE attnum > 0
              AND NOT attisdropped
              AND attnum < rel_emaj_verb_attnum
              AND rel_group = ANY (p_groups)
              AND rel_kind = 'r'
              AND upper_inf(rel_time_range))
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the structure of the application table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
             rel_log_schema || '"."' || rel_log_table || '").' AS msg
        FROM
          (
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
            )
          ) AS t
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (6): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all tables have their primary key if they belong to a rollbackable group.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key anymore.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
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
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (7): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- For rollbackable groups, check that no table has been altered as UNLOGGED or dropped and recreated as TEMP table after the tables
-- groups creation.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (8): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the primary key structure of all tables belonging to rollbackable groups is unchanged.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  array_to_string(rel_pk_cols, ',') AS registered_pk_columns,
                  string_agg(attname, ',' ORDER BY array_position(conkey, attnum)) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_constraint ON (conrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (attrelid = conrelid)
             WHERE rel_group = ANY (p_groups)
               AND rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND contype = 'p'
               AND attnum = ANY (conkey)
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_pk_columns <> current_pk_columns
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the "GENERATED AS expression" columns list of all tables have not changed.
-- (The expression of virtual generated columns be changed, without generating any trouble)
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the "GENERATED AS expression" columns list of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_gen_columns || ' => ' || current_gen_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  coalesce(array_to_string(rel_gen_expr_cols, ','), '<none>') AS registered_gen_columns,
                  coalesce(string_agg(attname, ',' ORDER BY attnum), '<none>') AS current_gen_columns
             FROM emaj.emaj_relation
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
             WHERE rel_group = ANY (p_groups)
               AND rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND attgenerated <> ''
               AND attnum > 0
               AND NOT attisdropped
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_gen_columns <> current_gen_columns
        ORDER BY 1,2,3,4
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (10): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check the array of triggers to ignore at rollback time only contains existing triggers.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'Error: In group "' || rel_group || '", the trigger "' || trg_name || '" for table "'
          || rel_schema || '"."' || rel_tblseq || '" is missing. '
          || 'Use the emaj_modify_table() function to adjust the list of application triggers that should not be'
          || ' automatically disabled at rollback time.' AS msg
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
        ORDER BY 1,2,3,4
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (11): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have the 6 required technical columns. It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log table "' ||
             rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
             string_agg(attname,', ' ORDER BY attname) || ').' AS msg
        FROM
          (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
               FROM emaj.emaj_relation,
                   (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
               WHERE rel_group = ANY (p_groups)
                 AND rel_kind = 'r'
                 AND upper_inf(rel_time_range)
                 AND EXISTS
                       (SELECT NULL
                          FROM pg_catalog.pg_class
                               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                          WHERE nspname = rel_log_schema
                            AND relname = rel_log_table
                       )
           EXCEPT
             SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
               FROM emaj.emaj_relation
                    JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                    JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
               WHERE rel_group = ANY (p_groups)
                 AND rel_kind = 'r'
                 AND upper_inf(rel_time_range)
                 AND attnum > 0
                 AND attisdropped = FALSE
                 AND attname IN ('emaj_verb', 'emaj_tuple', 'emaj_gid', 'emaj_changed', 'emaj_txid', 'emaj_user')
          ) AS t2
        GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (12): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj._drop_group(p_groupName TEXT, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_drop_group$
-- This function effectively processes a tables group deletion.
-- It also drops log schemas that are not useful anymore.
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT;
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbRel                  INT;
  BEGIN
    v_function = CASE WHEN p_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END;
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'D') INTO v_timeId;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the tables group.
    SELECT emaj._drop_groups(ARRAY[p_groupName], v_timeId) INTO v_nbRel;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any other created group).
    PERFORM emaj._drop_log_schemas(v_function, p_isForced);
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
--
    RETURN v_nbRel;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj._drop_groups(p_groups TEXT[], p_timeId BIGINT)
RETURNS INT LANGUAGE plpgsql AS
$_drop_groups$
-- This function effectively deletes several groups and their content.
-- It is called by emaj_drop_group() and emaj_import_groups_configuration() functions.
-- Input: group names array, time id of the operation
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- Register into emaj_relation_change the tables and sequences removal from their group, for completeness.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      SELECT p_timeId, rel_schema, rel_tblseq,
             CASE WHEN rel_kind = 'r' THEN 'REMOVE_TABLE'::emaj._relation_change_kind_enum
                                      ELSE 'REMOVE_SEQUENCE'::emaj._relation_change_kind_enum END,
             rel_group
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND upper_inf(rel_time_range)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range;
-- Delete the emaj objects and references for each table and sequences of the groups.
    FOR r_rel IN
      SELECT *
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
    LOOP
      PERFORM CASE r_rel.rel_kind
                WHEN 'r' THEN emaj._drop_tbl(r_rel, p_timeId)
                WHEN 'S' THEN emaj._drop_seq(r_rel, p_timeId)
              END;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Count the number of tables annd sequences owned by the groups.
    SELECT sum(group_nb_table + group_nb_sequence)
      INTO v_nbRel
      FROM emaj.emaj_group
      WHERE group_name = ANY (p_groups);
-- Delete groups row from the emaj_group table.
-- By cascade, it also deletes rows from emaj_mark.
    DELETE FROM emaj.emaj_group
      WHERE group_name = ANY (p_groups);
-- Update the last log session for the groups to set the time range upper bound
    UPDATE emaj.emaj_log_session
      SET lses_time_range = int8range(lower(lses_time_range), p_timeId, '[]')
      WHERE lses_group = ANY (p_groups)
        AND upper_inf(lses_time_range);
-- Update the last group history rows to set the time range upper bound
    UPDATE emaj.emaj_group_hist
      SET grph_time_range = int8range(lower(grph_time_range), p_timeId, '[]')
      WHERE grph_group = ANY (p_groups)
        AND upper_inf(grph_time_range);
--
    RETURN v_nbRel;
  END;
$_drop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of existing group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_groups(TEXT, TEXT) IS
$$Builds a groups array, filtered on their names.$$;

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
-- Verify that the installer role is allowed to execute COPY TO statements.
    PERFORM emaj._check_can_copy_to();
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

CREATE OR REPLACE FUNCTION emaj._export_groups_conf(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$_export_groups_conf$
-- This function generates a JSON formatted structure representing the current configuration of some or all tables groups.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the tables groups configuration in JSON format
  DECLARE
    v_groupsText             TEXT;
    v_unknownGroupsList      TEXT;
    v_groupsJson             JSON;
    r_group                  RECORD;
    r_table                  RECORD;
    r_sequence               RECORD;
  BEGIN
-- Build the comment heading the JSON structure.
    v_groupsText = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || statement_timestamp();
    IF p_groups IS NULL THEN
      v_groupsText = v_groupsText || E', including all tables groups",\n';
    ELSE
      v_groupsText = v_groupsText || E', including a tables groups subset",\n';
    END IF;
-- Check the group names array, if supplied. All the listed groups must exist.
    IF p_groups IS NOT NULL THEN
      SELECT string_agg(group_name, ', ' ORDER BY group_name)
        INTO v_unknownGroupsList
        FROM unnest(p_groups) AS grp(group_name)
        WHERE NOT EXISTS
               (SELECT group_name
                  FROM emaj.emaj_group
                  WHERE emaj_group.group_name = grp.group_name
               );
      IF v_unknownGroupsList IS NOT NULL THEN
        RAISE EXCEPTION '_export_groups_conf: The tables groups % are unknown.', v_unknownGroupsList;
      END IF;
    END IF;
-- Build the tables groups description.
    v_groupsText = v_groupsText
                || E'  "tables_groups": [\n';
    FOR r_group IN
      SELECT group_name, group_is_rollbackable, group_comment, group_nb_table, group_nb_sequence
        FROM emaj.emaj_group
        WHERE (p_groups IS NULL OR group_name = ANY(p_groups))
        ORDER BY group_name
    LOOP
      v_groupsText = v_groupsText
                  || E'    {\n'
                  ||  '      "group": ' || to_json(r_group.group_name) || E',\n'
                  ||  '      "is_rollbackable": ' || to_json(r_group.group_is_rollbackable) || E',\n';
      IF r_group.group_comment IS NOT NULL THEN
        v_groupsText = v_groupsText
                  ||  '      "comment": ' || to_json(r_group.group_comment) || E',\n';
      END IF;
      IF r_group.group_nb_table > 0 THEN
-- Build the tables list, if any.
        v_groupsText = v_groupsText
                    || E'      "tables": [\n';
        FOR r_table IN
          SELECT rel_schema, rel_tblseq, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp, rel_ignored_triggers
            FROM emaj.emaj_relation
            WHERE rel_kind = 'r'
              AND upper_inf(rel_time_range)
              AND rel_group = r_group.group_name
            ORDER BY rel_schema, rel_tblseq
        LOOP
          v_groupsText = v_groupsText
                      || E'        {\n'
                      ||  '          "schema": ' || to_json(r_table.rel_schema) || E',\n'
                      ||  '          "table": ' || to_json(r_table.rel_tblseq) || E',\n'
                      || coalesce('          "priority": '|| to_json(r_table.rel_priority) || E',\n', '')
                      || coalesce('          "log_data_tablespace": '|| to_json(r_table.rel_log_dat_tsp) || E',\n', '')
                      || coalesce('          "log_index_tablespace": '|| to_json(r_table.rel_log_idx_tsp) || E',\n', '')
                      || coalesce('          "ignored_triggers": ' || array_to_json(r_table.rel_ignored_triggers) || E',\n', '')
                      || E'        },\n';
        END LOOP;
        v_groupsText = v_groupsText
                    || E'      ],\n';
      END IF;
      IF r_group.group_nb_sequence > 0 THEN
-- Build the sequences list, if any.
        v_groupsText = v_groupsText
                    || E'      "sequences": [\n';
        FOR r_sequence IN
          SELECT rel_schema, rel_tblseq
            FROM emaj.emaj_relation
            WHERE rel_kind = 'S'
              AND upper_inf(rel_time_range)
              AND rel_group = r_group.group_name
            ORDER BY rel_schema, rel_tblseq
        LOOP
          v_groupsText = v_groupsText
                      || E'        {\n'
                      ||  '          "schema": ' || to_json(r_sequence.rel_schema) || E',\n'
                      ||  '          "sequence": ' || to_json(r_sequence.rel_tblseq) || E',\n'
                      || E'        },\n';
        END LOOP;
        v_groupsText = v_groupsText
                    || E'      ],\n';
      END IF;
      v_groupsText = v_groupsText
                  || E'    },\n';
    END LOOP;
    v_groupsText = v_groupsText
                || E'  ]\n';
-- Build the trailer and remove illicite commas at the end of arrays and attributes lists.
    v_groupsText = v_groupsText
                || E'}\n';
    v_groupsText = regexp_replace(v_groupsText, E',(\n *(\]|}))', '\1', 'g');
-- Test the JSON format by casting the text structure to json and report a warning in case of problem
-- (this should not fail, unless the function code is bogus).
    BEGIN
      v_groupsJson = v_groupsText::JSON;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '_export_groups_conf: The generated JSON structure is not properly formatted. '
                        'Please report the bug to the E-Maj project.';
    END;
--
    RETURN v_groupsJson;
  END;
$_export_groups_conf$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_json JSON, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%',
                                                                 p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_groups_configuration$
-- This function import a supplied JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--        - an optional mark name to set for tables groups in logging state (IMPORT_% by default)
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
  BEGIN
-- Just process the tables groups.
    RETURN emaj._import_groups_conf(p_json, p_groups, p_allowGroupsUpdate, NULL, p_mark, p_dropOtherGroups);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(JSON,TEXT[],BOOLEAN, TEXT, BOOLEAN) IS
$$Import a json structure describing tables groups to create or alter.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%',
                                                                 p_dropOtherGroups BOOLEAN DEFAULT FALSE)
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
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_groupsText             TEXT;
    v_json                   JSON;
  BEGIN
-- Verify that the installer role is allowed to execute COPY FROM statements.
    PERFORM emaj._check_can_copy_from();
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
    RETURN emaj._import_groups_conf(v_json, p_groups, p_allowGroupsUpdate, p_location, p_mark, p_dropOtherGroups);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(TEXT,TEXT[],BOOLEAN, TEXT, BOOLEAN) IS
$$Create or alter tables groups configuration from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf(p_json JSON, p_groups TEXT[], p_allowGroupsUpdate BOOLEAN,
                                                    p_location TEXT, p_mark TEXT, p_dropOtherGroups BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf$
-- This function processes a JSON formatted structure representing the tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- The expected JSON structure must contain an array like:
-- { "tables_groups": [
--   {
--     "group": "myGroup1", "is_rollbackable": true, "comment": "This is group #1",
--     "tables": [
--       {
--       "schema": "myschema1", "table": "mytbl1",
--       "priority": 1, "log_data_tablespace": "tblspc1", "log_index_tablespace": "tblspc2",
--       "ignored_triggers": [
--         { "trigger": "xxx" },
--         ...
--         ]
--       },
--       {
--       ...
--       }
--     ],
--     "sequences": [
--       {
--       "schema": "myschema1", "sequence": "mytbl1",
--       },
--       {
--       ...
--       }
--     ],
--   },
--   ...
--   ]
-- }
-- For tables groups, "group_is_rollbackable" and "group_comment" attributes are optional.
-- For tables, "priority", "log_data_tablespace" and "log_index_tablespace" attributes are optional.
-- A tables group may have no "tables" or "sequences" arrays.
-- A table may have no "ignored_triggers" array.
-- The function replaces the content of the tmp_app_table table for the imported tables groups by the content of the JSON configuration.
-- Non existing groups are created empty.
-- The _alter_groups() function is used to process the assignement, the move, the removal or the attributes change for tables and
-- sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - a boolean indicating whether tables groups to import may already exist
--        - the input file name, if any, to record in the emaj_hist table (NULL if direct import)
--        - the mark name to set for tables groups in logging state
--        - the request for the drop of all existing groups that are not in the imported configuration
-- Output: the number of created or altered tables groups
  DECLARE
    v_groupsJson             JSON;
    r_msg                    RECORD;
  BEGIN
-- Performs various checks on the groups content described in the supplied JSON structure.
    FOR r_msg IN
      SELECT rpt_message
        FROM emaj._check_json_groups_conf(p_json)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3, rpt_int_var_1
    LOOP
      RAISE WARNING '_import_groups_conf (1): %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the supplied JSON structure.';
    END IF;
-- Extract the "tables_groups" json path.
    v_groupsJson = p_json #> '{"tables_groups"}';
-- If not supplied by the caller, materialize the groups array, by aggregating all groups of the JSON structure.
    IF p_groups IS NULL THEN
      SELECT array_agg("group") INTO p_groups
        FROM json_to_recordset(v_groupsJson) AS x("group" TEXT);
    END IF;
-- Prepare the groups configuration import. This may report some other issues with the groups content.
    FOR r_msg IN
      SELECT rpt_message
        FROM emaj._import_groups_conf_prepare(p_json, p_groups, p_allowGroupsUpdate, p_location, p_dropOtherGroups)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3
    LOOP
      RAISE WARNING '_import_groups_conf (2): %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the JSON groups configuration.';
    END IF;
-- OK
    RETURN emaj._import_groups_conf_exec(p_json, p_groups, p_mark, p_dropOtherGroups);
 END;
$_import_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(p_groupsJson JSON, p_groups TEXT[],
                                                    p_allowGroupsUpdate BOOLEAN, p_location TEXT, p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepares the effective tables groups configuration import.
-- It is called by _import_groups_conf() and by Emaj_web
-- At the end of the function, the tmp_app_table table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: diagnostic records
  DECLARE
    v_prevCMM                TEXT;
    v_rollbackableGroups     TEXT[];
    v_ignoredTriggers        TEXT[];
    r_group                  RECORD;
    r_table                  RECORD;
    r_sequence               RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('IMPORT_GROUPS', 'BEGIN', array_to_string(p_groups, ', '), 'Input file: ' || quote_literal(p_location));
-- Extract the "tables_groups" json path.
    p_groupsJson = p_groupsJson #> '{"tables_groups"}';
-- Check that all tables groups listed in the p_groups array exist in the JSON structure.
    RETURN QUERY
      SELECT 250, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                   format('The tables group "%s" to import is not referenced in the JSON structure.',
                          group_name)
        FROM
          (  SELECT group_name
               FROM unnest(p_groups) AS g(group_name)
           EXCEPT
             SELECT "group"
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT)
          ) AS t;
    IF FOUND THEN
      RETURN;
    END IF;
-- If the p_allowGroupsUpdate flag is FALSE, check that no tables group already exists.
    IF NOT p_allowGroupsUpdate THEN
      RETURN QUERY
        SELECT 251, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('The tables group "%s" already exists.',
                            group_name)
          FROM
            (SELECT "group" AS group_name
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT), emaj.emaj_group
               WHERE group_name = "group"
                 AND "group" = ANY (p_groups)
            ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    ELSE
-- If the p_allowGroupsUpdate flag is TRUE, check that existing tables groups have the same type than in the JSON structure.
      RETURN QUERY
        SELECT 252, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('Changing the type of the tables group "%s" is not allowed. '
                            'You may drop this tables group before importing the configuration.',
                            group_name)
          FROM
            (SELECT "group" AS group_name
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT, "is_rollbackable" BOOLEAN)
                    JOIN emaj.emaj_group ON (group_name = "group")
               WHERE "group" = ANY (p_groups)
                 AND group_is_rollbackable <> coalesce(is_rollbackable, true)
            ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    END IF;
-- Drop temporary tables in case of...
    v_prevCMM = pg_catalog.current_setting('client_min_messages');
    SET client_min_messages TO WARNING;
    DROP TABLE IF EXISTS tmp_app_table, tmp_app_sequence;
    PERFORM pg_catalog.set_config ('client_min_messages', v_prevCMM, FALSE);
-- Create the temporary table that will hold the application tables configured in imported groups.
    CREATE TEMP TABLE tmp_app_table (
      tmp_group            TEXT NOT NULL,
      tmp_schema           TEXT NOT NULL,
      tmp_tbl_name         TEXT NOT NULL,
      tmp_priority         INTEGER,
      tmp_log_dat_tsp      TEXT,
      tmp_log_idx_tsp      TEXT,
      tmp_ignored_triggers TEXT[]
      );
-- Create the temporary table that will hold the application sequences configured in imported groups.
    CREATE TEMP TABLE tmp_app_sequence (
      tmp_schema           TEXT NOT NULL,
      tmp_seq_name         TEXT NOT NULL,
      tmp_group            TEXT
      );
-- In a second pass over the JSON structure, populate the tmp_app_table and tmp_app_sequence temporary tables.
    v_rollbackableGroups = '{}';
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(p_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
-- Build the rollbackable groups array.
      IF coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE) THEN
        v_rollbackableGroups = array_append(v_rollbackableGroups, r_group.groupJson ->> 'group');
      END IF;
-- Insert tables into tmp_app_table.
      FOR r_table IN
        SELECT value AS tableJson
          FROM json_array_elements(r_group.groupJson -> 'tables')
      LOOP
--   Prepare the trigger names array for the table,
        SELECT array_agg("value" ORDER BY "value") INTO v_ignoredTriggers
          FROM json_array_elements_text(r_table.tableJson -> 'ignored_triggers') AS t;
--   ... and insert
        INSERT INTO tmp_app_table(tmp_group, tmp_schema, tmp_tbl_name,
                                  tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp, tmp_ignored_triggers)
          VALUES (r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table',
                  (r_table.tableJson ->> 'priority')::INT, r_table.tableJson ->> 'log_data_tablespace',
                  r_table.tableJson ->> 'log_index_tablespace', v_ignoredTriggers);
      END LOOP;
-- Insert sequences into tmp_app_sequence.
      FOR r_sequence IN
        SELECT value AS sequenceJson
          FROM json_array_elements(r_group.groupJson -> 'sequences')
      LOOP
        INSERT INTO tmp_app_sequence(tmp_schema, tmp_seq_name, tmp_group)
          VALUES (r_sequence.sequenceJson ->> 'schema', r_sequence.sequenceJson ->> 'sequence', r_group.groupJson ->> 'group');
      END LOOP;
    END LOOP;
-- Add an index on each temporary table.
    CREATE INDEX ON tmp_app_table (tmp_schema, tmp_tbl_name);
    CREATE INDEX ON tmp_app_sequence (tmp_schema, tmp_seq_name);
-- Check that no table or sequence is referenced in several different groups or in its group.
    RETURN QUERY
      SELECT 270, 1, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::TEXT, nb_group::INT,
             format('The table %s.%s is referenced in %s different tables groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_tbl_name), nb_group)
        FROM (SELECT tmp_schema, tmp_tbl_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_table
                GROUP BY tmp_schema, tmp_tbl_name
                HAVING count(DISTINCT tmp_group) > 1) AS t1
      UNION
      SELECT 271, 1, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::TEXT, nb_group::INT,
             format('The sequence %s.%s is referenced in %s different tables groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_seq_name), nb_group)
        FROM (SELECT tmp_schema, tmp_seq_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_sequence
                GROUP BY tmp_schema, tmp_seq_name
                HAVING count(DISTINCT tmp_group) > 1) AS t2
      UNION
      SELECT 272, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is referenced several times.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM (SELECT tmp_group, tmp_schema, tmp_tbl_name, count(*)
                FROM tmp_app_table
                GROUP BY tmp_group, tmp_schema, tmp_tbl_name
                HAVING count(*) > 1) AS t3
      UNION
      SELECT 273, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the sequence %s.%s is referenced several times.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM (SELECT tmp_group, tmp_schema, tmp_seq_name, count(*)
                FROM tmp_app_sequence
                GROUP BY tmp_group, tmp_schema, tmp_seq_name
                HAVING count(*) > 1) AS t4;
    IF FOUND THEN
      RETURN;
    END IF;
-- Check that the tmp_app_table and tmp_app_sequence tables content is ok for the imported groups.
    RETURN QUERY
      SELECT *
        FROM emaj._import_groups_conf_check(p_groups, p_dropOtherGroups)
        WHERE ((rpt_text_var_1 = ANY (p_groups) AND rpt_severity = 1)
            OR (rpt_text_var_1 = ANY (v_rollbackableGroups) AND rpt_severity = 2))
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3;
--
    RETURN;
  END;
$_import_groups_conf_prepare$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_check(p_groupNames TEXT[], p_dropOtherGroups BOOLEAN)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_check$
-- This function verifies that the content of tables group as defined into the tmp_app_table and tmp_app_sequence tables is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them, depending on the tables group type.
-- It is called by the _import_groups_conf_prepare() function.
-- This function checks that the referenced application tables and sequences:
--  - exist, and only once
--  - are not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
--  - have the same owner as the installer role in non superuser emaj install,
-- It also checks that:
--  - tables are not TEMPORARY
--  - for rollbackable groups, tables are not UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for tables, configured tablespaces exist
-- Input: name array of the tables groups to check
--        flag to request for extra groups drop
-- Output: _report_message_type records representing diagnostic messages
--         the rpt_severity is set to 1 if the error blocks any type group creation or alter,
--                                 or 2 if the error only blocks ROLLBACKABLE groups creation
  DECLARE
    v_installedBySuperuser   BOOLEAN;
    v_installerRole          TEXT;
  BEGIN
-- Read some installation characteristics.
    SELECT inst_by_superuser, inst_installer_role
      INTO v_installedBySuperuser, v_installerRole
      FROM emaj.emaj_install_conf;
-- Check that all application tables listed for the group really exist.
    RETURN QUERY
      SELECT 1, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s does not exist.',
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
             format('In tables group "%s", the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'p';
-- Check no application schema listed for the group in the tmp_app_table table is an E-Maj schema.
    RETURN QUERY
      SELECT 3, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
-- Check that no table of the checked groups already belongs to other created groups.
    IF NOT p_dropOtherGroups THEN
      RETURN QUERY
        SELECT 4, 1, tmp_group, tmp_schema, tmp_tbl_name, rel_group, NULL::INT,
               format('In tables group "%s", the table %s.%s is already assigned to the group "%s".',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(rel_group))
          FROM tmp_app_table
               JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_tbl_name AND upper_inf(rel_time_range))
          WHERE NOT rel_group = ANY (p_groupNames);
    END IF;
-- Check that no table is a TEMP table.
    RETURN QUERY
      SELECT 5, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is a TEMPORARY table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r' AND relpersistence = 't';
-- In non superuser emaj install, check that all table owners are the installer role.
    IF NOT v_installedBySuperuser THEN
      RETURN QUERY
        SELECT 6, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
               format('In tables group "%s", the %s.%s table owner is not the emaj installer role.',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
          FROM tmp_app_table
               JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
          WHERE relkind = 'r'
            AND relowner::regrole::text <> current_user;
    END IF;
-- Check that the log data tablespaces for tables exist.
    RETURN QUERY
      SELECT 12, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_dat_tsp, NULL::INT,
             format('In tables group "%s" and for the table %s.%s, the data log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_dat_tsp))
        FROM tmp_app_table
        WHERE tmp_log_dat_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_dat_tsp
                );
-- Check that the log index tablespaces for tables exist.
    RETURN QUERY
      SELECT 13, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_idx_tsp, NULL::INT,
             format('In tables group "%s" and for the table %s.%s, the index log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_idx_tsp))
        FROM tmp_app_table
        WHERE tmp_log_idx_tsp IS NOT NULL
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
                  JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
             WHERE relkind = 'r'
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
                  JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
             WHERE relkind = 'r'
          ) AS t
        WHERE tmp_trigger IN ('emaj_trunc_trg', 'emaj_log_trg');
-- Check that no table is an unlogged table (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 20, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is an UNLOGGED table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND relpersistence = 'u';
-- Check every table has a primary key (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 22, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s has no PRIMARY KEY.',
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
             format('In tables group "%s", the sequence %s.%s does not exist.',
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
    IF NOT p_dropOtherGroups THEN
      RETURN QUERY
        SELECT 32, 1, tmp_group, tmp_schema, tmp_seq_name, rel_group, NULL::INT,
               format('In tables group "%s", the sequence %s.%s is already assigned to the group %s.',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name), quote_ident(rel_group))
          FROM tmp_app_sequence
               JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_seq_name AND upper_inf(rel_time_range))
          WHERE NOT rel_group = ANY (p_groupNames);
    END IF;
-- Check no application schema listed for the group in the tmp_app_sequence table is an E-Maj schema.
    RETURN QUERY
      SELECT 33, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the sequence %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM tmp_app_sequence
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
-- In non superuser emaj install, check that all sequence owners are the installer role.
    IF NOT v_installedBySuperuser THEN
      RETURN QUERY
        SELECT 34, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
               format('In tables group "%s", the %s.%s sequence owner is not the emaj installer role.',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
          FROM tmp_app_sequence
               JOIN pg_catalog.pg_class ON (relname = tmp_seq_name)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
          WHERE relkind = 'S'
            AND relowner::regrole::text <> current_user;
    END IF;
--
    RETURN;
  END;
$_import_groups_conf_check$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_exec(p_json JSON, p_groups TEXT[], p_mark TEXT,
                                                         p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf_exec$
-- This function completes a tables groups configuration import.
-- It is called by _import_groups_conf() and by Emaj_web
-- Non existing groups are created empty.
-- The _import_groups_conf_alter() function is used to process the assignement, the move, the removal or the attributes change for tables
-- and sequences and the extra groups drop.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process
--        - a boolean indicating whether tables groups to import may already exist
--        - the mark name to set for tables groups in logging state
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
  DECLARE
    v_function               TEXT = 'IMPORT_GROUPS';
    v_timeId                 BIGINT;
    v_groupsJson             JSON;
    v_nbGroup                INT;
    v_comment                TEXT;
    v_isRollbackable         BOOLEAN;
    v_loggingGroups          TEXT[];
    v_groupsToDrop           TEXT[];
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
-- Lock the concerned groups to avoid concurrent operation on these groups.
    PERFORM 0
      FROM emaj.emaj_group
      WHERE p_dropOtherGroups OR group_name = ANY (p_groups)
      FOR UPDATE;
-- Build the groups to drop array, if requested.
    IF p_dropOtherGroups THEN
      SELECT array_agg(group_name ORDER BY group_name) INTO v_groupsToDrop
        FROM emaj.emaj_group
        WHERE NOT group_name = ANY (p_groups);
    END IF;
-- Build the in logging state groups array.
    SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups
      FROM emaj.emaj_group
      WHERE (p_dropOtherGroups OR group_name = ANY (p_groups))
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
    PERFORM emaj._import_groups_conf_alter(p_groups, p_mark, v_timeId, v_groupsToDrop);
-- Check foreign keys with tables outside the groups in logging state.
    PERFORM emaj._check_fk_groups(v_loggingGroups);
-- Drop the now useless temporary tables.
    DROP TABLE tmp_app_table;
    DROP TABLE tmp_app_sequence;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbGroup || ' created or altered tables groups');
--
    RETURN v_nbGroup;
  END;
$_import_groups_conf_exec$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_alter(p_groupNames TEXT[], p_mark TEXT, p_timeId BIGINT, p_groupsToDrop TEXT[])
RETURNS VOID LANGUAGE plpgsql AS
$_import_groups_conf_alter$
-- This function effectively alters the tables groups to import.
-- It uses the content of tmp_app_table and tmp_app_sequence temporary tables and calls the appropriate elementary functions
-- It is called by the _import_groups_conf_exec() function.
-- Input: group names array,
--        the mark name to set on groups in logging state
--        the timestamp id
--        the groups to drop array
  DECLARE
    v_eventTriggers          TEXT[];
    v_function               TEXT = 'IMPORT_GROUPS';
  BEGIN
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Create the needed log schemas.
    PERFORM emaj._create_log_schemas('IMPORT_GROUPS', p_timeId);
-- Remove the tables that do not belong to the groups anymore.
    PERFORM emaj._remove_tbl(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
            AND upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND NOT EXISTS
                  (SELECT NULL
                     FROM tmp_app_table
                     WHERE tmp_schema = rel_schema
                     AND tmp_tbl_name = rel_tblseq
                  )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Remove the sequences that do not belong to the groups anymore.
    PERFORM emaj._remove_seq(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
            AND upper_inf(rel_time_range)
            AND rel_kind = 'S'
            AND NOT EXISTS
                  (SELECT NULL
                     FROM tmp_app_sequence
                     WHERE tmp_schema = rel_schema
                       AND tmp_seq_name = rel_tblseq
                  )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Repair the tables that are damaged or out of sync E-Maj components.
    PERFORM emaj._repair_tbl(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM                                   -- all damaged or out of sync tables
            (SELECT DISTINCT ver_schema, ver_tblseq
               FROM emaj._verify_groups(p_groupNames, FALSE)
            ) AS t
            JOIN emaj.emaj_relation ON (rel_schema = ver_schema AND rel_tblseq = ver_tblseq AND upper_inf(rel_time_range))
            JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
            JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the priority level when requested
-- (the later operations will be executed with the new priorities).
    PERFORM emaj._change_priority_tbl(rel_schema, rel_tblseq, rel_priority, tmp_priority, p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_priority, tmp_priority, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND ( (rel_priority IS NULL AND tmp_priority IS NOT NULL) OR
                  (rel_priority IS NOT NULL AND tmp_priority IS NULL) OR
                  (rel_priority <> tmp_priority) )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the log data tablespaces.
    PERFORM emaj._change_log_data_tsp_tbl(rel_schema, rel_tblseq, rel_log_schema, rel_log_table, rel_log_dat_tsp, tmp_log_dat_tsp,
                                          p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_log_table, rel_log_dat_tsp, tmp_log_dat_tsp, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'
            AND coalesce(rel_log_dat_tsp,'') <> coalesce(tmp_log_dat_tsp,'')
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the log index tablespaces.
    PERFORM emaj._change_log_index_tsp_tbl(rel_schema, rel_tblseq, rel_log_schema, rel_log_index, rel_log_idx_tsp, tmp_log_idx_tsp,
                                          p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_log_index, rel_log_idx_tsp, tmp_log_idx_tsp, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'
            AND coalesce(rel_log_idx_tsp,'') <> coalesce(tmp_log_idx_tsp,'')
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the arrays of triggers to ignore at rollback time for tables.
    PERFORM emaj._change_ignored_triggers_tbl(rel_schema, rel_tblseq, rel_ignored_triggers, tmp_ignored_triggers,
                                              p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_ignored_triggers, tmp_ignored_triggers, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND ( (rel_ignored_triggers IS NULL AND tmp_ignored_triggers IS NOT NULL) OR
                  (rel_ignored_triggers IS NOT NULL AND tmp_ignored_triggers IS NULL) OR
                  (rel_ignored_triggers <> tmp_ignored_triggers) )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the group ownership of tables.
    PERFORM emaj._move_tbl(rel_schema, rel_tblseq, rel_group, old_group_is_logging, tmp_group, new_group_is_logging,
                           p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, old_group.group_is_logging AS old_group_is_logging,
                                       tmp_group, new_group.group_is_logging AS new_group_is_logging
          FROM emaj.emaj_relation
              JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
              JOIN emaj.emaj_group old_group ON (old_group.group_name = rel_group)
              JOIN emaj.emaj_group new_group ON (new_group.group_name = tmp_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND rel_group <> tmp_group
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the group ownership of sequences.
    PERFORM emaj._move_seq(rel_schema, rel_tblseq, rel_group, old_group_is_logging, tmp_group, new_group_is_logging,
                           p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, old_group.group_is_logging AS old_group_is_logging,
                                       tmp_group, new_group.group_is_logging AS new_group_is_logging
          FROM emaj.emaj_relation
              JOIN tmp_app_sequence ON (tmp_schema = rel_schema AND tmp_seq_name = rel_tblseq)
              JOIN emaj.emaj_group old_group ON (old_group.group_name = rel_group)
              JOIN emaj.emaj_group new_group ON (new_group.group_name = tmp_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'S'
            AND rel_group = ANY (p_groupNames)
            AND rel_group <> tmp_group
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Add tables to the groups.
    PERFORM emaj._add_tbl(tmp_schema, tmp_tbl_name, tmp_group, tmp_priority, tmp_log_dat_tsp,
                          tmp_log_idx_tsp, tmp_ignored_triggers, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT tmp_schema, tmp_tbl_name, tmp_group, tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp,
               tmp_ignored_triggers, group_is_logging
          FROM tmp_app_table
               JOIN emaj.emaj_group ON (group_name = tmp_group)
          WHERE NOT EXISTS
                  (SELECT NULL
                     FROM emaj.emaj_relation
                     WHERE rel_schema = tmp_schema
                       AND rel_tblseq = tmp_tbl_name
                       AND upper_inf(rel_time_range)
                       AND rel_kind = 'r'
                       AND rel_group = ANY (p_groupNames)
                  )
          ORDER BY tmp_priority, tmp_schema, tmp_tbl_name
           ) AS t;
-- Add sequences to the groups.
    PERFORM emaj._add_seq(tmp_schema, tmp_seq_name, tmp_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT tmp_schema, tmp_seq_name, tmp_group, group_is_logging
          FROM tmp_app_sequence
               JOIN emaj.emaj_group ON (group_name = tmp_group)
          WHERE NOT EXISTS
                  (SELECT NULL
                     FROM emaj.emaj_relation
                     WHERE rel_schema = tmp_schema
                       AND rel_tblseq = tmp_seq_name
                       AND upper_inf(rel_time_range)
                       AND rel_kind = 'S'
                       AND rel_group = ANY (p_groupNames)
                  )
          ORDER BY tmp_schema, tmp_seq_name
           ) AS t;
-- Drop the not imported tables groups, if requested.
    IF p_groupsToDrop IS NOT NULL THEN
      PERFORM emaj._drop_groups(p_groupsToDrop, p_timeId);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        SELECT v_function, 'GROUP DROPPED', group_name
          FROM unnest(p_groupsToDrop) AS group_name;
    END IF;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any created group).
    PERFORM emaj._drop_log_schemas('IMPORT_GROUPS', FALSE);
-- Re-enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Update some attributes in the emaj_group table.
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = p_timeId,
          group_nb_table =
            (SELECT count(*)
               FROM emaj.emaj_relation
               WHERE rel_group = group_name
                 AND upper_inf(rel_time_range)
                 AND rel_kind = 'r'
             ),
          group_nb_sequence =
            (SELECT count(*)
               FROM emaj.emaj_relation
               WHERE rel_group = group_name
                 AND upper_inf(rel_time_range)
                 AND rel_kind = 'S'
            )
      WHERE group_name = ANY (p_groupNames);
--
    RETURN;
  END;
$_import_groups_conf_alter$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_logging_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of logging group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE group_is_logging
    AND (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_logging_groups(TEXT, TEXT) IS
$$Builds a logging groups array, filtered on their names.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_idle_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of idle group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE NOT group_is_logging
    AND (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_idle_groups(TEXT, TEXT) IS
$$Builds a idle groups array, filtered on their names.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_comment TEXT, p_multiGroup BOOLEAN,
                                                 p_eventToRecord BOOLEAN, p_loggedRlbkTargetMark TEXT DEFAULT NULL,
                                                 p_timeId BIGINT DEFAULT NULL, p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
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
    r_currSeq                emaj.emaj_sequence%ROWTYPE;
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
      r_currSeq = emaj._get_current_seq(r_seq.rel_schema, r_seq.rel_tblseq, p_timeId);
      INSERT INTO emaj.emaj_sequence VALUES (r_currSeq.*);
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
        FROM emaj._dblink_open_cnx('rlbk#1');
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
        FROM emaj._dblink_open_cnx('rlbk#1');
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
        FROM emaj._dblink_open_cnx('rlbk#' || p_session);
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

CREATE OR REPLACE FUNCTION emaj._log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
RETURNS SETOF emaj.emaj_log_stat_sequence_type LANGUAGE plpgsql AS
$_log_stat_sequence$
-- This function returns statistics about a single sequence, for the time period framed by a supplied time_id slice.
-- It is called by the various emaj_log_stat_sequence() function.
-- For each mark interval, possibly for different tables groups, it returns the number of increments and a flag to show any sequence
--   properties changes.
-- Input: schema and sequence names
--        start and end time_id.
-- Output: set of stats by time slice.
  DECLARE
    v_needCurrentState       BOOLEAN;
    r_endSeq                 emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence current state, if it still belongs to an active group.
    v_needCurrentState = EXISTS(
      SELECT 0
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND upper_inf(rel_time_range)
          AND group_is_logging
          AND p_endTimeId IS NULL
      );
    IF v_needCurrentState THEN
      r_endSeq = emaj._get_current_seq(p_schema, p_sequence, 0);
    END IF;
-- Compute and return the statistics by scanning the sequence history in the emaj_sequence table.
    RETURN QUERY
      WITH event AS (
        SELECT sequ_time_id, lower(rel_time_range), sequ_last_val, sequ_start_val, sequ_increment, sequ_max_val,
               sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called,
               rel_group, rel_time_range, time_clock_timestamp, time_event,
               coalesce(mark_name, '[deleted mark]') AS mark_name,
               (time_event = 'S' OR sequ_time_id = lower(rel_time_range)) AS log_start,
               (time_event = 'X' OR (NOT upper_inf(rel_time_range) AND sequ_time_id = upper(rel_time_range))) AS log_stop
          FROM emaj.emaj_sequence
               JOIN emaj.emaj_relation ON (rel_schema = sequ_schema AND rel_tblseq = sequ_name)
               JOIN emaj.emaj_time_stamp ON (time_id = sequ_time_id)
               LEFT OUTER JOIN emaj.emaj_mark ON (mark_group = rel_group AND mark_time_id = sequ_time_id)
          WHERE sequ_schema = p_schema
            AND sequ_name = p_sequence
            AND (sequ_time_id <@ rel_time_range OR sequ_time_id = upper(rel_time_range))
            AND (p_startTimeId IS NULL OR sequ_time_id >= p_startTimeId)
            AND (p_endTimeId IS NULL OR sequ_time_id <= p_endTimeId)
        UNION
-- ... and add the sequence current state, if the upper bound is not fixed and if the sequence belongs to an active group yet.
        SELECT NULL, NULL, r_endSeq.sequ_last_val, r_endSeq.sequ_start_val, r_endSeq.sequ_increment, r_endSeq.sequ_max_val,
               r_endSeq.sequ_min_val, r_endSeq.sequ_cache_val, r_endSeq.sequ_is_cycled, r_endSeq.sequ_is_called,
               rel_group, rel_time_range, clock_timestamp(), '', '[current state]', false, false
          FROM emaj.emaj_relation
          WHERE v_needCurrentState
            AND rel_schema = p_schema
            AND rel_tblseq = p_sequence
            AND upper_inf(rel_time_range)
        ORDER BY 1, 2
        ), time_slice AS (
-- Transform elementary time events into time slices
         SELECT sequ_time_id AS start_time_id, lead(sequ_time_id) OVER () AS end_time_id,
                sequ_last_val AS start_last_val, lead(sequ_last_val) OVER () AS end_last_val,
                sequ_start_val AS start_start_val, lead(sequ_start_val) OVER () AS end_start_val,
                sequ_increment AS start_increment, lead(sequ_increment) OVER () AS end_increment,
                sequ_max_val AS start_max_val, lead(sequ_max_val) OVER () AS end_max_val,
                sequ_min_val AS start_min_val, lead(sequ_min_val) OVER () AS end_min_val,
                sequ_cache_val AS start_cache_val, lead(sequ_cache_val) OVER () AS end_cache_val,
                sequ_is_cycled AS start_is_cycled, lead(sequ_is_cycled) OVER () AS end_is_cycled,
                sequ_is_called AS start_is_called, lead(sequ_is_called) OVER () AS end_is_called,
                rel_group,
                rel_time_range AS start_rel_time_range, lead(rel_time_range) OVER () AS end_rel_time_range,
                time_clock_timestamp AS start_timestamp, lead(time_clock_timestamp) OVER () AS end_timestamp,
                time_event AS start_time_event, lead(time_event) OVER () AS end_time_event,
                mark_name AS start_mark_name, lead(mark_name) OVER () AS end_mark_name,
                log_start AS start_log_start, lead(log_start) OVER () AS end_log_start,
                log_stop AS start_log_stop, lead(log_stop) OVER () AS end_log_stop
           FROM event
        )
-- Filter time slices and compute statistics aggregates
      SELECT rel_group AS stat_group, start_mark_name AS stat_first_mark, start_timestamp AS stat_first_mark_datetime,
             start_time_id AS stat_first_time_id, start_log_start AS stat_is_log_start, end_mark_name AS stat_last_mark,
             end_timestamp AS stat_last_mark_datetime, end_time_id AS stat_last_time_id, end_log_stop AS stat_is_log_stop,
             (end_last_val - start_last_val) / start_increment
               + CASE WHEN start_is_called THEN 0 ELSE 1 END
               - CASE WHEN end_is_called THEN 0 ELSE 1 END AS stat_increments,
             (start_start_val <> end_start_val OR start_increment <> end_increment OR start_max_val <> end_max_val
                OR start_min_val <> end_min_val OR start_is_cycled <> end_is_cycled) AS stat_has_structure_changed,
             count(rlbk_id)::INT AS stat_rollbacks
        FROM time_slice
             LEFT OUTER JOIN emaj.emaj_rlbk ON (rel_group = ANY (rlbk_groups) AND
                                                rlbk_time_id >= start_time_id AND (end_time_id IS NULL OR rlbk_time_id < end_time_id))
        WHERE end_timestamp IS NOT NULL                    -- time slice not starting with the very last event
          AND start_rel_time_range = end_rel_time_range    -- same rel_time_range on the slice
          AND NOT (start_log_stop AND end_log_start)       -- not a logging hole
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        ORDER BY start_time_id;
  END;
$_log_stat_sequence$;

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
--        the absolute pathname of the file that will hold the result.
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
-- Verify that the installer role is allowed to execute COPY TO file and COPY TO PROGRAM statements.
    PERFORM emaj._check_can_copy_to();
    PERFORM emaj._check_can_exec_program();
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
-- Verify that the installer role is allowed to execute COPY TO file and COPY TO PROGRAM statements.
    PERFORM emaj._check_can_copy_to();
    PERFORM emaj._check_can_exec_program();
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
-- For table without primary key, rows are sorted on all non generated columns.
-- There is no need for the group to be in IDLE state.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability:
--   - to create the directory (with proper permissions allowing the cluster to write into) before the emaj_snap_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name,
--        the absolute pathname of the directory where the files are to be created
--        the options to used in the COPY TO statements
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_nbRel                  INT = 0;
    v_colList                TEXT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
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
-- Verify that the installer role is allowed to execute COPY TO statements.
    PERFORM emaj._check_can_copy_to();
-- Check the copy options parameter doesn't contain unquoted ; that could be used for sql injection.
    IF regexp_replace(p_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: The COPY options parameter format is invalid.';
    END IF;
-- For each table/sequence of the emaj_relation table.
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_pk_cols,
             quote_ident(rel_schema) || '.' || quote_ident(rel_tblseq) AS full_relation_name,
             emaj._build_path_name(p_dir, rel_schema || '_' || rel_tblseq || '.snap') AS path_name
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = p_groupName
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- It is a table.
--   Build the order by columns list, using the PK, if it exists.
          IF r_tblsq.rel_pk_cols IS NOT NULL THEN
            SELECT string_agg(quote_ident(attname), ',')
              INTO v_colList
              FROM unnest(r_tblsq.rel_pk_cols) AS attname;
          ELSE
--   The table has no pkey, so get all columns, except generated ones.
            SELECT string_agg(quote_ident(attname), ',' ORDER BY attnum)
              INTO v_colList
              FROM pg_catalog.pg_attribute
              WHERE attrelid = (r_tblsq.full_relation_name)::regclass
                AND attnum > 0
                AND attisdropped = FALSE
                AND attgenerated = '';
          END IF;
--   Dump the table
          v_stmt = format('(SELECT * FROM %I.%I ORDER BY %s)',
                          r_tblsq.rel_schema, r_tblsq.rel_tblseq, v_colList);
          EXECUTE format('COPY %s TO %L %s',
                         v_stmt, r_tblsq.path_name, coalesce(p_copyOptions, ''));
        WHEN 'S' THEN
-- It is a sequence.
          v_stmt = format('(SELECT relname, rel.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, rel.is_called'
                          '  FROM %I.%I rel,'
                          '       pg_catalog.pg_sequence s'
                          '       JOIN pg_catalog.pg_class c ON (c.oid = s.seqrelid)'
                          '       JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)'
                          '  WHERE nspname = %L AND relname = %L)',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_schema, r_tblsq.rel_tblseq);
--    Dump the sequence properties.
          EXECUTE format ('COPY %s TO %L %s',
                          v_stmt, r_tblsq.path_name, coalesce(p_copyOptions, ''));
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || statement_timestamp()) || ')';
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
    v_prevCMM                TEXT;
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
-- Verify that the installer role is allowed to execute COPY TO statements.
    PERFORM emaj._check_can_copy_to();
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
      v_prevCMM = pg_catalog.current_setting('client_min_messages');
      SET client_min_messages TO WARNING;
      DROP TABLE IF EXISTS emaj_temp_script CASCADE;
      PERFORM pg_catalog.set_config ('client_min_messages', v_prevCMM, FALSE);
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
        FROM pg_catalog.pg_settings
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
-- Verify that the installer role is allowed to execute COPY TO statements.
    PERFORM emaj._check_can_copy_to();
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
-- Verify that the installer role is allowed to execute COPY FROM statements.
    PERFORM emaj._check_can_copy_from();
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
  DECLARE
    v_installedBySuperuser   BOOLEAN;
    v_installerRole          TEXT;
  BEGIN
-- Read some installation characteristics.
    SELECT inst_by_superuser, inst_installer_role
      INTO v_installedBySuperuser, v_installerRole
      FROM emaj.emaj_install_conf;
--
-- Errors detection.
--
-- Check that all application schemas referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: The application schema "' || rel_schema || '" does not exist anymore.' AS msg
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
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist anymore.' AS msg
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
             rel_schema || '"."' || rel_tblseq || '" has no primary key anymore.' AS msg
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
-- Check the primary key structure of all tables belonging to rollbackable groups is unchanged.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  array_to_string(rel_pk_cols, ',') AS registered_pk_columns,
                  string_agg(attname, ',' ORDER BY array_position(conkey, attnum)) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_constraint ON (conrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (attrelid = conrelid)
             WHERE rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND attnum = ANY (conkey)
               AND contype = 'p'
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_pk_columns <> current_pk_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that the "GENERATED AS expression" columns list of all tables have not changed.
-- (The expression of virtual generated columns be changed, without generating any trouble)
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the "GENERATED AS expression" columns list of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_gen_columns || ' => ' || current_gen_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  coalesce(array_to_string(rel_gen_expr_cols, ','), '<none>') AS registered_gen_columns,
                  coalesce(string_agg(attname, ',' ORDER BY attnum), '<none>') AS current_gen_columns
             FROM emaj.emaj_relation
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
             WHERE rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND attgenerated <> ''
               AND attnum > 0
               AND NOT attisdropped
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_gen_columns <> current_gen_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the array of triggers to ignore at rollback time only contains existing triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the trigger "' || trg_name || '" for table "'
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
-- In non superuser emaj install, check that all table owners are the installer role.
    IF NOT v_installedBySuperuser THEN
      RETURN QUERY
        SELECT 'Error: In tables group "' || rel_group || '", the owner of table ' || rel_schema || '"."' || rel_tblseq ||
               '" is not the emaj extension installer.'
               AS msg
          FROM emaj.emaj_relation
               JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
            WHERE relkind = 'r'
              AND relowner::regrole::text <> v_installerRole
          ORDER BY rel_schema, rel_tblseq;
-- In non superuser emaj install, check that all sequence owners are the installer role.
      RETURN QUERY
        SELECT 'Error: In tables group "' || rel_group || '", the owner of sequence ' || rel_schema || '"."' || rel_tblseq ||
               '" is not the emaj extension installer.'
               AS msg
          FROM emaj.emaj_relation
               JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
            WHERE relkind = 'S'
              AND relowner::regrole::text <> v_installerRole
          ORDER BY rel_schema, rel_tblseq;
    END IF;
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
-- Detect tables linked by a foreign key but not belonging to the same tables group and inherited FK that cannot be dropped and
--   recreated at rollback time.
    RETURN QUERY
      SELECT msg FROM
        (WITH fk_dependencies AS             -- all foreign keys that link 2 tables at least one of both belongs to a tables group
           (SELECT n.nspname AS tbl_schema, t.relname AS tbl_name,
                   c.conname, c. coninhcount, c.condeferrable, c.confdeltype, c.confupdtype,
                   nf.nspname AS reftbl_schema, tf.relname AS reftbl_name,
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
                AND t.relkind = 'r' AND tf.relkind = 'r'                  -- excluding partitionned tables
                AND (r.rel_group IS NOT NULL OR rf.rel_group IS NOT NULL) -- at least the table or the referenced table belongs to
                                                                          -- a tables group
           )
-- Referenced table not in a group.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on "' || tbl_schema || '"."' || tbl_name || '" references the table "' ||
                  reftbl_schema || '"."' || reftbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND tbl_group_is_rollbackable
               AND reftbl_group IS NULL
         UNION ALL
-- Referencing table not in a group.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || reftbl_group || '", the table "' || reftbl_schema || '"."' || reftbl_name ||
                  '" is referenced by the foreign key "' || conname || '" on "' ||
                  tbl_schema || '"."' || tbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE reftbl_group IS NOT NULL
               AND reftbl_group_is_rollbackable
               AND tbl_group IS NULL
         UNION ALL
-- Both tables in different groups.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on "' || tbl_schema || '"."' || tbl_name || '" references the table "' ||
                  reftbl_schema || '"."' || reftbl_name || '" that belongs to another group ("' ||
                  reftbl_group || '")' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND reftbl_group IS NOT NULL
               AND tbl_group <> reftbl_group
               AND (tbl_group_is_rollbackable OR reftbl_group_is_rollbackable)
-- Inherited FK that cannot be dropped/recreated.
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: The foreign key "' || conname || '" on "' || tbl_schema || '"."' || tbl_name || '" is inherited'
                  ' from a partitionned table and is not deferrable. This could block E-Maj rollbacks.' AS msg
             FROM fk_dependencies
             WHERE coninhcount > 0
               AND NOT condeferrable
               AND ((tbl_group IS NOT NULL AND tbl_group_is_rollbackable) OR
                    (reftbl_group IS NOT NULL AND reftbl_group_is_rollbackable))
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: The foreign key "' || conname || '" on "' || tbl_schema || '"."' || tbl_name || '" has ON DELETE'
                  ' / ON UPDATE clauses and is inherited from a partitionned table. This could block E-Maj rollbacks.' AS msg
             FROM fk_dependencies
             WHERE coninhcount > 0
               AND (confdeltype <> 'a' OR confupdtype <> 'a')
               AND ((tbl_group IS NOT NULL AND tbl_group_is_rollbackable) OR
                    (reftbl_group IS NOT NULL AND reftbl_group_is_rollbackable))
           ORDER BY 1,2,3
        ) AS t;
--
    RETURN;
  END;
$_verify_all_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in the emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_supportEventTriggers   BOOLEAN;
    v_errorFound             BOOLEAN = FALSE;
    v_status                 INT;
    v_schema                 TEXT;
    v_paramList              TEXT;
    r_object                 RECORD;
  BEGIN
-- Get the installation characteristics.
    SELECT inst_with_event_triggers INTO STRICT v_supportEventTriggers
      FROM emaj.emaj_install_conf;
-- Check the postgres version compatibility.
    IF emaj._pg_version_num() < 120000 THEN
      RETURN NEXT 'Error: The current postgres version (' || version()
               || ') is not compatible with this E-Maj version. It should be at least 12';
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
-- Report a warning if emaj_param contains an unknown parameter.
    SELECT string_agg(param_key, ', ' ORDER BY param_key)
      INTO v_paramList
      FROM emaj.emaj_visible_param
      WHERE param_key NOT IN
        ('dblink_user_password', 'history_retention', 'alter_log_table', 'avg_row_rollback_duration', 'avg_row_delete_log_duration',
         'avg_fkey_check_duration', 'fixed_step_rollback_duration', 'fixed_table_rollback_duration', 'fixed_dblink_rollback_duration');
    IF v_paramList IS NOT NULL THEN
      RETURN NEXT format('Warning: the emaj_param table contains unknown parameters (%s).',
                         v_paramList);
    END IF;
-- Report a warning if dblink connections are not operational.
    IF has_function_privilege('emaj._dblink_open_cnx(text)', 'execute') THEN
      SELECT p_status, p_schema INTO v_status, v_schema
        FROM emaj._dblink_open_cnx('emaj_verify_all');
      CASE v_status
        WHEN 0, 1 THEN
          PERFORM emaj._dblink_close_cnx('emaj_verify_all', v_schema);
        WHEN -1 THEN
          RETURN NEXT 'Warning: The dblink extension is not installed.';
        WHEN -3 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the installer role is not allowed to execute dblink_connect_u().';
        WHEN -4 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the transaction isolation level is not READ COMMITTED.';
        WHEN -5 THEN
          RETURN NEXT 'Warning: The ''dblink_user_password'' parameter value is not set in the emaj_param table.';
        WHEN -6 THEN
          RETURN NEXT 'Warning: The dblink connection attempt failed. The ''dblink_user_password'' parameter value may be incorrect.';
        WHEN -7 THEN
          RETURN NEXT 'Warning: The role set in the ''dblink_user_password'' parameter has not emaj administration rights.';
        WHEN -8 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the installer role has not the "pg_read_all_settings" privileges.';
        ELSE
          RETURN NEXT format('Warning: The dblink connection test failed for an unknown reason (status = %s).',
                             v_status::TEXT);
      END CASE;
    ELSE
      RETURN NEXT 'Warning: The dblink connection has not been tested (the current role is not granted emaj_adm).';
    END If;
-- Report a warning if the max_prepared_transaction GUC setting is not appropriate for parallel rollbacks.
    IF pg_catalog.current_setting('max_prepared_transactions')::INT <= 1 THEN
      RETURN NEXT format('Warning: The max_prepared_transactions parameter value (%s) on this cluster is too low to launch parallel '
                         'rollback.',
                         pg_catalog.current_setting('max_prepared_transactions'));
    END IF;
    IF v_supportEventTriggers THEN
-- Report a warning if the emaj_protection_trg event trigger should exist and is missing.
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

CREATE OR REPLACE FUNCTION emaj.emaj_disable_protection_by_event_triggers()
RETURNS INT LANGUAGE plpgsql AS
$emaj_disable_protection_by_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively disabled event triggers.
  DECLARE
    v_supportEventTriggers   BOOLEAN;
    v_eventTriggers          TEXT[];
  BEGIN
-- Raise a warning and return immediately if the extension doesn't support event triggers.
    SELECT inst_with_event_triggers INTO STRICT v_supportEventTriggers
      FROM emaj.emaj_install_conf;
    IF NOT v_supportEventTriggers THEN
      RAISE WARNING 'emaj_disable_protection_by_event_triggers: This emaj extension does not support event triggers.';
      RETURN 0;
    END IF;
-- Call the _disable_event_triggers() function and get the disabled event trigger names array.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Keep a trace into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('DISABLE_PROTECTION', 'EVENT TRIGGERS DISABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- Return the number of disabled event triggers.
    RETURN coalesce(array_length(v_eventTriggers,1),0);
  END;
$emaj_disable_protection_by_event_triggers$;
COMMENT ON FUNCTION emaj.emaj_disable_protection_by_event_triggers() IS
$$Disables the protection of E-Maj components by event triggers.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_enable_protection_by_event_triggers()
RETURNS INT LANGUAGE plpgsql AS
$emaj_enable_protection_by_event_triggers$
-- This function enables all known E-Maj event triggers that are in disabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively enabled event triggers.
  DECLARE
    v_supportEventTriggers   BOOLEAN;
    v_eventTriggers          TEXT[];
  BEGIN
-- Raise a warning and return immediately if the extension doesn't support event triggers.
    SELECT inst_with_event_triggers INTO STRICT v_supportEventTriggers
      FROM emaj.emaj_install_conf;
    IF NOT v_supportEventTriggers THEN
      RAISE WARNING 'emaj_enable_protection_by_event_triggers: This emaj extension does not support event triggers.';
      RETURN 0;
    END IF;
-- Build the event trigger names array from the pg_event_trigger table.
    SELECT coalesce(array_agg(evtname  ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger
      WHERE evtname LIKE 'emaj%'
        AND evtenabled = 'D';
-- Call the _enable_event_triggers() function.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Keep a trace into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS ENABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- Return the number of enabled event triggers.
    RETURN coalesce(array_length(v_eventTriggers,1),0);
  END;
$emaj_enable_protection_by_event_triggers$;
COMMENT ON FUNCTION emaj.emaj_enable_protection_by_event_triggers() IS
$$Enables the protection of E-Maj components by event triggers.$$;

CREATE OR REPLACE FUNCTION emaj._disable_event_triggers()
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_disable_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_disable_event_triggers_protection() function.
-- Output: array of effectively disabled event trigger names. It can be reused as input when calling _enable_event_triggers().
-- The function is declared as SECURITY DEFINER because only superusers can alter an event trigger.
-- The function doesn't execute anything when emaj has been installed with the emaj-devel.sql script by a non superuser role.
  DECLARE
    v_supportEventTriggers   BOOLEAN;
    v_eventTrigger           TEXT;
    v_eventTriggers          TEXT[] = ARRAY[]::TEXT[];
  BEGIN
-- Exit immediately if the extension doesn't support event triggers.
    SELECT inst_with_event_triggers INTO STRICT v_supportEventTriggers
      FROM emaj.emaj_install_conf;
    IF NOT v_supportEventTriggers THEN
      RETURN v_eventTriggers;
    END IF;
-- Build the event trigger names array from the pg_event_trigger table.
-- A single operation like _alter_groups() may call the function several times. But this is not an issue as only enabled triggers are
-- disabled.
    SELECT coalesce(array_agg(evtname ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger
      WHERE evtname LIKE 'emaj%'
        AND evtenabled <> 'D';
-- Disable each event trigger.
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I DISABLE',
                     v_eventTrigger);
    END LOOP;
--
    RETURN v_eventTriggers;
  END;
$_disable_event_triggers$;

CREATE OR REPLACE FUNCTION emaj._enable_event_triggers(p_eventTriggers TEXT[])
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_enable_event_triggers$
-- This function enables all event triggers supplied as parameter.
-- It also recreates the emaj_protection_trg event trigger if it does not exist. This event trigger is the only component
-- that is not linked to the emaj extension and cannot be protected by another event trigger.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_enable_event_triggers_protection() function.
-- Input: array of event trigger names to enable.
-- Output: same array.
-- The function is declared as SECURITY DEFINER because only superusers can alter an event trigger
-- The function doesn't execute anything when emaj has been installed with the emaj-devel.sql script by a non superuser role.
  DECLARE
    v_supportEventTriggers   BOOLEAN;
    v_eventTrigger           TEXT;
  BEGIN
-- Exit immediately if the extension doesn't support event triggers.
    SELECT inst_with_event_triggers INTO STRICT v_supportEventTriggers
      FROM emaj.emaj_install_conf;
    IF NOT v_supportEventTriggers THEN
      RETURN p_eventTriggers;
    END IF;
-- If the emaj_protection_trg event trigger does not exist, recreate it and report.
    PERFORM 0
      FROM pg_catalog.pg_event_trigger
      WHERE evtname = 'emaj_protection_trg';
    IF NOT FOUND THEN
      CREATE EVENT TRIGGER emaj_protection_trg
        ON sql_drop
        WHEN TAG IN ('DROP EXTENSION','DROP SCHEMA')
        EXECUTE PROCEDURE public._emaj_protection_event_trigger_fnct();
      COMMENT ON EVENT TRIGGER emaj_protection_trg IS
      $$Blocks the removal of the emaj extension or schema.$$;
      RAISE WARNING '_enable_event_triggers: the emaj_protection_trg event trigger has been recreated.';
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
        VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS RECREATED', 'emaj_protection_trg');
    END If;
    FOREACH v_eventTrigger IN ARRAY p_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I ENABLE',
                     v_eventTrigger);
    END LOOP;
--
  RETURN p_eventTriggers;
  END;
$_enable_event_triggers$;

CREATE OR REPLACE FUNCTION emaj.emaj_drop_extension()
RETURNS VOID LANGUAGE plpgsql AS
$emaj_drop_extension$
-- This function drops emaj from the current database, with both installation kinds,
-- - either as EXTENSION (i.e. with a CREATE EXTENSION SQL statement),
-- - or with the alternate psql script.
  DECLARE
    v_createdAsExtension     BOOLEAN;
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
    v_isSuperuser            BOOLEAN;
    v_isRoleCreaterole       BOOLEAN;
    v_nbObject               INTEGER;
    v_roleToDrop             BOOLEAN;
    v_dbList                 TEXT;
    v_granteeRoleList        TEXT;
    v_granteeClassList       TEXT;
    v_granteeFunctionList    TEXT;
    v_tspList                TEXT;
    r_object                 RECORD;
  BEGIN
-- Read some installation characteristics.
    SELECT inst_as_extension, inst_with_emaj_adm, inst_with_emaj_viewer
      INTO STRICT v_createdAsExtension, v_supportEmajAdm, v_supportEmajViewer
      FROM emaj.emaj_install_conf;
-- Perform some checks to verify that the conditions to execute the function are met.
--
-- For extensions, check the current role is superuser.
    SELECT rolsuper, rolcreaterole INTO v_isSuperuser, v_isRoleCreaterole
      FROM pg_catalog.pg_roles
      WHERE rolname = current_user;
    IF v_createdAsExtension THEN
      IF NOT v_isSuperuser THEN
        RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be a superuser';
      END IF;
    ELSE
-- Otherwise, check the current role is the owner of the emaj schema (i.e. the role who installed emaj) or is a superuser.
      PERFORM 1 FROM pg_catalog.pg_roles, pg_catalog.pg_namespace
        WHERE nspowner = pg_roles.oid AND nspname = 'emaj' AND rolname = current_user;
      IF NOT FOUND AND NOT v_isSuperuser THEN
        RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be the owner of the emaj schema or a superuser';
      END IF;
    END IF;
--
-- Check that no E-Maj schema contains any non E-Maj object.
    v_nbObject = 0;
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_schemas() msg
        WHERE msg NOT LIKE 'Error: The E-Maj schema % does not exist anymore.'
      LOOP
-- An E-Maj schema contains objects that do not belong to the extension.
      RAISE WARNING 'emaj_drop_extension - schema consistency checks: %',r_object.msg;
      v_nbObject = v_nbObject + 1;
    END LOOP;
    IF v_nbObject > 0 THEN
      RAISE EXCEPTION 'emaj_drop_extension: There are % unexpected objects in E-Maj schemas. Drop them before reexecuting the uninstall'
                      ' function.', v_nbObject;
    END IF;
--
-- OK, perform the removal actions.
--
-- Disable event triggers that would block the DROP EXTENSION command.
    PERFORM emaj._disable_event_triggers();
--
-- If the emaj_demo_cleanup function exists (created by the emaj_demo.sql script), execute it.
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_demo_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_demo_cleanup();
    END IF;
-- If the emaj_parallel_rollback_test_cleanup function exists (created by the emaj_prepare_parallel_rollback_test.sql script), execute it.
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_parallel_rollback_test_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_parallel_rollback_test_cleanup();
    END IF;
--
-- Drop all created groups, bypassing potential errors, to remove all components not directly linked to the EXTENSION.
    PERFORM emaj.emaj_force_drop_group(group_name) FROM emaj.emaj_group;
--
-- Drop the emaj extension, if it is an EXTENSION.
    DROP EXTENSION IF EXISTS emaj CASCADE;
--
-- Drop the primary schema.
    DROP SCHEMA IF EXISTS emaj CASCADE;
--
-- Drop the event trigger that protects the extension against unattempted drop and its function (they are external to the extension).
    DROP FUNCTION IF EXISTS public._emaj_protection_event_trigger_fnct() CASCADE;
--
-- Determine whether the emaj_adm role can be dropped.
-- If emaj_adm is not supported in this emaj instance, just warn.
    IF NOT v_supportEmajAdm THEN
      RAISE WARNING 'emaj_drop_extension: The emaj_adm role is not supported in this database.';
    ELSIF NOT v_isRoleCreaterole THEN
      RAISE WARNING 'emaj_drop_extension: The current role has not the CREATEROLE rights needed to drop emaj_adm.';
    ELSE
      v_roleToDrop = TRUE;
-- Is emaj_adm also used in other databases of the cluster ?
      v_dbList = NULL;
      SELECT string_agg(DISTINCT datname, ', ' ORDER BY datname)
        INTO v_dbList
        FROM pg_database db
             JOIN pg_catalog.pg_shdepend shd ON (dbid = db.oid)
             JOIN pg_catalog.pg_roles r ON (r.oid = refobjid)
        WHERE rolname = 'emaj_adm'
          AND datname <> current_database();
      IF v_dbList IS NOT NULL THEN
        RAISE WARNING 'emaj_drop_extension: The emaj_adm role is also referenced in some other databases (%)', v_dbList;
        v_roleToDrop = FALSE;
      END IF;
-- Is emaj_adm granted to other roles ?
      v_granteeRoleList = NULL;
      SELECT string_agg(q.rolname, ', ' ORDER BY q.rolname)
        INTO v_granteeRoleList
        FROM pg_catalog.pg_roles q
             JOIN pg_catalog.pg_auth_members m ON (m.member = q.oid)
             JOIN pg_catalog.pg_roles r ON (r.oid = m.roleid)
        WHERE r.rolname = 'emaj_adm';
      IF v_granteeRoleList IS NOT NULL THEN
        RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_adm role.',
              v_granteeRoleList;
        v_roleToDrop = FALSE;
      END IF;
-- Is emaj_adm granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
      v_granteeClassList = NULL;
      SELECT string_agg(nspname || '.' || relname, ', ' ORDER BY nspname, relname)
        INTO v_granteeClassList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
        WHERE array_to_string (relacl,';') LIKE '%emaj_adm=%';
      IF v_granteeClassList IS NOT NULL THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_adm role has some remaining grants on tables, views or sequences (%).',
                      v_granteeClassList;
        v_roleToDrop = FALSE;
      END IF;
-- Is emaj_adm granted to functions (other than just dropped emaj ones) ?
      v_granteeFunctionList = NULL;
      SELECT string_agg(nspname || '.' || proname || '()', ', ' ORDER BY nspname, proname)
        INTO v_granteeFunctionList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_proc ON (pronamespace = pg_namespace.oid)
        WHERE array_to_string (proacl,';') LIKE '%emaj_adm=%';
      IF v_granteeFunctionList IS NOT NULL THEN
        IF length(v_granteeFunctionList) > 200 THEN
          v_granteeClassList = substr(v_granteeFunctionList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_adm role has some remaining grants on functions (%).',
                      v_granteeFunctionList;
        v_roleToDrop = FALSE;
      END IF;
-- If emaj_adm can be dropped, do it.
      IF v_roleToDrop THEN
-- Revoke first the remaining grants set on tablespaces
        SELECT string_agg(spcname, ', ')
          INTO v_tspList
          FROM pg_catalog.pg_tablespace
          WHERE array_to_string (spcacl,';') LIKE '%emaj_adm=%';
        IF v_tspList IS NOT NULL THEN
          EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_adm';
        END IF;
-- ... and drop the role.
        DROP ROLE emaj_viewer;
        RAISE WARNING 'emaj_drop_extension: the emaj_adm role has been dropped.';
      ELSE
        RAISE WARNING 'emaj_drop_extension: For these reasons, emaj_adm has not been dropped by this script.';
      END IF;
    END IF;
--
-- Determine whether the emaj_viewer role can be dropped.
-- If emaj_viewer is not supported in this emaj instance, just warn.
    IF NOT v_supportEmajViewer THEN
      RAISE WARNING 'emaj_drop_extension: The emaj_viewer role is not supported in this database.';
    ELSIF NOT v_isRoleCreaterole THEN
      RAISE WARNING 'emaj_drop_extension: The current role has not the CREATEROLE rights needed to drop emaj_viewer.';
    ELSE
--
-- Is emaj_viewer also used in other databases of the cluster ?
      v_dbList = NULL;
      SELECT string_agg(DISTINCT datname, ', ' ORDER BY datname)
        INTO v_dbList
        FROM pg_database db
             JOIN pg_catalog.pg_shdepend shd ON (dbid = db.oid)
             JOIN pg_catalog.pg_roles r ON (r.oid = refobjid)
        WHERE rolname = 'emaj_viewer'
          AND datname <> current_database();
      IF v_dbList IS NOT NULL THEN
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role is also referenced in some other databases (%)', v_dbList;
        v_roleToDrop = FALSE;
      END IF;
--
-- Is emaj_viewer granted to other roles ?
      v_granteeRoleList = NULL;
      SELECT string_agg(q.rolname, ', ' ORDER BY q.rolname)
        INTO v_granteeRoleList
        FROM pg_catalog.pg_roles q
             JOIN pg_catalog.pg_auth_members m ON (m.member = q.oid)
             JOIN pg_catalog.pg_roles r ON (r.oid = m.roleid)
        WHERE r.rolname = 'emaj_viewer';
      IF v_granteeRoleList IS NOT NULL THEN
        RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_viewer role.',
                      v_granteeRoleList;
        v_roleToDrop = FALSE;
      END IF;
--
-- Is emaj_viewer granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
      v_granteeClassList = NULL;
      SELECT string_agg(nspname || '.' || relname, ', ' ORDER BY nspname, relname)
        INTO v_granteeClassList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
        WHERE array_to_string (relacl,';') LIKE '%emaj_viewer=%';
      IF v_granteeClassList IS NOT NULL THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has some remaining grants on tables, views or sequences (%).',
                      v_granteeClassList;
        v_roleToDrop = FALSE;
      END IF;
--
-- Is emaj_viewer granted to functions (other than just dropped emaj ones) ?
      v_granteeFunctionList = NULL;
      SELECT string_agg(nspname || '.' || proname || '()', ', ' ORDER BY nspname, proname)
        INTO v_granteeFunctionList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_proc ON (pronamespace = pg_namespace.oid)
        WHERE array_to_string (proacl,';') LIKE '%emaj_viewer=%';
      IF v_granteeFunctionList IS NOT NULL THEN
        IF length(v_granteeFunctionList) > 200 THEN
          v_granteeFunctionList = substr(v_granteeFunctionList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has some remaining grants on functions (%).',
                      v_granteeFunctionList;
        v_roleToDrop = FALSE;
      END IF;
--
-- If emaj_viewer can be dropped, do it.
      IF v_roleToDrop THEN
-- Revoke first the remaining grants set on tablespaces
        SELECT string_agg(spcname, ', ')
          INTO v_tspList
          FROM pg_catalog.pg_tablespace
          WHERE array_to_string (spcacl,';') LIKE '%emaj_viewer=%';
        IF v_tspList IS NOT NULL THEN
          EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_viewer';
        END IF;
-- ... and drop the role.
        DROP ROLE emaj_viewer;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has been dropped.';
      ELSE
        RAISE WARNING 'emaj_drop_extension: For these reasons, the emaj_viewer role has not been dropped by this script.';
      END IF;
    END IF;
--
    RETURN;
  END;
$emaj_drop_extension$;
COMMENT ON FUNCTION emaj.emaj_drop_extension() IS
$$Uninstalls the E-Maj components from the current database.$$;

--<end_functions>                                pattern used by the tool that extracts and inserts the functions definition

----------------------------------------------------------------
--                                                            --
--                       Event triggers                       --
--                                                            --
----------------------------------------------------------------

----------------------------------------------------------------
--                                                            --
--                 Rights on emaj components                  --
--                                                            --
----------------------------------------------------------------

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA emaj FROM PUBLIC;

GRANT ALL ON ALL TABLES IN SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL SEQUENCES IN SCHEMA emaj TO emaj_adm;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA emaj TO emaj_adm;

GRANT SELECT ON ALL TABLES IN SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA emaj TO emaj_viewer;
REVOKE SELECT ON TABLE emaj.emaj_param FROM emaj_viewer;

GRANT EXECUTE ON FUNCTION emaj._get_current_seq(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_schema(p_schema TEXT, p_exceptionIfMissing BOOLEAN, p_checkNotEmaj BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_assigned_group_table(p_schema TEXT, p_table TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_assigned_group_sequence(p_schema TEXT, p_sequence TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_logging_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_idle_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;

----------------------------------------------------------------
--                                                            --
--                    Complete the upgrade                    --
--                                                            --
----------------------------------------------------------------

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
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.7.1 completed');

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
