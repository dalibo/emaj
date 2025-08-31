--
-- E-Maj: migration from 4.6.0 to 4.7.0
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
-- The current emaj version must be '4.6.0'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.6.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.6.0',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 12.
    IF current_setting('server_version_num')::int < 120000 THEN
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj 4.7.0', 'Upgrade from 4.6.0 started');

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
-- Process the emaj_relation table
--

-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_schema_old (LIKE emaj.emaj_schema);

INSERT INTO emaj_schema_old SELECT * FROM emaj.emaj_schema;

-- drop the old table
DROP TABLE emaj.emaj_schema CASCADE;

-- create the new table, with its comment and constraints (except foreign key)...
CREATE TABLE emaj.emaj_schema (
  sch_name                     TEXT        NOT NULL,       -- schema name
  sch_time_id                  BIGINT,                     -- insertion time (NULL for emaj)
  PRIMARY KEY (sch_name),
  FOREIGN KEY (sch_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_schema IS
$$Contains the E-Maj related schemas (emaj and all schemas hosting log tables, sequences and functions).$$;

-- Populate the new table.
-- The new rel_gen_expr_cols column content is built using the postgres catalog content.
-- We assume that the "GENERATED ALWAYS AS expression" columns list was the same in the past that today.
--   So populate the old emaj_relation rows (i.e. those for which upper_inf(rel_time_range) is FALSE) with the current
--   table structure view.

INSERT INTO emaj.emaj_schema (
              sch_name, sch_time_id
              )
  SELECT      s.sch_name, time_id
    FROM emaj_schema_old s
         LEFT OUTER JOIN emaj.emaj_time_stamp ON (time_tx_timestamp = s.sch_datetime);

-- Create indexes.
--   there is no secondary indexes on this table

-- recreate the foreign keys that point on this table
ALTER TABLE emaj.emaj_relation ADD
  FOREIGN KEY (rel_log_schema) REFERENCES emaj.emaj_schema (sch_name);

-- and finaly drop the temporary table
DROP TABLE emaj_schema_old;

--
-- Process the emaj_relation table
--

-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_relation_old (LIKE emaj.emaj_relation);

INSERT INTO emaj_relation_old SELECT * FROM emaj.emaj_relation;

-- drop the old table
DROP TABLE emaj.emaj_relation CASCADE;

-- create the new table, with its comment and constraints (except foreign key)...
CREATE TABLE emaj.emaj_relation (
  rel_schema                   TEXT        NOT NULL,       -- schema name containing the relation
  rel_tblseq                   TEXT        NOT NULL,       -- application table or sequence name
  rel_time_range               INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
  rel_group                    TEXT        NOT NULL,       -- name of the group that owns the relation
  rel_kind                     TEXT,                       -- similar to the relkind column of pg_class table
                                                           --   ('r' = table, 'S' = sequence)
-- Next columns are specific for tables and remain NULL for sequences.
  rel_priority                 INTEGER,                    -- priority level of processing inside the group
  rel_log_schema               TEXT,                       -- schema for the log table, functions and sequence
  rel_log_table                TEXT,                       -- name of the log table associated
  rel_log_dat_tsp              TEXT,                       -- tablespace for the log table
  rel_log_index                TEXT,                       -- name of the index of the log table
  rel_log_idx_tsp              TEXT,                       -- tablespace for the log index
  rel_log_sequence             TEXT,                       -- name of the log sequence
  rel_log_function             TEXT,                       -- name of the function associated to the log trigger
                                                           -- created on the application table
  rel_ignored_triggers         TEXT[],                     -- names array of application trigger to ignore at rollback time
  rel_pk_cols                  TEXT[],                     -- PK columns names array
  rel_gen_expr_cols            TEXT[],                     -- generated as expression columns names array
  rel_emaj_verb_attnum         SMALLINT,                   -- column number (attnum) of the log table's emaj_verb column in the
                                                           --   pg_attribute table
  rel_has_always_ident_col     BOOLEAN,                    -- are there any "generated always as identity" column ?
  rel_sql_rlbk_columns         TEXT,                       -- piece of sql used to rollback: list of the columns (excluding GENERATED
                                                           --   ALWAYS AS expression columns)
  rel_sql_gen_ins_col          TEXT,                       -- piece of sql used for SQL generation: list of columns to insert
  rel_sql_gen_ins_val          TEXT,                       -- piece of sql used for SQL generation: list of column values to insert
  rel_sql_gen_upd_set          TEXT,                       -- piece of sql used for SQL generation: set clause for updates
  rel_sql_gen_pk_conditions    TEXT,                       -- piece of sql used for SQL generation: equality conditions on the pk columns
  rel_log_seq_last_value       BIGINT,                     -- last value of the log sequence when the table is removed from the group
                                                           --   (NULL otherwise)
  PRIMARY KEY (rel_schema, rel_tblseq, rel_time_range),
  FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name),
  FOREIGN KEY (rel_log_schema) REFERENCES emaj.emaj_schema (sch_name),
  EXCLUDE USING gist (rel_schema WITH =, rel_tblseq WITH =, rel_time_range WITH &&)
  );
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;

-- Populate the new table.
-- The new rel_gen_expr_cols column content is built using the postgres catalog content.
-- We assume that the "GENERATED ALWAYS AS expression" columns list was the same in the past that today.
--   So populate the old emaj_relation rows (i.e. those for which upper_inf(rel_time_range) is FALSE) with the current
--   table structure view.

WITH gen_cols AS (
  SELECT DISTINCT rel_schema, rel_tblseq,
         array_agg(attname ORDER BY attnum) AS rel_gen_expr_cols
    FROM emaj_relation_old
         JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
         JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
    WHERE rel_kind = 'r'
      AND attgenerated <> ''
      AND attnum > 0
      AND NOT attisdropped
    GROUP BY 1,2
  )
  INSERT INTO emaj.emaj_relation (
                rel_schema, rel_tblseq, rel_time_range,
                rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
                rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_ignored_triggers,
                rel_pk_cols, rel_gen_expr_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns, rel_sql_gen_ins_col,
                rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions, rel_log_seq_last_value
                )
    SELECT      r.rel_schema, r.rel_tblseq, rel_time_range,
                rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
                rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_ignored_triggers,
                rel_pk_cols, g.rel_gen_expr_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns, rel_sql_gen_ins_col,
                rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions, rel_log_seq_last_value
      FROM emaj_relation_old r
           LEFT OUTER JOIN gen_cols g ON (g.rel_schema = r.rel_schema AND g.rel_tblseq = r.rel_tblseq);

-- Create indexes.
-- Index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration.
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- Index on emaj_relation used to speedup _verify_all_schemas() with large E-Maj configuration.
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- recreate the foreign keys that point on this table
--   there is no fkey for this table

-- and finaly drop the temporary table
DROP TABLE emaj_relation_old;
--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--
SELECT pg_catalog.pg_extension_config_dump('emaj_relation','');
SELECT pg_catalog.pg_extension_config_dump('emaj_schema','');

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

CREATE TYPE emaj.emaj_log_stat_group_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_table                   TEXT,                       -- table name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the lower bound mark
  stat_first_time_id           BIGINT,                     -- internal time id of the lower bound mark
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the upper bound mark
  stat_last_time_id            BIGINT,                     -- internal time id of the upper bound mark
  stat_rows                    BIGINT                      -- estimated number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_log_stat_group_type IS
$$Represents the structure of rows returned by the emaj_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_detailed_log_stat_group_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_table                   TEXT,                       -- table name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the lower bound mark
  stat_first_time_id           BIGINT,                     -- internal time id of the lower bound mark
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the upper bound mark
  stat_last_time_id            BIGINT,                     -- internal time id of the upper bound mark
  stat_role                    TEXT,                       -- user having generated update events
  stat_verb                    TEXT,                       -- type of SQL statement (INSERT/UPDATE/DELETE/TRUNCATE)
  stat_rows                    BIGINT                      -- real number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_detailed_log_stat_group_type IS
$$Represents the structure of rows returned by the emaj_detailed_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_sequence_stat_group_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_sequence                TEXT,                       -- sequence name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the lower bound mark
  stat_first_time_id           BIGINT,                     -- internal time id of the lower bound mark
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the upper bound mark
  stat_last_time_id            BIGINT,                     -- internal time id of the upper bound mark
  stat_increments              BIGINT,                     -- number of sequence increments for this sequence during the time interval
  stat_has_structure_changed   BOOLEAN                     -- TRUE if any property other than last_value has changed
  );
COMMENT ON TYPE emaj.emaj_sequence_stat_group_type IS
$$Represents the structure of rows returned by the emaj_sequence_stat_group() function.$$;

CREATE TYPE emaj.emaj_log_stat_table_type AS (
  stat_group                   TEXT,                       -- tables group name owning the marks and the table
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time slice
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the lower bound mark
  stat_first_time_id           BIGINT,                     -- internal time id of the lower bound mark
  stat_is_log_start            BOOLEAN,                    -- indicator whether the lower bound mark is the first mark of the log session
                                                           --   for the table
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the upper bound mark
  stat_last_time_id            BIGINT,                     -- internal time id of the upper bound mark
  stat_is_log_stop             BOOLEAN,                    -- indicator whether the upper bound mark is the last mark of the log session
                                                           --   for the table
  stat_changes                 BIGINT,                     -- number of elementary table changes recorded during the time slice
  stat_rollbacks               INT                         -- number of E-maj rollbacks executed in the time slice
  );
COMMENT ON TYPE emaj.emaj_log_stat_table_type IS
$$Represents the structure of rows returned by the emaj_log_stat_table() function.$$;

CREATE TYPE emaj.emaj_log_stat_sequence_type AS (
  stat_group                   TEXT,                       -- tables group name owning the marks and the sequence
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time slice
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the lower bound mark
  stat_first_time_id           BIGINT,                     -- internal time id of the lower bound mark
  stat_is_log_start            BOOLEAN,                    -- indicator whether the lower bound mark is the first mark of the log session
                                                           --   for the table
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the upper bound mark
  stat_last_time_id            BIGINT,                     -- internal time id of the upper bound mark
  stat_is_log_stop             BOOLEAN,                    -- indicator whether the upper bound mark is the last mark of the log session
                                                           --   for the sequence
  stat_increments              BIGINT,                     -- number of sequence increments for this sequence during the time slice
  stat_has_structure_changed   BOOLEAN,                    -- TRUE if any property other than last_value has changed
  stat_rollbacks               INT                         -- number of E-maj rollbacks executed in the time slice
  );
COMMENT ON TYPE emaj.emaj_log_stat_sequence_type IS
$$Represents the structure of rows returned by the emaj_log_stat_sequence() function.$$;

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
DROP FUNCTION IF EXISTS emaj._create_log_schemas(P_FUNCTION TEXT);
DROP FUNCTION IF EXISTS emaj._build_sql_tbl(P_FULLTABLENAME TEXT,OUT P_PKCOLS TEXT[],OUT P_RLBKCOLLIST TEXT,OUT P_GENCOLLIST TEXT,OUT P_GENVALLIST TEXT,OUT P_GENSETLIST TEXT,OUT P_GENPKCONDITIONS TEXT,OUT P_NBGENALWAYSIDENTCOL INT);
DROP FUNCTION IF EXISTS emaj.emaj_log_stat_group(P_GROUPNAME TEXT,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_log_stat_groups(P_GROUPNAMES TEXT[],P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj._log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_detailed_log_stat_group(P_GROUPNAME TEXT,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_detailed_log_stat_groups(P_GROUPNAMES TEXT[],P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj._detailed_log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_sequence_stat_group(P_GROUPNAME TEXT,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_sequence_stat_groups(P_GROUPNAMES TEXT[],P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj._sequence_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._pg_version_num()
RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS
$$
-- This function returns as an integer the current postgresql version.
SELECT pg_catalog.current_setting('server_version_num')::INT;
$$;

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
--          -7 the dblink user/password from emaj_param has not emaj_adm rights
--         name of the schema that holds the dblink extension (used later to schema qualify all calls to dblink functions)
-- The function is defined as SECURITY DEFINER because reading the unix_socket_directories GUC needs to be at least a member
--   of pg_read_all_settings.
  DECLARE
    v_nbCnx                  INT;
    v_userPassword           TEXT;
    v_connectString          TEXT;
    v_stmt                   TEXT;
    v_isEmajadm              BOOLEAN;
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
    ELSIF NOT pg_catalog.has_function_privilege(p_callerRole, quote_ident(p_schema) || '.dblink_connect_u(text, text)', 'execute') THEN
      p_status = -3;                      -- current role has not the execute rights on dblink functions
    ELSIF (p_cnxName LIKE 'rlbk#%' OR p_cnxName = 'emaj_verify_all') AND
          pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
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
                              WHEN pg_catalog.current_setting('listen_addresses') = ''
                                THEN coalesce(substring(pg_catalog.current_setting('unix_socket_directories') from '(.*?)\s*,'),
                                              pg_catalog.current_setting('unix_socket_directories'))
                              ELSE 'localhost'
                            END
                         || ' port=' || pg_catalog.current_setting('port')
                         || ' dbname=' || pg_catalog.current_database()
                         || ' ' || v_userPassword;
-- Try to connect.
          BEGIN
            EXECUTE format('SELECT %I.dblink_connect_u(%L ,%L)',
                           p_schema, p_cnxName, v_connectString);
            p_status = 1;                   -- the connection is successful
-- For E-Maj rollback first connections and test connections, check the role is member of emaj_adm.
            IF (p_cnxName LIKE 'rlbk#1' OR p_cnxName = 'emaj_verify_all') THEN
              v_stmt = 'SELECT pg_catalog.pg_has_role(''emaj_adm'', ''MEMBER'') AS is_emaj_adm';
              EXECUTE format('SELECT is_emaj_adm FROM %I.dblink(%L, %L) AS (is_emaj_adm BOOLEAN)',
                             p_schema, p_cnxName, v_stmt)
                INTO v_isEmajadm;
              IF NOT v_isEmajadm THEN
                p_status = -7;              -- the dblink user/password from emaj_param has not emaj_adm rights
                EXECUTE format('SELECT %I.dblink_disconnect(%L)',
                               p_schema, p_cnxName);
              END IF;
            END IF;
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

CREATE OR REPLACE FUNCTION emaj._get_default_tablespace()
RETURNS TEXT LANGUAGE plpgsql AS
$_get_default_tablespace$
-- This function returns the name of a default tablespace to use when moving an existing log table or index.
-- Output: tablespace name
-- The function is called at alter group time.
  DECLARE
    v_tablespace             TEXT;
  BEGIN
-- Get the default tablespace set for the current session or set for the entire instance by GUC.
    SELECT setting INTO v_tablespace
      FROM pg_catalog.pg_settings
      WHERE name = 'default_tablespace';
    IF v_tablespace = '' THEN
-- Get the default tablespace for the current database (pg_default if no specific tablespace name has been set for the database).
      SELECT spcname INTO v_tablespace
        FROM pg_catalog.pg_database
             JOIN pg_catalog.pg_tablespace ON (pg_tablespace.oid = dattablespace)
        WHERE datname = current_database();
    END IF;
--
    RETURN v_tablespace;
  END;
$_get_default_tablespace$;

CREATE OR REPLACE FUNCTION emaj._check_schema(p_schema TEXT, p_exceptionIfMissing BOOLEAN, p_checkNotEmaj BOOLEAN)
RETURNS BOOLEAN LANGUAGE plpgsql AS
$_check_schema$
-- This function checks if a schema exists. If not, it either raises an exception or just a warning.
-- If requested, it also checks that the schema is not an emaj schema. If the check fails, it raises an exception.
-- Input: schema, boolean indicating whether a missing schema raises an exception,
--        boolean indicating whether the schema must not be an emaj schema.
-- Output : boolean indicating whether the schema exists
  DECLARE
    v_schemaExists           BOOLEAN = TRUE;
  BEGIN
-- Check that the schema exists.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      IF p_exceptionIfMissing THEN
        RAISE EXCEPTION '_check_schema: The schema "%" does not exist!', p_schema;
      ELSE
        RAISE WARNING '_check_schema: The schema "%" does not exist.', p_schema;
        v_schemaExists = FALSE;
      END IF;
    END IF;
-- Check that the schema is not an emaj schema.
    IF p_checkNotEmaj THEN
      IF EXISTS
           (SELECT 0
              FROM emaj.emaj_schema
              WHERE sch_name = p_schema
           ) THEN
        RAISE EXCEPTION '_check_schema: The schema "%" is an E-Maj schema.', p_schema;
      END IF;
    END IF;
--
    RETURN v_schemaExists;
  END;
$_check_schema$;

CREATE OR REPLACE FUNCTION emaj._check_tables_for_rollbackable_group(p_schema TEXT, p_tables TEXT[], p_arrayFromRegex BOOLEAN,
                                                                     p_callingFunction TEXT)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_tables_for_rollbackable_group$
-- This function filters or verifies that tables are compatible with ROLLBACKABLE groups.
-- (they must have a PK and no be UNLOGGED or WITH OIDS)
-- Input: schema, array of tables names, boolean indicating whether the tables list is built from regexp, calling function name
-- Output: updated tables name array
  DECLARE
    v_list                   TEXT;
    v_array                  TEXT[];
  BEGIN
-- Check or discard tables without primary key.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class t
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema AND t.relname = ANY(p_tables)
        AND relkind = 'r'
        AND NOT EXISTS
              (SELECT 0
                 FROM pg_catalog.pg_class c
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = c.relnamespace)
                      JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = c.oid)
                           WHERE contype = 'p'
                             AND nspname = p_schema
                             AND c.relname = t.relname
              );
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '%: In schema %, some tables (%) have no PRIMARY KEY.', p_callingFunction, quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '%: Some tables without PRIMARY KEY (%) are not selected.', p_callingFunction, v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- Check or discard UNLOGGED tables.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'r'
        AND relpersistence = 'u';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '%: In schema %, some tables (%) are UNLOGGED tables.', p_callingFunction, quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '%: Some UNLOGGED tables (%) are not selected.', p_callingFunction, v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
--
    RETURN p_tables;
  END;
$_check_tables_for_rollbackable_group$;

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
-- Process empty filters as NULL.
    SELECT CASE WHEN p_includeFilter = '' THEN NULL ELSE p_includeFilter END,
           CASE WHEN p_excludeFilter = '' THEN NULL ELSE p_excludeFilter END
      INTO p_includeFilter, p_excludeFilter;
-- Build and return the list of relations names satisfying the pattern.
    RETURN array_agg(rel_tblseq)
      FROM (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq ~ p_includeFilter
            AND (p_excludeFilter IS NULL OR rel_tblseq !~ p_excludeFilter)
            AND rel_kind = p_relkind
            AND upper_inf(rel_time_range)
          ORDER BY rel_tblseq
           ) AS t;
  END;
$_build_tblseqs_array_from_regexp$;

CREATE OR REPLACE FUNCTION emaj._check_tblseqs_array(p_schema TEXT, p_tblseqs TEXT[], p_relkind TEXT, p_exceptionIfMissing BOOLEAN)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_tblseqs_array$
-- The function checks a names array of tables or sequences.
-- Depending on the p_exceptionIfMissing parameter, it warns or raises an error if the schema or any table or sequence doesn't exist.
-- (WARNING only is used by removal functions so that it is possible to remove a dropped or renamed relation from its group)
-- It verifies that the schema and the tables or sequences belong to a tables group (not necessary the same one).
-- It returns the names array, duplicates and empty names being removed.
-- Inputs: schema,
--         tables or sequences names array,
--         relation kind ('r' or 'S'),
--         boolean indicating whether a missing schema or relation must raise an exception or just a warning.
-- Outputs: tables or sequences names array.
  DECLARE
    v_relationKind           TEXT;
    v_schemaExists           BOOLEAN;
    v_list                   TEXT;
    v_tblseqs                TEXT[];
  BEGIN
-- Setup constant.
    IF p_relkind = 'r' THEN
      v_relationKind = 'tables';
    ELSE
      v_relationKind = 'sequences';
    END IF;
-- Check that the schema exists.
    v_schemaExists = emaj._check_schema(p_schema, p_exceptionIfMissing, FALSE);
-- Clean up the relation names array: remove duplicates values, NULL and empty strings.
    SELECT array_agg(DISTINCT tblseq) INTO v_tblseqs
      FROM unnest(p_tblseqs) AS tblseq
      WHERE tblseq IS NOT NULL AND tblseq <> '';
-- If the schema exists, check that all relations exist.
    IF v_schemaExists THEN
      WITH tblseqs AS (
        SELECT unnest(v_tblseqs) AS tblseq
        )
      SELECT string_agg(quote_ident(tblseq), ', ') INTO v_list
        FROM
          (SELECT tblseq
             FROM tblseqs
             WHERE NOT EXISTS
                     (SELECT 0
                        FROM pg_catalog.pg_class
                             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        WHERE nspname = p_schema
                          AND relname = tblseq
                          AND relkind = p_relkind
                     )
          ) AS t;
      IF v_list IS NOT NULL THEN
        IF p_exceptionIfMissing THEN
          RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not exist!', p_schema, v_relationKind, v_list;
        ELSE
          RAISE WARNING '_check_tblseqs_array: In schema "%", some % (%) do not exist.', p_schema, v_relationKind, v_list;
        END IF;
      END IF;
    END IF;
-- Check that the relations currently belong to a tables group (not necessarily the same for all relations).
    WITH all_supplied_tblseqs AS (
      SELECT unnest(v_tblseqs) AS tblseq
      ),
         tblseqs_in_groups AS (
      SELECT rel_tblseq
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(v_tblseqs)
          AND rel_kind = p_relkind
          AND upper_inf(rel_time_range)
      )
    SELECT string_agg(quote_ident(tblseq), ', ') INTO v_list
      FROM
        (  SELECT tblseq
             FROM all_supplied_tblseqs
         EXCEPT
           SELECT rel_tblseq FROM tblseqs_in_groups
        ) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not currently belong to any tables group.',
                      p_schema, v_relationKind, v_list;
    END IF;
--
    RETURN v_tblseqs;
  END;
$_check_tblseqs_array$;

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
      INSERT INTO emaj.emaj_schema (sch_name, sch_time_id)
        VALUES (r_schema.log_schema, p_timeId);
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
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
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

CREATE OR REPLACE FUNCTION emaj._build_sql_tbl(p_fullTableName TEXT, OUT p_pkCols TEXT[], OUT p_genExprCols TEXT[],
                                               OUT p_rlbkColList TEXT, OUT p_genColList TEXT, OUT p_genValList TEXT,
                                               OUT p_genSetList TEXT, OUT p_genPkConditions TEXT, OUT p_nbGenAlwaysIdentCol INT)
LANGUAGE plpgsql AS
$_build_sql_tbl$
-- This function builds, for one application table:
--   - the PK columns names array
--   - all pieces of SQL that will be recorded into the emaj_relation table.
-- They will later be used at rollback or SQL script generation time.
-- All SQL pieces are left NULL or empty if the table has no pkey, neither rollback nor sql script generation operations being possible
--   in this case.
-- The Insert columns list remains empty if it is not needed to have a specific list (i.e. when the application table does not contain
--   any generated column).
-- Input: the full application table name
-- Output: PK columns names array, 5 pieces of SQL, and the number of columns declared GENERATED ALWAYS AS IDENTITY
  DECLARE
    v_unquotedType           CONSTANT TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                                     'int2','int4','int8','serial','bigserial',
                                                     'real','double precision','float','float4','float8','oid'];
    r_col                    RECORD;
  BEGIN
-- Build the pkey columns array and the "equality on the primary key" conditions for the UPDATE and DELETE statements of the
--   sql generation function.
    SELECT array_agg(attname),
           string_agg(
             CASE WHEN format_type = ANY(v_unquotedType) THEN
               quote_ident(replace(attname,'''','''''')) || ' = '' || o.' || quote_ident(attname) || ' || '''
                  ELSE
               quote_ident(replace(attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(attname) || ') || '''
             END, ' AND ')
      INTO p_pkCols, p_genPkConditions
      FROM
        (SELECT attname, regexp_replace(format_type(atttypid,atttypmod),E'\\(.*$','') AS format_type
           FROM pg_catalog.pg_attribute
                JOIN  pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
           WHERE attnum = ANY (indkey)
             AND indrelid = p_fullTableName::regclass
             AND indisprimary
             AND attnum > 0
             AND attisdropped = FALSE
           ORDER BY attnum
        ) AS t;
-- Build the generated columns array.
    SELECT array_agg(attname ORDER BY attnum)
      INTO p_genExprCols
      FROM pg_catalog.pg_attribute
      WHERE attrelid = p_fullTableName::regclass
        AND attnum > 0
        AND attisdropped = FALSE
        AND attgenerated <> '';
-- Retrieve from pg_attribute simple columns list and indicators.
-- If the table has no pkey, keep all the sql pieces to NULL (rollback or sql script generation operations being impossible).
    IF p_pkCols IS NOT NULL THEN
      SELECT string_agg('tbl.' || quote_ident(attname), ',') FILTER (WHERE attgenerated = ''),
--               the columns list for rollback, excluding the GENERATED ALWAYS AS (expression) columns
             string_agg(quote_ident(attname), ', ') FILTER (WHERE attgenerated = ''),
--               the INSERT columns list for sql generation, excluding the GENERATED ALWAYS AS (expression) columns
             count(*) FILTER (WHERE attidentity = 'a')
--               the number of GENERATED ALWAYS AS IDENTITY columns
        INTO p_rlbkColList, p_genColList, p_nbGenAlwaysIdentCol
        FROM (
          SELECT attname, attidentity, attgenerated
            FROM pg_catalog.pg_attribute
            WHERE attrelid = p_fullTableName::regclass
              AND attnum > 0 AND NOT attisdropped
          ORDER BY attnum) AS t;
      IF p_genExprCols IS NULL THEN
-- If the table doesn't contain any generated as expression columns, there is no need for the columns list in the INSERT clause.
        p_genColList = '';
      END IF;
-- Retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements.
-- The logic is too complex to be build with aggregate functions. So loop on all columns.
      p_genValList = '';
      p_genSetList = '';
      FOR r_col IN EXECUTE format(
        ' SELECT attname, format_type(atttypid,atttypmod) AS format_type, attidentity, attgenerated'
        ' FROM pg_catalog.pg_attribute'
        ' WHERE attrelid = %s::regclass'
        '   AND attnum > 0 AND NOT attisdropped'
        ' ORDER BY attnum',
        quote_literal(p_fullTableName))
      LOOP
-- Test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric
-- data types)
        IF regexp_replace(r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- Literal for this column can remain as is.
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            p_genValList = p_genValList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::TEXT,''NULL'') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            p_genSetList = p_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.'
                                        || quote_ident(r_col.attname) || ' ::TEXT,''NULL'') || '', ';
          END IF;
        ELSE
-- Literal for this column must be quoted.
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            p_genValList = p_genValList || ''' || quote_nullable(o.' || quote_ident(r_col.attname) || ') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            p_genSetList = p_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_nullable(n.'
                                        || quote_ident(r_col.attname) || ') || '', ';
          END IF;
        END IF;
      END LOOP;
-- Suppress the final separators.
      p_genValList = substring(p_genValList FROM 1 FOR char_length(p_genValList) - 2);
      p_genSetList = substring(p_genSetList FROM 1 FOR char_length(p_genSetList) - 2);
    END IF;
--
    RETURN;
  END;
$_build_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._move_tbl(p_schema TEXT, p_table TEXT, p_oldGroup TEXT, p_oldGroupIsLogging BOOLEAN, p_newGroup TEXT,
                                          p_newGroupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- Grant appropriate rights to both emaj roles.
    EXECUTE format('ALTER TABLE %I.%I OWNER TO emaj_adm',
                   v_logSchema, v_currentLogTable);
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO emaj_viewer',
                   v_logSchema, v_currentLogTable);
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
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
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
                     '       JOIN pg_catalog.pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)'
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

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence timerange.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess.
  BEGIN
-- Delete rows from emaj_sequence, but only when dealing with the last timerange of the sequence.
    IF NOT EXISTS
        (SELECT 0
           FROM emaj.emaj_relation
           WHERE rel_schema = r_rel.rel_schema
             AND rel_tblseq = r_rel.rel_tblseq
             AND rel_time_range <> r_rel.rel_time_range
        ) THEN
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq;
    END IF;
-- Keep a trace of the sequence group ownership history and finaly delete the sequence timerange from the emaj_relation table.
    WITH deleted AS
      (DELETE FROM emaj.emaj_relation
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
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._get_current_seq(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql AS
$_get_current_seq$
-- This function reads the current characteristics of a sequence and returns it in an emaj_sequence format.
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

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, p_minGlobalSeq BIGINT, p_maxGlobalSeq BIGINT, p_nbSession INT,
                                          p_isLoggedRlbk BOOLEAN, p_isReplRoleReplica BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence.
-- The function is called by emaj._rlbk_session_exec().
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence (non inclusive) lower and (inclusive) upper limits covering the rollback time frame
--        number of sessions
--        a boolean indicating whether the rollback is logged
--        a boolean indicating whether the rollback is to be performed with a session_replication_role set to replica
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_pkColsList             TEXT;
    v_pkCondition            TEXT;
    v_nbPk                   BIGINT;
  BEGIN
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName,
              'All log rows where emaj_gid > ' || p_minGlobalSeq || ' and <= ' || p_maxGlobalSeq
              || CASE WHEN p_isReplRoleReplica THEN ', in session_replication_role = replica mode' ELSE '' END);
-- Build pieces of code
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
    SELECT string_agg(quote_ident(attname), ','),
           string_agg('tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname), ' AND ')
      INTO v_pkColsList, v_pkCondition
      FROM unnest(r_rel.rel_pk_cols) AS attname;
-- Set the session_replication_role if needed.
    IF p_isReplRoleReplica THEN
      SET session_replication_role = 'replica';
    END IF;
-- Create the temporary table containing all primary key values with their earliest emaj_gid.
    IF p_nbSession = 1 THEN
      v_tableType = 'TEMP';
      v_tmpTable = 'emaj_tmp_' || pg_catalog.pg_backend_pid();
    ELSE
-- With multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
-- but it may be an UNLOGGED table.
      v_tableType = 'UNLOGGED';
      v_tmpTable = 'emaj.emaj_tmp_' || pg_catalog.pg_backend_pid();
    END IF;
    EXECUTE format('CREATE %s TABLE %s AS '
                   '  SELECT %s, min(emaj_gid) as emaj_gid FROM %s'
                   '    WHERE emaj_gid > %s AND emaj_gid <= %s'
                   '    GROUP BY %s',
                   v_tableType, v_tmpTable, v_pkColsList, v_logTableName,
                   p_minGlobalSeq, p_maxGlobalSeq, v_pkColsList);
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- Delete all rows from the application table corresponding to each touched primary key.
-- This deletes rows inserted or updated during the rolled back period.
    EXECUTE format('DELETE FROM ONLY %s tbl USING %s keys WHERE %s',
                   v_fullTableName, v_tmpTable, v_pkCondition);
-- For logged rollbacks, if the number of pkey to process is greater than 1.000, ANALYZE the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement.
    IF p_isLoggedRlbk AND v_nbPk > 1000 THEN
      EXECUTE format('ANALYZE %s',
                     v_logTableName);
    END IF;
-- Insert into the application table rows that were deleted or updated during the rolled back period.
    EXECUTE format('INSERT INTO %s (%s) %s'
                   '  SELECT %s FROM %s tbl, %s keys '
                   '    WHERE %s AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
                   '      AND tbl.emaj_gid > %s AND tbl.emaj_gid <= %s',
                   v_fullTableName, replace(r_rel.rel_sql_rlbk_columns, 'tbl.',''),
                   CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
                   r_rel.rel_sql_rlbk_columns, v_logTableName, v_tmpTable,
                   v_pkCondition,
                   p_minGlobalSeq, p_maxGlobalSeq);
-- Drop the now useless temporary table.
    EXECUTE format('DROP TABLE %s',
                   v_tmpTable);
-- Reset the session_replication_role if changed at the beginning of the function.
    IF p_isReplRoleReplica THEN
      RESET session_replication_role;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
--
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

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
    r_markSeq                emaj.emaj_sequence%ROWTYPE;
    r_currSeq                emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Read sequence's characteristics at mark time.
    SELECT *
      INTO r_markSeq
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_timeId;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".',
        p_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END IF;
-- Read the current sequence's characteristics.
    r_currSeq = emaj._get_current_seq(r_rel.rel_schema, r_rel.rel_tblseq, 0);
-- Build the ALTER SEQUENCE statement, depending on the differences between the current sequence state and its characteristics
-- at the requested mark time.
    SELECT emaj._build_alter_seq(r_currSeq, r_markSeq) INTO v_stmt;
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

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_gen_sql_tbl$
-- This function generates elementary SQL statements representing all updates performed on a table between 2 marks
-- or beetween a mark and the current state.
-- These statements are stored into a temporary table created by the _gen_sql_groups() calling function.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        the global sequence value at requested start and end marks
-- Output: number of generated SQL statements
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_rqInsert               TEXT;
    v_rqUpdate               TEXT;
    v_rqDelete               TEXT;
    v_rqTruncate             TEXT;
    v_conditions             TEXT;
    v_lastEmajGidRel         BIGINT;
    v_nbSQL                  BIGINT;
  BEGIN
-- Build schema specified table name and log table name.
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- Prepare sql skeletons for each statement type, using the pieces of sql recorded in the emaj_relation row at table assignment time.
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''')
              || CASE WHEN r_rel.rel_sql_gen_ins_col <> '' THEN ' (' || r_rel.rel_sql_gen_ins_col || ')' ELSE '' END
              || CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END
              || ' VALUES (' || r_rel.rel_sql_gen_ins_val || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''')
              || ' SET ' || r_rel.rel_sql_gen_upd_set || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''')
              || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqTruncate = '''TRUNCATE ONLY ' || replace(v_fullTableName,'''','''''') || ' CASCADE;''';
-- Build the restriction conditions on emaj_gid, depending on supplied marks range and the relation time range upper bound.
    v_conditions = 'o.emaj_gid > ' || p_firstEmajGid;
--   Get the EmajGid of the relation time range upper bound, if any.
    IF NOT upper_inf(r_rel.rel_time_range) THEN
      SELECT time_last_emaj_gid INTO v_lastEmajGidRel
        FROM emaj.emaj_time_stamp
        WHERE time_id = upper(r_rel.rel_time_range);
    END IF;
--   If the relation time range upper bound is before the requested end mark, restrict the EmajGid upper limit.
    IF v_lastEmajGidRel IS NOT NULL AND
       (p_lastEmajGid IS NULL OR (p_lastEmajGid IS NOT NULL AND v_lastEmajGidRel < p_lastEmajGid)) THEN
      p_lastEmajGid = v_lastEmajGidRel;
    END IF;
--   Complete the restriction conditions.
    IF p_lastEmajGid IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || p_lastEmajGid;
    END IF;
-- Now scan the log table to process all statement types at once.
    EXECUTE format('INSERT INTO emaj_temp_script '
                   'SELECT o.emaj_gid, 0, o.emaj_txid,'
                   '  CASE'
                   '    WHEN o.emaj_verb = ''INS'' THEN %s'
                   '    WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN %s'
                   '    WHEN o.emaj_verb = ''DEL'' THEN %s'
                   '    WHEN o.emaj_verb = ''TRU'' THEN %s'
                   '  END'
                   '  FROM %s o'
                   '       LEFT OUTER JOIN %s n ON n.emaj_gid = o.emaj_gid'
                   '                          AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'')'
                   '  WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
                   '    AND NOT (o.emaj_verb = ''TRU'' AND o.emaj_tuple <> '''')'
                   '    AND %s',
                   v_rqInsert, v_rqUpdate, v_rqDelete, v_rqTruncate, v_logTableName, v_logTableName, v_conditions);
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
--
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

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
    SELECT pg_catalog.pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
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
    SELECT pg_catalog.pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
    IF v_lastValue IS NULL THEN
-- The is_called attribute seems to be false, so reach the sequence itself.
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value -'
                     '         (SELECT seqincrement'
                     '            FROM pg_catalog.pg_sequence s'
                     '                 JOIN pg_catalog.pg_class c ON (c.oid = s.seqrelid)'
                     '                 JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)'
                     '            WHERE nspname = %L AND relname = %L'
                     '         ) END as last_value FROM %I.%I',
                     p_schema, p_sequence, p_schema, p_sequence)
        INTO STRICT v_lastValue;
    END IF;
    RETURN v_lastValue;
  END;
$_get_app_sequence_last_value$;

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
                  string_agg(attname, ',' ORDER BY attnum) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_index ON (indrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_index.indrelid)
             WHERE rel_group = ANY (p_groups)
               AND rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND attnum = ANY (indkey)
               AND indisprimary
               AND attnum > 0
               AND NOT attisdropped
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
        ORDER BY 1,2,3
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
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (11): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have the 6 required technical columns. It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log table "' ||
             rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
             string_agg(attname,', ') || ').' AS msg
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

CREATE OR REPLACE FUNCTION emaj._check_fk_groups(p_groupNames TEXT[])
RETURNS VOID LANGUAGE plpgsql AS
$_check_fk_groups$
-- This function checks foreign key constraints for tables of a groups array.
-- Tables from audit_only groups are ignored in this check because they will never be rolled back.
-- Input: group names array
  DECLARE
    v_isEmajExtension        BOOLEAN;
    r_fk                     RECORD;
  BEGIN
-- Issue a warning if a table of the groups has a foreign key that references a table outside the groups.
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class t ON (t.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid AND n.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid  = t.oid)
             JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND tf.relkind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table currently outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = nf.nspname
                     AND rel_tblseq = tf.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groupNames)
                )
        ORDER BY 1,2,3
    LOOP
      RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" references the table "%.%" that is outside the groups (%).',
        r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,array_to_string(p_groupNames,',');
    END LOOP;
-- Issue a warning if a table of the groups is referenced by a table outside the groups.
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class tf ON (tf.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace AND nf.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.confrelid  = tf.oid)
             JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
             JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND t.relkind = 'r'                                       -- only constraints referenced by true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = n.nspname
                     AND rel_tblseq = t.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groupNames)
                )
        ORDER BY 1,2,3
    LOOP
      RAISE WARNING '_check_fk_groups: The table "%.%" is referenced by the foreign key "%" on the table "%.%" that is outside'
                    ' the groups (%).', r_fk.rel_schema, r_fk.rel_tblseq, r_fk.conname, r_fk.nspname, r_fk.relname,
                    array_to_string(p_groupNames,',');
    END LOOP;
-- Issue a warning for rollbackable groups if a FK on a partition is actualy set on the partitionned table.
-- The warning message depends on the FK characteristics.
    v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
    FOR r_fk IN
      SELECT rel_schema, rel_tblseq, c.conname, confupdtype, confdeltype, condeferrable
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class t ON (t.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid)
        WHERE contype = 'f'                                         -- FK constraints only
          AND coninhcount > 0                                       -- inherited FK
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND r.rel_kind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
        ORDER BY 1,2,3
    LOOP
      IF r_fk.confupdtype = 'a' AND r_fk.confdeltype = 'a' AND NOT r_fk.condeferrable THEN
        -- Advise DEFERRABLE FK if there is no ON UPDATE|DELETE clause.
        RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table. It should'
                      ' be set as DEFERRABLE to avoid E-Maj rollback failure.',
                      r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
      ELSIF r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
        -- Warn about potential rollback failure with FK having ON UPDATE|DELETE clause.
        IF v_isEmajExtension THEN
          RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table. An E-Maj'
                        ' rollback targeting a mark set before the latest table assignement could fail.',
                        r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
        ELSE
          RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table and has'
                        ' ON DELETE and/or ON UPDATE clause. This will generate E-Maj rollback failures if this FK needs to be'
                        ' temporarily dropped and recreated.',
                        r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
        END IF;
      END IF;
    END LOOP;
--
    RETURN;
  END;
$_check_fk_groups$;

CREATE OR REPLACE FUNCTION emaj._drop_group(p_groupName TEXT, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group.
-- It also drops log schemas that are not useful anymore.
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT;
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbRel                  INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
    v_function = CASE WHEN p_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END;
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'D') INTO v_timeId;
-- Register into emaj_relation_change the tables and sequences removal from their group, for completeness.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      SELECT v_timeId, rel_schema, rel_tblseq,
             CASE WHEN rel_kind = 'r' THEN 'REMOVE_TABLE'::emaj._relation_change_kind_enum
                                      ELSE 'REMOVE_SEQUENCE'::emaj._relation_change_kind_enum END,
             p_groupName
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND upper_inf(rel_time_range)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Delete the emaj objects and references for each table and sequences of the group.
    FOR r_rel IN
      SELECT *
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
    LOOP
      PERFORM CASE r_rel.rel_kind
                WHEN 'r' THEN emaj._drop_tbl(r_rel, v_timeId)
                WHEN 'S' THEN emaj._drop_seq(r_rel, v_timeId)
              END;
    END LOOP;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any other created group).
    PERFORM emaj._drop_log_schemas(v_function, p_isForced);
-- Delete group row from the emaj_group table.
-- By cascade, it also deletes rows from emaj_mark.
    DELETE FROM emaj.emaj_group
      WHERE group_name = p_groupName
      RETURNING group_nb_table + group_nb_sequence INTO v_nbRel;
-- Update the last log session for the group to set the time range upper bound
    UPDATE emaj.emaj_log_session
      SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[]')
      WHERE lses_group = p_groupName
        AND upper_inf(lses_time_range);
-- Update the last group history row to set the time range upper bound
    UPDATE emaj.emaj_group_hist
      SET grph_time_range = int8range(lower(grph_time_range), v_timeId, '[]')
      WHERE grph_group = p_groupName
        AND upper_inf(grph_time_range);
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
--
    RETURN v_nbRel;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_groups_configuration$
-- This function returns a JSON formatted structure representing some or all configured tables groups
-- The function can be called by clients like Emaj_web.
-- This is just a wrapper of the internal _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the tables groups content in JSON format
  BEGIN
    RETURN emaj._export_groups_conf(p_groups);
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT[]) IS
$$Generates a json structure describing configured tables groups.$$;

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
-- Build the header of the JSON structure.
    v_groupsText = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || statement_timestamp() || E'",\n';
-- Check the group names array, if supplied. All the listed groups must exist.
    IF p_groups IS NOT NULL THEN
      SELECT string_agg(group_name, ', ') INTO v_unknownGroupsList
        FROM
          (SELECT *
             FROM unnest(p_groups) AS grp(group_name)
             WHERE NOT EXISTS
                    (SELECT group_name
                       FROM emaj.emaj_group
                       WHERE emaj_group.group_name = grp.group_name
                    )
          ) AS t;
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

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(p_groupsJson JSON, p_groups TEXT[],
                                                    p_allowGroupsUpdate BOOLEAN, p_location TEXT)
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
-- Create the temporary table that will hold the application tables configured in groups.
    CREATE TEMP TABLE tmp_app_table (
      tmp_group            TEXT NOT NULL,
      tmp_schema           TEXT NOT NULL,
      tmp_tbl_name         TEXT NOT NULL,
      tmp_priority         INTEGER,
      tmp_log_dat_tsp      TEXT,
      tmp_log_idx_tsp      TEXT,
      tmp_ignored_triggers TEXT[],
      PRIMARY KEY (tmp_schema, tmp_tbl_name)
      );
-- Create the temporary table that will hold the application sequences configured in groups.
    CREATE TEMP TABLE tmp_app_sequence (
      tmp_schema           TEXT NOT NULL,
      tmp_seq_name         TEXT NOT NULL,
      tmp_group            TEXT,
      PRIMARY KEY (tmp_schema, tmp_seq_name)
      );
-- In a second pass over the JSON structure, populate the tmp_app_table and tmp_app_sequence temporary tables.
    v_rollbackableGroups = '{}';
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(p_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
-- Build the array of rollbackable groups.
      IF coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE) THEN
        v_rollbackableGroups = array_append(v_rollbackableGroups, r_group.groupJson ->> 'group');
      END IF;
-- Insert tables into tmp_app_table.
      FOR r_table IN
        SELECT value AS tableJson
          FROM json_array_elements(r_group.groupJson -> 'tables')
      LOOP
--   Prepare the array of trigger names for the table,
        SELECT array_agg("value" ORDER BY "value") INTO v_ignoredTriggers
          FROM json_array_elements_text(r_table.tableJson -> 'ignored_triggers') AS t;
--   ... and insert
        INSERT INTO tmp_app_table(tmp_group, tmp_schema, tmp_tbl_name,
                                  tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp, tmp_ignored_triggers)
          VALUES (r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table',
                  (r_table.tableJson ->> 'priority')::INT, r_table.tableJson ->> 'log_data_tablespace',
                  r_table.tableJson ->> 'log_index_tablespace', v_ignoredTriggers);
      END LOOP;
-- Insert sequences into tmp_app_table.
      FOR r_sequence IN
        SELECT value AS sequenceJson
          FROM json_array_elements(r_group.groupJson -> 'sequences')
      LOOP
        INSERT INTO tmp_app_sequence(tmp_schema, tmp_seq_name, tmp_group)
          VALUES (r_sequence.sequenceJson ->> 'schema', r_sequence.sequenceJson ->> 'sequence', r_group.groupJson ->> 'group');
      END LOOP;
    END LOOP;
-- Check the just imported tmp_app_table content is ok for the groups.
    RETURN QUERY
      SELECT *
        FROM emaj._import_groups_conf_check(p_groups)
        WHERE ((rpt_text_var_1 = ANY (p_groups) AND rpt_severity = 1)
            OR (rpt_text_var_1 = ANY (v_rollbackableGroups) AND rpt_severity = 2))
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3;
--
    RETURN;
  END;
$_import_groups_conf_prepare$;

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
-- It is called by _import_groups_conf() and by Emaj_web
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

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_alter(p_groupNames TEXT[], p_mark TEXT, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_import_groups_conf_alter$
-- This function effectively alters the tables groups to import.
-- It uses the content of tmp_app_table and tmp_app_sequence temporary tables and calls the appropriate elementary functions
-- It is called by the _import_groups_conf_exec() function.
-- Input: group names array,
--        the mark name to set on groups in logging state
--        the timestamp id
  DECLARE
    v_eventTriggers          TEXT[];
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
-- Reset the concerned groups in IDLE state, before changing tablespaces.
    PERFORM emaj._reset_groups(array_agg(group_name ORDER BY group_name))
      FROM emaj.emaj_group
      WHERE group_name = ANY (p_groupNames)
        AND NOT group_is_logging;
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
          RAISE WARNING '_stop_groups: The schema "%" does not exist anymore.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist anymore.', r_schema.rel_schema;
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
              RAISE WARNING '_stop_groups: The table "%.%" does not exist anymore.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            ELSE
              RAISE EXCEPTION '_stop_groups: The table "%.%" does not exist anymore.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
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
                  RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
            BEGIN
              EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg',
                             r_tblsq.rel_schema, r_tblsq.rel_tblseq);
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist anymore.',
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

CREATE OR REPLACE FUNCTION emaj._delete_intermediate_mark_group(p_groupName TEXT, p_markName TEXT, p_markTimeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_delete_intermediate_mark_group$
-- This function effectively deletes an intermediate mark for a group.
-- It is called by the emaj_delete_mark_group() function.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained.
-- Input: group name, mark name, mark id and mark time stamp id of the mark to delete
  DECLARE
    v_lsesTimeRange          INT8RANGE;
    v_previousMark           TEXT;
    v_nextMark               TEXT;
    v_previousMarkTimeId     BIGINT;
    v_nextMarkTimeId         BIGINT;
  BEGIN
-- Get the log session time range that contains the mark time id.
    SELECT lses_time_range
      INTO STRICT v_lsesTimeRange
      FROM emaj.emaj_log_session
      WHERE lses_group = p_groupName
        AND p_markTimeId <@ lses_time_range;
-- Delete the sequences related to the mark to delete, if it is not a log session boundary.
    IF p_markTimeId <> lower(v_lsesTimeRange) AND (upper_inf(v_lsesTimeRange) OR p_markTimeId <> upper(v_lsesTimeRange) - 1) THEN
-- Delete data related to the application sequences (those attached to the group at the set mark time, but excluding the relation time
-- range bounds).
      DELETE FROM emaj.emaj_sequence
        USING emaj.emaj_relation
        WHERE sequ_schema = rel_schema
          AND sequ_name = rel_tblseq
          AND rel_time_range @> sequ_time_id
          AND rel_group = p_groupName
          AND rel_kind = 'S'
          AND sequ_time_id = p_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
-- Delete data related to the log sequences for tables (those attached to the group at the set mark time, but excluding the relation time
-- range bounds).
      DELETE FROM emaj.emaj_table
        USING emaj.emaj_relation
        WHERE tbl_schema = rel_schema
          AND tbl_name = rel_tblseq
          AND rel_time_range @> tbl_time_id
          AND rel_group = p_groupName
          AND rel_kind = 'r'
          AND tbl_time_id = p_markTimeId
          AND lower(rel_time_range) <> tbl_time_id;
    END IF;
-- Physically delete the mark from emaj_mark.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = p_markName;
-- Adjust the mark_log_rows_before_next column of the previous mark.
-- Get the name of the mark immediately preceeding the mark to delete.
    SELECT mark_name, mark_time_id INTO v_previousMark, v_previousMarkTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id < p_markTimeId
      ORDER BY mark_time_id DESC
      LIMIT 1;
-- Get the name of the first mark succeeding the mark to delete.
    SELECT mark_name, mark_time_id INTO v_nextMark, v_nextMarkTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id > p_markTimeId
      ORDER BY mark_time_id
      LIMIT 1;
    IF NOT FOUND THEN
-- No next mark, so update the previous mark with NULL.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = p_groupName
          AND mark_name = v_previousMark;
    ELSE
-- Update the previous mark by computing the sum of _log_stat_tbl() call's result for all relations that belonged
-- to the group at the time when the mark before the deleted mark had been set.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next =
          (SELECT sum(emaj._log_stat_tbl(emaj_relation, v_previousMarkTimeId, v_nextMarkTimeId))
             FROM emaj.emaj_relation
             WHERE rel_group = p_groupName
               AND rel_kind = 'r'
               AND rel_time_range @> v_previousMarkTimeId
          )
        WHERE mark_group = p_groupName
          AND mark_name = v_previousMark;
    END IF;
-- Reset the mark_logged_rlbk_target_mark column to null for other marks of the group that may have the deleted mark
-- as target mark from a previous logged rollback operation.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = p_groupName
        AND mark_logged_rlbk_target_mark = p_markName;
--
    RETURN;
  END;
$_delete_intermediate_mark_group$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(p_rlbkId INT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called to perform a rollback operation. It is also called to simulate a rollback operation and get its duration estimate.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viewer role can write into rollback tables, when estimating the rollback
--   duration, without having specific privileges on them to do it.
  DECLARE
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_nbSequence             INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_markTimeId             BIGINT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_fixed_table_rlbk       INTERVAL;
    v_effNbTable             INT;
    v_isEmajExtension        BOOLEAN;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimDurationRlbkSeq   INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    v_fkList                 TEXT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- Get the rollback characteristics for the emaj_rlbk event.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session, rlbk_nb_sequence,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_nbSequence,
           v_ctrlStepName
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get some mark attributes from emaj_mark.
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Get all duration parameters that will be needed later from the emaj_param table, or get default values for rows
-- that are not present in emaj_param table.
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_fkey_check_duration'),'5 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_step_rollback_duration'),'2.5 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::INTERVAL)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk, v_fixed_table_rlbk;
-- Process the sequences, if any in the tables groups.
    IF v_nbSequence > 0 THEN
-- Compute the cost for each RLBK_SEQUENCES step and keep it for later.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDurationRlbkSeq
        FROM emaj._estimate_rlbk_step_duration('RLBK_SEQUENCES', NULL, NULL, NULL, v_nbSequence, v_fixed_step_rlbk, v_fixed_table_rlbk);
-- Insert a RLBK_SEQUENCES step into emaj_rlbk_plan.
-- Assign it the first session, so that it will be executed by the same session as the start mark set when the rollback is logged.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_session, rlbp_batch_number,
                                       rlbp_estimated_quantity, rlbp_estimated_duration, rlbp_estimate_method)
        VALUES (p_rlbkId, 'RLBK_SEQUENCES', '', '', '', 1, 1,
                v_nbSequence, v_estimDurationRlbkSeq, v_estimMethod);
    END IF;
-- Insert into emaj_rlbk_plan a RLBK_TABLE step per table to effectively rollback.
-- The numbers of log rows is computed using the _log_stat_tbl() function.
-- A final check will be performed after tables will be locked to be sure no new table will have been updated.
    INSERT INTO emaj.emaj_rlbk_plan
            (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_is_repl_role_replica, rlbp_target_time_id,
             rlbp_estimated_quantity)
      SELECT p_rlbkId, 'RLBK_TABLE', rel_schema, rel_tblseq, '', FALSE, greatest(v_markTimeId, lower(rel_time_range)),
             emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL)
        FROM
          (SELECT *
             FROM emaj.emaj_relation
             WHERE upper_inf(rel_time_range)
               AND rel_group = ANY (v_groupNames)
               AND rel_kind = 'r'
          ) AS t
        WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0;
    GET DIAGNOSTICS v_effNbTable = ROW_COUNT;
-- If nothing has to be rolled back, return quickly
    IF v_nbSequence = 0 AND v_effNbTable = 0 THEN
      RETURN 0;
    END IF;
-- Insert into emaj_rlbk_plan a LOCK_TABLE step per table currently belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_is_repl_role_replica)
      SELECT p_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, '', FALSE
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = ANY(v_groupNames)
          AND rel_kind = 'r';
-- For tables to effectively rollback, add related steps (for FK, triggers, E-Maj logs) and adjust step properties.
    IF v_effNbTable > 0 THEN
-- Set the rlbp_is_repl_role_replica flag to TRUE for tables having all foreign keys linking tables:
--   1) in the rolled back groups and 2) with the same rollback target mark.
-- This only concerns emaj installed as an extension because one needs to be sure that the _rlbk_tbl() function is executed with a
-- superuser role (this is needed to set the session_replication_role to 'replica').
      v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
      IF v_isEmajExtension THEN
        WITH fkeys AS (
            -- the foreign keys belonging to tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, nf.nspname, tf.relname,
                   (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames) AND
                    rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)))
                     AS are_both_tables_in_groups_with_the_same_target_mark
                     -- (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames)) AS are_both_tables_in_groups,
                     -- rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)) AS have_both_tables_the_same_target_mark
              FROM emaj.emaj_rlbk_plan,
                   pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation ON (rel_schema = nf.nspname AND rel_tblseq = tf.relname
                                                          AND upper_inf(rel_time_range))
              WHERE rlbp_rlbk_id = p_rlbkId                               -- The RLBK_TABLE steps for this rollback operation
                AND rlbp_step = 'RLBK_TABLE'
                AND contype = 'f'                                         -- FK constraints
                AND tf.relkind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                          --   partitionned tables
                AND t.relname = rlbp_table
                AND n.nspname = rlbp_schema
          UNION ALL
            -- the foreign keys referencing tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, n.nspname, t.relname,
                   (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames) AND
                    rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)))
                     AS are_both_tables_in_groups_with_the_same_target_mark
              FROM emaj.emaj_rlbk_plan,
                   pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation ON (rel_schema = n.nspname AND rel_tblseq = t.relname
                                                          AND upper_inf(rel_time_range))
              WHERE rlbp_rlbk_id = p_rlbkId                               -- The RLBK_TABLE steps for this rollback operation
                AND rlbp_step = 'RLBK_TABLE'
                AND contype = 'f'                                         -- FK constraints
                AND t.relkind = 'r'                                       -- only constraints referenced by true tables, ie. excluding
                                                                          --   partitionned tables
                AND tf.relname = rlbp_table
                AND nf.nspname = rlbp_schema
        ), fkeys_agg AS (
          -- aggregated foreign keys by tables to rollback
          SELECT rlbp_schema, rlbp_table,
                 count(*) AS nb_fk,
                 count(*) FILTER (WHERE are_both_tables_in_groups_with_the_same_target_mark) AS nb_fk_ok
            FROM fkeys
            GROUP BY 1,2
        )
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_is_repl_role_replica = TRUE
          FROM fkeys_agg
          WHERE rlbp_rlbk_id = p_rlbkId                                    -- The RLBK_TABLE steps for this rollback operation
            AND rlbp_step IN ('RLBK_TABLE', 'LOCK_TABLE')
            AND emaj_rlbk_plan.rlbp_table = fkeys_agg.rlbp_table
            AND emaj_rlbk_plan.rlbp_schema = fkeys_agg.rlbp_schema
            AND nb_fk = nb_fk_ok                                           -- all fkeys are linking tables 1) in the rolled back groups
                                                                           -- and 2) with the same rollback target mark
        ;
      END IF;
--
-- Group tables into batchs to process all tables linked by foreign keys as a batch.
--
-- Start at 2, 1 being allocated to the RLBK_SEQUENCES step, if exists.
      v_batchNumber = 2;
-- Allocate tables with rows to rollback to batch number starting with the heaviest to rollback tables as reported by the
-- emaj_log_stat_group() function.
      FOR r_tbl IN
        SELECT rlbp_schema, rlbp_table, rlbp_is_repl_role_replica
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'
          ORDER BY rlbp_estimated_quantity DESC, rlbp_schema, rlbp_table
      LOOP
-- If the table is not already allocated to a batch number (it may have been already allocated because of a fkey link).
        IF EXISTS
            (SELECT 0
               FROM emaj.emaj_rlbk_plan
               WHERE rlbp_rlbk_id = p_rlbkId
                 AND rlbp_step = 'RLBK_TABLE'
                 AND rlbp_schema = r_tbl.rlbp_schema
                 AND rlbp_table = r_tbl.rlbp_table
                 AND rlbp_batch_number IS NULL
            ) THEN
-- Allocate the table to the batch number, with all other tables linked by foreign key constraints.
          PERFORM emaj._rlbk_set_batch_number(p_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table,
                                              r_tbl.rlbp_is_repl_role_replica);
          v_batchNumber = v_batchNumber + 1;
        END IF;
      END LOOP;
--
-- If unlogged rollback, register into emaj_rlbk_plan "disable log triggers", "deletes from log tables"
-- and "enable log trigger" steps.
--
      IF NOT v_isLoggedRlbk THEN
-- Compute the cost for each DIS_LOG_TRG step.
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('DIS_LOG_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all DIS_LOG_TRG steps.
        INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                         rlbp_estimated_duration, rlbp_estimate_method)
          SELECT p_rlbkId, 'DIS_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
            FROM emaj.emaj_rlbk_plan
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'RLBK_TABLE';
-- Insert all DELETE_LOG steps. But the duration estimates will be computed later.
-- The estimated number of log rows to delete is set to the estimated number of updates. This is underestimated in particular when
-- SQL UPDATES are logged. But the collected statistics used for duration estimates are also based on the estimated number of updates.
        INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id,
                                         rlbp_batch_number, rlbp_estimated_quantity)
          SELECT p_rlbkId, 'DELETE_LOG', rlbp_schema, rlbp_table, '', rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
            FROM emaj.emaj_rlbk_plan
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'RLBK_TABLE';
-- Compute the cost for each ENA_LOG_TRG step.
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('ENA_LOG_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all ENA_LOG_TRG steps.
        INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                         rlbp_estimated_duration, rlbp_estimate_method)
          SELECT p_rlbkId, 'ENA_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number, v_estimDuration, v_estimMethod
            FROM emaj.emaj_rlbk_plan
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'RLBK_TABLE';
      END IF;
--
-- Process application triggers to temporarily set as ALWAYS triggers.
-- This concerns triggers that must be kept enabled during the rollback processing but the rollback function for its table is executed
-- with session_replication_role = replica.
--
-- Compute the cost for each SET_ALWAYS_APP_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('SET_ALWAYS_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all SET_ALWAYS_APP_TRG steps.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                       rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'SET_ALWAYS_APP_TRG', rlbp_schema, rlbp_table, tgname, rlbp_batch_number, v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
               JOIN pg_catalog.pg_class ON (relname = rlbp_table)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
               JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'                               -- rollback step
            AND rlbp_is_repl_role_replica                              -- ... in session_replication_role = replica
            AND NOT tgisinternal                                       -- application triggers only
            AND tgname NOT IN ('emaj_trunc_trg','emaj_log_trg')
            AND tgenabled = 'O'                                        -- ... enabled in local mode
            AND EXISTS                                                 -- ... and to be kept enabled
                  (SELECT 0
                     FROM emaj.emaj_relation
                     WHERE rel_schema = rlbp_schema
                       AND rel_tblseq = rlbp_table
                       AND upper_inf(rel_time_range)
                       AND tgname = ANY (rel_ignored_triggers)
                  );
-- Compute the cost for each SET_LOCAL_APP_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('SET_LOCAL_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all SET_LOCAL_APP_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
                                       rlbp_batch_number, rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'SET_LOCAL_APP_TRG', rlbp_schema, rlbp_table, rlbp_object,
               rlbp_batch_number, v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'SET_ALWAYS_APP_TRG';
--
-- Process application triggers to disable and re-enable.
-- This concerns triggers that must be disabled during the rollback processing and the rollback function for its table is not executed
-- with session_replication_role = replica.
--
-- Compute the cost for each DIS_APP_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('DIS_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all DIS_APP_TRG steps.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                       rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'DIS_APP_TRG', rlbp_schema, rlbp_table, tgname, rlbp_batch_number, v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
               JOIN pg_catalog.pg_class ON (relname = rlbp_table)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
               JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'                               -- rollback step
            AND NOT tgisinternal                                       -- application triggers only
            AND tgname NOT IN ('emaj_trunc_trg','emaj_log_trg')
            AND (tgenabled IN ('A', 'R')                               -- enabled ALWAYS or REPLICA triggers
                OR (tgenabled = 'O' AND NOT rlbp_is_repl_role_replica) -- or enabled ORIGIN triggers for rollbacks not processed
                )                                                      --   in session_replication_role = replica)
            AND NOT EXISTS                                             -- ... that must be disabled
                  (SELECT 0
                     FROM emaj.emaj_relation
                     WHERE rel_schema = rlbp_schema
                       AND rel_tblseq = rlbp_table
                       AND upper_inf(rel_time_range)
                       AND tgname = ANY (rel_ignored_triggers)
                  );
-- Compute the cost for each ENA_APP_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('ENA_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all ENA_APP_TRG steps.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
                                       rlbp_app_trg_type,
                                       rlbp_batch_number, rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'ENA_APP_TRG', rlbp_schema, rlbp_table, rlbp_object,
               CASE tgenabled WHEN 'A' THEN 'ALWAYS' WHEN 'R' THEN 'REPLICA' ELSE '' END,
               rlbp_batch_number, v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
               JOIN pg_catalog.pg_class ON (relname = rlbp_table)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
               JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid AND tgname = rlbp_object)
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'DIS_APP_TRG';
--
-- Process foreign key to define which action to perform on them
--
-- First compute the fixed duration estimates for each 'DROP_FK' and 'SET_FK_DEF' steps.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimDropFkMethod, v_estimDropFkDuration
        FROM emaj._estimate_rlbk_step_duration('DROP_FK', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimSetFkDefMethod, v_estimSetFkDefDuration
        FROM emaj._estimate_rlbk_step_duration('SET_FK_DEF', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Select all foreign keys belonging to or referencing the tables to process.
      FOR r_fk IN
          SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, c.condeferrable,
                 c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
            FROM emaj.emaj_rlbk_plan r
                 JOIN pg_catalog.pg_class t ON (t.relname = r.rlbp_table)
                 JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rlbp_schema)
                 JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid)
            WHERE c.contype = 'f'                                            -- FK constraints only
              AND rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'RLBK_TABLE'                                   -- Tables to rollback
              AND NOT rlbp_is_repl_role_replica                              -- ... not in a session_replication_role = replica
        UNION
          SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, c.condeferrable,
                 c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
            FROM emaj.emaj_rlbk_plan r
                 JOIN pg_catalog.pg_class rt ON (rt.relname = r.rlbp_table)
                 JOIN pg_catalog.pg_namespace rn ON (rn.oid = rt.relnamespace AND rn.nspname = r.rlbp_schema)
                 JOIN pg_catalog.pg_constraint c ON (c.confrelid = rt.oid)
                 JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                 JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
            WHERE c.contype = 'f'                                            -- FK constraints only
              AND rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'RLBK_TABLE'                                   -- Tables to rollback
              AND NOT rlbp_is_repl_role_replica                              -- ... not in a session_replication_role = replica
            ORDER BY nspname, relname, conname
      LOOP
-- Depending on the foreign key characteristics, record as 'to be dropped' or 'to be set deferred' or 'to just be reset immediate'.
        IF NOT r_fk.condeferrable OR r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
-- Non deferrable fkeys and fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped.
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            p_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimDropFkDuration, v_estimDropFkMethod
            );
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_object_def,
            rlbp_estimated_quantity
            ) VALUES (
            p_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            pg_catalog.pg_get_constraintdef(r_fk.conoid), r_fk.reltuples
            );
        ELSE
-- Other deferrable but not deferred fkeys need to be set deferred.
          IF NOT r_fk.condeferred THEN
            INSERT INTO emaj.emaj_rlbk_plan (
              rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
              rlbp_estimated_duration, rlbp_estimate_method
              ) VALUES (
              p_rlbkId, 'SET_FK_DEF', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
              v_estimSetFkDefDuration, v_estimSetFkDefMethod
              );
          END IF;
-- Deferrable fkeys are recorded as 'to be set immediate at the end of the rollback operation'.
-- Compute the number of fkey values to check at set immediate time.
          SELECT (coalesce(
-- Get the number of rolled back rows in the referencing table, if any.
             (SELECT rlbp_estimated_quantity
                FROM emaj.emaj_rlbk_plan
                WHERE rlbp_rlbk_id = p_rlbkId
                  AND rlbp_step = 'RLBK_TABLE'                                   -- tables of the rollback event
                  AND rlbp_schema = r_fk.nspname
                  AND rlbp_table = r_fk.relname)                                 -- referencing schema.table
              , 0)) + (coalesce(
-- Get the number of rolled back rows in the referenced table, if any.
             (SELECT rlbp_estimated_quantity
                FROM emaj.emaj_rlbk_plan
                     JOIN pg_catalog.pg_class rt ON (rt.relname = rlbp_table)
                     JOIN pg_catalog.pg_namespace rn ON (rn.oid = rt.relnamespace AND rn.nspname = rlbp_schema)
                     JOIN pg_catalog.pg_constraint c ON (c.confrelid  = rt.oid)
                WHERE rlbp_rlbk_id = p_rlbkId
                  AND rlbp_step = 'RLBK_TABLE'                                   -- tables of the rollback event
                  AND c.oid = r_fk.conoid                                        -- constraint id
             )
              , 0)) INTO v_checks;
-- And record the SET_FK_IMM step.
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_estimated_quantity
            ) VALUES (
            p_rlbkId, 'SET_FK_IMM', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, v_checks
            );
        END IF;
      END LOOP;
-- Raise an exception if DROP_FK steps concerns inherited FK (i.e. FK set on a partitionned table)
      SELECT string_agg(rlbp_schema || '.' || rlbp_table || '.' || rlbp_object, ', ')
        INTO v_fkList
        FROM emaj.emaj_rlbk_plan r
             JOIN pg_catalog.pg_class t ON (t.relname = r.rlbp_table)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rlbp_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid AND c.conname = r.rlbp_object)
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'DROP_FK'
          AND coninhcount > 0;
      IF v_fkList IS NOT NULL THEN
        RAISE EXCEPTION '_rlbk_planning: Some foreign keys (%) would need to be temporarily dropped during the operation. '
                        'But this would fail because they are inherited from a partitionned table.', v_fkList;
      END IF;
--
-- Now compute the estimation duration for each complex step ('RLBK_TABLE', 'DELETE_LOG', 'ADD_FK', 'SET_FK_IMM').
--
-- Compute the rollback duration estimates for the tables.
      FOR r_tbl IN
        SELECT *
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'
      LOOP
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('RLBK_TABLE', r_tbl.rlbp_schema, r_tbl.rlbp_table, NULL,
                                                 r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_row_rlbk);
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'
            AND rlbp_schema = r_tbl.rlbp_schema
            AND rlbp_table = r_tbl.rlbp_table;
      END LOOP;
-- Compute the estimated log rows delete duration.
      FOR r_tbl IN
        SELECT *
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'DELETE_LOG'
      LOOP
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('DELETE_LOG', r_tbl.rlbp_schema, r_tbl.rlbp_table, NULL,
                                                 r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_row_del_log);
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'DELETE_LOG'
            AND rlbp_schema = r_tbl.rlbp_schema
            AND rlbp_table = r_tbl.rlbp_table;
      END LOOP;
-- Compute the fkey recreation duration.
      FOR r_fk IN
        SELECT *
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'ADD_FK'
      LOOP
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('ADD_FK', r_tbl.rlbp_schema, r_tbl.rlbp_table, r_fk.rlbp_object,
                                                 r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_fkey_check);
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'ADD_FK'
            AND rlbp_schema = r_fk.rlbp_schema
            AND rlbp_table = r_fk.rlbp_table
            AND rlbp_object = r_fk.rlbp_object;
      END LOOP;
-- Compute the fkey checks duration.
      FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'SET_FK_IMM'
      LOOP
        SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
          FROM emaj._estimate_rlbk_step_duration('SET_FK_IMM', r_tbl.rlbp_schema, r_tbl.rlbp_table, r_fk.rlbp_object,
                                                 r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_fkey_check);
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'SET_FK_IMM'
            AND rlbp_schema = r_fk.rlbp_schema
            AND rlbp_table = r_fk.rlbp_table
            AND rlbp_object = r_fk.rlbp_object;
      END LOOP;
--
-- Allocate batches to sessions to spread the load on sessions as best as possible.
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys.
--
      IF v_nbSession = 1 THEN
-- In single session rollback, assign all steps to session 1 at once.
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_session = 1
          WHERE rlbp_rlbk_id = p_rlbkId;
      ELSE
-- Initialisation (for session 1, the RLBK_SEQUENCES step may have been already assigned).
        v_sessionLoad [1] = coalesce(v_estimDurationRlbkSeq, '0 SECONDS'::INTERVAL);
        FOR v_session IN 2 .. v_nbSession LOOP
          v_sessionLoad [v_session] = '0 SECONDS'::INTERVAL;
        END LOOP;
-- Allocate tables batch to sessions, starting with the heaviest to rollback batch.
        FOR r_batch IN
          SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
            FROM emaj.emaj_rlbk_plan
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_batch_number IS NOT NULL
              AND rlbp_session IS NULL
            GROUP BY rlbp_batch_number
            ORDER BY sum(rlbp_estimated_duration) DESC
        LOOP
-- Compute the least loaded session.
          v_minSession = 1; v_minDuration = v_sessionLoad [1];
          FOR v_session IN 2 .. v_nbSession LOOP
            IF v_sessionLoad [v_session] < v_minDuration THEN
              v_minSession = v_session;
              v_minDuration = v_sessionLoad [v_session];
            END IF;
          END LOOP;
-- Allocate the batch to the session.
          UPDATE emaj.emaj_rlbk_plan
            SET rlbp_session = v_minSession
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_batch_number = r_batch.rlbp_batch_number;
          v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + r_batch.batch_duration;
        END LOOP;
      END IF;
    END IF;
-- Assign all not yet assigned 'LOCK_TABLE' steps to session 1.
    UPDATE emaj.emaj_rlbk_plan
      SET rlbp_session = 1
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_session IS NULL;
--
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate.
--
-- Get the number of recorded steps (except LOCK_TABLE).
    SELECT count(*) INTO v_nbStep
      FROM emaj.emaj_rlbk_plan
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_step <> 'LOCK_TABLE';
    IF v_nbStep > 0 THEN
-- If CTRLxDBLINK statistics are available, compute an average cost.
      SELECT sum(rlbt_duration) * v_nbStep / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = v_ctrlStepName
          AND rlbt_quantity > 0;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
-- Otherwise, use the fixed_step_rollback_duration parameter.
        v_estimDuration = v_fixed_dblink_rlbk * v_nbStep;
        v_estimMethod = 3;
      END IF;
-- Insert the 'CTRLxDBLINK' pseudo step.
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_estimated_quantity,
          rlbp_estimated_duration, rlbp_estimate_method
        ) VALUES (
          p_rlbkId, v_ctrlStepName, '', '', '', v_nbStep, v_estimDuration, v_estimMethod
        );
    END IF;
-- Return the number of tables to effectively rollback.
    RETURN v_effNbTable;
  END;
$_rlbk_planning$;

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
    v_timeId                 BIGINT;
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
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND NOT new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'ADD_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND NOT new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
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
--   Get a timestamp via dblink, if it is usable,
      v_stmt = 'SELECT emaj._set_time_stamp(''' || v_function || ''', ''M'')';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
--   ... and effectively set the mark.
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_markComment, p_multiGroup, TRUE, v_mark, v_timeId, v_dblinkSchema);
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

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_group_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics about logged data changes between 2 marks or between a mark and the current state for a single group.
-- It is used to quickly get simple statistics of changes logged between 2 marks.
-- Input: group name, the 2 mark names defining a range
-- Output: set of stat rows by table (including tables without any data change)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_rows
        FROM emaj._log_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged data changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_group_type LANGUAGE plpgsql AS
$emaj_log_stat_groups$
-- This function returns statistics about logged data changes between 2 marks or between a mark and the current state for a groups array.
-- It is used to quickly get simple statistics of changes logged between 2 marks.
-- Input: group names array, the 2 mark names defining a range
-- Output: set of stat rows by table (including tables without any data change)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_rows
        FROM emaj._log_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns global statistics about logged data changes between 2 marks for several groups.$$;

CREATE OR REPLACE FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_group_type LANGUAGE plpgsql AS
$_log_stat_groups$
-- This function effectively returns statistics about logged data changes between 2 marks or between a mark and the current state for 1
-- or several groups.
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function, the 2 mark names defining a
--          range
--   a NULL value or an empty string as last_mark indicates the current state
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark
--   parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of stat rows by table (including tables without any data change)
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
  BEGIN
-- Check the groups name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT checked.p_firstMark, checked.p_lastMark, p_firstMarkTimeId, p_lastMarkTimeId, p_firstMarkTs, p_lastMarkTs
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark) checked;
-- For each table of the group, get the number of log rows and return the statistics.
-- Shorten the timeframe if the table did not belong to the group on the entire requested time frame.
      RETURN QUERY
        SELECT rel_group, rel_schema, rel_tblseq,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN p_firstMark
                    ELSE coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = lower(rel_time_range)
                                AND mark_group = rel_group
                           ),'[deleted mark]')
                 END AS stat_first_mark,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMarkTs
                    ELSE (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = lower(rel_time_range)
                         )
                 END AS stat_first_mark_datetime,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMarkTimeId
                    ELSE lower(rel_time_range)
                 END AS stat_first_mark_time_id,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN coalesce(
                                (SELECT mark_name
                                   FROM emaj.emaj_mark
                                   WHERE mark_time_id = upper(rel_time_range)
                                     AND mark_group = rel_group
                                ),'[deleted mark]')
                    ELSE p_lastMark
                 END AS stat_last_mark,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN (SELECT time_clock_timestamp
                                 FROM emaj.emaj_time_stamp
                                 WHERE time_id = upper(rel_time_range)
                              )
                    ELSE v_lastMarkTs
                 END AS stat_last_mark_datetime,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN upper(rel_time_range)
                    ELSE v_lastMarkTimeId
                 END AS stat_last_mark_datetime,
               CASE WHEN v_firstMarkTimeId IS NULL THEN 0                                       -- group just created but without any mark
                    ELSE emaj._log_stat_tbl(emaj_relation,
                                            CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                   THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                            CASE WHEN NOT upper_inf(rel_time_range)
                                                   AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                   THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
                 END AS nb_rows
          FROM emaj.emaj_relation
          WHERE rel_group = ANY(p_groupNames)
            AND rel_kind = 'r'                                                                  -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)        --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range;
    ELSE
      RETURN;
    END IF;
  END;
$_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_group_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_group$
-- This function returns statistics on logged data changes between 2 marks as viewed through the log tables for one tables group.
-- It provides much precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range
-- Output: set of stat rows by table, user and SQL type
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_role, stat_verb, stat_rows
        FROM emaj._detailed_log_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged data changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_group_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_groups$
-- This function returns statistics on logged data changes between 2 marks as viewed through the log tables for several tables groups.
-- It provides much precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group names array, the 2 marks names defining a range
-- Output: set of stat rows by table, user and SQL type
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_role, stat_verb, stat_rows
        FROM emaj._detailed_log_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_detailed_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns detailed statistics about logged data changes between 2 marks for several groups.$$;

CREATE OR REPLACE FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_group_type LANGUAGE plpgsql AS
$_detailed_log_stat_groups$
-- This function effectively returns statistics on logged data changes executed between 2 marks as viewed through the log tables for one
-- or several groups.
-- It provides muche precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function,
--        the 2 mark names defining a range
--   a NULL value or an empty string as last_mark indicates the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: set of stat rows by table, user and SQL type
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_lowerBoundMark         TEXT;
    v_lowerBoundTimeId       BIGINT;
    v_lowerBoundMarkTs       TIMESTAMPTZ;
    v_lowerBoundGid          BIGINT;
    v_upperBoundMark         TEXT;
    v_upperBoundTimeId       BIGINT;
    v_upperBoundMarkTs       TIMESTAMPTZ;
    v_upperBoundGid          BIGINT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE);
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT *
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark);
-- For each table currently belonging to the group, count the number of operations per type (INSERT, UPDATE and DELETE) and role.
      FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_group, rel_time_range, rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_group = ANY(p_groupNames)
            AND rel_kind = 'r'                                                                         -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range
      LOOP
-- Compute the lower bound for this table.
        IF v_firstMarkTimeId >= lower(r_tblsq.rel_time_range) THEN
-- Usual case: the table belonged to the group at statistics start mark.
          v_lowerBoundMark = p_firstMark;
          v_lowerBoundMarkTs = v_firstMarkTs;
          v_lowerBoundTimeId = v_firstMarkTimeId;
          v_lowerBoundGid = v_firstEmajGid;
        ELSE
-- Special case: the table has been added to the group after the statistics start mark.
          SELECT mark_name INTO v_lowerBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = lower(r_tblsq.rel_time_range)
              AND mark_group = r_tblsq.rel_group;
          IF v_lowerBoundMark IS NULL THEN
-- The mark set at alter_group time may have been deleted.
            v_lowerBoundMark = '[deleted mark]';
          END IF;
          SELECT time_id, time_clock_timestamp, time_last_emaj_gid INTO v_lowerBoundTimeId, v_lowerBoundMarkTs, v_lowerBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = lower(r_tblsq.rel_time_range);
        END IF;
-- Compute the upper bound for this table.
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- No supplied end mark and the table has not been removed from its group => the current state.
          v_upperBoundMark = NULL;
          v_upperBoundMarkTs = NULL;
          v_upperBoundTimeId = NULL;
          v_upperBoundGid = NULL;
        ELSIF NOT upper_inf(r_tblsq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_tblsq.rel_time_range) < v_lastMarkTimeId) THEN
-- Special case: the table has been removed from its group before the statistics end mark.
          SELECT mark_name INTO v_upperBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = upper(r_tblsq.rel_time_range)
              AND mark_group = r_tblsq.rel_group;
          IF v_upperBoundMark IS NULL THEN
-- The mark set at alter_group time may have been deleted.
            v_upperBoundMark = '[deleted mark]';
          END IF;
          SELECT time_id, time_clock_timestamp, time_last_emaj_gid INTO v_upperBoundTimeId, v_upperBoundMarkTs, v_upperBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = upper(r_tblsq.rel_time_range);
        ELSE
-- Usual case: the table belonged to the group at statistics end mark.
          v_upperBoundMark = p_lastMark;
          v_upperBoundMarkTs = v_lastMarkTs;
          v_upperBoundTimeId = v_lastMarkTimeId;
          v_upperBoundGid = v_lastEmajGid;
        END IF;
-- Build the statement.
        v_stmt= 'SELECT ' || quote_literal(r_tblsq.rel_group) || '::TEXT AS stat_group, '
             || quote_literal(r_tblsq.rel_schema) || '::TEXT AS stat_schema, '
             || quote_literal(r_tblsq.rel_tblseq) || '::TEXT AS stat_table, '
             || quote_literal(v_lowerBoundMark) || '::TEXT AS stat_first_mark, '
             || quote_literal(v_lowerBoundMarkTs) || '::TIMESTAMPTZ AS stat_first_mark_datetime, '
             || quote_literal(v_lowerBoundTimeId) || '::BIGINT AS stat_first_time_id, '
             || coalesce(quote_literal(v_upperBoundMark), 'NULL') || '::TEXT AS stat_last_mark, '
             || coalesce(quote_literal(v_upperBoundMarkTs), 'NULL') || '::TIMESTAMPTZ AS stat_last_mark_datetime, '
             || coalesce(quote_literal(v_upperBoundTimeId), 'NULL') || '::BIGINT AS stat_last_time_id, '
             || ' emaj_user::TEXT AS stat_user,'
             || ' CASE emaj_verb WHEN ''INS'' THEN ''INSERT'''
             ||                ' WHEN ''UPD'' THEN ''UPDATE'''
             ||                ' WHEN ''DEL'' THEN ''DELETE'''
             ||                ' WHEN ''TRU'' THEN ''TRUNCATE'''
             ||                             ' ELSE ''?'' END AS stat_verb,'
             || ' count(*) AS stat_rows'
             || ' FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table)
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'') AND NOT (emaj_verb = ''TRU'' AND emaj_tuple = '''')'
             || ' AND emaj_gid > '|| v_lowerBoundGid
             || coalesce(' AND emaj_gid <= '|| v_upperBoundGid, '')
             || ' GROUP BY stat_user, stat_verb'
             || ' ORDER BY stat_user, stat_verb';
-- Execute the statement.
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END LOOP;
    END IF;
-- Final return.
    RETURN;
  END;
$_detailed_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_sequence_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_group_type LANGUAGE plpgsql AS
$emaj_sequence_stat_group$
-- This function returns statistics on sequences changes recorded between 2 marks or between a mark and the current state
--   for a single group.
-- Input: group name, both mark names defining a range
-- Output: set of stats by sequence (including unchanged sequences)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_increments, stat_has_structure_changed
        FROM emaj._sequence_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_sequence_stat_group$;
COMMENT ON FUNCTION emaj.emaj_sequence_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about recorded sequences changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_sequence_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_group_type LANGUAGE plpgsql AS
$emaj_sequence_stat_groups$
-- This function returns statistics on sequences changes recorded between 2 marks or between a mark and the current state
--   for a groups array.
-- Input: group names array, both mark names defining a range
-- Output: set of stats by sequence (including unchanged sequences)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_mark_datetime, stat_first_time_id,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_increments, stat_has_structure_changed
        FROM emaj._sequence_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_sequence_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_sequence_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns global statistics about recorded sequences changes between 2 marks for several groups.$$;

CREATE OR REPLACE FUNCTION emaj._sequence_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_group_type LANGUAGE plpgsql AS
$_sequence_stat_groups$
-- This function effectively returns statistics on sequences changes recorded between 2 marks or between a mark and the current state for
-- one or several groups.
-- Input: group names array, a boolean indicating whether the calling function is a multi_groups function, both mark names defining a
--          range
--   a NULL value or an empty string as last_mark indicates the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of stats by sequence (including unchanged sequences).
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    r_seq                    emaj.emaj_relation%ROWTYPE;
    r_stat                   emaj.emaj_sequence_stat_group_type%ROWTYPE;
  BEGIN
-- Check the groups name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT checked.p_firstMark, checked.p_lastMark, p_firstMarkTimeId, p_lastMarkTimeId, p_firstMarkTs, p_lastMarkTs
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark,
                                     p_checkLogSession := FALSE) AS checked;
-- For each sequence of the group, get and return the statistics.
      FOR r_seq IN
          SELECT *
            FROM emaj.emaj_relation
            WHERE rel_group = ANY(p_groupNames)
              AND rel_kind = 'S'                                                                  -- sequences belonging to the groups
              AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)        --   at the requested time frame
              AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
            ORDER BY rel_schema, rel_tblseq, rel_time_range
      LOOP
        r_stat.stat_group = r_seq.rel_group;
        r_stat.stat_schema = r_seq.rel_schema;
        r_stat.stat_sequence = r_seq.rel_tblseq;
        r_stat.stat_first_mark = p_firstMark;
        r_stat.stat_first_mark_datetime = v_firstMarkTs;
        r_stat.stat_first_time_id = v_firstMarkTimeId;
        r_stat.stat_last_mark = p_lastMark;
        r_stat.stat_last_mark_datetime = v_lastMarkTs;
        r_stat.stat_last_time_id = v_lastMarkTimeId;
-- Shorten the time frame if the sequence did not belong to the group on the entire requested time frame.
        IF v_firstMarkTimeId < lower(r_seq.rel_time_range) THEN
          r_stat.stat_first_time_id = lower(r_seq.rel_time_range);
          r_stat.stat_first_mark = coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = r_stat.stat_first_time_id
                                AND mark_group = r_seq.rel_group
                           ),'[deleted mark]');
          r_stat.stat_first_mark_datetime = (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = r_stat.stat_first_time_id
                         );
        END IF;
        IF NOT upper_inf(r_seq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_seq.rel_time_range) < v_lastMarkTimeId) THEN
          r_stat.stat_last_time_id = upper(r_seq.rel_time_range);
          r_stat.stat_last_mark = coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = r_stat.stat_last_time_id
                                AND mark_group = r_seq.rel_group
                           ),'[deleted mark]');
          r_stat.stat_last_mark_datetime = (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = r_stat.stat_last_time_id
                         );
        END IF;
-- Get the stats for the sequence.
        SELECT p_increments, p_hasStructureChanged
          INTO r_stat.stat_increments, r_stat.stat_has_structure_changed
          FROM emaj._sequence_stat_seq(r_seq, r_stat.stat_first_time_id, r_stat.stat_last_time_id);
-- Return the stat row for the sequence.
        RETURN NEXT r_stat;
      END LOOP;
    END IF;
    RETURN;
  END;
$_sequence_stat_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_table(p_schema TEXT, p_table TEXT, p_startGroup TEXT, p_startMark TEXT,
                                                    p_endGroup TEXT DEFAULT NULL, p_endMark TEXT DEFAULT NULL)
RETURNS SETOF emaj.emaj_log_stat_table_type LANGUAGE plpgsql AS
$emaj_log_stat_table$
-- This function returns statistics about a single table, for the time period framed by a supplied marks interval.
-- For each marks interval, possibly for different tables groups, it returns the number of recorded changes.
-- It calls the _log_stat_table() function version with time id interval to compute the statistics.
-- Input: schema and table names,
--        start and end marks.
-- Output: set of stats by time interval.
  DECLARE
    v_startTimeId            BIGINT;
    v_endTimeId              BIGINT;
  BEGIN
-- Check the table is known in the emaj_relation table.
    IF NOT EXISTS (
      SELECT 0
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND rel_kind = 'r'
                  ) THEN
      RAISE EXCEPTION 'emaj_log_stat_table: Table %.% is not known by E-Maj.', p_schema, p_table;
    END IF;
-- If the start group is known, check it with its mark.
    IF p_startGroup IS NOT NULL THEN
      PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_startGroup], p_mayBeNull := FALSE, p_lockGroups := FALSE);
      SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_startGroup], p_mark := p_startMark) INTO p_startMark;
-- ... and get its timeId.
      SELECT mark_time_id INTO v_startTimeId
        FROM emaj.emaj_mark
        WHERE mark_group = p_startGroup
          AND mark_name = p_startMark;
    END IF;
-- If supplied, check the end group and mark.
    IF p_endGroup IS NOT NULL THEN
      PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_endGroup], p_mayBeNull := FALSE, p_lockGroups := FALSE);
      SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_endGroup], p_mark := p_endMark) INTO p_endMark;
-- ... and get its timestamp.
      SELECT mark_time_id INTO v_endTimeId
        FROM emaj.emaj_mark
        WHERE mark_group = p_endGroup
          AND mark_name = p_endMark;
    END IF;
-- Check the mark lower bound has been set before the mark upper bound.
    IF v_startTimeId IS NOT NULL AND v_endTimeId IS NOT NULL AND v_startTimeId > v_endTimeId THEN
      RAISE EXCEPTION 'emaj_log_stat_table: The start mark has been set after the end mark.';
    END IF;
-- OK.
-- If there is no emaj_table row for the start time id, get the previous one, if exists.
    IF p_startGroup IS NOT NULL THEN
      SELECT tbl_time_id
        INTO v_startTimeId
        FROM emaj.emaj_table
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND tbl_time_id <= v_startTimeId
        ORDER BY tbl_time_id DESC
        LIMIT 1;
    END IF;
-- If there is no emaj_table row for the end time id, get the next one, if exists.
    IF p_endGroup IS NOT NULL THEN
      SELECT tbl_time_id
        INTO v_endTimeId
        FROM emaj.emaj_table
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND tbl_time_id >= v_endTimeId
        ORDER BY tbl_time_id
        LIMIT 1;
    END IF;
-- Compute the statistics.
    RETURN QUERY
      SELECT stat_group, stat_first_mark, stat_first_mark_datetime, stat_first_time_id, stat_is_log_start,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
        FROM emaj._log_stat_table(p_schema, p_table, v_startTimeId, v_endTimeId);
  END;
$emaj_log_stat_table$;
COMMENT ON FUNCTION emaj.emaj_log_stat_table(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) IS
$$Returns statistics about logged data changes between 2 marks for a single table.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_table(p_schema TEXT, p_table TEXT,
                                                    p_startTs TIMESTAMPTZ DEFAULT NULL, p_endTs TIMESTAMPTZ DEFAULT NULL)
RETURNS SETOF emaj.emaj_log_stat_table_type LANGUAGE plpgsql AS
$emaj_log_stat_table$
-- This function returns statistics about a single table, for the time period framed by a supplied timestamp interval,
--   by default the entire time range when the table is managed by E-Maj.
-- For each mark interval, possibly for different tables groups, it returns the number of recorded changes.
-- Input: schema and table names
--        start and end timestamps.
-- Output: set of stats by time interval.
  DECLARE
    v_startTimeId            BIGINT;
    v_endTimeId              BIGINT;
  BEGIN
-- Check the table is known in the emaj_relation table.
    IF NOT EXISTS (
      SELECT 0
        FROM emaj.emaj_relation
             JOIN emaj.emaj_time_stamp l ON (l.time_id = lower(rel_time_range))
             LEFT OUTER JOIN emaj.emaj_time_stamp u ON (u.time_id = upper(rel_time_range))
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND rel_kind = 'r'
                  ) THEN
      RAISE EXCEPTION 'emaj_log_stat_table: The table %.% is not known by E-Maj.', p_schema, p_table;
    END IF;
-- Check the lower bound TS is not greater than the upper bound TS.
    IF p_startTs IS NOT NULL AND p_endTs IS NOT NULL AND p_startTs > p_endTs THEN
      RAISE EXCEPTION 'emaj_log_stat_table: The start timestamp is greater than the end timestamp.';
    END IF;
-- OK
-- Get the start time id, if a start TS is supplied. Select the time id just before the supplied TS.
    IF p_startTs IS NOT NULL THEN
      SELECT time_id
        INTO v_startTimeId
        FROM emaj.emaj_table
             JOIN emaj.emaj_time_stamp ON (time_id = tbl_time_id)
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND time_clock_timestamp <= p_startTs
        ORDER BY time_id DESC
        LIMIT 1;
    END IF;
-- Get the end time id, if an end TS is supplied. Select the time id just after the supplied TS.
    IF p_endTs IS NOT NULL THEN
      SELECT time_id
        INTO v_endTimeId
        FROM emaj.emaj_table
             JOIN emaj.emaj_time_stamp ON (time_id = tbl_time_id)
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND time_clock_timestamp >= p_endTs
        ORDER BY time_id
        LIMIT 1;
    END IF;
-- Call the _log_stat_table() function to build the result.
    RETURN QUERY
      SELECT stat_group, stat_first_mark, stat_first_mark_datetime, stat_first_time_id, stat_is_log_start,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
        FROM emaj._log_stat_table(p_schema, p_table, v_startTimeId, v_endTimeId);
  END;
$emaj_log_stat_table$;
COMMENT ON FUNCTION emaj.emaj_log_stat_table(TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) IS
$$Returns statistics about logged data changes between 2 timestamps for a single table.$$;

CREATE OR REPLACE FUNCTION emaj._log_stat_table(p_schema TEXT, p_table TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
RETURNS SETOF emaj.emaj_log_stat_table_type LANGUAGE plpgsql AS
$_log_stat_table$
-- This function returns statistics about a single table, for the time period framed by a supplied time_id slice.
-- It is called by the various emaj_log_stat_table() function.
-- For each mark interval, possibly for different tables groups, it returns the number of recorded changes.
-- Input: schema and table names
--        start and end time_id.
-- Output: set of stats by time slice.
  DECLARE
    v_needCurrentState       BOOLEAN;
  BEGIN
-- Determine whether the table current state if needed, i.e. if it still belongs to an active group.
    v_needCurrentState = EXISTS(
      SELECT 0
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range)
          AND group_is_logging
          AND p_endTimeId IS NULL
      );
-- OK, compute and return the statistics by scanning the table history in the emaj_table table.
    RETURN QUERY
      WITH event AS (
        SELECT tbl_time_id, lower(rel_time_range), tbl_log_seq_last_val,
               rel_group, rel_time_range, time_clock_timestamp, time_event,
               coalesce(mark_name, '[deleted mark]') AS mark_name,
               (time_event = 'S' OR tbl_time_id = lower(rel_time_range)) AS log_start,
               (time_event = 'X' OR (NOT upper_inf(rel_time_range) AND tbl_time_id = upper(rel_time_range))) AS log_stop
          FROM emaj.emaj_table
               JOIN emaj.emaj_relation ON (rel_schema = tbl_schema AND rel_tblseq = tbl_name)
               JOIN emaj.emaj_time_stamp ON (time_id = tbl_time_id)
               LEFT OUTER JOIN emaj.emaj_mark ON (mark_group = rel_group AND mark_time_id = tbl_time_id)
          WHERE tbl_schema = p_schema
            AND tbl_name = p_table
            AND (tbl_time_id <@ rel_time_range OR tbl_time_id = upper(rel_time_range))
            AND (p_startTimeId IS NULL OR tbl_time_id >= p_startTimeId)
            AND (p_endTimeId IS NULL OR tbl_time_id <= p_endTimeId)
        UNION
-- ... and add the table current state, if the upper bound is not fixed and if the table belongs to an active group yet.
        SELECT NULL, NULL, emaj._get_log_sequence_last_value(rel_log_schema, rel_log_sequence),
               rel_group, rel_time_range, clock_timestamp(), '', '[current state]', false, false
          FROM emaj.emaj_relation
          WHERE v_needCurrentState
            AND rel_schema = p_schema
            AND rel_tblseq = p_table
            AND upper_inf(rel_time_range)
        ORDER BY 1, 2
        ), time_slice AS (
-- transform elementary time events into time slices
         SELECT tbl_time_id AS start_time_id, lead(tbl_time_id) OVER () AS end_time_id,
                tbl_log_seq_last_val AS start_last_val, lead(tbl_log_seq_last_val) OVER () AS end_last_val,
                rel_group,
                rel_time_range AS start_rel_time_range, lead(rel_time_range) OVER () AS end_rel_time_range,
                time_clock_timestamp AS start_timestamp, lead(time_clock_timestamp) OVER () AS end_timestamp,
                time_event AS start_time_event, lead(time_event) OVER () AS end_time_event,
                mark_name AS start_mark_name, lead(mark_name) OVER () AS end_mark_name,
                log_start AS start_log_start, lead(log_start) OVER () AS end_log_start,
                log_stop AS start_log_stop, lead(log_stop) OVER () AS end_log_stop
           FROM event
        )
-- filter time slices and compute statistics aggregates
-- (take into account log sequence holes generated by unlogged rollbacks)
      SELECT rel_group AS stat_group, start_mark_name AS stat_first_mark, start_timestamp AS stat_first_mark_datetime,
             start_time_id AS stat_first_time_id, start_log_start AS stat_is_log_start, end_mark_name AS stat_last_mark,
             end_timestamp AS stat_last_mark_datetime, end_time_id AS stat_last_time_id, end_log_stop AS stat_is_log_stop,
             end_last_val - start_last_val -
               (SELECT coalesce(sum(sqhl_hole_size),0)
                   FROM emaj.emaj_seq_hole
                   WHERE sqhl_schema = p_schema
                     AND sqhl_table = p_table
                     AND sqhl_begin_time_id >= start_time_id
                     AND (end_time_id IS NULL OR sqhl_end_time_id <= end_time_id))::BIGINT
               AS stat_changes,
             count(rlbk_id)::INT AS stat_rollbacks
        FROM time_slice
             LEFT OUTER JOIN emaj.emaj_rlbk ON (rel_group = ANY (rlbk_groups) AND
                                                rlbk_time_id >= start_time_id AND (end_time_id IS NULL OR rlbk_time_id < end_time_id))
        WHERE end_timestamp IS NOT NULL                    -- time slice not starting with the very last event
          AND start_rel_time_range = end_rel_time_range    -- same rel_time_range on the slice
          AND NOT (start_log_stop AND end_log_start)       -- not a logging hole
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ORDER BY start_time_id;
  END;
$_log_stat_table$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startGroup TEXT, p_startMark TEXT,
                                                       p_endGroup TEXT DEFAULT NULL, p_endMark TEXT DEFAULT NULL)
RETURNS SETOF emaj.emaj_log_stat_sequence_type LANGUAGE plpgsql AS
$emaj_log_stat_sequence$
-- This function returns statistics about a single sequence, for the time period framed by a supplied marks interval.
-- For each marks interval, possibly for different tables groups, it returns the number of increments and a flag to show any sequence
--   properties changes.
-- It calls the _log_stat_sequence() function version with time id interval to compute the statistics.
-- Input: schema and sequence names,
--        start and end marks.
-- Output: set of stats by time interval.
  DECLARE
    v_startTimeId            BIGINT;
    v_endTimeId              BIGINT;
  BEGIN
-- Check the sequence is known in the emaj_relation table.
    IF NOT EXISTS (
      SELECT 0
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND rel_kind = 'S'
                  ) THEN
      RAISE EXCEPTION 'emaj_log_stat_sequence: Sequence %.% is not known by E-Maj.', p_schema, p_sequence;
    END IF;
-- If the start group is known, check it with its mark.
    IF p_startGroup IS NOT NULL THEN
      PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_startGroup], p_mayBeNull := FALSE, p_lockGroups := FALSE);
      SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_startGroup], p_mark := p_startMark) INTO p_startMark;
-- ... and get its timeId.
      SELECT mark_time_id INTO v_startTimeId
        FROM emaj.emaj_mark
        WHERE mark_group = p_startGroup
          AND mark_name = p_startMark;
    END IF;
-- If supplied, check the end group and mark.
    IF p_endGroup IS NOT NULL THEN
      PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_endGroup], p_mayBeNull := FALSE, p_lockGroups := FALSE);
      SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_endGroup], p_mark := p_endMark) INTO p_endMark;
-- ... and get its timestamp.
      SELECT mark_time_id INTO v_endTimeId
        FROM emaj.emaj_mark
        WHERE mark_group = p_endGroup
          AND mark_name = p_endMark;
    END IF;
-- Check the mark lower bound has been set before the mark upper bound.
    IF v_startTimeId IS NOT NULL AND v_endTimeId IS NOT NULL AND v_startTimeId > v_endTimeId THEN
      RAISE EXCEPTION 'emaj_log_stat_sequence: The start mark has been set after the end mark.';
    END IF;
-- OK.
-- If there is no emaj_sequence row for this time id, get the next one, if exists.
    IF p_endGroup IS NOT NULL THEN
      SELECT sequ_time_id
        INTO v_endTimeId
        FROM emaj.emaj_sequence
        WHERE sequ_schema = p_schema
          AND sequ_name = p_sequence
          AND sequ_time_id >= v_endTimeId
        ORDER BY sequ_time_id
        LIMIT 1;
    END IF;
-- If there is no emaj_sequence row for this time id, get the previous one, if exists.
    IF p_startGroup IS NOT NULL THEN
      SELECT sequ_time_id
        INTO v_startTimeId
        FROM emaj.emaj_sequence
        WHERE sequ_schema = p_schema
          AND sequ_name = p_sequence
          AND sequ_time_id <= v_startTimeId
        ORDER BY sequ_time_id DESC
        LIMIT 1;
    END IF;
-- Compute the statistics.
    RETURN QUERY
      SELECT stat_group, stat_first_mark, stat_first_mark_datetime, stat_first_time_id, stat_is_log_start,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_is_log_stop,
             stat_increments, stat_has_structure_changed, stat_rollbacks
        FROM emaj._log_stat_sequence(p_schema, p_sequence, v_startTimeId, v_endTimeId);
  END;
$emaj_log_stat_sequence$;
COMMENT ON FUNCTION emaj.emaj_log_stat_sequence(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) IS
$$Returns statistics about recorded changes between 2 marks for a single sequence.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_sequence(p_schema TEXT, p_sequence TEXT,
                                                       p_startTs TIMESTAMPTZ DEFAULT NULL, p_endTs TIMESTAMPTZ DEFAULT NULL)
RETURNS SETOF emaj.emaj_log_stat_sequence_type LANGUAGE plpgsql AS
$emaj_log_stat_sequence$
-- This function returns statistics about a single sequence, for the time period framed by a supplied timestamp interval,
--   by default the entire time range when the sequence is managed by E-Maj.
-- For each mark interval, possibly for different tables groups, it returns the number of increments and a flag to show any sequence
--   properties changes.
-- Input: schema and sequence names
--        start and end timestamps.
-- Output: set of stats by time interval.
  DECLARE
    v_startTimeId            BIGINT;
    v_endTimeId              BIGINT;
  BEGIN
-- Check the sequence is known in the emaj_relation table.
    IF NOT EXISTS (
      SELECT 0
        FROM emaj.emaj_relation
             JOIN emaj.emaj_time_stamp l ON (l.time_id = lower(rel_time_range))
             LEFT OUTER JOIN emaj.emaj_time_stamp u ON (u.time_id = upper(rel_time_range))
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND rel_kind = 'S'
                  ) THEN
      RAISE EXCEPTION 'emaj_log_stat_sequence: The sequence %.% is not known by E-Maj.', p_schema, p_sequence;
    END IF;
-- Check the lower bound TS is not greater than the upper bound TS.
    IF p_startTs IS NOT NULL AND p_endTs IS NOT NULL AND p_startTs > p_endTs THEN
      RAISE EXCEPTION 'emaj_log_stat_sequence: The start timestamp is greater than the end timestamp.';
    END IF;
-- OK.
-- Get the start time id, if a start TS is supplied. Select the time id just before the supplied TS.
    IF p_startTs IS NOT NULL THEN
      SELECT time_id
        INTO v_startTimeId
        FROM emaj.emaj_sequence
             JOIN emaj.emaj_time_stamp ON (time_id = sequ_time_id)
        WHERE sequ_schema = p_schema
          AND sequ_name = p_sequence
          AND time_clock_timestamp <= p_startTs
        ORDER BY time_id DESC
        LIMIT 1;
    END IF;
-- Get the end time id, if an end TS is supplied. Select the time id just after the supplied TS.
    IF p_endTs IS NOT NULL THEN
      SELECT time_id
        INTO v_endTimeId
        FROM emaj.emaj_sequence
             JOIN emaj.emaj_time_stamp ON (time_id = sequ_time_id)
        WHERE sequ_schema = p_schema
          AND sequ_name = p_sequence
          AND time_clock_timestamp >= p_endTs
        ORDER BY time_id
        LIMIT 1;
    END IF;
-- Call the _log_stat_sequence() function to build the result.
    RETURN QUERY
      SELECT stat_group, stat_first_mark, stat_first_mark_datetime, stat_first_time_id, stat_is_log_start,
             stat_last_mark, stat_last_mark_datetime, stat_last_time_id, stat_is_log_stop,
             stat_increments, stat_has_structure_changed, stat_rollbacks
        FROM emaj._log_stat_sequence(p_schema, p_sequence, v_startTimeId, v_endTimeId);
  END;
$emaj_log_stat_sequence$;
COMMENT ON FUNCTION emaj.emaj_log_stat_sequence(TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) IS
$$Returns statistics about recorded changes between 2 timestamps for a single sequence.$$;

CREATE OR REPLACE FUNCTION emaj._log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
RETURNS SETOF emaj.emaj_log_stat_sequence_type LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_log_stat_sequence$
-- This function returns statistics about a single sequence, for the time period framed by a supplied time_id slice.
-- It is called by the various emaj_log_stat_sequence() function.
-- For each mark interval, possibly for different tables groups, it returns the number of increments and a flag to show any sequence
--   properties changes.
-- Input: schema and sequence names
--        start and end time_id.
-- Output: set of stats by time slice.
-- The function is defined as SECURITY DEFINER so that emaj_adm roles can use it even without SELECT right on the sequence.
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
-- OK, compute and return the statistics by scanning the sequence history in the emaj_sequence table.
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
-- transform elementary time events into time slices
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
-- filter time slices and compute statistics aggregates
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

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                            p_optionsList TEXT, p_tblseqs TEXT[], p_genSqlOnly BOOLEAN,
                                                            OUT p_nbStmt INT, OUT p_copyOptions TEXT, OUT p_noEmptyFiles BOOLEAN,
                                                            OUT p_isPsqlCopy BOOLEAN)
LANGUAGE plpgsql AS
$_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- It is called by both emaj_gen_sql_dump_changes_group() and emaj_dump_changes_group() functions to prepare the SQL statements to be
--   stored or executed.
-- The function checks the supplied parameters, including the options that may be common or specific to both calling functions.
-- Input: group name, 2 mark names defining the time range,
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        a boolean indentifying the calling function.
-- Output: the number of generated SQL statements, excluding comments, but including SET or RESET statements, if any.
--         the COPY_OPTIONS and NO_EMPTY_FILES options needed by the emaj_dump_changes_group() function,
--         a flag for generated psql \copy meta-commands needed by the emaj_gen_sql_dump_changes_group() function.
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_optionsList            TEXT;
    v_options                TEXT[];
    v_option                 TEXT;
    v_colsOrder              TEXT;
    v_consolidation          TEXT;
    v_copyOptions            TEXT;
    v_psqlCopyDir            TEXT;
    v_psqlCopyOptions        TEXT;
    v_emajColumnsList        TEXT;
    v_isPsqlCopy             BOOLEAN = FALSE;
    v_noEmptyFiles           BOOLEAN = FALSE;
    v_orderBy                TEXT;
    v_prevCMM                TEXT;
    v_sequencesOnly          BOOLEAN = FALSE;
    v_sqlFormat              TEXT = 'RAW';
    v_tablesOnly             BOOLEAN = FALSE;
    v_tableWithoutPkList     TEXT;
    v_nbStmt                 INT = 0;
    v_relFirstMark           TEXT;
    v_relLastMark            TEXT;
    v_relFirstEmajGid        BIGINT;
    v_relLastEmajGid         BIGINT;
    v_relFirstTimeId         BIGINT;
    v_relLastTimeId          BIGINT;
    v_stmt                   TEXT;
    v_comment                TEXT;
    v_copyOutputFile         TEXT;
    r_rel                    RECORD;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the marks range and get some data about both marks.
    SELECT * INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
      FROM emaj._check_marks_range(p_groupNames := ARRAY[p_groupName], p_firstMark := p_firstMark, p_lastMark := p_lastMark,
                                   p_finiteUpperBound := TRUE);
-- Analyze the options parameter.
    IF p_optionsList IS NOT NULL THEN
      v_optionsList = p_optionsList;
      IF NOT p_genSqlOnly THEN
-- Extract the COPY_OPTIONS list, if any, before removing spaces.
        v_copyOptions = (regexp_match(v_optionsList, 'COPY_OPTIONS\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_copyOptions IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_copyOptions, '');
        END IF;
      END IF;
      IF p_genSqlOnly THEN
-- Extract the PSQL_COPY_DIR and PSQL_COPY_OPTIONS options, if any, before removing spaces.
        v_psqlCopyDir = (regexp_match(v_optionsList, 'PSQL_COPY_DIR\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_psqlCopyDir IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_psqlCopyDir, '');
        END IF;
        v_psqlCopyOptions = (regexp_match(v_optionsList, 'PSQL_COPY_OPTIONS\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_psqlCopyOptions IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_psqlCopyOptions, '');
        END IF;
      END IF;
-- Remove spaces, tabs and newlines from the options list.
      v_optionsList = regexp_replace(v_optionsList, '\s', '', 'g');
-- Extract the option values list, if any.
      v_emajColumnsList = (regexp_match(v_optionsList, 'EMAJ_COLUMNS\s*?=\s*?\((.*?)\)', 'i'))[1];
      IF v_emajColumnsList IS NOT NULL THEN
        v_optionsList = replace(v_optionsList, v_emajColumnsList, '');
      END IF;
-- Process each option from the comma separated list.
      v_options = regexp_split_to_array(upper(v_optionsList), ',');
      FOREACH v_option IN ARRAY v_options
      LOOP
        CASE
          WHEN v_option LIKE 'COLS_ORDER=%' THEN
            CASE
              WHEN v_option = 'COLS_ORDER=LOG_TABLE' THEN
                v_colsOrder = 'LOG_TABLE';
              WHEN v_option = 'COLS_ORDER=PK' THEN
                v_colsOrder = 'PK';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COLS_ORDER option only accepts '
                                'LOG_TABLE or PK values).',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'CONSOLIDATION=%' THEN
            CASE
              WHEN v_option = 'CONSOLIDATION=NONE' THEN
                v_consolidation = 'NONE';
              WHEN v_option = 'CONSOLIDATION=PARTIAL' THEN
                v_consolidation = 'PARTIAL';
              WHEN v_option = 'CONSOLIDATION=FULL' THEN
                v_consolidation = 'FULL';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The CONSOLIDATION option only accepts '
                                'NONE or PARTIAL or FULL values).',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'COPY_OPTIONS=%' AND NOT p_genSqlOnly THEN
            IF v_option <> 'COPY_OPTIONS=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COPY options must be set between ().',
                              v_option;
            END IF;
-- Check the copy options parameter doesn't contain unquoted semicolon that could be used for sql injection.
            IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Unquoted semi-column in COPY options is illegal.';
            END IF;
            v_copyOptions = '(' || v_copyOptions || ')';
          WHEN v_option LIKE 'EMAJ_COLUMNS=%' THEN
            CASE
              WHEN v_option = 'EMAJ_COLUMNS=ALL' THEN
                v_emajColumnsList = '*';
              WHEN v_option = 'EMAJ_COLUMNS=MIN' THEN
                v_emajColumnsList = 'MIN';
              WHEN v_option = 'EMAJ_COLUMNS=()' THEN
                IF v_emajColumnsList NOT ILIKE '%emaj_tuple%' THEN
                  RAISE EXCEPTION '_gen_sql_dump_changes_group: In the EMAJ_COLUMN option, the "emaj_tuple" column must be part '
                                  'of the columns list.';
                END IF;
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The EMAJ_COLUMNS option only accepts '
                                'ALL or MIN values or a (columns list).',
                                v_option;
            END CASE;
          WHEN v_option = 'NO_EMPTY_FILES' AND NOT p_genSqlOnly THEN
            v_noEmptyFiles = TRUE;
          WHEN v_option LIKE 'ORDER_BY=%' THEN
            CASE
              WHEN v_option = 'ORDER_BY=PK' THEN
                v_orderBy = 'PK';
              WHEN v_option = 'ORDER_BY=TIME' THEN
                v_orderBy = 'TIME';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The ORDER_BY option only accepts '
                                'PK or TIME values.',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'PSQL_COPY_DIR%' AND p_genSqlOnly THEN
            v_isPsqlCopy = TRUE;
            IF v_option <> 'PSQL_COPY_DIR=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The directory name must be set between ().',
                              v_option;
            END IF;
          WHEN v_option LIKE 'PSQL_COPY_OPTIONS=%' AND p_genSqlOnly THEN
            IF v_option <> 'PSQL_COPY_OPTIONS=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COPY options list must be set between ().',
                               v_option;
            END IF;
          WHEN v_option = 'SEQUENCES_ONLY' THEN
            v_sequencesOnly = TRUE;
          WHEN v_option LIKE 'SQL_FORMAT=%' AND p_genSqlOnly THEN
            CASE
              WHEN v_option = 'SQL_FORMAT=RAW' THEN
                v_sqlFormat = 'RAW';
              WHEN v_option = 'SQL_FORMAT=PRETTY' THEN
                v_sqlFormat = 'PRETTY';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The SQL_FORMAT option only accepts '
                                'RAW or PRETTY values.',
                                v_option;
            END CASE;
          WHEN v_option = 'TABLES_ONLY' THEN
            v_tablesOnly = TRUE;
          ELSE
            IF v_option <> '' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: The option "%" is unknown.', v_option;
            END IF;
        END CASE;
      END LOOP;
-- Validate the relations between options.
-- SEQUENCES_ONLY and TABLES_ONLY are not compatible.
      IF v_sequencesOnly AND v_tablesOnly THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: SEQUENCES_ONLY and TABLES_ONLY options are mutually exclusive.';
      END IF;
-- PSQL_COPY_OPTIONS needs a PSQL_COPY_DIR to be set;
      IF v_psqlCopyOptions IS NOT NULL AND NOT v_isPsqlCopy THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: the PSQL_COPY_OPTIONS option needs a PSQL_COPY_DIR option to be set.';
      END IF;
-- PSQL_COPY_DIR and FORMAT=PRETTY are not compatible (for a psql \copy, the statement must be one a single line).
      IF v_isPsqlCopy AND v_sqlFormat = 'PRETTY' THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: PSQL_COPY_DIR and FORMAT=PRETTY options are mutually exclusive.';
      END IF;
-- When one or several options need PRIMARY KEYS, check that all selected tables have a PK.
      IF v_consolidation IN ('PARTIAL', 'FULL') OR v_colsOrder = 'PK' OR v_orderBy = 'PK' THEN
        SELECT string_agg(table_name, ', ')
          INTO v_tableWithoutPkList
          FROM (
            SELECT rel_schema || '.' || rel_tblseq AS table_name
              FROM emaj.emaj_relation
              WHERE rel_group = p_groupName
                AND rel_kind = 'r' AND NOT v_sequencesOnly
                AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))
                AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
                AND rel_pk_cols IS NULL
              ORDER BY rel_schema, rel_tblseq, rel_time_range
            ) AS t;
        IF v_tableWithoutPkList IS NOT NULL THEN
          RAISE EXCEPTION '_gen_sql_dump_changes_group: A CONSOLIDATION level set to PARTIAL or FULL or a COLS_ORDER set to PK or an '
                          'ORDER_BY set to PK cannot support tables without primary key. And no primary key is defined for tables "%"',
                          v_tableWithoutPkList;
        END IF;
      END IF;
    END IF;
-- If table/sequence names to filter are supplied, check them.
    IF p_tblseqs IS NOT NULL THEN
      p_tblseqs = emaj._check_tblseqs_filter(p_tblseqs, ARRAY[p_groupName], v_firstMarkTimeId, v_lastMarkTimeId, FALSE);
    END IF;
-- End of checks.
-- Set options default values.
    v_consolidation = coalesce(v_consolidation, 'NONE');
    v_copyOptions = coalesce(v_copyOptions, '');
    v_colsOrder = coalesce(v_colsOrder, CASE WHEN v_consolidation = 'NONE' THEN 'LOG_TABLE' ELSE 'PK' END);
    v_emajColumnsList = coalesce(v_emajColumnsList, CASE WHEN v_consolidation = 'NONE' THEN '*' ELSE 'emaj_tuple' END);
    v_orderBy = coalesce(v_orderBy, CASE WHEN v_consolidation = 'NONE' THEN 'TIME' ELSE 'PK' END);
-- Resolve the MIN value for the EMAJ_COLUMNS option, depending on the final consolidation level.
    IF v_emajColumnsList = 'MIN' THEN
      v_emajColumnsList = CASE WHEN v_consolidation = 'NONE' THEN 'emaj_gid,emaj_tuple' ELSE 'emaj_tuple' END;
    END IF;
-- Set the ORDER_BY clause if not explicitely done in the supplied options.
    v_orderBy = coalesce(v_orderBy, CASE WHEN v_consolidation = 'NONE' THEN 'TIME' ELSE 'PK' END);
-- Create a temporary table to hold the SQL statements.
    v_prevCMM = pg_catalog.current_setting('client_min_messages');
    SET client_min_messages TO WARNING;
    DROP TABLE IF EXISTS emaj_temp_sql CASCADE;
    PERFORM pg_catalog.set_config ('client_min_messages', v_prevCMM, FALSE);
    CREATE TEMP TABLE emaj_temp_sql (
      sql_stmt_number        INT,                 -- SQL statement number
      sql_line_number        INT,                 -- line number for the statement (0 for an initial comment)
      sql_rel_kind           TEXT,                -- either "table" or "sequence"
      sql_schema             TEXT,                -- the application schema
      sql_tblseq             TEXT,                -- the table or sequence name
      sql_first_mark         TEXT,                -- the first mark name
      sql_last_mark          TEXT,                -- the last mark name
      sql_group              TEXT,                -- the group name
      sql_nb_changes         BIGINT,              -- the estimated number of changes to process (NULL for sequences)
      sql_file_name_suffix   TEXT,                -- the file name suffix to use to build the output file name if the statement
                                                  --   has to be executed by a COPY statement or a \copy meta-command
      sql_text               TEXT,                -- the generated sql text
      sql_result             BIGINT               -- a column available for caller usage (if needed, some other can be added by the
                                                  --   caller with an ALTER TABLE)
    );
-- Add an initial comment reporting the supplied options.
    v_comment = format('-- Generated SQL for dumping changes in tables group "%s" %sbetween marks "%s" and "%s"%s',
                       p_groupName, CASE WHEN p_tblseqs IS NOT NULL THEN '(subset) ' ELSE '' END, p_firstMark, p_lastMark,
                       CASE WHEN p_optionsList IS NOT NULL AND p_optionsList <> '' THEN ' using options: ' || p_optionsList ELSE '' END);
    INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
      VALUES (0, 0, v_comment);
-- If the requested consolidation level is FULL, then add a SET statement to disable nested-loop nodes in the execution plan.
--   This solves a performance issue with the generated SQL statements for log tables analysis.
    IF v_consolidation = 'FULL' THEN
      v_nbStmt = v_nbStmt + 1;
      INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
        VALUES (v_nbStmt, 1, 'SET enable_nestloop = FALSE;');
    END IF;
-- Process each log table or sequence from the emaj_relation table that enters in the marks range, starting with tables.
    FOR r_rel IN
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind,
             rel_log_schema, rel_log_table, rel_emaj_verb_attnum, rel_pk_cols,
             CASE WHEN rel_kind = 'r' THEN 'table' ELSE 'sequence' END AS kind,
             count(*) OVER (PARTITION BY rel_schema, rel_tblseq) AS nb_time_range,
             row_number() OVER (PARTITION BY rel_schema, rel_tblseq ORDER BY rel_time_range) AS time_range_rank,
             CASE WHEN rel_kind = 'S'
                    THEN NULL
                    ELSE emaj._log_stat_tbl(emaj_relation,
                                            CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                   THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                            CASE WHEN NOT upper_inf(rel_time_range)
                                                   AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                   THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
                 END AS nb_changes
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND ((rel_kind = 'r' AND NOT v_sequencesOnly) OR (rel_kind = 'S' AND NOT v_tablesOnly))
          AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))
          AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
        ORDER BY rel_kind DESC, rel_schema, rel_tblseq, rel_time_range
    LOOP
-- Compute the real mark and gid range for the relation (the relation time range can be shorter that the requested mark range).
      IF lower(r_rel.rel_time_range) <= v_firstMarkTimeId THEN
        v_relFirstMark = p_firstMark;
        v_relFirstEmajGid = v_firstEmajGid;
        v_relFirstTimeId = v_firstMarkTimeId;
      ELSE
        v_relFirstMark = coalesce((SELECT mark_name
                                     FROM emaj.emaj_mark
                                     WHERE mark_time_id = lower(r_rel.rel_time_range)
                                       AND mark_group = r_rel.rel_group
                                  ),'[deleted mark]');
        SELECT time_last_emaj_gid INTO STRICT v_firstEmajGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = lower(r_rel.rel_time_range);
        v_relFirstTimeId = lower(r_rel.rel_time_range);
      END IF;
      IF upper_inf(r_rel.rel_time_range) OR upper(r_rel.rel_time_range) >= v_lastMarkTimeId THEN
        v_relLastMark = p_lastMark;
        v_relLastEmajGid = v_lastEmajGid;
        v_relLastTimeId = v_lastMarkTimeId;
      ELSE
        v_relLastMark = coalesce((SELECT mark_name
                                     FROM emaj.emaj_mark
                                     WHERE mark_time_id = upper(r_rel.rel_time_range)
                                       AND mark_group = r_rel.rel_group
                                  ),'[deleted mark]');
        SELECT time_last_emaj_gid INTO STRICT v_lastEmajGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = upper(r_rel.rel_time_range);
        v_relLastTimeId = upper(r_rel.rel_time_range);
      END IF;
      v_nbStmt = v_nbStmt + 1;
-- Generate the comment and the statement for the table or sequence.
      IF r_rel.rel_kind = 'r' THEN
        v_comment = format('-- Dump changes for table %s.%s between marks "%s" and "%s" (%s changes)',
                           r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark, r_rel.nb_changes);
        v_stmt = emaj._gen_sql_dump_changes_tbl(r_rel.rel_log_schema, r_rel.rel_log_table, r_rel.rel_emaj_verb_attnum,
                                                r_rel.rel_pk_cols, v_relFirstEmajGid, v_relLastEmajGid, v_consolidation,
                                                v_emajColumnsList, v_colsOrder, v_orderBy);
      ELSE
        v_comment = format('-- Dump changes for sequence %s.%s between marks "%s" and "%s"',
                           r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark);
        v_stmt = emaj._gen_sql_dump_changes_seq(r_rel.rel_schema, r_rel.rel_tblseq,
                                                v_relFirstTimeId, v_relLastTimeId, v_consolidation);
      END IF;
-- If the output is a psql script, build the output file name for the \copy command.
      IF v_isPsqlCopy THEN
-- As several files may be generated for a single table or sequence, add a "_nn" to the file name suffix.
        v_copyOutputFile = emaj._build_path_name(v_psqlCopyDir, r_rel.rel_schema || '_' || r_rel.rel_tblseq ||
                             CASE WHEN r_rel.nb_time_range > 1 THEN '_' || r_rel.time_range_rank ELSE '' END || '.changes');
        v_stmt = '\copy (' || v_stmt || ') TO ' || quote_literal(v_copyOutputFile) || coalesce(' (' || v_psqlCopyOptions || ')', '');
      ELSE
        IF p_genSqlOnly THEN
          v_stmt = v_stmt || ';';
        END IF;
      END IF;
-- Record the comment on line 0.
      INSERT INTO emaj_temp_sql
        VALUES (v_nbStmt, 0, r_rel.kind, r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark, r_rel.rel_group,
                r_rel.nb_changes, NULL, v_comment);
-- Record the statement on 1 or several rows, depending on the SQL_FORMAT option.
-- In raw format, newlines and consecutive spaces are removed.
      IF v_sqlFormat = 'RAW' THEN
        v_stmt = replace(v_stmt, E'\n', ' ');
        v_stmt = regexp_replace(v_stmt, '\s{2,}', ' ', 'g');
      END IF;
      INSERT INTO emaj_temp_sql
        SELECT v_nbStmt, row_number() OVER (), r_rel.kind, r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark,
               r_rel.rel_group, r_rel.nb_changes,
               CASE WHEN r_rel.nb_time_range > 1 THEN '_' || r_rel.time_range_rank ELSE '' END || '.changes', line
          FROM regexp_split_to_table(v_stmt, E'\n') AS line;
    END LOOP;
-- If the requested consolidation level is FULL, then add a RESET statement to revert the previous 'SET enable_nestloop = FALSE'.
    IF v_consolidation = 'FULL' THEN
      v_nbStmt = v_nbStmt + 1;
      INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
        VALUES (v_nbStmt, 1, 'RESET enable_nestloop;');
    END IF;
-- Return output parameters.
  p_nbStmt = v_nbStmt;
  p_copyOptions = v_copyOptions;
  p_noEmptyFiles = v_noEmptyFiles;
  p_isPsqlCopy = v_isPsqlCopy;
  RETURN;
  END;
$_gen_sql_dump_changes_group$;

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
--        the absolute pathname of the directory where the files are to be created and the options to used in the COPY TO statements
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
-- Check the copy options parameter doesn't contain unquoted ; that could be used for sql injection.
    IF regexp_replace(p_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: The COPY options parameter format is invalid.';
    END IF;
-- For each table/sequence of the emaj_relation table.
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind,
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
          SELECT string_agg(quote_ident(attname), ',') INTO v_colList
            FROM
              (SELECT attname
                 FROM pg_catalog.pg_attribute
                      JOIN pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
                 WHERE attnum = ANY (indkey)
                   AND indrelid = (r_tblsq.full_relation_name)::regclass
                   AND indisprimary
                   AND attnum > 0
                   AND attisdropped = FALSE
              ) AS t;
          IF v_colList IS NULL THEN
--   The table has no pkey, so get all columns, except generated ones.
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList
              FROM
                (SELECT attname
                   FROM pg_catalog.pg_attribute
                   WHERE attrelid = (r_tblsq.full_relation_name)::regclass
                     AND attnum > 0
                     AND attisdropped = FALSE
                     AND attgenerated = ''
                ) AS t;
          END IF;
--   Dump the table
          v_stmt = format('(SELECT * FROM %I.%I ORDER BY %s)', r_tblsq.rel_schema, r_tblsq.rel_tblseq, v_colList);
          EXECUTE format ('COPY %s TO %L %s',
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

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration()
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_parameters_configuration$
-- This function returns a JSON formatted structure representing all the parameters registered in the emaj_param table.
-- The function can be called by clients like Emaj_web.
-- This is just a wrapper of the internal _export_param_conf() function.
-- Output: the parameters content in JSON format
  BEGIN
    RETURN emaj._export_param_conf();
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration() IS
$$Generates a json structure describing the E-Maj parameters.$$;

CREATE OR REPLACE FUNCTION emaj._export_param_conf()
RETURNS JSON LANGUAGE plpgsql AS
$_export_param_conf$
-- This function generates a JSON formatted structure representing the parameters registered in the emaj_param table.
-- All parameters are extracted, except the "emaj_version" key that is directly linked to the extension and thus is not updatable.
-- The E-Maj version is already displayed in the generated comment at the beginning of the structure.
-- Output: the parameters content in JSON format
  DECLARE
    v_params                 TEXT;
    v_paramsJson             JSON;
    r_param                  RECORD;
  BEGIN
-- Build the header of the JSON structure.
    v_params = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || statement_timestamp() || E'",\n' ||
               E'  "_help": "Known parameter keys: dblink_user_password, history_retention (default = 1 year), alter_log_table, '
                'avg_row_rollback_duration (default = 00:00:00.0001), avg_row_delete_log_duration (default = 00:00:00.00001), '
                'avg_fkey_check_duration (default = 00:00:00.00002), fixed_step_rollback_duration (default = 00:00:00.0025), '
                'fixed_table_rollback_duration (default = 00:00:00.001) and fixed_dblink_rollback_duration (default = 00:00:00.004).",\n';
-- Build the parameters description.
    v_params = v_params || E'  "parameters": [\n';
    FOR r_param IN
      SELECT param_key AS key,
             coalesce(to_json(param_value_text),
                      to_json(param_value_interval),
                      to_json(param_value_boolean),
                      to_json(param_value_numeric),
                      'null') as value
        FROM emaj.emaj_param
             JOIN (VALUES (1::INT, 'dblink_user_password'), (2, 'history_retention'), (3, 'alter_log_table'),
                          (10, 'avg_row_rollback_duration'), (11, 'avg_row_delete_log_duration'),
                          (12, 'avg_fkey_check_duration'), (13, 'fixed_step_rollback_duration'),
                          (14, 'fixed_table_rollback_duration'), (15, 'fixed_dblink_rollback_duration')
                  ) AS p(rank,key) ON (p.key = param_key)
        ORDER BY rank
    LOOP
      v_params = v_params || E'    {\n'
                          ||  '      "key": ' || to_json(r_param.key) || E',\n'
                          ||  '      "value": ' || r_param.value || E'\n'
                          || E'    },\n';
    END LOOP;
    v_params = v_params || E'  ]\n';
-- Build the trailer and remove illicite commas at the end of arrays and attributes lists.
    v_params = v_params || E'}\n';
    v_params = regexp_replace(v_params, E',(\n *(\]|}))', '\1', 'g');
-- Test the JSON format by casting the text structure to json and report a warning in case of problem
-- (this should not fail, unless the function code is bogus).
    BEGIN
      v_paramsJson = v_params::JSON;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '_export_param_conf: The generated JSON structure is not properly formatted. '
                        'Please report the bug to the E-Maj project.';
    END;
--
    RETURN v_paramsJson;
  END;
$_export_param_conf$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(p_paramsJson JSON, p_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_parameters_configuration$
-- This function import a supplied JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- The function can be called by clients like Emaj_web.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - the parameter configuration structure in JSON format
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_nbParam                INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN');
-- Load the parameters.
    SELECT emaj._import_param_conf(p_paramsJson, p_deleteCurrentConf) INTO v_nbParam;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
--
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(JSON,BOOLEAN) IS
$$Import a json structure describing E-Maj parameters to load.$$;

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

CREATE OR REPLACE FUNCTION emaj._verify_all_schemas()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_schemas$
-- The function verifies that all E-Maj schemas only contains E-Maj objects.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  BEGIN
-- Verify that the expected E-Maj schemas still exist.
    RETURN QUERY
      SELECT DISTINCT 'Error: The E-Maj schema "' || sch_name || '" does not exist anymore.' AS msg
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
-- Report a warning if dblink connections are not operational
    IF has_function_privilege('emaj._dblink_open_cnx(text,text)', 'execute') THEN
      SELECT p_status, p_schema INTO v_status, v_schema
        FROM emaj._dblink_open_cnx('emaj_verify_all', current_role);
      CASE v_status
        WHEN 0, 1 THEN
          PERFORM emaj._dblink_close_cnx('emaj_verify_all', v_schema);
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
        WHEN -7 THEN
          RETURN NEXT 'Warning: The role set in the ''dblink_user_password'' parameter has not emaj_adm rights.';
        ELSE
          RETURN NEXT format('Warning: The dblink connection test failed for an unknown reason (status = %s).',
                             v_status::TEXT);
      END CASE;
    ELSE
      RETURN NEXT 'Warning: The dblink connection has not been tested (the current role is not granted emaj_adm).';
    END If;
-- Report a warning if the max_prepared_transaction GUC setting is not appropriate for parallel rollbacks
    IF pg_catalog.current_setting('max_prepared_transactions')::INT <= 1 THEN
      RETURN NEXT format('Warning: The max_prepared_transactions parameter value (%s) on this cluster is too low to launch parallel '
                         'rollback.',
                         pg_catalog.current_setting('max_prepared_transactions'));
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

CREATE OR REPLACE FUNCTION emaj._event_trigger_sql_drop_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_sql_drop_fnct$
-- This function is called by the emaj_sql_drop_trg event trigger.
-- The function blocks any ddl operation that leads to a drop of
--   - an application table or a sequence registered into an active (not stopped) E-Maj group, or a schema containing such tables/sequence
--   - an E-Maj schema, a log table, a log sequence, a log function or a log trigger
-- The drop of emaj schema or extension is managed by another event trigger.
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when dropping their objects.
  DECLARE
    v_groupName              TEXT;
    v_tableName              TEXT;
    r_dropped                RECORD;
  BEGIN
-- Scan all dropped objects.
    FOR r_dropped IN
      SELECT object_type, schema_name, object_name, object_identity, original
        FROM pg_catalog.pg_event_trigger_dropped_objects()
    LOOP
      CASE
        WHEN r_dropped.object_type = 'schema' THEN
-- The object is a schema.
-- Look at the emaj_relation table to verify that the schema being dropped does not belong to any active (not stopped) group.
          SELECT string_agg(DISTINCT rel_group, ', ' ORDER BY rel_group) INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF v_groupName IS NOT NULL THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application schema "%". But it belongs to the active tables'
                            ' groups "%".', r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_schema table to verify that the schema being dropped is not an E-Maj schema containing log tables.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_schema
                  WHERE sch_name = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "%". But dropping an E-Maj schema is not allowed.',
              r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'table' THEN
-- The object is a table.
-- Look at the emaj_relation table to verify that the table being dropped does not currently belong to any active (not stopped) group.
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application table "%.%". But it belongs to the active tables'
                            ' group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_relation table to verify that the table being dropped is not a log table.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_relation
                  WHERE rel_log_schema = r_dropped.schema_name
                    AND rel_log_table = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log table "%.%". But dropping an E-Maj log table is not allowed.',
                            r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'sequence' THEN
-- The object is a sequence.
-- Look at the emaj_relation table to verify that the sequence being dropped does not currently belong to any active (not stopped) group.
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application sequence "%.%". But it belongs to the active'
                            ' tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_relation table to verify that the sequence being dropped is not a log sequence.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_relation
                  WHERE rel_log_schema = r_dropped.schema_name
                    AND rel_log_sequence = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log sequence "%.%". But dropping an E-Maj sequence is not'
                           ' allowed.', r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'function' THEN
-- The object is a function.
-- Look at the emaj_relation table to verify that the function being dropped is not a log function.
          IF EXISTS
            (SELECT 0
               FROM emaj.emaj_relation
               WHERE r_dropped.object_identity = quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_function) || '()'
            ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log function "%". But dropping an E-Maj log function is not'
                            ' allowed.', r_dropped.object_identity;
          END IF;
-- Verify that the function is not public._emaj_protection_event_trigger_fnct() (which is intentionaly not linked to the emaj extension)
          IF r_dropped.object_identity = 'public._emaj_protection_event_trigger_fnct()' THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the public._emaj_protection_event_trigger_fnct() function. '
                            'But dropping this E-Maj technical function is not allowed.';
          END IF;
        WHEN r_dropped.object_type = 'trigger' THEN
-- The object is a trigger.
-- Look at the trigger name pattern to identify emaj trigger.
-- Do not raise an exception if the triggers drop is derived from a drop of a table or a function.
          IF r_dropped.original AND
             (r_dropped.object_identity LIKE 'emaj_log_trg%' OR r_dropped.object_identity LIKE 'emaj_trunc_trg%') THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the "%" E-Maj trigger. But dropping an E-Maj trigger is not allowed.',
              r_dropped.object_identity;
          END IF;
        WHEN r_dropped.object_type = 'table constraint' AND tg_tag = 'ALTER TABLE' THEN
-- The object is a table constraint. It may be primary key or another constraint.
-- Verify that if the targeted table belongs to a rollbackable group, its primary key still exists.
-- The table name can be found in the last part of the object_identity variable,
-- after the '.' that separates the schema and table names, and possibly between double quotes
          v_tableName = substring(r_dropped.object_identity from '\.(?:"?)(.*)(?:"?)');
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = v_tableName
              AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = c.relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = c.oid)
                             WHERE contype = 'p'
                               AND nspname = rel_schema
                               AND c.relname = rel_tblseq
                );
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the primary key for the application table "%.%". But it belongs to'
                            ' the rollbackable tables group "%".', r_dropped.schema_name, v_tableName, v_groupName;
          END IF;
        ELSE
          CONTINUE;
      END CASE;
    END LOOP;
  END;
$_event_trigger_sql_drop_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_sql_drop_fnct() IS
$$E-Maj extension: support of the emaj_sql_drop_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_drop_extension()
RETURNS VOID LANGUAGE plpgsql AS
$emaj_drop_extension$
-- This function drops emaj from the current database, with both installation kinds,
-- - either as EXTENSION (i.e. with a CREATE EXTENSION SQL statement),
-- - or with the alternate psql script.
  DECLARE
    v_nbObject              INTEGER;
    v_roleToDrop            BOOLEAN;
    v_dbList                TEXT;
    v_granteeRoleList       TEXT;
    v_granteeClassList      TEXT;
    v_granteeFunctionList   TEXT;
    v_tspList               TEXT;
    r_object                RECORD;
  BEGIN
-- First perform some checks to verify that the conditions to execute the function are met.
--
-- Check emaj schema is present.
    PERFORM 1 FROM pg_catalog.pg_namespace WHERE nspname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_drop_extension: The schema ''emaj'' doesn''t exist';
    END IF;
--
-- For extensions, check the current role is superuser.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj') THEN
       PERFORM 1 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
       IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be a superuser';
       END IF;
    ELSE
-- Otherwise, check the current role is the owner of the emaj schema, i.e. the role who installed emaj.
      PERFORM 1 FROM pg_catalog.pg_roles, pg_catalog.pg_namespace
        WHERE nspowner = pg_roles.oid AND nspname = 'emaj' AND rolname = current_user;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be the owner of the emaj schema';
      END IF;
    END IF;
--
-- Check that no E-Maj schema contain any non E-Maj object.
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
    PERFORM emaj.emaj_disable_protection_by_event_triggers();
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
-- Revoke also the grant given to emaj_adm on the dblink_connect_u function at install time.
    FOR r_object IN
      SELECT nspname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
        WHERE pronamespace = pg_namespace.oid AND proname = 'dblink_connect_u' AND pronargs = 2
      LOOP
        BEGIN
          EXECUTE 'REVOKE ALL ON FUNCTION ' || r_object.nspname || '.dblink_connect_u(text,text) FROM emaj_adm';
        EXCEPTION
          WHEN insufficient_privilege THEN
            RAISE WARNING 'emaj_drop_extension: Trying to REVOKE grants on function dblink_connect_u() raises an exception. Continue...';
        END;
    END LOOP;
--
-- Check if emaj roles can be dropped.
    v_roleToDrop = true;
--
-- Are emaj_roles also used in other databases of the cluster ?
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_viewer' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_adm' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: emaj_adm role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to other roles ?
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList
      FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_viewer';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_viewer role.',
                    v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList
      FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_adm';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_adm role.',
            v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role has some remaining grants on tables, views or sequences (%).',
                    v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_adm=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_adm role has some remaining grants on tables, views or sequences (%).',
                    v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to functions (other than just dropped emaj ones) ?
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeFunctionList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role has some remaining grants on functions (%).',
                    v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_adm=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeClassList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_adm role has some remaining grants on functions (%).',
                    v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
-- If emaj roles can be dropped, drop them.
    IF v_roleToDrop THEN
-- Revoke the remaining grants set on tablespaces
      SELECT string_agg(spcname, ', ') INTO v_tspList
        FROM pg_catalog.pg_tablespace
        WHERE array_to_string (spcacl,';') LIKE '%emaj_viewer=%' OR array_to_string (spcacl,';') LIKE '%emaj_adm=%';
      IF v_tspList IS NOT NULL THEN
        EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_viewer, emaj_adm';
      END IF;
-- ... and drop both emaj_viewer and emaj_adm roles.
      DROP ROLE emaj_viewer, emaj_adm;
      RAISE WARNING 'emaj_drop_extension: emaj_adm and emaj_viewer roles have been dropped.';
    ELSE
      RAISE WARNING 'emaj_drop_extension: For these reasons, emaj roles are not dropped by this script.';
    END IF;
--
    RETURN;
  END;
$emaj_drop_extension$;
COMMENT ON FUNCTION emaj.emaj_drop_extension() IS
$$Uninstalls the E-Maj components from the current database.$$;

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

GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_sequence_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_sequence_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._sequence_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, p_firstMarkTimeId BIGINT, p_lastMarkTimeId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_table(p_schema TEXT, p_table TEXT, p_startTs TIMESTAMPTZ, p_endTs TIMESTAMPTZ)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_table(p_schema TEXT, p_table TEXT, p_startGroup TEXT, p_startMark TEXT,
                                                   p_endGroup TEXT, p_endMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_table(p_schema TEXT, p_table TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startTs TIMESTAMPTZ, p_endTs TIMESTAMPTZ)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startGroup TEXT, p_startMark TEXT,
                                                      p_endGroup TEXT, p_endMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                  OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN) TO emaj_viewer;

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
    SELECT '4.7.0', TSTZRANGE(clock_timestamp(), null, '[]'), duration
      FROM start_time_data;

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj 4.7.0', 'Upgrade from 4.6.0 completed');

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
