--
-- E-Maj: migration from 4.2.0 to <devel>
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
    v_nbNoError              INT;
    v_nbWarning              INT;
  BEGIN
-- The emaj version registered in emaj_param must be '4.2.0'.
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '4.2.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.2.0',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 11.
    IF current_setting('server_version_num')::int < 110000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 11.', current_setting('server_version');
    END IF;
-- Check E-Maj environment state.
    SELECT count(msg) FILTER (WHERE msg = 'No error detected'),
           count(msg) FILTER (WHERE msg LIKE 'Warning:%')
      INTO v_nbNoError, v_nbWarning
      FROM emaj.emaj_verify_all() AS t(msg);
    IF v_nbNoError = 0 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. You may execute '
                      '"SELECT * FROM emaj.emaj_verify_all();" to get more details.';
    END IF;
    IF v_nbWarning > 0 THEN
      RAISE WARNING 'E-Maj upgrade: the E-Maj environment health check reports warning. You may execute "SELECT * FROM '
                    'emaj.emaj_verify_all();" to get more details.';
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.2.0 started');

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
-- process the emaj_relation table
--

-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_relation_old (LIKE emaj.emaj_relation);

INSERT INTO emaj_relation_old SELECT * FROM emaj.emaj_relation;

-- drop the old table
-- removing linked objects from the extension is a workaround for a bug in postgres extensions management (now fixed in latest versions)
--ALTER EXTENSION emaj DROP FUNCTION _drop_tbl(emaj_relation,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _drop_seq(emaj_relation,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _rlbk_tbl(emaj_relation,bigint,bigint,integer,boolean);
--ALTER EXTENSION emaj DROP FUNCTION _delete_log_tbl(emaj_relation,bigint,bigint,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _rlbk_seq(emaj_relation,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _log_stat_tbl(emaj_relation,bigint,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _gen_sql_tbl(emaj_relation,bigint,bigint);
--ALTER EXTENSION emaj DROP FUNCTION _gen_sql_seq(emaj_relation,bigint,bigint,bigint);

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

-- populate the new table
-- The new rel_pk_cols column content must be built using the rel_sql_gen_pk_conditions column.
--   The rel_sql_rlbk_pk_cols was simplier to process but was set to null when rel_time_range upper bound is not infinite, i.e. when
--   log tables are not usable for rollback anymore.
--   The regular expression used to decode the conditions looks like /o\.(.*?)(?:\)| \|\| )/g, meaning:
--   all characters strings preceeded by 'o.' and followed by either ')' or ' || ' 
WITH decoded_pk_cols AS (
  -- aggregated column names for each emaj_relation row
  SELECT t2.rel_schema, t2.rel_tblseq, t2.rel_time_range, array_agg(col[1]) AS rel_pk_cols FROM (
    -- each column in literal format
    SELECT t1.rel_schema, t1.rel_tblseq, t1.rel_time_range, parse_ident(ident[1]) AS col FROM (
      -- each column in identifier format
      SELECT rel_schema, rel_tblseq, rel_time_range, regexp_matches(rel_sql_gen_pk_conditions,'o\.(.*?)(?:\)| \|\| )', 'g') AS ident
        FROM emaj_relation_old
        WHERE rel_sql_gen_pk_conditions IS NOT NULL
      ) AS t1
    ) AS t2
    GROUP BY 1,2,3
  )
  INSERT INTO emaj.emaj_relation (
                rel_schema, rel_tblseq, rel_time_range,
                rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
                rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_ignored_triggers,
                rel_pk_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns, rel_sql_gen_ins_col,
                rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions, rel_log_seq_last_value
                )
    SELECT      r.rel_schema, r.rel_tblseq, r.rel_time_range,
                rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
                rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_ignored_triggers,
                p.rel_pk_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns, rel_sql_gen_ins_col,
                rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions, rel_log_seq_last_value
      FROM emaj_relation_old r
           LEFT OUTER JOIN decoded_pk_cols p
             ON (r.rel_schema = p.rel_schema AND r.rel_tblseq = p.rel_tblseq AND r.rel_time_range = p.rel_time_range);

-- create indexes
-- Index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration.
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- Index on emaj_relation used to speedup _verify_all_schemas() with large E-Maj configuration.
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- recreate the foreign keys that point on this table
--   there is no fkey for this table

-- and finaly drop the temporary table
DROP TABLE emaj_relation_old;

--
-- process the emaj_rlbk table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_rlbk_old (LIKE emaj.emaj_rlbk);

INSERT INTO emaj_rlbk_old SELECT * FROM emaj.emaj_rlbk;

-- drop the old table
----ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_rlbk_rlbk_id_seq;
DROP TABLE emaj.emaj_rlbk CASCADE;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
-- table containing rollback events
CREATE TABLE emaj.emaj_rlbk (
  rlbk_id                      INT         NOT NULL        -- rollback id
                                           GENERATED BY DEFAULT AS IDENTITY,
  rlbk_groups                  TEXT[]      NOT NULL,       -- groups array to rollback
  rlbk_mark                    TEXT        NOT NULL,       -- mark to rollback to (the original value at rollback time)
  rlbk_mark_time_id            BIGINT      NOT NULL,       -- time stamp id of the mark to rollback to
  rlbk_time_id                 BIGINT,                     -- time stamp id at the rollback exec start
  rlbk_is_logged               BOOLEAN     NOT NULL,       -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN,                    -- flag allowing to rollback to a mark set before alter group operations
                                                           -- (NULL with old rollback functions)
  rlbk_comment                 TEXT,                       -- comment about this rollback
  rlbk_nb_session              INT         NOT NULL,       -- number of requested rollback sessions
  rlbk_nb_table                INT,                        -- number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences in groups
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_eff_nb_sequence         INT,                        -- number of sequences with attributes to change
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_begin_hist_id           BIGINT,                     -- hist_id of the rollback BEGIN event in the emaj_hist
                                                           --   used to know if the rollback has been committed or not
  rlbk_dblink_schema           TEXT,                       -- schema that holds the dblink extension
  rlbk_is_dblink_used          BOOLEAN,                    -- boolean indicating whether dblink connection are used
  rlbk_start_datetime          TIMESTAMPTZ,                -- clock timestamp of the rollback start
  rlbk_end_planning_datetime   TIMESTAMPTZ,                -- clock timestamp of the planning step end
  rlbk_end_locking_datetime    TIMESTAMPTZ,                -- clock timestamp of the locking step end
  rlbk_end_datetime            TIMESTAMPTZ,                -- clock time the rollback has been completed,
  rlbk_messages                TEXT[],                     -- result messages array
  PRIMARY KEY (rlbk_id),
  FOREIGN KEY (rlbk_time_id) REFERENCES emaj.emaj_time_stamp (time_id),
  FOREIGN KEY (rlbk_mark_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk IS
$$Contains description of rollback events.$$;

-- populate the new table
-- For old rollbacks, the rlbk_eff_nb_sequence column is set to the rlbk_nb_sequence value.
INSERT INTO emaj.emaj_rlbk (
         rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
         rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence,
         rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
         rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_end_locking_datetime, rlbk_end_datetime, rlbk_messages)
  SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
         NULL, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence,
         rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
         hist_datetime, null, time_clock_timestamp, rlbk_end_datetime, rlbk_messages
    FROM emaj_rlbk_old
         LEFT OUTER JOIN emaj.emaj_hist ON (rlbk_begin_hist_id = hist_id)
         LEFT OUTER JOIN emaj.emaj_time_stamp ON (rlbk_time_id = time_id);

-- set the rlbk_end_planning_datetime values by reading the emaj_hist table
DO
$do$
  DECLARE
    v_datetime               TIMESTAMPTZ;
    r_rlbk                   RECORD;
  BEGIN
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_begin_hist_id
        FROM emaj.emaj_rlbk
    LOOP
      SELECT hist_datetime INTO v_datetime
        FROM emaj.emaj_hist
        WHERE hist_id > r_rlbk.rlbk_begin_hist_id
          AND hist_function = 'LOCK_GROUP' AND hist_event = 'BEGIN'
        ORDER BY hist_id
        LIMIT 1;
      IF v_datetime IS NULL THEN
        RAISE WARNING 'E-Maj upgrade: emaj_hist scanning unsuccessful';
      ELSE
        UPDATE emaj.emaj_rlbk SET rlbk_end_planning_datetime = v_datetime
          WHERE rlbk_id = r_rlbk.rlbk_id;
      END IF;
    END LOOP;
  END;
$do$;


-- create indexes
-- Partial index on emaj_rlbk targeting in progress rollbacks (not yet committed or marked as aborted).
CREATE INDEX emaj_rlbk_idx1 ON emaj.emaj_rlbk (rlbk_status)
    WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED');

-- recreate the foreign keys that point on this table
ALTER TABLE emaj.emaj_rlbk_session ADD FOREIGN KEY (rlbs_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);
ALTER TABLE emaj.emaj_rlbk_plan ADD FOREIGN KEY (rlbp_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);
ALTER TABLE emaj.emaj_rlbk_stat ADD FOREIGN KEY (rlbt_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);

-- set the last value for the sequence associated to the serial column
SELECT CASE WHEN EXISTS (SELECT 1 FROM emaj.emaj_rlbk)
              THEN setval('emaj.emaj_rlbk_rlbk_id_seq', (SELECT max(rlbk_id) FROM emaj.emaj_rlbk))
       END;

-- and finaly drop the temporary table
DROP TABLE emaj_rlbk_old;

--
-- process the emaj_mark table
--
-- Reset the mark_log_rows_before_next column of the last mark to NULL for each group.
UPDATE emaj.emaj_mark m
  SET mark_log_rows_before_next = NULL
  WHERE (mark_group, mark_time_id) IN 
    (SELECT mark_group, max(mark_time_id)
       FROM emaj.emaj_mark
       WHERE NOT mark_is_deleted
       GROUP BY mark_group
    );

--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--
SELECT pg_catalog.pg_extension_config_dump('emaj_relation','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_rlbk_id_seq','');

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------
DROP TYPE emaj.emaj_rollback_activity_type CASCADE;
CREATE TYPE emaj.emaj_rollback_activity_type AS (
  rlbk_id                      INT,                        -- rollback id
  rlbk_groups                  TEXT[],                     -- groups array to rollback
  rlbk_mark                    TEXT,                       -- mark to rollback to
  rlbk_mark_datetime           TIMESTAMPTZ,                -- timestamp of the mark as recorded into emaj_mark
  rlbk_is_logged               BOOLEAN,                    -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN,                    -- flag allowing to rollback to a mark set before alter group operations
  rlbk_comment                 TEXT,                       -- comment about this rollback
  rlbk_nb_session              INT,                        -- number of requested sessions
  rlbk_nb_table                INT,                        -- number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences in groups
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_eff_nb_sequence         INT,                        -- number of sequences with attributes to change
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_start_datetime          TIMESTAMPTZ,                -- clock timestamp of the rollback start
  rlbk_planning_duration       INTERVAL,                   -- planning phase duration, if completed
  rlbk_locking_duration        INTERVAL,                   -- tables locking phase duration, if completed
  rlbk_elapse                  INTERVAL,                   -- elapse time since the begining of the execution
  rlbk_remaining               INTERVAL,                   -- estimated remaining time to complete the rollback
  rlbk_completion_pct          SMALLINT                    -- estimated percentage of the rollback operation
  );
COMMENT ON TYPE emaj.emaj_rollback_activity_type IS
$$Represents the structure of rows returned by the emaj_rollback_activity() function.$$;

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
DROP FUNCTION IF EXISTS emaj._check_group_names(P_GROUPNAMES TEXT[],P_MAYBENULL BOOLEAN,P_LOCKGROUPS BOOLEAN,P_CHECKLIST TEXT);
DROP FUNCTION IF EXISTS emaj._check_mark_name(P_GROUPNAMES TEXT[],P_MARK TEXT,P_CHECKLIST TEXT);
DROP FUNCTION IF EXISTS emaj._check_marks_range(P_GROUPNAMES TEXT[],INOUT P_FIRSTMARK TEXT,INOUT P_LASTMARK TEXT,OUT P_FIRSTMARKTIMEID BIGINT,OUT P_LASTMARKTIMEID BIGINT);
DROP FUNCTION IF EXISTS emaj._copy_to_file(P_SOURCE TEXT,P_LOCATION TEXT,P_COPYOPTIONS TEXT);
DROP FUNCTION IF EXISTS emaj._build_sql_tbl(P_FULLTABLENAME TEXT,OUT P_RLBKCOLLIST TEXT,OUT P_RLBKPKCOLLIST TEXT,OUT P_RLBKPKCONDITIONS TEXT,OUT P_GENCOLLIST TEXT,OUT P_GENVALLIST TEXT,OUT P_GENSETLIST TEXT,OUT P_GENPKCONDITIONS TEXT,OUT P_NBGENALWAYSIDENTCOL INT);
DROP FUNCTION IF EXISTS emaj.emaj_rollback_group(P_GROUPNAME TEXT,P_MARK TEXT,P_ISALTERGROUPALLOWED BOOLEAN,OUT RLBK_SEVERITY TEXT,OUT RLBK_MESSAGE TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_rollback_groups(P_GROUPNAMES TEXT[],P_MARK TEXT,P_ISALTERGROUPALLOWED BOOLEAN,OUT RLBK_SEVERITY TEXT,OUT RLBK_MESSAGE TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_logged_rollback_group(P_GROUPNAME TEXT,P_MARK TEXT,P_ISALTERGROUPALLOWED BOOLEAN,OUT RLBK_SEVERITY TEXT,OUT RLBK_MESSAGE TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_logged_rollback_groups(P_GROUPNAMES TEXT[],P_MARK TEXT,P_ISALTERGROUPALLOWED BOOLEAN,OUT RLBK_SEVERITY TEXT,OUT RLBK_MESSAGE TEXT);
DROP FUNCTION IF EXISTS emaj._rlbk_groups(P_GROUPNAMES TEXT[],P_MARK TEXT,P_ISLOGGEDRLBK BOOLEAN,P_MULTIGROUP BOOLEAN,P_ISALTERGROUPALLOWED BOOLEAN,OUT RLBK_SEVERITY TEXT,OUT RLBK_MESSAGE TEXT);
DROP FUNCTION IF EXISTS emaj._rlbk_init(P_GROUPNAMES TEXT[],P_MARK TEXT,P_ISLOGGEDRLBK BOOLEAN,P_NBSESSION INT,P_MULTIGROUP BOOLEAN,P_ISALTERGROUPALLOWED BOOLEAN);
DROP FUNCTION IF EXISTS emaj.emaj_snap_log_group(P_GROUPNAME TEXT,P_FIRSTMARK TEXT,P_LASTMARK TEXT,P_DIR TEXT,P_COPYOPTIONS TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._set_time_stamp(p_timeStampType CHAR(1))
RETURNS BIGINT LANGUAGE SQL AS
$$
-- This function inserts a new time stamp in the emaj_time_stamp table and returns the identifier of the new row.
INSERT INTO emaj.emaj_time_stamp (time_last_emaj_gid, time_event)
  SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END, p_timeStampType
    FROM emaj.emaj_global_seq
    RETURNING time_id;
$$;

CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(p_cnxName TEXT, OUT p_status INT, OUT p_schema TEXT)
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
-- Look for the schema holding the dblink functions.
--   (NULL if the dblink_connect_u function is not available, which should not happen)
    SELECT nspname INTO p_schema
      FROM pg_catalog.pg_proc
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
      WHERE proname = 'dblink_connect_u'
      LIMIT 1;
    IF NOT FOUND THEN
      p_status = -1;                      -- dblink is not installed
    ELSIF NOT has_function_privilege(quote_ident(p_schema) || '.dblink_connect_u(text, text)', 'execute') THEN
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
        SELECT param_value_text INTO v_UserPassword
          FROM emaj.emaj_param
          WHERE param_key = 'dblink_user_password';
        IF NOT FOUND THEN
          p_status = -5;                    -- no 'dblink_user_password' parameter is defined in the emaj_param table
        ELSE
-- ... build the connect string
          v_connectString = 'host=localhost port=' || current_setting('port') ||
                            ' dbname=' || current_database() || ' ' || v_userPassword;
-- ... and try to connect
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

CREATE OR REPLACE FUNCTION emaj._check_group_names(p_groupNames TEXT[], p_mayBeNull BOOLEAN, p_lockGroups BOOLEAN,
                                                   p_checkIdle BOOLEAN DEFAULT FALSE, p_checkLogging BOOLEAN DEFAULT FALSE,
                                                   p_checkRollbackable BOOLEAN DEFAULT FALSE, p_checkUnprotected BOOLEAN DEFAULT FALSE)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_group_names$
-- This function performs various checks on a group names array.
-- The NULL, empty strings and duplicate values are removed from the array. If the array is empty raise either an exception or a warning.
-- Checks are then perform to verify:
-- - that all groups exist,
-- - if requested are ROLLBACKABLE,
-- - if requested are in LOGGING or IDLE state,
-- - if requested are not PROTECTED against rollback operations.
-- A SELECT FOR UPDATE is executed if requested, to avoid other sensitive actions in parallel on the same groups.
-- Input: group names array,
--        a boolean that tells whether a NULL array only raise a WARNING,
--        a boolean that tells whether the groups have to be locked,
--        a string that lists the checks to perform, with the following possible values: IDLE, LOGGING, ROLLBACKABLE and UNPROTECTED.
-- Output: validated group names array
  DECLARE
    v_groupList              TEXT;
    v_count                  INT;
  BEGIN
-- Remove duplicates values, NULL and empty strings from the supplied group names array.
    SELECT array_agg(DISTINCT group_name) INTO p_groupNames
      FROM unnest(p_groupNames) AS group_name
      WHERE group_name IS NOT NULL
        AND group_name <> '';
-- Process empty array.
    IF p_groupNames IS NULL THEN
      IF p_mayBeNull THEN
        RAISE WARNING '_check_group_names: No group to process.';
        RETURN NULL;
      ELSE
        RAISE EXCEPTION '_check_group_names: No group to process.';
      END IF;
    END IF;
-- Check that all groups exist.
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
      FROM (  SELECT unnest(p_groupNames)
            EXCEPT
              SELECT group_name
                FROM emaj.emaj_group
           ) AS t(group_name);
    IF v_count > 0 THEN
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" does not exist.', v_groupList;
      ELSE
        RAISE EXCEPTION '_check_group_names: The groups "%" do not exist.', v_groupList;
      END IF;
    END IF;
-- Lock the groups if requested.
    IF p_lockGroups THEN
      PERFORM 0
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
        FOR UPDATE;
    END IF;
-- Checks ROLLBACKABLE type, if requested.
    IF p_checkRollbackable THEN
      SELECT string_agg(group_name,', '  ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND NOT group_is_rollbackable;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" has been created as AUDIT_ONLY.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" have been created as AUDIT_ONLY.', v_groupList;
      END IF;
    END IF;
-- Checks IDLE state, if requested
    IF p_checkIdle THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND group_is_logging;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is not in IDLE state.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are not in IDLE state.', v_groupList;
      END IF;
    END IF;
-- Checks LOGGING state, if requested.
    IF p_checkLogging THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND NOT group_is_logging;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is not in LOGGING state.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are not in LOGGING state.', v_groupList;
      END IF;
    END IF;
-- Checks UNPROTECTED type, if requested.
    IF p_checkUnprotected THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND group_is_rlbk_protected;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is currently protected against rollback operations.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are currently protected against rollback operations.', v_groupList;
      END IF;
    END IF;
--
    RETURN p_groupNames;
  END;
$_check_group_names$;

CREATE OR REPLACE FUNCTION emaj._check_mark_name(p_groupNames TEXT[], p_mark TEXT, p_checkActive BOOLEAN DEFAULT FALSE)
RETURNS TEXT LANGUAGE plpgsql AS
$_check_mark_name$
-- This function verifies that a mark name exists for one or several groups.
-- It processes the EMAJ_LAST_MARK keyword.
-- When several groups are supplied, it checks that the mark represents the same point in time for all groups.
-- Input: array of group names, name of the mark to check, boolean to ask for a mark is active check
-- Output: internal name of the mark
  DECLARE
    v_markName               TEXT = p_mark;
    v_groupList              TEXT;
    v_count                  INTEGER;
  BEGIN
-- Process the 'EMAJ_LAST_MARK' keyword, if needed.
    IF p_mark = 'EMAJ_LAST_MARK' THEN
-- Detect groups that have no recorded mark.
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM
          (  SELECT unnest(p_groupNames)
           EXCEPT
             SELECT mark_group
               FROM emaj.emaj_mark
          ) AS t(group_name);
      IF v_count > 0 THEN
        IF v_count = 1 THEN
          RAISE EXCEPTION '_check_mark_name: The group "%" has no mark.', v_groupList;
        ELSE
          RAISE EXCEPTION '_check_mark_name: The groups "%" have no mark.', v_groupList;
        END IF;
      END IF;
-- Count the number of distinct lastest mark_time_id for all concerned groups.
      SELECT count(DISTINCT mark_time_id) INTO v_count
        FROM
          (SELECT mark_group, max(mark_time_id) AS mark_time_id
             FROM emaj.emaj_mark
             WHERE mark_group = ANY (p_groupNames)
             GROUP BY 1
          ) AS t;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_mark_name: The EMAJ_LAST_MARK does not represent the same point in time for all groups.';
      END IF;
-- Get the name of the last mark for the first group in the array, as we now know that all groups share the same last mark.
      SELECT mark_name INTO v_markName
        FROM emaj.emaj_mark
        WHERE mark_group = p_groupNames[1]
        ORDER BY mark_time_id DESC
        LIMIT 1;
    ELSE
-- For usual mark name (i.e. not EMAJ_LAST_MARK),
-- ... Check that the mark exists for all groups.
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM
          (  SELECT unnest(p_groupNames)
           EXCEPT
             SELECT mark_group
               FROM emaj.emaj_mark
               WHERE mark_name = v_markName
          ) AS t(group_name);
      IF v_count > 0 THEN
        IF v_count = 1 THEN
          RAISE EXCEPTION '_check_mark_name: The mark "%" does not exist for the group "%".', v_markName, v_groupList;
        ELSE
          RAISE EXCEPTION '_check_mark_name: The mark "%" does not exist for the groups "%".', v_markName, v_groupList;
        END IF;
      END IF;
-- ... Check that the mark represents the same point in time for all groups.
      SELECT count(DISTINCT mark_time_id) INTO v_count
        FROM emaj.emaj_mark
        WHERE mark_name = v_markName
          AND mark_group = ANY (p_groupNames);
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_mark_name: The mark "%" does not represent the same point in time for all groups.', v_markName;
      END IF;
    END IF;
-- If requested, check the mark is active for all groups.
    IF p_checkActive THEN
      SELECT string_agg(mark_group,', ' ORDER BY mark_group), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_mark
        WHERE mark_name = v_markName
          AND mark_group = ANY(p_groupNames)
          AND mark_is_deleted;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_mark_name: For the group "%", the mark "%" is DELETED.', v_groupList, v_markName;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_mark_name: For the groups "%", the mark "%" is DELETED.', v_groupList, v_markName;
      END IF;
    END IF;
--
    RETURN v_markName;
  END;
$_check_mark_name$;

CREATE OR REPLACE FUNCTION emaj._check_marks_range(p_groupNames TEXT[], INOUT p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                   p_finiteUpperBound BOOLEAN DEFAULT FALSE,
                                                   OUT p_firstMarkTimeId BIGINT, OUT p_lastMarkTimeId BIGINT,
                                                   OUT p_firstMarkTs TIMESTAMPTZ, OUT p_lastMarkTs TIMESTAMPTZ,
                                                   OUT p_firstMarkEmajGid BIGINT, OUT p_lastMarkEmajGid BIGINT)
LANGUAGE plpgsql AS
$_check_marks_range$
-- This function verifies that a marks range is valid for one or several groups and return useful data about both marks.
-- It checks that both marks defining the bounds exist and are in chronological order.
-- It processes the EMAJ_LAST_MARK keyword.
-- A last mark (upper bound) set to NULL means "the current state". In this case, no specific checks is performed.
-- When several groups are supplied, it checks that the marks represent the same point in time for all groups.
-- Input: array of group names, name of the first mark, name of the last mark
-- Output: name, time id, clock timestamp and emaj_gid for both marks
  BEGIN
-- Check that the first mark is not NULL or empty.
    IF p_firstMark IS NULL OR p_firstMark = '' THEN
      RAISE EXCEPTION '_check_marks_range: The first mark cannot be NULL or empty.';
    END IF;
-- Checks the supplied first mark.
    SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_firstMark) INTO p_firstMark;
-- Get some additional data about the first mark.
-- (use the first group of the array, as we are now sure that all groups share the same mark).
    SELECT mark_time_id, time_clock_timestamp, time_last_emaj_gid INTO p_firstMarkTimeId, p_firstMarkTs, p_firstMarkEmajGid
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupNames[1]
        AND mark_name = p_firstMark;
    IF p_lastMark IS NULL OR p_lastMark = '' THEN
      IF p_finiteUpperBound THEN
        RAISE EXCEPTION '_check_marks_range: The last mark cannot be NULL or empty.';
      END IF;
    ELSE
-- The last mark is not NULL or empty, so check it.
      SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_lastMark) INTO p_lastMark;
-- Get some additional data about the last mark (that may be NULL)
-- (use the first group of the array, as we are now sure that all groups share the same mark).
      SELECT mark_time_id, time_clock_timestamp, time_last_emaj_gid INTO p_lastMarkTimeId, p_lastMarkTs, p_lastMarkEmajGid
        FROM emaj.emaj_mark
             JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
        WHERE mark_group = p_groupNames[1]
          AND mark_name = p_lastMark;
-- And check that the last mark has been set after the first mark.
      IF p_firstMarkTimeId > p_lastMarkTimeId THEN
        RAISE EXCEPTION '_check_marks_range: The start mark "%" (%) has been set after the end mark "%" (%).',
          p_firstMark, p_firstMarkTs, p_lastMark, p_lastMarkTs;
      END IF;
    END IF;
--
    RETURN;
  END;
$_check_marks_range$;

CREATE OR REPLACE FUNCTION emaj._check_tblseqs_filter(INOUT p_tblseqs TEXT[], p_groupNames TEXT[], p_firstMarkTimeId BIGINT,
                                                      p_lastMarkTimeId BIGINT, p_checkInGroupAtStartMark BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql AS
$_check_tblseqs_filter$
-- This function verifies that a schema qualified table/sequence names array is valid for one or several groups and in a marks range.
-- Input: array of table/sequence names, array of group names, time id of the first and last marks,
--        and a boolean indicating whether the tables and sequences must be owned by one group at start mark time
-- Output: the array of table/sequence names, without empty or duplicates (the array is empty if it does not contain any relation
  DECLARE
    v_tblseqErr              TEXT;
    v_count                  INT;
  BEGIN
-- Remove duplicates values, NULL and empty strings from the supplied tables/sequences names array.
    SELECT coalesce(array_agg(DISTINCT table_seq_name), ARRAY[]::TEXT[]) INTO p_tblseqs
      FROM unnest(p_tblseqs) AS table_seq_name
      WHERE table_seq_name IS NOT NULL
        AND table_seq_name <> '';
    IF p_tblseqs = ARRAY[]::TEXT[] THEN
      RAISE WARNING '_check_tblseqs_filter: The table/sequence names array is empty.';
      RETURN;
    END IF;
    IF p_checkInGroupAtStartMark THEN
-- Each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups.
      SELECT string_agg(t,', ' ORDER BY t), count(*) INTO v_tblseqErr, v_count
        FROM
          (  SELECT t
               FROM unnest(p_tblseqs) AS t
           EXCEPT
             SELECT rel_schema || '.' || rel_tblseq
               FROM emaj.emaj_relation
               WHERE rel_time_range @> p_firstMarkTimeId              -- tables/sequences that belong to their group
                 AND rel_group = ANY (p_groupNames)                   -- at the start mark time
          ) AS t2;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_check_tblseqs_filter: % tables/sequences (%) did not belong to any of the selected tables groups '
                        'at start mark time.', v_count, v_tblseqErr;
      END IF;
    ELSE
-- Each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups.
      SELECT string_agg(t,', ' ORDER BY t), count(*) INTO v_tblseqErr, v_count
        FROM
          (  SELECT t
               FROM unnest(p_tblseqs) AS t
           EXCEPT
             SELECT rel_schema || '.' || rel_tblseq
               FROM emaj.emaj_relation
               WHERE rel_time_range && int8range(p_firstMarkTimeId, p_lastMarkTimeId,'[)')
                 AND rel_group = ANY (p_groupNames)
          ) AS t2;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_check_tblseqs_filter: % tables/sequences (%) never belonged to any of the selected tables groups '
                        'during the requested marks range.', v_count, v_tblseqErr;
      END IF;
    END IF;
--
    RETURN;
  END;
$_check_tblseqs_filter$;

CREATE OR REPLACE FUNCTION emaj._copy_to_file(p_source TEXT, p_location TEXT, p_copyOptions TEXT DEFAULT '',
                                              p_removeEmptyFile BOOLEAN DEFAULT FALSE, p_psqlScript BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_copy_to_file$
-- The function performs an elementary COPY TO to unload a table or a statement's result to a file.
-- Inputs: the schema qualified table to unload (double_quoted if needed) or the SQL statement to execute (between parenthesis),
--         the output file pathname,
--         the options for the COPY statement,
--         a boolean indicating whether the output file has to be removed if it is empty,
--         a boolean indicating whether the output file is a psql script.
-- Output: the number of effectively writen file (0 or 1).
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
-- The empty files removal is performed using the pg_file_unlink() function from the adminpack extension.
-- If the output file is a psql script, the '\\' are transformed into '\', using a sed command.
-- The caller is responsible for checking:
--  - the output file location (directory, permissions),
--  - the adminpack extension availability when p_removeEmptyFile is set to TRUE,
--  - the sed command availability when p_psqlScript is set to TRUE.
  DECLARE
    v_stack                  TEXT;
    v_nbRows                 BIGINT;
  BEGIN
-- Check that the caller is allowed to do that by checking the calling function.
    GET DIAGNOSTICS v_stack = PG_CONTEXT;
    IF v_stack NOT LIKE '%emaj.emaj_export_groups_configuration(text,text[])%' AND
       v_stack NOT LIKE '%emaj.emaj_export_parameters_configuration(text)%' AND
       v_stack NOT LIKE '%emaj.emaj_snap_group(text,text,text)%' AND
       v_stack NOT LIKE '%emaj.emaj_dump_changes_group(text,text,text,text,text[],text)%' AND
       v_stack NOT LIKE '%emaj.emaj_gen_sql_dump_changes_group(text,text,text,text,text[],text)%' AND
       v_stack NOT LIKE '%emaj._gen_sql_groups(text[],boolean,text,text,text,text[])%' THEN
      RAISE EXCEPTION '_copy_to_file: the calling function is not allowed to reach this sensitive function.';
    END IF;
-- Perform the requested COPY TO action.
    IF p_psqlScript THEN
-- For psql scripts, the doubled antislashes generated by the COPY processing are transformed into single antislashes.
--   This uses the 'sed' shell command.
      EXECUTE format ('COPY %s TO PROGRAM ''sed "s/\\\\\\\\/\\\\/g" >%s'' %s',
                      p_source, p_location, coalesce (p_copyOptions, ''));
    ELSE
      EXECUTE format ('COPY %s TO %L %s',
                      p_source, p_location, coalesce (p_copyOptions, ''));
    END IF;
-- If the output file is empty, remove it, if requested.
    IF p_removeEmptyFile THEN
      GET DIAGNOSTICS v_nbRows = ROW_COUNT;
      IF v_nbRows = 0 THEN
        PERFORM pg_catalog.pg_file_unlink(p_location);
        RETURN 0;
       END IF;
    END IF;
--
    RETURN 1;
  END;
$_copy_to_file$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(p_schema TEXT, p_tables TEXT[], p_group TEXT, p_properties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB p_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
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
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
--   Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--   vacuum operation.
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
--   And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, FALSE, TRUE, NULL, v_timeId);
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
-- Create the schema.
        PERFORM emaj._create_log_schema(v_logSchema, CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END);
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
    v_uselessTables          TEXT[];
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
-- Remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that the tables currently belong to a tables group (not necessarily the same for all table).
      WITH all_supplied_tables AS (
        SELECT unnest(p_tables) AS table_name
        ),
           tables_in_group AS (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range)
        )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(table_name), ', ' ORDER BY table_name) INTO v_list
        FROM
          (  SELECT table_name
               FROM all_supplied_tables
           EXCEPT
             SELECT rel_tblseq
               FROM tables_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_move_tables: some tables (%) do not currently belong to any tables group.', v_list;
      END IF;
-- Remove tables that already belong to the new group.
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
        INTO v_list, v_uselessTables
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(p_tables)
          AND upper_inf(rel_time_range)
          AND rel_group = p_newGroup;
      IF v_list IS NOT NULL THEN
        RAISE WARNING '_move_tables: some tables (%) already belong to the tables group %.', v_list, p_newGroup;
        SELECT array_agg(tbl) INTO p_tables
          FROM unnest(p_tables) AS tbl
          WHERE tbl <> ALL(v_uselessTables);
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these tables, if any, and count the number of AUDIT_ONLY groups.
-- It locks the target and source tables groups so that no other operation simultaneously occurs these groups
-- (the CTE is needed for the FOR UPDATE clause not allowed when aggregate functions).
    WITH tables_group AS (
      SELECT group_name, group_is_logging, group_is_rollbackable FROM emaj.emaj_group
        WHERE group_name = p_newGroup OR
              group_name IN
               (SELECT DISTINCT rel_group FROM emaj.emaj_relation
                  WHERE rel_schema = p_schema
                    AND rel_tblseq = ANY(p_tables)
                    AND upper_inf(rel_time_range))
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging),
           count(group_name) FILTER (WHERE NOT group_is_rollbackable AND group_name <> p_newGroup)
      INTO v_groups, v_loggingGroups, v_nbAuditOnlyGroups
      FROM tables_group;
-- If at least 1 source tables group is of type AUDIT_ONLY and the target tables group is ROLLBACKABLE, add some checks on tables.
-- They may be incompatible with ROLLBACKABLE groups.
    IF v_nbAuditOnlyGroups > 0 AND v_newGroupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_move_tables');
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_move_tables: No table to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj._create_tbl(p_schema TEXT, p_tbl TEXT, p_groupName TEXT, p_priority INT, p_logDatTsp TEXT,
                                            p_logIdxTsp TEXT, p_ignoredTriggers TEXT[], p_timeId BIGINT, p_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
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
    PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_log_trg');
    PERFORM emaj._handle_trigger_fk_tbl('ADD_TRIGGER', v_fullTableName, 'emaj_log_trg', v_logFnctName);
    PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
    PERFORM emaj._handle_trigger_fk_tbl('ADD_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
    IF p_groupIsLogging THEN
-- If the group is in logging state, set the triggers as ALWAYS triggers, so that they can fire at rollback time.
      PERFORM emaj._handle_trigger_fk_tbl('SET_TRIGGER', v_fullTableName, 'emaj_log_trg', 'ALWAYS');
      PERFORM emaj._handle_trigger_fk_tbl('SET_TRIGGER', v_fullTableName, 'emaj_trunc_trg', 'ALWAYS');
    ELSE
-- If the group is idle, deactivate the triggers (they will be enabled at emaj_start_group time).
      PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_log_trg');
      PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
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

CREATE OR REPLACE FUNCTION emaj._build_sql_tbl(p_fullTableName TEXT, OUT p_pkCols TEXT[], OUT p_rlbkColList TEXT,
                                               OUT p_genColList TEXT, OUT p_genValList TEXT, OUT p_genSetList TEXT,
                                               OUT p_genPkConditions TEXT, OUT p_nbGenAlwaysIdentCol INT)
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
    v_stmt                   TEXT;
    v_nbGenAlwaysExprCol     INTEGER;
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
-- Retrieve from pg_attribute simple columns list and indicators.
-- If the table has no pkey, keep all the sql pieces to NULL (rollback or sql script generation operations being impossible).
    IF p_pkCols IS NOT NULL THEN
      v_stmt = 'SELECT string_agg(''tbl.'' || quote_ident(attname), '','') FILTER (WHERE attgenerated = ''''),'
--                             the columns list for rollback, excluding the GENERATED ALWAYS AS (expression) columns
               '       string_agg(quote_ident(replace(attname,'''''''','''''''''''')), '', '') FILTER (WHERE attgenerated = ''''),'
--                             the INSERT columns list for sql generation, excluding the GENERATED ALWAYS AS (expression) columns
               '       count(*) FILTER (WHERE attidentity = ''a''),'
--                             the number of GENERATED ALWAYS AS IDENTITY columns
               '       count(*) FILTER (WHERE attgenerated <> '''')'
--                             the number of GENERATED ALWAYS AS (expression) columns
               '  FROM ('
               '  SELECT attname, attidentity, %s AS attgenerated'
               '    FROM pg_catalog.pg_attribute'
               '    WHERE attrelid = %s::regclass'
               '      AND attnum > 0 AND NOT attisdropped'
               '  ORDER BY attnum) AS t';
      EXECUTE format(v_stmt,
                     CASE WHEN emaj._pg_version_num() >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                     quote_literal(p_fullTableName))
        INTO p_rlbkColList, p_genColList, p_nbGenAlwaysIdentCol, v_nbGenAlwaysExprCol;
      IF v_nbGenAlwaysExprCol = 0 THEN
-- If the table doesn't contain any generated columns, there is no need for the columns list in the INSERT clause.
        p_genColList = '';
      END IF;
-- Retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements.
-- The logic is too complex to be build with aggregate functions. So loop on all columns.
      p_genValList = '';
      p_genSetList = '';
      FOR r_col IN EXECUTE format(
        ' SELECT attname, format_type(atttypid,atttypmod) AS format_type, attidentity, %s AS attgenerated'
        ' FROM pg_catalog.pg_attribute'
        ' WHERE attrelid = %s::regclass'
        '   AND attnum > 0 AND NOT attisdropped'
        ' ORDER BY attnum',
        CASE WHEN emaj._pg_version_num() >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
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

CREATE OR REPLACE FUNCTION emaj._remove_tbl(p_schema TEXT, p_table TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                            p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_remove_tbl$
-- The function removes a table from a group. It is called during an alter group or a dynamic removal operation.
-- If the group is in idle state, it simply calls the _drop_tbl() function.
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group
-- time for instance).
-- Required inputs: schema and sequence to remove, related group name and logging state,
--                  time stamp id of the operation, main calling function.
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
      v_fullTableName  = quote_ident(p_schema) || '.' || quote_ident(p_table);
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              WHERE nspname = p_schema
                AND relname = p_table
                AND relkind = 'r'
           ) THEN
        PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_log_trg');
        PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
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
                                    rel_ignored_triggers, rel_pk_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
                                    rel_sql_rlbk_columns,
                                    rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
                                    rel_log_seq_last_value)
      SELECT rel_schema, rel_tblseq, int8range(p_timeId, NULL, '[)'), p_newGroup, rel_kind, rel_priority, rel_log_schema,
             v_currentLogTable, rel_log_dat_tsp, v_currentLogIndex, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
             rel_ignored_triggers, rel_pk_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
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
RETURNS VOID LANGUAGE plpgsql AS
$_drop_tbl$
-- The function deletes a timerange for a table. This centralizes the deletion of all what has been created by _create_tbl() function.
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess, time id.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
  BEGIN
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
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
        PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_log_trg');
        PERFORM emaj._handle_trigger_fk_tbl('DROP_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
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
-- Process empty filters as NULL.
    SELECT CASE WHEN p_sequencesIncludeFilter = '' THEN NULL ELSE p_sequencesIncludeFilter END,
           CASE WHEN p_sequencesExcludeFilter = '' THEN NULL ELSE p_sequencesExcludeFilter END
      INTO p_sequencesIncludeFilter, p_sequencesExcludeFilter;
-- Build the list of sequences names satisfying the pattern.
    SELECT array_agg(relname) INTO v_sequences
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND relname ~ p_sequencesIncludeFilter
             AND (p_sequencesExcludeFilter IS NULL OR relname !~ p_sequencesExcludeFilter)
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
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, FALSE, TRUE, NULL, v_timeId);
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
    v_uselessSequences       TEXT[];
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
-- Remove duplicates values, NULL and empty strings from the supplied sequence names array.
      SELECT array_agg(DISTINCT sequence_name) INTO p_sequences
        FROM unnest(p_sequences) AS sequence_name
        WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- Check that the sequences currently belong to a tables group (not necessarily the same for all sequences).
      WITH all_supplied_sequences AS
        (SELECT unnest(p_sequences) AS sequence_name
        ),
           sequences_in_group AS
        (SELECT rel_tblseq
           FROM emaj.emaj_relation
           WHERE rel_schema = p_schema
             AND rel_tblseq = ANY(p_sequences)
             AND upper_inf(rel_time_range)
        )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(sequence_name), ', ' ORDER BY sequence_name) INTO v_list
        FROM
          (  SELECT sequence_name
               FROM all_supplied_sequences
           EXCEPT
             SELECT rel_tblseq
               FROM sequences_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_move_sequences: some sequences (%) do not currently belong to any tables group.', v_list;
      END IF;
-- Remove sequences that already belong to the new group.
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
        INTO v_list, v_uselessSequences
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(p_sequences)
          AND upper_inf(rel_time_range)
          AND rel_group = p_newGroup;
      IF v_list IS NOT NULL THEN
        RAISE WARNING '_move_sequences: some sequences (%) already belong to the tables group %.', v_list, p_newGroup;
        SELECT array_agg(seq) INTO p_sequences
          FROM unnest(p_sequences) AS seq
          WHERE seq <> ALL(v_uselessSequences);
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these sequences, if any.
-- It locks the tables groups so that no other operation simultaneously occurs these groups
-- (the CTE is needed for the FOR UPDATE clause not allowed when aggregate functions).
    WITH tables_group AS
      (SELECT group_name, group_is_logging
         FROM emaj.emaj_group
         WHERE group_name = p_newGroup
            OR group_name IN
                 (SELECT DISTINCT rel_group
                    FROM emaj.emaj_relation
                    WHERE rel_schema = p_schema
                      AND rel_tblseq = ANY(p_sequences)
                      AND upper_inf(rel_time_range)
                 )
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging)
      INTO v_groups, v_loggingGroups
      FROM tables_group;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_move_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
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
      v_tmpTable = 'emaj_tmp_' || pg_backend_pid();
    ELSE
-- With multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
-- but it may be an UNLOGGED table.
      v_tableType = 'UNLOGGED';
      v_tmpTable = 'emaj.emaj_tmp_' || pg_backend_pid();
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
    EXECUTE format('INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)'
                   ' VALUES (%L, %L, %s, %s, ('
                   '   SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM %I.%I'
                   '   )-('
                   '   SELECT tbl_log_seq_last_val FROM emaj.emaj_table'
                   '     WHERE tbl_schema = %L AND tbl_name = %L AND tbl_time_id = %s))',
                   r_rel.rel_schema, r_rel.rel_tblseq, p_beginTimeId, p_endTimeId, r_rel.rel_log_schema, r_rel.rel_log_sequence,
                   r_rel.rel_schema, r_rel.rel_tblseq, p_beginTimeId);
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
    SELECT *
      INTO curr_seq_rec
      FROM emaj._get_current_sequence_state(r_rel.rel_schema, r_rel.rel_tblseq, NULL);
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
-- It is called by the emaj_log_stat_group(), _rlbk_planning(), _rlbk_start_mark() and _gen_sql_groups() functions.
-- These statistics are computed using the log sequence associated to each application table and holes is sequences recorded into
--   emaj_seq_hole at rollback time or rollback consolidation time.
-- Input: row from emaj_relation corresponding to the appplication table to proccess, the time stamp ids defining the time range to examine
--        (a end time stamp id set to NULL indicates the current state)
-- Output: number of log rows between both marks for the table
  DECLARE
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- Get the log sequence last value at begin time id.
    SELECT tbl_log_seq_last_val INTO STRICT v_beginLastValue
      FROM emaj.emaj_table
      WHERE tbl_schema = r_rel.rel_schema
        AND tbl_name = r_rel.rel_tblseq
        AND tbl_time_id = p_beginTimeId;
    IF p_endTimeId IS NULL THEN
-- Last time id is NULL, so examine the current state of the log sequence,
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_sequence)
        INTO STRICT v_endLastValue;
-- ... and count the sum of hole from the start time to now.
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole
        FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema
          AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_begin_time_id >= p_beginTimeId;
    ELSE
-- Last time id is not NULL, so get the log sequence last value at end time id,
      SELECT tbl_log_seq_last_val INTO v_endLastValue
        FROM emaj.emaj_table
        WHERE tbl_schema = r_rel.rel_schema
          AND tbl_name = r_rel.rel_tblseq
          AND tbl_time_id = p_endTimeId;
--  ... and count the sum of hole from the start time to the end time.
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole
        FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema
          AND sqhl_table = r_rel.rel_tblseq
          AND sqhl_begin_time_id >= p_beginTimeId
          AND sqhl_end_time_id <= p_endTimeId;
    END IF;
-- Return the stat row for the table.
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole);
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
      SELECT *
        INTO trg_seq_rec
        FROM emaj._get_current_sequence_state(r_rel.rel_schema, r_rel.rel_tblseq, NULL);
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
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (1): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log table for all tables referenced in the emaj_relation table still exist.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (2): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log function for each table referenced in the emaj_relation table still exists.
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function || '" is not found.'
               AS msg
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (3): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that log and truncate triggers for all tables referenced in the emaj_relation table still exist.
--   Start with the log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (4): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   Then the truncate trigger.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (5): % %',r_object.msg,v_hint; END IF;
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
             'In group "' || rel_group || '", the structure of the application table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (6): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all tables have their primary key if they belong to a rollbackable group.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (7): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- For rollbackable groups, check that no table has been altered as UNLOGGED or dropped and recreated as TEMP table after the tables
-- groups creation.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (8): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- For rollbackable groups, with PG 11-, check that no table has been altered as WITH OIDS after tables groups creation.
    IF emaj._pg_version_num() < 120000 THEN
      FOR r_object IN
        SELECT rel_schema, rel_tblseq, rel_group,
               'In rollbackable group "' || rel_group || '", the table "' ||
               rel_schema || '"."' || rel_tblseq || '" is declared WITH OIDS.' AS msg
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
               JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
          WHERE rel_group = ANY (p_groups)
            AND rel_kind = 'r'
            AND upper_inf(rel_time_range)
            AND group_is_rollbackable
            AND relhasoids
          ORDER BY 1,2,3
      LOOP
        IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %',r_object.msg,v_hint; END IF;
        RETURN NEXT r_object;
      END LOOP;
    END IF;
-- Check that the primary key structure of all tables belonging to rollbackable groups is unchanged.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the primary key of the table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (10): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have the 6 required technical columns. It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log table "' ||
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
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (11): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_group(p_groupName TEXT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_group$
-- This function sets or modifies a comment on a group by updating the group_comment of the emaj_group table.
-- Input: group name, comment
--   To reset an existing comment for a group, the supplied comment can be NULL.
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Update the group_comment column from emaj_group table.
    UPDATE emaj.emaj_group SET group_comment = p_comment WHERE group_name = p_groupName;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_GROUP', p_groupName);
--
    RETURN;
  END;
$emaj_comment_group$;
COMMENT ON FUNCTION emaj.emaj_comment_group(TEXT,TEXT) IS
$$Sets a comment on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_drop_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('DROP_GROUP', 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE, p_checkIdle := TRUE);
-- Effectively drop the group.
    SELECT emaj._drop_group(p_groupName, FALSE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DROP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_drop_group$;
COMMENT ON FUNCTION emaj.emaj_drop_group(TEXT) IS
$$Drops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_force_drop_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_force_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- It differs from emaj_drop_group by the fact that:
--   - the group may be in LOGGING state,
--   - a missing component in the drop processing does not generate any error.
-- This allows to drop a group that is not consistent, following hasardeous operations.
-- This function should not be used, except if the emaj_drop_group fails.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('FORCE_DROP_GROUP', 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Effectively drop the group.
    SELECT emaj._drop_group(p_groupName, TRUE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('FORCE_DROP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_force_drop_group$;
COMMENT ON FUNCTION emaj.emaj_force_drop_group(TEXT) IS
$$Drops an E-Maj group, even in LOGGING state.$$;

CREATE OR REPLACE FUNCTION emaj._start_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
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
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','),
              CASE WHEN p_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE, p_checkIdle := TRUE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- If there is at least 1 group to process, go on.
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(p_groupNames);
-- Purge the history tables, if needed.
      PERFORM emaj._purge_histories();
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
-- For each relation currently belonging to the group,
      FOR r_tblsq IN
        SELECT rel_kind, quote_ident(rel_schema) || '.' || quote_ident(rel_tblseq) AS full_relation_name
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, enable the emaj log and truncate triggers.
          PERFORM emaj._handle_trigger_fk_tbl('ENABLE_TRIGGER', r_tblsq.full_relation_name, 'emaj_log_trg', 'ALWAYS');
          PERFORM emaj._handle_trigger_fk_tbl('ENABLE_TRIGGER', r_tblsq.full_relation_name, 'emaj_trunc_trg', 'ALWAYS');
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
-- Update the state of the group row from the emaj_group table.
      UPDATE emaj.emaj_group
        SET group_is_logging = TRUE
        WHERE group_name = ANY (p_groupNames);
-- Set the first mark for each group.
      PERFORM emaj._set_mark_groups(p_groupNames, v_markName, p_multiGroup, TRUE);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'END', array_to_string(p_groupNames,','),
              v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
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
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'BEGIN', array_to_string(p_groupNames,','));
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE)
      INTO p_groupNames;
-- For all already IDLE groups, generate a warning message and remove them from the list of the groups to process.
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
       AND NOT group_is_logging;
    IF v_count = 1 THEN
      RAISE WARNING '_stop_groups: The group "%" is already in IDLE state.', v_groupList;
    END IF;
    IF v_count > 1 THEN
      RAISE WARNING '_stop_groups: The groups "%" are already in IDLE state.', v_groupList;
    END IF;
    SELECT array_agg(DISTINCT group_name) INTO p_groupNames
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
        AND group_is_logging;
-- Process the LOGGING groups.
    IF p_groupNames IS NOT NULL THEN
-- Check and process the supplied mark name (except if the function is called by emaj_force_stop_group()).
      IF p_mark IS NULL OR p_mark = '' THEN
        p_mark = 'STOP_%';
      END IF;
      IF NOT p_isForced THEN
        SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
      END IF;
-- OK (no error detected and at least one group in logging state)
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
-- For each relation currently belonging to the groups to process.
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
            v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
            BEGIN
              PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_log_trg');
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
              PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
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
        PERFORM emaj._set_mark_groups(p_groupNames, v_markName, p_multiGroup, TRUE);
-- and set the number of log rows to 0 for these marks.
        UPDATE emaj.emaj_mark m
          SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (p_groupNames)
            AND (mark_group, mark_time_id) IN                        -- select only last mark of each concerned group
                  (SELECT mark_group, max(mark_time_id)
                     FROM emaj.emaj_mark
                     WHERE mark_group = ANY (p_groupNames)
                       AND NOT mark_is_deleted
                     GROUP BY mark_group
                  );
      END IF;
-- Set all marks for the groups from the emaj_mark table as 'DELETED' to avoid any further rollback and remove protection, if any.
      UPDATE emaj.emaj_mark
        SET mark_is_deleted = TRUE, mark_is_rlbk_protected = FALSE
        WHERE mark_group = ANY (p_groupNames)
          AND NOT mark_is_deleted;
-- Update the state of the groups rows from the emaj_group table (the rollback protection of rollbackable groups is reset).
      UPDATE emaj.emaj_group
        SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (p_groupNames);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_group$
-- This function sets a protection on a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name
-- Output: 1 if successful, 0 if the group was already in protected state
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkLogging := TRUE, p_checkRollbackable := TRUE);
-- OK, set the protection.
    UPDATE emaj.emaj_group
      SET group_is_rlbk_protected = TRUE
      WHERE group_name = p_groupName
        AND NOT group_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_GROUP', p_groupName, 'Status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_protect_group$;
COMMENT ON FUNCTION emaj.emaj_protect_group(TEXT) IS
$$Sets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_group$
-- This function unsets a protection on a group against accidental rollback.
-- Input: group name
-- Output: 1 if successful, 0 if the group was not already in protected state
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- OK, unset the protection.
    UPDATE emaj.emaj_group
      SET group_is_rlbk_protected = FALSE
      WHERE group_name = p_groupName
        AND group_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_GROUP', p_groupName, 'Status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_unprotect_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_group(TEXT) IS
$$Unsets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_group(p_groupName TEXT, p_mark TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group.
-- Input: group name, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
-- Output: number of processed tables and sequences
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
    SELECT emaj._set_mark_groups(array[p_groupName], v_markName, FALSE, FALSE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'END', p_groupName, v_markName);
--
    RETURN v_nbRel;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_groups(p_groupNames TEXT[], p_mark TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time.
-- Input: array of group names, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
-- Output: number of processed tables and sequences
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
      SELECT emaj._set_mark_groups(p_groupNames, v_markName, TRUE, FALSE) INTO v_nbTblseq;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(p_groupNames,','), p_mark);
--
    RETURN v_nbTblseq;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT) IS
$$Sets a mark on several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_eventToRecord BOOLEAN,
                                                 p_loggedRlbkTargetMark TEXT DEFAULT NULL, p_timeId BIGINT DEFAULT NULL,
                                                 p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into all
-- log tables between this previous mark and the new mark.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like
-- functions that start or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
--        name of the rollback target mark when this mark is created by the logged_rollback functions (NULL by default)
--        time stamp identifier to reuse (NULL by default) (this parameter is set when the mark is a rollback start mark)
--        dblink schema when the mark is set by a rollback operation and dblink connection are used (NULL by default)
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    v_stmt                   TEXT;
  BEGIN
-- If requested by the calling function, record the set mark begin in emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS'
                     ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','), p_mark);
    END IF;
-- Get the time stamp of the operation, if not supplied as input parameter.
    IF p_timeId IS NULL THEN
      SELECT emaj._set_time_stamp('M') INTO p_timeId;
    END IF;
-- Record sequences state as early as possible (no lock protects them from other transactions activity).
-- The join on pg_namespace and pg_class filters the potentially dropped application sequences.
    WITH seq AS                          -- selected sequences
      (SELECT rel_schema, rel_tblseq
         FROM emaj.emaj_relation
              JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
              JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'S'
           AND rel_group = ANY (p_groupNames)
      )
    INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
      SELECT t.*
        FROM seq,
             LATERAL emaj._get_current_sequence_state(rel_schema, rel_tblseq, p_timeId) AS t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- Record the number of log rows for the old last mark of each selected group.
-- The statement updates no row in case of emaj_start_group(s)
    WITH stat_group1 AS                        -- for each group, get the time id of the last active mark
      (SELECT mark_group, max(mark_time_id) AS last_mark_time_id
         FROM emaj.emaj_mark
         WHERE NOT mark_is_deleted
           AND mark_group = ANY(p_groupNames)
         GROUP BY mark_group
      ),
         stat_group2 AS                        -- compute the number of logged changes for all tables currently belonging to these groups
      (SELECT mark_group, last_mark_time_id, coalesce(
                (SELECT sum(emaj._log_stat_tbl(emaj_relation, greatest(last_mark_time_id, lower(rel_time_range)),NULL))
                   FROM emaj.emaj_relation
                   WHERE rel_group = mark_group
                     AND rel_kind = 'r'
                     AND upper_inf(rel_time_range)
                ), 0) AS mark_stat
        FROM stat_group1
      )
    UPDATE emaj.emaj_mark m
      SET mark_log_rows_before_next = mark_stat
      FROM stat_group2 s
      WHERE s.mark_group = m.mark_group
        AND s.last_mark_time_id = m.mark_time_id;
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
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, p_mark, p_timeId, FALSE, FALSE, p_loggedRlbkTargetMark
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
        VALUES (CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(p_groupNames,','), p_mark);
    END IF;
--
    RETURN v_nbSeq + v_nbTbl;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_mark_group(p_groupName TEXT, p_mark TEXT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_mark_group$
-- This function sets or modifies a comment on a mark by updating the mark_comment of the emaj_mark table.
-- Input: group name, mark to comment, comment
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
--   To reset an existing comment for a mark, the supplied comment can be NULL.
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- OK, update the mark_comment from emaj_mark table.
    UPDATE emaj.emaj_mark
      SET mark_comment = p_comment
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('COMMENT_MARK_GROUP', p_groupName, 'Mark ' || p_mark);
--
    RETURN;
  END;
$emaj_comment_mark_group$;
COMMENT ON FUNCTION emaj.emaj_comment_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a comment on a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_datetime TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given date and time.
-- It may return unpredictable result in case of system date or time change.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, date and time
-- Output: mark name, or NULL if there is no mark before the given date and time
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Find the requested mark.
    RETURN mark_name
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND time_clock_timestamp < p_datetime
      ORDER BY time_clock_timestamp DESC
      LIMIT 1;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TIMESTAMPTZ) IS
$$Returns the latest mark name preceeding a point in time.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given mark for a group.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, mark name
--   The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark.
-- Output: mark name, or NULL if there is no mark before the given mark
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Find the requested mark.
    RETURN emaj._get_previous_mark_group(p_groupName, p_mark);
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TEXT) IS
$$Returns the latest mark name preceeding a given mark for a group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function.
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence.
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained.
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_markTimeId             BIGINT;
    v_previousMarkTimeId     BIGINT;
    v_previousMarkName       TEXT;
    v_previousMarkGlobalSeq  BIGINT;
    v_nextMarkTimeId         BIGINT;
    v_nextMarkName           TEXT;
    v_nextMarkGlobalSeq      BIGINT;
    v_timeIdNewMin           BIGINT;
    v_markNewMin             TEXT;
    v_count                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Count the number of marks in the group.
    SELECT count(*) INTO v_count
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName;
-- Check there are at least 2 marks for the group.
    IF v_count < 2 THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: "%" is the only mark of the group. It cannot be deleted.', p_mark;
    END IF;
-- OK, now get the time stamp id of the mark to delete,
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- ... and the timestamp of the future first mark,
    SELECT mark_time_id, mark_name INTO v_timeIdNewMin, v_markNewMin
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name <> p_mark
      ORDER BY mark_time_id
      LIMIT 1;
-- ... and the name, the time id and the last global sequence value of the previous mark,
    SELECT emaj._get_previous_mark_group(p_groupName, p_mark) INTO v_previousMarkName;
    SELECT mark_time_id, time_last_emaj_gid INTO v_previousMarkTimeId, v_previousMarkGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = v_previousMarkName;
-- ... and the name, the time id and the last global sequence value of the next mark,
    SELECT mark_name INTO v_nextMarkName
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id >
              (SELECT mark_time_id
                 FROM emaj.emaj_mark
                 WHERE mark_group = p_groupName
                   AND mark_name = p_mark
              )
      ORDER BY mark_time_id ASC
      LIMIT 1;
    SELECT mark_time_id, time_last_emaj_gid INTO v_nextMarkTimeId, v_nextMarkGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = v_nextMarkName;
-- Effectively delete the mark for the group.
    IF v_previousMarkTimeId IS NULL THEN
-- If the mark to delete is the first one, process its deletion with _delete_before_mark_group(), as the first rows of log tables become
-- useless.
      PERFORM emaj._delete_before_mark_group(p_groupName, v_markNewMin);
    ELSE
-- Otherwise, the mark to delete is an intermediate mark for the group.
-- Process the mark deletion with _delete_intermediate_mark_group().
      PERFORM emaj._delete_intermediate_mark_group(p_groupName, p_mark, v_markTimeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'END', p_groupName, p_mark);
--
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_before_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
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
    v_nbMark                 INT;
  BEGIN
-- Insert a BEGIN event into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Return NULL if the mark name is NULL.
    IF p_mark IS NULL THEN
      RETURN NULL;
    END IF;
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Effectively delete all marks before the supplied mark.
    SELECT emaj._delete_before_mark_group(p_groupName, p_mark) INTO v_nbMark;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', p_groupName,  v_nbMark || ' marks deleted ; ' || p_mark || ' is now the initial mark' );
--
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rename_mark_group(p_groupName TEXT, p_mark TEXT, p_newName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be in LOGGING or not.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly.
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Check the new mark name.
    SELECT emaj._check_new_mark(ARRAY[p_groupName], p_newName) INTO p_newName;
-- OK, update the emaj_mark table.
    UPDATE emaj.emaj_mark
      SET mark_name = p_newName
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- Also rename mark names recorded in the mark_logged_rlbk_target_mark column, if needed.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = p_newName
      WHERE mark_group = p_groupName
        AND mark_logged_rlbk_target_mark = p_mark;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'END', p_groupName, p_mark || ' renamed ' || p_newName);
--
    RETURN;
  END;
$emaj_rename_mark_group$;
COMMENT ON FUNCTION emaj.emaj_rename_mark_group(TEXT,TEXT,TEXT) IS
$$Renames a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_mark_group$
-- This function sets a protection on a mark for a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name, mark to protect
-- Output: 1 if successful, 0 if the mark was already in protected state
-- The group must be ROLLBACKABLE and in LOGGING state.
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark, p_checkActive := TRUE) INTO p_mark;
-- OK, set the protection, if not already set, and return 1, or 0 if the mark was already protected.
    UPDATE emaj.emaj_mark
      SET mark_is_rlbk_protected = TRUE
      WHERE mark_group = p_groupName
        AND mark_name = p_mark
        AND NOT mark_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_MARK_GROUP', p_groupName, 'Mark ' || p_mark || ' ; status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_protect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_protect_mark_group(TEXT,TEXT) IS
$$Sets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_mark_group$
-- This function unsets a protection on a mark for a group against accidental rollback.
-- Input: group name, mark to unprotect
-- Output: 1 if successful, 0 if the mark was already in unprotected state
-- The group must be ROLLBACKABLE and in LOGGING state.
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- OK, unset the protection, and return 1, or 0 if the mark was already unprotected.
    UPDATE emaj.emaj_mark
      SET mark_is_rlbk_protected = FALSE
      WHERE mark_group = p_groupName
        AND mark_name = p_mark
        AND mark_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_MARK_GROUP', p_groupName, 'Mark ' || p_mark || ' ; status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_unprotect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_mark_group(TEXT,TEXT) IS
$$Unsets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(p_groupName TEXT, p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                    p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
--        operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just rollback the group, with boolean: isLoggedRlbk = false, multiGroup = false.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(array[p_groupName], p_mark, FALSE, FALSE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT,BOOLEAN,TEXT) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                     p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
-- operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just rollback the groups, with boolean: isLoggedRlbk = false, multiGroup = true.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(p_groupNames, p_mark, FALSE, TRUE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT,BOOLEAN,TEXT) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(p_groupName TEXT, p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                           p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
--        operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just "logged-rollback" the group, with boolean: isLoggedRlbk = true, multiGroup = false.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(array[p_groupName], p_mark, TRUE, FALSE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT,BOOLEAN,TEXT) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                            p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter
--          group operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just "logged-rollback" the groups, with boolean: isLoggedRlbk = true, multiGroup = true.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(p_groupNames, p_mark, TRUE, TRUE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT,BOOLEAN,TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_groups(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN, p_multiGroup BOOLEAN,
                                             p_isAlterGroupAllowed BOOLEAN, p_comment TEXT,
                                             OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_groups$
-- The function rollbacks all tables and sequences of a groups array up to a mark in the history.
-- It is called by emaj_rollback_group(), emaj_rollback_groups(), emaj_logged_rollback_group() and emaj_logged_rollback_group().
-- It effectively manages the rollback operation for each table or sequence.
-- Its activity is split into smaller functions that are also called by the parallel restore php function.
-- Input: group name,
--        mark to rollback to,
--        a boolean indicating whether the rollback is a logged rollback, a boolean indicating whether the function is a multi_group
--          function
--        a boolean saying whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
    v_rlbkId                 INT;
  BEGIN
-- Check the group names (the groups lock and the state checks are delayed for the later - needed for rollbacks generated by the web
-- application).
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If the group names array is null, immediately return.
    IF p_groupNames IS NULL THEN
       rlbk_severity = 'Notice'; rlbk_message = 0;
       RETURN NEXT;
      RETURN;
    END IF;
-- Check supplied parameter and prepare the rollback operation.
    SELECT emaj._rlbk_init(p_groupNames, p_mark, p_isLoggedRlbk, 1, p_multiGroup, p_isAlterGroupAllowed, p_comment) INTO v_rlbkId;
-- Lock all tables.
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
-- Set a rollback start mark if logged rollback.
    PERFORM emaj._rlbk_start_mark(v_rlbkId, p_multiGroup);
-- Execute the rollback planning.
    PERFORM emaj._rlbk_session_exec(v_rlbkId, 1);
-- Process sequences, complete the rollback operation and return the execution report.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_end(v_rlbkId, p_multiGroup);
  END;
$_rlbk_groups$;

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

CREATE OR REPLACE FUNCTION emaj._rlbk_check(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN, isRollbackSimulation BOOLEAN)
RETURNS TEXT LANGUAGE plpgsql AS
$_rlbk_check$
-- This functions performs checks on group names and mark names supplied as parameter for the emaj_rollback_groups()
-- and emaj_estimate_rollback_groups() functions.
-- It returns the real mark name, or NULL if the groups array is NULL or empty.
  DECLARE
    v_markName               TEXT;
    v_aGroupName             TEXT;
    v_markTimeId             BIGINT;
    v_protectedMarksList     TEXT;
  BEGIN
-- Check the group names and states.
    IF isRollbackSimulation THEN
      SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := FALSE, p_lockGroups := FALSE,
                                     p_checkLogging := TRUE, p_checkRollbackable := TRUE) INTO p_groupNames;
    ELSE
      SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                     p_checkLogging := TRUE, p_checkRollbackable := TRUE, p_checkUnprotected := TRUE) INTO p_groupNames;
    END IF;
    IF p_groupNames IS NOT NULL THEN
-- Check the mark name.
      SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_mark, p_checkActive := TRUE) INTO v_markName;
      IF NOT isRollbackSimulation THEN
-- Check that for each group that the rollback wouldn't delete protected marks (check disabled for rollback simulation).
        FOREACH v_aGroupName IN ARRAY p_groupNames LOOP
--   Get the target mark time id,
          SELECT mark_time_id INTO v_markTimeId
            FROM emaj.emaj_mark
            WHERE mark_group = v_aGroupName
              AND mark_name = v_markName;
--   ... and look at the protected mark.
          SELECT string_agg(mark_name,', ' ORDER BY mark_name) INTO v_protectedMarksList
            FROM
              (SELECT mark_name
                 FROM emaj.emaj_mark
                 WHERE mark_group = v_aGroupName
                   AND mark_time_id > v_markTimeId
                   AND mark_is_rlbk_protected
                 ORDER BY mark_time_id
              ) AS t;
          IF v_protectedMarksList IS NOT NULL THEN
            RAISE EXCEPTION '_rlbk_check: Protected marks (%) for the group "%" block the rollback to the mark "%".',
              v_protectedMarksList, v_aGroupName, v_markName;
          END IF;
        END LOOP;
      END IF;
-- If the isAlterGroupAllowed flag is not explicitely set to true, check that the rollback would not cross any structure change for
-- the groups.
      IF p_isAlterGroupAllowed IS NULL OR NOT p_isAlterGroupAllowed THEN
        SELECT mark_time_id INTO v_markTimeId
          FROM emaj.emaj_mark
          WHERE mark_group = p_groupNames[1]
            AND mark_name = v_markName;
        IF EXISTS
             (SELECT 0
                FROM emaj.emaj_relation_change
                WHERE rlchg_time_id > v_markTimeId
                  AND rlchg_group = ANY (p_groupNames)
                  AND rlchg_rlbk_id IS NULL
             ) THEN
          RAISE EXCEPTION '_rlbk_check: This rollback operation would cross some previous structure group change operations,'
                          ' which is not allowed by the current function parameters.';
        END IF;
      END IF;
    END IF;
--
    RETURN v_markName;
  END;
$_rlbk_check$;

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
-- Non deferrable fkeys and deferrable fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped.
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
            p_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, pg_get_constraintdef(r_fk.conoid),
            r_fk.reltuples
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

CREATE OR REPLACE FUNCTION emaj._rlbk_set_batch_number(p_rlbkId INT, p_batchNumber INT, p_schema TEXT, p_table TEXT,
                                                       p_isReplRoleReplica BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_set_batch_number$
-- This function updates the emaj_rlbk_plan table to set the batch_number for one table.
-- It also looks for other tables to rollback that are linked to this table by foreign keys to force them to be allocated to the same
--   batch number.
-- If the rollback operation for the table is performed with a session_replication_role set to replica, there is no need to force
--   referenced and referencing tables to be in the same batch.
-- The function is called by _rlbk_planning().
-- As those linked tables can also be linked to other tables by other foreign keys, the function has to be recursiley called.
  DECLARE
    v_fullTableName          TEXT;
    r_tbl                    RECORD;
  BEGIN
-- Set the batch number to this application table (there is a 'LOCK_TABLE' step and potentialy a 'RLBK_TABLE' step).
    UPDATE emaj.emaj_rlbk_plan
      SET rlbp_batch_number = p_batchNumber
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_schema = p_schema
        AND rlbp_table = p_table;
-- If the rollback is not performed with session_replication_role set to replica, look for all other application tables linked by foreign
-- key relationships.
    IF NOT p_isReplRoleReplica THEN
      v_fullTableName = quote_ident(p_schema) || '.' || quote_ident(p_table);
      FOR r_tbl IN
        SELECT rlbp_schema, rlbp_table, rlbp_is_repl_role_replica
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_batch_number IS NULL            -- not yet allocated
            AND (rlbp_schema, rlbp_table) IN         -- list of (schema,table) linked to the original table by fkeys
              (  SELECT nspname, relname
                   FROM pg_catalog.pg_constraint
                        JOIN pg_catalog.pg_class t ON (t.oid = conrelid)
                        JOIN pg_catalog.pg_namespace n ON (relnamespace = n.oid)
                   WHERE contype = 'f'
                     AND confrelid = v_fullTableName::regclass
               UNION ALL
                 SELECT nspname, relname
                   FROM pg_catalog.pg_constraint
                        JOIN pg_catalog.pg_class t ON (t.oid = confrelid)
                        JOIN pg_catalog.pg_namespace n ON (relnamespace = n.oid)
                   WHERE contype = 'f'
                     AND conrelid = v_fullTableName::regclass
              )
      LOOP
-- Recursive call to allocate these linked tables to the same batch_number.
        PERFORM emaj._rlbk_set_batch_number(p_rlbkId, p_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table, r_tbl.rlbp_is_repl_role_replica);
      END LOOP;
    END IF;
--
    RETURN;
  END;
$_rlbk_set_batch_number$;

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(p_rlbkId INT, p_multiGroup BOOLEAN)
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
-- Get the dblink usage characteristics for the current rollback.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema INTO v_isDblinkUsed, v_dblinkSchema
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get a time stamp for the rollback operation.
    v_stmt = 'SELECT emaj._set_time_stamp(''R'')';
    SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
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
-- If rollback is a "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time.
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, p_multiGroup, TRUE, NULL, v_timeId, v_dblinkSchema);
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
    v_msg                    TEXT;
    v_msgList                TEXT;
    r_msg                    RECORD;
  BEGIN
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
-- Delete the marks that are suppressed by the rollback (the related sequences have been already deleted by rollback functions)
-- with a logging in the history.
      WITH deleted AS
        (DELETE FROM emaj.emaj_mark
           WHERE mark_group = ANY (v_groupNames)
             AND mark_time_id > v_markTimeId
           RETURNING mark_time_id, mark_group, mark_name
        ),
           sorted_deleted AS                                         -- the sort is performed to produce stable results in regression tests
        (SELECT mark_group, mark_name
           FROM deleted
           ORDER BY mark_time_id, mark_group
        )
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END,
               'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted'
        FROM sorted_deleted;
-- And reset the mark_log_rows_before_next column for the new last mark.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND (mark_group, mark_time_id) IN                -- select only the last non deleted mark of each concerned group
              (SELECT mark_group, max(mark_time_id)
                 FROM emaj.emaj_mark
                 WHERE mark_group = ANY (v_groupNames)
                   AND NOT mark_is_deleted
                 GROUP BY mark_group
              );
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
               (CASE rlchg_change_kind
                  WHEN 'ADD_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'ADD_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
          FROM
-- Suppress duplicate ADD_TABLE / MOVE_TABLE / REMOVE_TABLE or ADD_SEQUENCE / MOVE_SEQUENCE / REMOVE_SEQUENCE for same table or sequence,
-- by keeping the most recent changes.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND rlchg_group = ANY (v_groupNames)
                      AND rlchg_tblseq <> ''
                      AND rlchg_rlbk_id IS NULL
                      AND rlchg_change_kind IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                  ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE rlchg_time_id = time_id
      UNION
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               (CASE rlchg_change_kind
                  WHEN 'CHANGE_PRIORITY' THEN
                    'Tables group change not rolled back: E-Maj priority for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_DATA_TABLESPACE' THEN
                    'Tables group change not rolled back: log data tablespace for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_INDEX_TABLESPACE' THEN
                    'Tables group change not rolled back: log index tablespace for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_IGNORED_TRIGGERS' THEN
                    'Tables group change not rolled back: ignored triggers list for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
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
                      AND rlchg_rlbk_id IS NULL
                      AND rlchg_change_kind NOT IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                 ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2
        ORDER BY rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq
    LOOP
      v_messages = array_append(v_messages, 'Warning: ' || r_msg.message);
    END LOOP;
-- Update the groups structure changes that are been covered by the rollback.
    UPDATE emaj.emaj_relation_change
      SET rlchg_rlbk_id = p_rlbkId
      WHERE rlchg_time_id > v_markTimeId
        AND rlchg_group = ANY (v_groupNames)
        AND rlchg_rlbk_id IS NULL;
-- If rollback is a "logged" rollback, automatically set a mark representing the tables state just after the rollback.
-- This mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time.
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_DONE';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, p_multiGroup, TRUE, v_mark);
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
      VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END',
              array_to_string(v_groupNames,','), 'Rollback_id ' || p_rlbkId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_comment_rollback(p_rlbkId INT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_rollback$
-- This function sets or modifies a comment on a rollback by updating the rlbk_comment of the emaj_rlbk table.
-- Input: rollback identifier, comment
--   To reset an existing comment for a rollback, set the supplied comment to NULL.
  BEGIN
-- Check the rollback id.
    PERFORM 0
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_rollback: The rollback identifier % does not exist.',
        p_rlbkId;
    END IF;
-- Update the rlbk_comment column from the emaj_rlbk table.
    UPDATE emaj.emaj_rlbk SET rlbk_comment = p_comment WHERE rlbk_id = p_rlbkId;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_ROLLBACK', p_rlbkId);
--
    RETURN;
  END;
$emaj_comment_rollback$;
COMMENT ON FUNCTION emaj.emaj_comment_rollback(INT, TEXT) IS
$$Sets a comment on an E-Maj Rollback.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_consolidate_rollback_group(p_groupName TEXT, p_endRlbkMark TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$emaj_consolidate_rollback_group$
-- This function "consolidates" a rollback for a group. It transforms an already completed logged rollback into an unlogged rollback.
-- All marks and update logs between a mark used as reference by an unlogged rollback operation and the final mark set by this rollback
-- are suppressed.
-- The group may be in any state (logging or idle).
-- Input: group name, name of the final mark set by the rollback operation to consolidate
-- Output: number of sequences and tables effectively processed
  DECLARE
    v_firstMark              TEXT;
    v_lastMark               TEXT;
    v_nbMark                 INT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the supplied end rollback mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_endRlbkMark) INTO v_lastMark;
-- Check that no group is damaged.
    PERFORM 0
      FROM emaj._verify_groups(ARRAY[p_groupName], TRUE);
-- Check the supplied mark is known as an end rollback mark.
    SELECT mark_logged_rlbk_target_mark INTO v_firstMark
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = v_lastMark;
    IF v_firstMark IS NULL THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The mark "%" for the group "%" is not an end rollback mark.',
        v_lastMark, p_groupName;
    END IF;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'BEGIN', p_groupName, 'Erase all between marks ' || v_firstMark || ' and ' || v_lastMark);
-- Check the first mark really exists (it should, because deleting or renaming a mark must update the mark_logged_rlbk_mark_name column).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_mark
            WHERE mark_group = p_groupName
              AND mark_name = v_firstMark
         ) THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The rollback target mark "%" for the group "%" has not been found.',
        v_firstMark, p_groupName;
    END IF;
-- Perform the consolidation operation.
    SELECT * INTO v_nbMark, v_nbTbl
      FROM emaj._delete_between_marks_group(p_groupName, v_firstMark, v_lastMark);
-- Get the number of sequences belonging to the group.
    SELECT group_nb_sequence INTO v_nbSeq
      FROM emaj.emaj_group
      WHERE group_name = p_groupName;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'END', p_groupName,
              v_nbTbl || ' tables and ' || v_nbSeq || ' sequences processed ; ' || v_nbMark || ' marks deleted');
--
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_consolidate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_consolidate_rollback_group(TEXT,TEXT) IS
$$Consolidate a rollback for a group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_reset_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves.
-- It calls the emaj_rst_group function to do the job.
-- Input: group name
-- Output: number of processed tables
  DECLARE
    v_nbRel                  INT = 0;
    v_eventTriggers          TEXT[];
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('RESET_GROUP', 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE, p_checkIdle := TRUE);
-- Perform the reset operation.
    SELECT emaj._reset_groups(ARRAY[p_groupName]) INTO v_nbRel;
-- Drop the log schemas that would have been emptied by the _reset_groups() call.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
    PERFORM emaj._drop_log_schemas('RESET_GROUP', FALSE);
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RESET_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj._log_stat_type LANGUAGE plpgsql AS
$_log_stat_groups$
-- This function effectively returns statistics on row updates executed between 2 marks or between a mark and the current state for 1
-- or several groups.
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time.
-- The function is directly called by Emaj_web.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function, the 2 mark names defining a
--          range
--   a NULL value or an empty string as last_mark indicates the current state
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark
--   parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of log rows by table (including tables with 0 rows to rollback)
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
  BEGIN
-- Check the groups name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT *
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark);
-- For each table of the group, get the number of log rows and return the statistics.
-- Shorten the timeframe if the table did not belong to the group on the entire requested time frame.
      RETURN QUERY
        SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table,
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
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstEmajGid
                    ELSE (SELECT time_last_emaj_gid
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = lower(rel_time_range)
                         )
                 END AS stat_first_mark_gid,
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
                         THEN (SELECT time_last_emaj_gid
                                 FROM emaj.emaj_time_stamp
                                 WHERE time_id = upper(rel_time_range)
                              )
                    ELSE v_lastEmajGid
                 END AS stat_last_mark_gid,
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

CREATE OR REPLACE FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj._detailed_log_stat_type LANGUAGE plpgsql AS
$_detailed_log_stat_groups$
-- This function effectively returns statistics on row updates executed between 2 marks as viewed through the log tables for one or several
-- groups.
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- The function is directly called by Emaj_web.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function,
--        the 2 mark names defining a range
--   a NULL value or an empty string as last_mark indicates the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of updates by user and table
-- This function may be directly called by the Emaj_web client.
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
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_lowerBoundMarkTs, v_lowerBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = lower(r_tblsq.rel_time_range);
        END IF;
-- Compute the upper bound for this table.
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- No supplied end mark and the table has not been removed from its group => the current state.
          v_upperBoundMark = NULL;
          v_upperBoundMarkTs = NULL;
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
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_upperBoundMarkTs, v_upperBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = upper(r_tblsq.rel_time_range);
        ELSE
-- Usual case: the table belonged to the group at statistics end mark.
          v_upperBoundMark = p_lastMark;
          v_upperBoundMarkTs = v_lastMarkTs;
          v_upperBoundGid = v_lastEmajGid;
        END IF;
-- Build the statement.
        v_stmt= 'SELECT ' || quote_literal(r_tblsq.rel_group) || '::TEXT AS stat_group, '
             || quote_literal(r_tblsq.rel_schema) || '::TEXT AS stat_schema, '
             || quote_literal(r_tblsq.rel_tblseq) || '::TEXT AS stat_table, '
             || quote_literal(r_tblsq.rel_log_schema) || '::TEXT AS stat_log_schema, '
             || quote_literal(r_tblsq.rel_log_table) || '::TEXT AS stat_log_table, '
             || quote_literal(v_lowerBoundMark) || '::TEXT AS stat_first_mark, '
             || quote_literal(v_lowerBoundMarkTs) || '::TIMESTAMPTZ AS stat_first_mark_datetime, '
             || v_lowerBoundGid || '::BIGINT AS stat_first_mark_gid, '
             || coalesce(quote_literal(v_upperBoundMark),'NULL') || '::TEXT AS stat_last_mark, '
             || coalesce(quote_literal(v_upperBoundMarkTs),'NULL') || '::TIMESTAMPTZ AS stat_last_mark_datetime, '
             || coalesce(v_upperBoundGid::text,'NULL') || '::BIGINT AS stat_last_mark_gid, '
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
-- And execute the statement.
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END LOOP;
    END IF;
-- Final return.
    RETURN;
  END;
$_detailed_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_mark TEXT, p_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary.
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: group names array, a boolean indicating whether the groups array may contain several groups,
--        the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
  BEGIN
-- Check the group names (the groups state checks are delayed for later).
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If the group names array is null, immediately return NULL.
    IF p_groupNames IS NULL THEN
      RETURN NULL;
    END IF;
-- Check supplied group names and mark parameters with the isAlterGroupAllowed and isRollbackSimulation flags set to true.
    SELECT emaj._rlbk_check(p_groupNames, p_mark, TRUE, TRUE) INTO v_markName;
-- Compute the number of tables and sequences contained in groups to rollback.
    SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTbl, v_nbSeq
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames);
-- Compute a random negative rollback-id (not to interfere with ids of real rollbacks).
    SELECT (random() * -2147483648)::INT INTO v_rlbkId;
--
-- Simulate a rollback planning.
--
    BEGIN
-- Insert a row into the emaj_rlbk table for this simulated rollback operation.
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
                                  rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence)
        SELECT v_rlbkId, p_groupNames, v_markName, mark_time_id, p_isLoggedRlbk, FALSE, 1, v_nbTbl, v_nbSeq
          FROM emaj.emaj_mark
          WHERE mark_group = p_groupNames[1]
            AND mark_name = v_markName;
-- Call the _rlbk_planning function to build the simulated plan with the duration estimate for elementary steps.
      PERFORM emaj._rlbk_planning(v_rlbkId);
-- Compute the sum of the duration estimates of all elementary steps (except LOCK_TABLE).
      SELECT coalesce(sum(rlbp_estimated_duration), '0 SECONDS'::INTERVAL) INTO v_estimDuration
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId
          AND rlbp_step <> 'LOCK_TABLE';
-- Cancel the effect of the rollback planning.
      RAISE EXCEPTION '';
    EXCEPTION
      WHEN RAISE_EXCEPTION THEN                 -- catch the raised exception and continue
    END;
-- Get the "fixed_table_rollback_duration" parameter from the emaj_param table.
    SELECT coalesce(
             (SELECT param_value_interval
                FROM emaj.emaj_param
                WHERE param_key = 'fixed_table_rollback_duration'
             ),'1 millisecond'::INTERVAL) INTO v_fixed_table_rlbk;
-- Compute the final estimated duration, by adding the minimum cost for LOCK_TABLE steps.
    v_estimDuration = v_estimDuration + (v_nbTbl * v_fixed_table_rlbk);
--
    RETURN v_estimDuration;
  END;
$_estimate_rollback_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$emaj_rollback_activity$
-- This function returns the list of rollback operations currently in execution, with information about their progress.
-- It returns a set of emaj_rollback_activity_type records.
  BEGIN
-- Cleanup the freshly completed rollback operations, if any.
    PERFORM emaj._cleanup_rollback_state();
-- And retrieve information regarding the rollback operations that are always in execution.
    RETURN QUERY
      SELECT *
        FROM emaj._rollback_activity();
  END;
$emaj_rollback_activity$;
COMMENT ON FUNCTION emaj.emaj_rollback_activity() IS
$$Returns the list of rollback operations currently in execution, with information about their progress.$$;

CREATE OR REPLACE FUNCTION emaj._rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$_rollback_activity$
-- This function effectively builds the list of rollback operations currently in execution.
-- It is called by the emaj_rollback_activity() function.
-- This is a separate function to help in testing the feature (avoiding the effects of _cleanup_rollback_state()).
-- The number of parallel rollback sessions is not taken into account here,
--   as it is difficult to estimate the benefit brought by several parallel sessions.
  DECLARE
    v_now                    TIMESTAMPTZ;        -- The clock timestamp at the function entry
    v_ipsDuration            INTERVAL;           -- In Progress Steps Duration
    v_nyssDuration           INTERVAL;           -- Not Yes Started Steps Duration
    v_nbNyss                 INT;                -- Number of Net Yes Started Steps
    v_ctrlDuration           INTERVAL;
    v_currentTotalEstimate   INTERVAL;
    r_rlbk                   emaj.emaj_rollback_activity_type;
  BEGIN
    v_now = clock_timestamp();
-- Retrieve all not completed rollback operations (ie in 'PLANNING', 'LOCKING' or 'EXECUTING' state).
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_groups, rlbk_mark, tm.time_clock_timestamp, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence,
             rlbk_status, rlbk_start_datetime,
             rlbk_end_planning_datetime - rlbk_start_datetime AS rlbk_planning_duration,
             rlbk_end_locking_datetime - rlbk_end_planning_datetime AS rlbk_locking_duration,
             v_now - rlbk_start_datetime AS "elapse", NULL, 0
        FROM emaj.emaj_rlbk
             JOIN emaj.emaj_time_stamp tm ON (tm.time_id = rlbk_mark_time_id)
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING')
        ORDER BY rlbk_id
    LOOP
-- Compute the estimated remaining duration for rollback operations in 'PLANNING' state, the remaining duration is NULL.
      IF r_rlbk.rlbk_status IN ('LOCKING', 'EXECUTING') THEN
-- Estimated duration of remaining work of in progress steps.
        SELECT coalesce(
               sum(CASE WHEN rlbp_start_datetime + rlbp_estimated_duration - v_now > '0'::INTERVAL
                        THEN rlbp_start_datetime + rlbp_estimated_duration - v_now
                        ELSE '0'::INTERVAL END),'0'::INTERVAL) INTO v_ipsDuration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_start_datetime IS NOT NULL
            AND rlbp_duration IS NULL;
-- Estimated duration and number of not yet started steps.
        SELECT coalesce(sum(rlbp_estimated_duration),'0'::INTERVAL), count(*) INTO v_nyssDuration, v_nbNyss
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_start_datetime IS NULL
            AND rlbp_step NOT IN ('CTRL-DBLINK','CTRL+DBLINK');
-- Estimated duration of inter-step duration for not yet started steps.
        SELECT coalesce(sum(rlbp_estimated_duration) * v_nbNyss / sum(rlbp_estimated_quantity),'0'::INTERVAL)
          INTO v_ctrlDuration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_step IN ('CTRL-DBLINK','CTRL+DBLINK');
-- Update the global remaining duration estimate.
        r_rlbk.rlbk_remaining = v_ipsDuration + v_nyssDuration + v_ctrlDuration;
      END IF;
-- Compute the completion pct for rollback operations in 'PLANNING' or 'LOCKING' state, the completion_pct = 0.
      IF r_rlbk.rlbk_status = 'EXECUTING' THEN
-- First compute the new total duration estimate, using the estimate of the remaining work,
        SELECT v_now - rlbk_start_datetime + r_rlbk.rlbk_remaining INTO v_currentTotalEstimate
          FROM emaj.emaj_rlbk
          WHERE rlbk_id = r_rlbk.rlbk_id;
-- ... and then the completion pct.
        IF v_currentTotalEstimate <> '0'::INTERVAL THEN
          SELECT 100 - (extract(epoch FROM r_rlbk.rlbk_remaining) * 100
                      / extract(epoch FROM v_currentTotalEstimate))::SMALLINT
            INTO r_rlbk.rlbk_completion_pct;
        END IF;
      END IF;
      RETURN NEXT r_rlbk;
    END LOOP;
--
    RETURN;
  END;
$_rollback_activity$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                                p_optionsList TEXT, p_tblseqs TEXT[])
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- The SQL statements are stored a temporary table named emaj_temp_sql.
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
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations).
-- Output: Message with the number of generated SQL statements.
  DECLARE
    v_nbStmt                 INT;
    v_isPsqlCopy             BOOLEAN;
  BEGIN
-- Call the _gen_sql_dump_changes_group() function to proccess options and build the SQL statements.
    SELECT p_nbStmt, p_isPsqlCopy
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, TRUE)
      INTO v_nbStmt, v_isPsqlCopy;
-- Just add an index on the temporary table to help its use by the client.
    CREATE INDEX ON emaj_temp_sql(sql_stmt_number, sql_line_number);
-- Return a formatted message.
    RETURN format('%s SQL statements are available in the "emaj_temp_sql" temporary table', v_nbStmt);
  END;
$emaj_gen_sql_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[]) IS
$$Generate SQL statements to dump recorded changes between two marks for application tables and sequences of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                                p_optionsList TEXT, p_tblseqs TEXT[], p_scriptLocation TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
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
-- If there are psql \copy meta-commands, ask the _copy_to_file() function to remove the doubled antislash characters.
      BEGIN
        PERFORM emaj._copy_to_file('(SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number)',
                                   p_scriptLocation, NULL, FALSE, TRUE);
      EXCEPTION
        WHEN OTHERS THEN
--   If it fails (typically because the sed command is not available), write the script as is, and warn the user about the doubled
--     antislashes he has to remove.
          PERFORM emaj._copy_to_file('(SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number)',
                                     p_scriptLocation, NULL, FALSE, FALSE);
          RAISE WARNING 'emaj_gen_sql_dump_changes_group: the shell sed command does not seem to exist.'
                        ' Generated doubled antislash characters will need to be removed manually.';
      END;
    ELSE
-- There is no psql meta-command so no antislashes to remove.
      PERFORM emaj._copy_to_file('(SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number)',
                                 p_scriptLocation, NULL, FALSE, FALSE);
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
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_dump_changes_group$
-- This function reads log tables and sequences states to export into files the data changes recorded between 2 marks for a group.
-- The function performs COPY TO statements, using the options provided by the caller.
-- Some options may be set to customize the changes dump:
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - COPY_OPTIONS=(options) sets the options to use for COPY TO statements
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - NO_EMPTY_FILES ... remove empty files (requires the adminpack extension to be installed)
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
  DECLARE
    v_copyOptions            TEXT;
    v_noEmptyFiles           BOOLEAN;
    v_nbFile                 INT = 1;
    v_fileName               TEXT;
    v_copyResult             INT;
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
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, FALSE) g
      INTO v_copyOptions, v_noEmptyFiles, p_lastMark;
-- Test the supplied output directory and copy options.
    IF p_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_dump_changes_group: The directory parameter cannot be NULL.';
    END IF;
    PERFORM emaj._copy_to_file('(SELECT '''')', p_dir || '/_INFO', v_copyOptions);
-- Execute each generated SQL statement.
    FOR r_sql IN
      SELECT sql_stmt_number, sql_schema, sql_tblseq, sql_file_name_suffix, sql_text
        FROM emaj_temp_sql
        WHERE sql_line_number = 1
        ORDER BY sql_stmt_number
      LOOP
-- Dump the log table.
        v_fileName = p_dir || '/' || translate(r_sql.sql_schema || '_' || r_sql.sql_tblseq || r_sql.sql_file_name_suffix,
                                               E' /\\$<>*', '_______');
        v_copyResult = emaj._copy_to_file('(' || r_sql.sql_text || ')', v_fileName, v_copyOptions, v_noEmptyFiles);
        v_nbFile = v_nbFile + v_copyResult;
-- Keep a trace of the dump execution.
        UPDATE emaj_temp_sql
          SET sql_result = v_copyResult
          WHERE sql_stmt_number = r_sql.sql_stmt_number AND sql_line_number = 1;
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
    PERFORM emaj._copy_to_file(v_stmt, p_dir || '/_INFO');
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
-- Output: the number of generated SQL statements,
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
              WHEN v_option = 'COLS_ORDER=TABLE_LOG' THEN
                v_colsOrder = 'TABLE_LOG';
              WHEN v_option = 'COLS_ORDER=PK' THEN
                v_colsOrder = 'PK';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COLS_ORDER option only accepts '
                                'TABLE_LOG or PK values).',
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
-- Reject the empty files removing if the adminpack extension is not installed.
      IF v_noEmptyFiles THEN
        PERFORM 1 FROM pg_catalog.pg_extension WHERE extname = 'adminpack';
        IF NOT FOUND THEN
          RAISE WARNING 'emaj_dump_changes_group: the NO_EMPTY_FILES option cannot be satisfied because the adminpack extension is not '
                        'installed.';
          v_noEmptyFiles = FALSE;
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
    DROP TABLE IF EXISTS emaj_temp_sql CASCADE;
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
        v_copyOutputFile = v_psqlCopyDir || '/' || translate(
                               r_rel.rel_schema || '_' || r_rel.rel_tblseq ||
                                 CASE WHEN r_rel.nb_time_range > 1 THEN '_' || r_rel.time_range_rank ELSE '' END || '.changes',
                               E' /\\$<>*', '_______');
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
-- Return output parameters.
  p_nbStmt = v_nbStmt;
  p_copyOptions = v_copyOptions;
  p_noEmptyFiles = v_noEmptyFiles;
  p_isPsqlCopy = v_isPsqlCopy;
  RETURN;
  END;
$_gen_sql_dump_changes_group$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_tbl(p_logSchema TEXT, p_logTable TEXT, p_emajVerbAttnum INT, p_pkCols TEXT[],
                                                          p_firstEmajGid BIGINT, p_lastEmajGid BIGINT, p_consolidationLevel TEXT,
                                                          p_emajColumnsList TEXT, p_colsOrder TEXT, p_orderBy TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_gen_sql_dump_changes_tbl$
-- This function builds a SQL statement that snaps a log table subset, with or without consolidation.
-- Input: the log schema and table names to process, with its emaj_verb attribute number and its PK columns array,
--        the emaj sequence range corresponding to the selected mark range,
--        the requested consolidation level (NONE or PARTIAL or FULL),
--        the list of emaj columns to add to the application columns,
--        the criteria to use for the columns order (LOG_TABLE or PK)
--        the criteria to use for the ORDER BY clause (TIME or PK).
-- Output: the formatted SQL statement.
-- When CONSOLIDATION=NONE, the SQL statements return all rows from the log tables corresponding to the marks range.
-- When CONSOLIDATION=PARTIAL or FULL, there are at the most 1 row of type OLD and 1 row of type NEW for each primary key value,
--   representing the net changes for this primary key value, without taking care of the columns content.
-- When CONSOLIDATION=FULL, changes that produce the same row content are not visible.
  DECLARE
    v_logTableName           TEXT;
    v_stmt                   TEXT;
    v_allAppCols             TEXT[];
    v_allAppColumnsList      TEXT;
    v_allEmajCols            TEXT[];
    v_colsWithoutEqualOp     TEXT[];
    v_col                    TEXT;
    v_pkColsList             TEXT;
    v_prefixedPkColsList     TEXT;
    v_pkConditions           TEXT;
    v_nonPkCols              TEXT[];
    v_prefixedNonPkColsList  TEXT;
    v_columnsList            TEXT;
    v_extraEmajColumnsList   TEXT;
    v_isEmajgidInList        BOOLEAN;
    v_orderByColumns         TEXT;
    v_r1PkColumns            TEXT;
    v_r1R2PkCond             TEXT;
    v_r1R2NonPkCond          TEXT;
    v_conditions             TEXT;
    v_template               TEXT;
  BEGIN
-- Build columns arrays.
    v_logTableName = quote_ident(p_logSchema) || '.' || quote_ident(p_logTable);
    v_stmt = 'SELECT array_agg(attname) FILTER (WHERE attnum < %s),'
             '       string_agg(''tbl.'' || quote_ident(attname), '','') FILTER (WHERE attnum < %s),'
             '       array_agg(attname) FILTER (WHERE attnum >= %s),'
             '       array_agg(attname) FILTER (WHERE no_equal_operator)'
             '  FROM ('
             '  SELECT attname, attnum, oprname IS NULL AS no_equal_operator'
             '    FROM pg_catalog.pg_attribute'
             '         JOIN pg_catalog.pg_type ON (atttypid=pg_type.oid)'
             '         LEFT OUTER JOIN pg_catalog.pg_operator ON (pg_type.oid = oprleft AND oprright = oprleft AND oprname = ''='')'
             '    WHERE attrelid = %L::regclass'
             '      AND attnum > 0 AND NOT attisdropped'
             '  ORDER BY attnum) AS t';
    EXECUTE format(v_stmt,
                   p_emajVerbAttnum, p_emajVerbAttnum, p_emajVerbAttnum, v_logTableName)
      INTO v_allAppCols, v_allAppColumnsList, v_allEmajCols, v_colsWithoutEqualOp;
    SELECT array_agg(col ORDER BY rownum), string_agg('tbl.' || quote_ident(col), ',' ORDER BY rownum)
      INTO v_nonPkCols, v_prefixedNonPkColsList
      FROM (SELECT col, row_number() OVER () AS rownum FROM unnest(v_allAppCols) AS col WHERE col <> ALL(p_pkCols)) AS t;
-- Check the emaj columns from the EMAJ_COLUMNS option, for this table (emaj columns may differ from a table to another).
    IF p_emajColumnsList <> '*' THEN
      FOREACH v_col IN ARRAY string_to_array(p_emajColumnsList, ',')
        LOOP
        IF v_col <> ALL(v_allEmajCols) THEN
          RAISE EXCEPTION '_gen_sql_dump_changes_tbl: The emaj column "%" from the EMAJ_COLUMNS option (%) is not valid for table %.%.',
                          v_col, p_emajColumnsList, p_logSchema, p_logTable;
        END IF;
      END LOOP;
    END IF;
-- Build the PK columns lists and conditions.
    v_pkColsList = array_to_string(p_pkCols, ',');
    v_prefixedPkColsList = 'tbl.' || array_to_string(p_pkCols, ',tbl.');
    SELECT string_agg('tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname), ' AND ')
      INTO v_pkConditions
      FROM unnest(p_pkCols) AS attname;
-- Build the columns list.
    IF p_colsOrder = 'LOG_TABLE' THEN
      IF p_emajColumnsList = '*' THEN
        v_columnsList = 'tbl.*';
      ELSE
        v_columnsList = v_allAppColumnsList || ',' || p_emajColumnsList;
      END IF;
    ELSE           -- COLS_ORDER=PK
      IF p_emajColumnsList = '*' THEN
        p_emajColumnsList = array_to_string(v_allEmajCols, ',');
      END IF;
      v_extraEmajColumnsList = replace(replace(p_emajColumnsList, 'emaj_tuple,', ''), 'emaj_tuple', '');
      v_isEmajgidInList = (position('emaj_gid' IN v_extraEmajColumnsList) > 0);
      IF v_isEmajgidInList THEN
        v_extraEmajColumnsList = replace(replace(v_extraEmajColumnsList, 'emaj_gid,', ''), 'emaj_gid', '');
      END IF;
      v_columnsList = v_prefixedPkColsList ||
                      CASE WHEN v_isEmajgidInList THEN ',emaj_gid,emaj_tuple' ELSE ',emaj_tuple' END ||
                      CASE WHEN v_prefixedNonPkColsList <> '' THEN ',' || v_prefixedNonPkColsList ELSE '' END ||
                      CASE WHEN v_extraEmajColumnsList <> '' THEN ',' || v_extraEmajColumnsList ELSE '' END;
    END IF;
-- Build the conditions on emaj_gid.
    v_conditions = 'emaj_gid > ' || p_firstEmajGid || coalesce(' AND emaj_gid <= ' || p_lastEmajGid, '');
-- Build the ORDER BY columns list.
    IF p_orderBy = 'TIME' THEN
      v_orderByColumns = 'emaj_gid, emaj_tuple DESC';
    ELSE
      v_orderByColumns = 'tbl.' || array_to_string(p_pkCols, ',tbl.') || ', emaj_gid, emaj_tuple DESC';
    END IF;
-- Build the final statement.
    CASE p_consolidationLevel
      WHEN 'NONE' THEN
        v_template =
          E'SELECT %s\n'
           '  FROM %I.%I tbl\n'
           '  WHERE %s\n'
           '  ORDER BY %s';
        v_stmt = format(v_template,
                        v_columnsList, p_logSchema, p_logTable, v_conditions, v_orderByColumns);
      WHEN 'PARTIAL' THEN
        v_template =
          E'WITH keys AS (\n'
           '  SELECT %s, min(emaj_gid) AS min_gid, max(emaj_gid) AS max_gid\n'
           '    FROM %I.%I\n'
           '    WHERE %s\n'
           '    GROUP BY %s\n'
           '  ) \n'
           'SELECT %s\n'
           '  FROM %I.%I tbl\n'
           '       JOIN keys ON (%s)\n'
           '  WHERE (tbl.emaj_tuple = ''OLD'' AND tbl.emaj_gid = keys.min_gid)\n'
           '     OR (tbl.emaj_tuple = ''NEW'' AND tbl.emaj_gid = keys.max_gid)\n'
           '  ORDER BY %s';
        v_stmt = format(v_template,
                        v_pkColsList, p_logSchema, p_logTable, v_conditions, v_pkColsList,
                        v_columnsList, p_logSchema, p_logTable, v_pkConditions, v_orderByColumns);
      WHEN 'FULL' THEN
-- Some additional SQL pieces for full consolidation.
        v_r1PkColumns = 'r1.' || array_to_string(p_pkCols, ',r1.');
        SELECT string_agg(condition, ' AND ') INTO v_r1R2PkCond
          FROM (
            SELECT 'r1.' || col || '=r2.' || col
              FROM unnest(p_pkCols) AS col
            ) AS t(condition);
        SELECT string_agg(condition, ' AND ') INTO v_r1R2NonPkCond
          FROM (
            SELECT CASE
                     WHEN col = ANY(v_colsWithoutEqualOp) THEN     -- columns without '=' operator are casted into TEXT for the comparison
                          '(r1.' || col || '::text=r2.' || col || '::text OR (r1.' || col || ' IS NULL AND r2.' || col || ' IS NULL))'
                     ELSE '(r1.' || col || '=r2.' || col || ' OR (r1.' || col || ' IS NULL AND r2.' || col || ' IS NULL))'
                   END
              FROM unnest(v_nonPkCols) AS col
            ) AS t(condition);
-- And the final statement.
        v_template =
          E'WITH keys AS (\n'
           '  SELECT %s, min(emaj_gid) AS min_gid, max(emaj_gid) AS max_gid\n'
           '    FROM %I.%I\n'
           '    WHERE %s\n'
           '    GROUP BY %s\n'
           '  ),\n'
           '     consolidated AS (\n'
           '  SELECT tbl.*\n'
           '    FROM %I.%I tbl\n'
           '         JOIN keys ON (%s)\n'
           '    WHERE (tbl.emaj_tuple = ''OLD'' AND tbl.emaj_gid = keys.min_gid)\n'
           '       OR (tbl.emaj_tuple = ''NEW'' AND tbl.emaj_gid = keys.max_gid)\n'
           '  ),\n'
           '     unchanged_keys AS (\n'
           '  SELECT %s\n'
           '    FROM consolidated r1\n'
           '         JOIN consolidated r2 ON (%s)\n'
           '    WHERE r1.emaj_tuple = ''OLD'' AND r2.emaj_tuple = ''NEW''\n'
           '      AND %s\n'
           '  )\n'
           '  SELECT %s\n'
           '    FROM consolidated tbl\n'
           '    WHERE NOT EXISTS (SELECT 0 FROM unchanged_keys keys WHERE %s)\n'
           '    ORDER BY %s';
        v_stmt = format(v_template,
                        v_pkColsList, p_logSchema, p_logTable, v_conditions, v_pkColsList,
                        p_logSchema, p_logTable, v_pkConditions,
                        v_r1PkColumns, v_r1R2PkCond, v_r1R2NonPkCond,
                        v_columnsList, v_pkConditions, v_orderByColumns);
    END CASE;
    IF v_stmt IS NULL THEN
      RAISE EXCEPTION '_gen_sql_dump_changes_tbl: Internal error - the generated statement is NULL.';
    END IF;
    RETURN v_stmt;
  END;
$_gen_sql_dump_changes_tbl$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_seq(p_schema TEXT, p_sequence TEXT, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT,
                                                          p_consolidationLevel TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_gen_sql_dump_changes_seq$
-- This function builds a SQL statement that gets sequence state at 2 marks.
-- Input: the schema and sequence names,
--        the emaj sequence range corresponding to the selected marks range,
--        the requested consolidation level.
-- Output: the formatted SQL statement.
-- When CONSOLIDATION=NONE or PARTIAL, the SQL statements return 1 row of type OLD with the initial sequence characteristics and 1 row
--   of type NEW with the final sequence characteristics, even if the sequence has not been changed.
-- When CONSOLIDATION=FULL, no row is returned if the sequence has not been changed between mark bounds.
  DECLARE
    v_template               TEXT;
    v_stmt                   TEXT;
  BEGIN
-- Build the statement, depending on the requested consolidation level.
    CASE WHEN p_consolidationLevel IN ('NONE', 'PARTIAL') THEN
        v_template =
          E'SELECT ''OLD'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '  WHERE sequ_schema = %L\n'
           '    AND sequ_name = %L\n'
           '    AND sequ_time_id = %s\n'
           ' UNION ALL \n'
           'SELECT ''NEW'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '  WHERE sequ_schema = %L\n'
           '    AND sequ_name = %L\n'
           '    AND sequ_time_id = %s\n'
           ' ORDER BY emaj_tuple DESC\n';
        v_stmt = format(v_template,
                        p_schema, p_sequence, p_firstEmajGid,
                        p_schema, p_sequence, p_lastEmajGid);
      WHEN p_consolidationLevel = 'FULL' THEN
        v_template =
          E'WITH seq_begin AS (\n'
           '  SELECT ''OLD'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '    WHERE sequ_schema = %L\n'
           '      AND sequ_name = %L\n'
           '      AND sequ_time_id = %s\n'
           '  ), seq_end AS (\n'
           '  SELECT ''NEW'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '    WHERE sequ_schema = %L\n'
           '      AND sequ_name = %L\n'
           '      AND sequ_time_id = %s\n'
           '  ), seq_agg AS (\n'
           '  SELECT ''<>'' FROM seq_begin b, seq_end e\n'     -- 0 row if the sequence has the same characteristics at both mark times
           '    WHERE (CASE WHEN b.sequ_is_called THEN b.sequ_last_val ELSE b.sequ_last_val - b.sequ_increment END <>\n'
           '           CASE WHEN e.sequ_is_called THEN e.sequ_last_val ELSE e.sequ_last_val - e.sequ_increment END) OR\n'
           '           b.sequ_start_val <> e.sequ_start_val OR b.sequ_increment <> e.sequ_increment OR\n'
           '           b.sequ_max_val <> e.sequ_max_val OR b.sequ_min_val <> e.sequ_min_val OR\n'
           '           b.sequ_cache_val <> e.sequ_cache_val OR b.sequ_is_cycled <> e.sequ_is_cycled\n'
           '  )\n'
           'SELECT t.* FROM ( SELECT * FROM seq_begin UNION ALL SELECT * FROM seq_end ) AS t, seq_agg\n'
           '  ORDER BY emaj_tuple DESC\n';
        v_stmt = format(v_template,
                        p_schema, p_sequence, p_firstEmajGid,
                        p_schema, p_sequence, p_lastEmajGid);
    END CASE;
    IF v_stmt IS NULL THEN
      RAISE EXCEPTION '_gen_sql_dump_changes_seq: Internal error - the generated statement is NULL.';
    END IF;
    RETURN v_stmt;
  END;
$_gen_sql_dump_changes_seq$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(p_groupName TEXT, p_dir TEXT, p_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql AS
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
  DECLARE
    v_nbRel                  INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_colList                TEXT;
    v_fileName               TEXT;
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
      v_fileName = p_dir || '/' || translate(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap', E' /\\$<>*', '_______');
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- It is a table.
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
                     AND indrelid = v_fullTableName::regclass
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
                   WHERE attrelid = v_fullTableName::regclass
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          END IF;
--   Dump the table
          v_stmt= '(SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ')';
          PERFORM emaj._copy_to_file(v_stmt, v_fileName, p_copyOptions);
        WHEN 'S' THEN
-- If it is a sequence, the statement has no order by.
          v_stmt = '(SELECT sequencename, rel.last_value, start_value, increment_by, max_value, '
                || 'min_value, cache_size, cycle, rel.is_called '
                || 'FROM ' || v_fullTableName || ' rel, pg_catalog.pg_sequences '
                || 'WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = '
                || quote_literal(r_tblsq.rel_tblseq) ||')';
--    Dump the sequence properties.
          PERFORM emaj._copy_to_file(v_stmt, v_fileName, p_copyOptions);
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || transaction_timestamp()) || ')';
    PERFORM emaj._copy_to_file(v_stmt, p_dir || '/_INFO', NULL);
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
    RETURN emaj._gen_sql_groups(array[p_groupName], FALSE, p_firstMark, p_lastMark, p_location, p_tblseqs);
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
    RETURN emaj._gen_sql_groups(p_groupNames, TRUE, p_firstMark, p_lastMark, p_location, p_tblseqs);
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(TEXT[],TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script replaying all updates performed on a tables groups set between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT,
                                                p_location TEXT, p_tblseqs TEXT[])
RETURNS BIGINT LANGUAGE plpgsql
SET DateStyle = 'ISO, YMD' SET standard_conforming_strings = ON AS
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
-- Output: number of generated SQL statements (non counting comments and transaction management)
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
        PERFORM emaj._copy_to_file('(SELECT 0)', p_location, NULL);
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
        PERFORM emaj._copy_to_file('(SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid)', p_location, NULL);
        DROP TABLE IF EXISTS emaj_temp_script;
      ELSE
-- Otherwise create a view to ease the generation script export.
        CREATE TEMPORARY VIEW emaj_sql_script AS
          SELECT scr_sql
            FROM emaj_temp_script
            ORDER BY scr_emaj_gid NULLS LAST, scr_subid;
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
      SELECT 'Error: In the group "' || r.rel_group || '", the ' ||
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
      SELECT 'Error: In the group "' || rel_group || '", the log table "' ||
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
      SELECT 'Error: In the group "' || rel_group || '", the log sequence "' ||
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
      SELECT 'Error: In the group "' || rel_group || '", the log function "' ||
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
      SELECT 'Error: In the group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
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
      SELECT 'Error: In the group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
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
               'Error: In the group "' || rel_group || '", the structure of the application table "' ||
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
                'Error: In the group "' || rel_group || '", the log table "' ||
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
                  'Warning: In the group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
                  '" is linked to the table "' || tbl_schema || '"."' || tbl_name ||
                  '" but this table does not belong to any tables group.' AS msg
             FROM serial_dependencies
             WHERE tbl_group IS NULL
         UNION ALL
           SELECT DISTINCT seq_schema, seq_name,
                  'Warning: In the group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
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
                  'Warning: In the group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on the table "' || tbl_schema || '"."' || tbl_name ||
                  '" references the table "' || reftbl_schema || '"."' || reftbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND tbl_group_is_rollbackable
               AND reftbl_group IS NULL
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: In the group "' || reftbl_group || '", the table "' || reftbl_schema || '"."' || reftbl_name ||
                  '" is referenced by the foreign key "' || conname ||
                  '" of the table "' || tbl_schema || '"."' || tbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE reftbl_group IS NOT NULL
               AND reftbl_group_is_rollbackable
               AND tbl_group IS NULL
        UNION ALL
          SELECT tbl_schema, tbl_name,
                 'Warning: In the group "' || tbl_group || '", the foreign key "' || conname ||
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
    IF has_function_privilege('emaj._dblink_open_cnx(text)', 'execute') THEN
      SELECT p_status, p_schema INTO v_status, v_schema
        FROM emaj._dblink_open_cnx('test');
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

GRANT EXECUTE ON FUNCTION emaj._check_group_names(p_groupNames TEXT[], p_mayBeNull BOOLEAN, p_lockGroups BOOLEAN,
                                                  p_checkIdle BOOLEAN, p_checkLogging BOOLEAN,
                                                  p_checkRollbackable BOOLEAN, p_checkUnprotected BOOLEAN)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_mark_name(p_groupNames TEXT[], p_mark TEXT, p_checkActive BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_marks_range(p_groupNames TEXT[], INOUT p_firstMark TEXT, INOUT p_lastMark TEXT,
                          p_finiteUpperBound BOOLEAN,
                          OUT p_firstMarkTimeId BIGINT, OUT p_lastMarkTimeId BIGINT,
                          OUT p_firstMarkTs TIMESTAMPTZ, OUT p_lastMarkTs TIMESTAMPTZ,
                          OUT p_firstMarkEmajGid BIGINT, OUT p_lastMarkEmajGid BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, p_firstMarkTimeId BIGINT, p_lastMarkTimeId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_optionsList TEXT,
                                                               p_tblseqs TEXT[]) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                           p_optionsList TEXT, p_tblseqs TEXT[], p_genSqlOnly BOOLEAN,
                                                           OUT p_nbStmt INT, OUT p_copyOptions TEXT, OUT p_noEmptyFiles BOOLEAN,
                                                           OUT p_isPsqlCopy BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_tbl(p_logSchema TEXT, p_logTable TEXT, p_emajVerbAttnum INT, p_pkCols TEXT[],
                                                         p_firstEmajGid BIGINT, p_lastEmajGid BIGINT, p_consolidationLevel TEXT,
                                                         p_emajColumnsList TEXT, p_colsOrder TEXT, p_orderBy TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_seq(p_schema TEXT, p_sequence TEXT, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT,
                                                         p_consolidationLevel TEXT) TO emaj_viewer;

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

-- Update the version id in the emaj_param table.
ALTER TABLE emaj.emaj_param DISABLE TRIGGER emaj_param_change_trg;
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';
ALTER TABLE emaj.emaj_param ENABLE TRIGGER emaj_param_change_trg;

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.2.0 completed');

-- Post installation checks.
DO
$tmp$
  DECLARE
    v_adminpackVersion       TEXT;
  BEGIN
-- Check the max_prepared_transactions GUC value.
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback '
                    'is possible.', current_setting('max_prepared_transactions');
    END IF;
-- Warn if the adminpack extension is not created.
    SELECT installed_version INTO v_adminpackVersion FROM pg_catalog.pg_available_extensions WHERE name = 'adminpack';
    IF NOT FOUND THEN
      RAISE WARNING 'E-Maj installation: The adminpack extension is not installed, and thus can''t be created into the database. The'
                    ' NO_EMPTY_FILE option of the emaj_dump_changes_group() function will be disabled.';
    ELSIF v_adminpackVersion IS NULL THEN
      RAISE WARNING 'E-Maj installation: The adminpack extension is available but not yet created. Execute a "CREATE EXTENSION adminpack;"'
                    ' statement to enable the NO_EMPTY_FILE option use of the emaj_dump_changes_group() function.';
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
