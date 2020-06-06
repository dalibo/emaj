--
-- E-Maj: migration from 2.0.1 to 2.1.0
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
-- the emaj version registered in emaj_param must be '2.0.1'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '2.0.1' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 2.0.1',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.1
    IF current_setting('server_version_num')::int < 90100 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.1.', current_setting('server_version');
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

-- enum of the possible values for the alter groups steps
CREATE TYPE emaj._alter_step_enum AS ENUM (
  'REMOVE_TBL',              -- remove a table from a group
  'REMOVE_SEQ',              -- remove a sequence from a group
  'CREATE_LOG_SCHEMA',       -- create a secondary log schema
  'REPAIR_TBL',              -- repair a damaged table
  'REPAIR_SEQ',              -- repair a damaged sequence
  'RESET_GROUP',             -- reset an idle group
  'CHANGE_TBL_LOG_SCHEMA',   -- change the log schema for a table
  'CHANGE_TBL_NAMES_PREFIX', -- change the E-Maj names prefix for a table
  'CHANGE_TBL_LOG_DATA_TSP', -- change the log data tablespace for a table
  'CHANGE_TBL_LOG_INDEX_TSP',-- change the log index tablespace for a table
  'ASSIGN_REL',              -- move a table or a sequence from one group to another
  'CHANGE_REL_PRIORITY',     -- change the priority level for a table or a sequence
  'ADD_TBL',                 -- add a table to a group
  'ADD_SEQ',                 -- add a sequence to a group
  'DROP_LOG_SCHEMA'          -- drop a secondary log schema
  );

--
-- Adjust the emaj_group_def table content
-- If 'tspemaj' was used as default tablespace, explicitely set this value in the emaj_group_def table.
-- (tspemaj is not a default tablespace anymore at tables groups creation)
--
UPDATE emaj.emaj_group_def SET grpdef_log_dat_tsp = rel_log_dat_tsp
  FROM emaj.emaj_relation
  WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
    AND rel_log_dat_tsp = 'tspemaj' AND grpdef_log_dat_tsp IS NULL;
UPDATE emaj.emaj_group_def SET grpdef_log_idx_tsp = rel_log_idx_tsp
  FROM emaj.emaj_relation
  WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
    AND rel_log_idx_tsp = 'tspemaj' AND grpdef_log_idx_tsp IS NULL;

-- New table containing the elementary steps to perform alter_groups operations
-- the steps concerning a relation are identified by the altr_schema and altr_tblseq columns
-- the steps concerning a schema are identified by the altr_schema column
-- the steps concerning a group are identified by the altr_group column
CREATE TABLE emaj.emaj_alter_plan (
  altr_time_id                 BIGINT      NOT NULL,       -- time stamp id of the alter_groups operation
  altr_step                    emaj._alter_step_enum
                                           NOT NULL,       -- elementary step of the alter groups operation
  altr_schema                  TEXT        NOT NULL,       -- schema name, depending on the step ('' when meaningless)
  altr_tblseq                  TEXT        NOT NULL,       -- table or sequence name, depending on the step ('' when meaningless)
  altr_group                   TEXT        NOT NULL,       -- group that owns the table or the sequence ('' when meaningless)
  altr_priority                INT         ,               -- priority level, with the same meaning and representation than in emaj_group_def
  altr_group_is_logging        BOOLEAN     ,               -- copy of the emaj_group.group_is_logging column at alter time
  altr_new_group               TEXT        ,               -- target group name, when the relation changes its group ownership
  altr_new_priority            INT         ,               -- target priority level, when the relation changes its priority level
  altr_new_group_is_logging    BOOLEAN     ,               -- state of the target group, when the relation changes its group ownership
  PRIMARY KEY (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group),
  FOREIGN KEY (altr_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_alter_plan IS
$$Contains elementary steps of alter_groups operations.$$;

--
-- process the emaj_rlbk table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_rlbk_old (LIKE emaj.emaj_rlbk);

INSERT INTO emaj_rlbk_old SELECT * FROM emaj.emaj_rlbk;

-- drop the old table

ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_rlbk_rlbk_id_seq;
DROP TABLE emaj.emaj_rlbk CASCADE;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
CREATE TABLE emaj.emaj_rlbk (
  rlbk_id                      SERIAL      NOT NULL,       -- rollback id
  rlbk_groups                  TEXT[]      NOT NULL,       -- groups array to rollback
  rlbk_mark                    TEXT        NOT NULL,       -- mark to rollback to
  rlbk_mark_time_id            BIGINT      NOT NULL,       -- time stamp id of the mark to rollback to
  rlbk_time_id                 BIGINT,                     -- time stamp id at the rollback start
  rlbk_is_logged               BOOLEAN     NOT NULL,       -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN     NOT NULL,       -- flag allowing to rollback to a mark set before alter group operations
  rlbk_nb_session              INT         NOT NULL,       -- number of requested rollback sessions
  rlbk_nb_table                INT,                        -- total number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences to rollback
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_begin_hist_id           BIGINT,                     -- hist_id of the rollback BEGIN event in the emaj_hist
                                                           --   used to know if the rollback has been committed or not
  rlbk_is_dblink_used          BOOLEAN,                    -- boolean indicating whether dblink connection are used
  rlbk_end_datetime            TIMESTAMPTZ,                -- clock time the rollback has been completed,
                                                           --   NULL if rollback is in progress or aborted
  rlbk_msg                     TEXT,                       -- result message
  PRIMARY KEY (rlbk_id),
  FOREIGN KEY (rlbk_time_id) REFERENCES emaj.emaj_time_stamp (time_id),
  FOREIGN KEY (rlbk_mark_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk IS
$$Contains description of rollback events.$$;

-- populate the new table
INSERT INTO emaj.emaj_rlbk (
         rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
         rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id,
         rlbk_is_dblink_used, rlbk_end_datetime, rlbk_msg)
  SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, FALSE,
         rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id,
         rlbk_is_dblink_used, rlbk_end_datetime, rlbk_msg
    FROM emaj_rlbk_old;

-- create indexes
-- partial index on emaj_rlbk targeting in progress rollbacks (not yet committed or marked as aborted)
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
-- add created or recreated tables and sequences to the list of content to save by pg_dump
--
SELECT pg_catalog.pg_extension_config_dump('emaj_alter_plan','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_rlbk_id_seq','');

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------
DROP FUNCTION emaj.emaj_rollback_activity();
DROP FUNCTION emaj._rollback_activity();
DROP TYPE emaj.emaj_rollback_activity_type;

CREATE TYPE emaj.emaj_rollback_activity_type AS (
  rlbk_id                      INT,                        -- rollback id
  rlbk_groups                  TEXT[],                     -- groups array to rollback
  rlbk_mark                    TEXT,                       -- mark to rollback to
  rlbk_mark_datetime           TIMESTAMPTZ,                -- timestamp of the mark as recorded into emaj_mark
  rlbk_is_logged               BOOLEAN,                    -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN,                    -- flag allowing to rollback to a mark set before alter group operations
  rlbk_nb_session              INT,                        -- number of requested sessions
  rlbk_nb_table                INT,                        -- total number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences to rollback
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_start_datetime          TIMESTAMPTZ,                -- clock timestamp of the rollback start recorded just after tables lock
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
-- recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$emaj_rollback_activity$
-- This function returns the list of rollback operations currently in execution, with information about their progress
-- It doesn't need input parameter.
-- It returns a set of emaj_rollback_activity_type records.
  BEGIN
-- cleanup the freshly completed rollback operations, if any
    PERFORM emaj.emaj_cleanup_rollback_state();
-- and retrieve information regarding the rollback operations that are always in execution
    RETURN QUERY SELECT * FROM emaj._rollback_activity();
  END;
$emaj_rollback_activity$;
COMMENT ON FUNCTION emaj.emaj_rollback_activity() IS
$$Returns the list of rollback operations currently in execution, with information about their progress.$$;

--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------
DROP FUNCTION emaj._check_group_content(V_GROUPNAME TEXT);
DROP FUNCTION emaj._create_tbl(R_GRPDEF EMAJ.EMAJ_GROUP_DEF,V_GROUPNAME TEXT,V_ISROLLBACKABLE BOOLEAN,V_DEFTSP TEXT);
DROP FUNCTION emaj._create_seq(GRPDEF EMAJ.EMAJ_GROUP_DEF,V_GROUPNAME TEXT);
DROP FUNCTION emaj.emaj_create_group(V_GROUPNAME TEXT,V_ISROLLBACKABLE BOOLEAN);
DROP FUNCTION emaj.emaj_alter_group(V_GROUPNAME TEXT);
DROP FUNCTION emaj._rlbk_groups(V_GROUPNAMES TEXT[],V_MARK TEXT,V_ISLOGGEDRLBK BOOLEAN,V_MULTIGROUP BOOLEAN);
DROP FUNCTION emaj._rlbk_async(V_RLBKID INT,V_MULTIGROUP BOOLEAN);
DROP FUNCTION emaj._rlbk_init(V_GROUPNAMES TEXT[],V_MARK TEXT,V_ISLOGGEDRLBK BOOLEAN,V_NBSESSION INT,V_MULTIGROUP BOOLEAN);
DROP FUNCTION emaj._rlbk_check(V_GROUPNAMES TEXT[],V_MARK TEXT,ISROLLBACKSIMULATION BOOLEAN);
DROP FUNCTION emaj._rlbk_session_exec(V_RLBKID INT,V_SESSION INT);
DROP FUNCTION emaj._rlbk_end(V_RLBKID INT,V_MULTIGROUP BOOLEAN);
DROP FUNCTION emaj._reset_group(V_GROUPNAME TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._get_default_tablespace()
RETURNS TEXT LANGUAGE plpgsql AS
$_get_default_tablespace$
-- This function returns the name of a default tablespace to use when moving an existing log table or index.
-- Output: tablespace name
-- The function is called at alter group time.
  DECLARE
    v_tablespace             TEXT;
  BEGIN
-- get the default tablespace set for the current session or set for the entire instance by GUC
    SELECT setting INTO v_tablespace FROM pg_settings
      WHERE name = 'default_tablespace';
    IF v_tablespace = '' THEN
-- get the default tablespace for the current database (pg_default if no specific tablespace name has been set for the database)
      SELECT spcname INTO v_tablespace FROM pg_database, pg_tablespace
        WHERE dattablespace = pg_tablespace.oid AND datname = current_database();
    END IF;
    RETURN v_tablespace;
  END;
$_get_default_tablespace$;

CREATE OR REPLACE FUNCTION emaj._purge_hist()
RETURNS VOID LANGUAGE plpgsql AS
$_purge_hist$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter, but
--   not deleting event traces neither after the oldest active mark or after the oldest not committed or aborted rollback operation.
-- It also purges oldest rows from the maj_exec_plan, emaj_rlbk_session and emaj_rlbk_plan tables, using the same rules.
-- The function is called at start group time and when oldest marks are deleted.
  DECLARE
    v_datetimeLimit          TIMESTAMPTZ;
    v_nbPurgedHist           BIGINT;
    v_maxTimeId              BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbPurgedRlbk           BIGINT;
    v_nbPurgedAlter          BIGINT;
    v_wording                TEXT = '';
  BEGIN
-- compute the timestamp limit
    SELECT MIN(datetime) INTO v_datetimeLimit FROM
      (                                           -- compute the timestamp limit from the history_retention parameter
        (SELECT current_timestamp -
           coalesce((SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'),'1 YEAR'))
      UNION ALL                                   -- get the transaction timestamp of the oldest non deleted mark for all groups
        (SELECT MIN(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_mark
           WHERE time_id = mark_time_id AND NOT mark_is_deleted)
      UNION ALL                                   -- get the transaction timestamp of the oldest non committed or aborted rollback
        (SELECT MIN(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_rlbk
           WHERE time_id = rlbk_time_id AND rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED'))
      ) AS t(datetime);
-- get the greatest timestamp identifier corresponding to the timeframe to purge, if any
    SELECT MAX(time_id) INTO v_maxTimeId FROM emaj.emaj_time_stamp
      WHERE time_tx_timestamp < v_datetimeLimit;
-- delete oldest rows from emaj_hist
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < v_datetimeLimit;
    GET DIAGNOSTICS v_nbPurgedHist = ROW_COUNT;
    IF v_nbPurgedHist > 0 THEN
      v_wording = v_nbPurgedHist || ' emaj_hist rows deleted';
    END IF;
-- purge the emaj_alter_plan table
    WITH deleted_alter AS (
      DELETE FROM emaj.emaj_alter_plan
        WHERE altr_time_id <= v_maxTimeId
        RETURNING altr_time_id
      )
      SELECT COUNT (DISTINCT altr_time_id) INTO v_nbPurgedAlter FROM deleted_alter;
    IF v_nbPurgedAlter > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedAlter || ' alter groups events deleted';
    END IF;
-- get the greatest rollback identifier to purge
    SELECT MAX(rlbk_id) INTO v_maxRlbkId FROM emaj.emaj_rlbk
      WHERE rlbk_time_id <= v_maxTimeId;
-- and purge the emaj_rlbk_plan and emaj_rlbk_session tables
    IF v_maxRlbkId IS NOT NULL THEN
      DELETE FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id <= v_maxRlbkId;
      WITH deleted_rlbk AS (
        DELETE FROM emaj.emaj_rlbk_session
          WHERE rlbs_rlbk_id <= v_maxRlbkId
          RETURNING rlbs_rlbk_id
        )
        SELECT COUNT (DISTINCT rlbs_rlbk_id) INTO v_nbPurgedRlbk FROM deleted_rlbk;
      v_wording = v_wording || ' ; ' || v_nbPurgedRlbk || ' rollback events deleted';
    END IF;
-- record the purge into the history if there are significant data
    IF v_wording <> '' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_wording)
        VALUES ('PURGE_HISTORY', v_wording);
    END IF;
    RETURN;
  END;
$_purge_hist$;

CREATE OR REPLACE FUNCTION emaj._check_names_array(v_names TEXT[], v_type TEXT)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_names_array$
-- This function build a array of names similar to the supplied array, except that NULL
-- values, empty string and duplicate names are suppressed. Issue a warning if the result array is NULL.
-- The function is used to validate group names array or table and sequence names array.
-- Input: names array
--        type of element, used to format warning messages
-- Output: validated names array
  DECLARE
    v_outputNames            TEXT[];
    v_aName                  TEXT;
  BEGIN
    IF v_names IS NOT NULL AND array_upper(v_names,1) >= 1 THEN
-- if there are elements, build the result array
      FOREACH v_aName IN ARRAY v_names LOOP
-- look for not NULL & not empty name
        IF v_aName IS NULL OR v_aName = '' THEN
          RAISE WARNING '_check_names_array: A % name is NULL or empty.', v_type;
-- look for duplicate name
        ELSEIF v_outputNames IS NOT NULL AND v_aName = ANY (v_outputNames) THEN
          RAISE WARNING '_check_names_array: Duplicate % name "%".', v_type, v_aName;
        ELSE
-- OK, keep the name
          v_outputNames = v_outputNames || v_aName;
        END IF;
      END LOOP;
    END IF;
-- check for NULL result
    IF v_outputNames IS NULL THEN
      RAISE WARNING '_check_names_array: No % name to process.', v_type;
    END IF;
    RETURN v_outputNames;
  END;
$_check_names_array$;

CREATE OR REPLACE FUNCTION emaj._check_groups_content(v_groupNames TEXT[], v_isRollbackable BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_check_groups_content$
-- This function verifies that the content of tables group as defined into the emaj_group_def table is correct.
-- Any issue is reported as a warning message. If at least one issue is detected, an exception is raised before exiting the function.
-- It is called by emaj_create_group() and emaj_alter_group() functions.
-- This function checks that the referenced application tables and sequences:
--  - exist,
--  - is not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
--  - will not generate conflicts on emaj objects to create (when emaj names prefix is not the default one)
-- It also checks that:
--  - tables are not TEMPORARY, UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for sequences, the tablespaces, emaj log schema and emaj object name prefix are all set to NULL
-- Input: name array of the tables group to check,
--        flag indicating whether the group is rollbackable or not (NULL when called by _alter_groups(), the groups state will be read from emaj_group)
  DECLARE
    v_nbError                INT = 0 ;
    r                        RECORD;
  BEGIN
-- check that all application tables and sequences listed for the group really exist
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq FROM (
        SELECT grpdef_schema, grpdef_tblseq
          FROM emaj.emaj_group_def WHERE grpdef_group = ANY(v_groupNames)
        EXCEPT
        SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S','p')
        ORDER BY 1,2) AS t
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table or sequence %.% does not exist.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj)
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq
          AND grpdef_group = ANY(v_groupNames) AND relkind = 'p'
        ORDER BY 1,2
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table %.% is a partitionned table (only elementary partitions are supported by E-Maj).', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- check no application schema listed for the group in the emaj_group_def table is an E-Maj schema
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def
        WHERE grpdef_group = ANY (v_groupNames)
          AND grpdef_schema IN (
                SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
                UNION
                SELECT 'emaj')
        ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table or sequence %.% belongs to an E-Maj schema.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- check that no table or sequence of the checked groups already belongs to other created groups
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq, rel_group
        FROM emaj.emaj_group_def, emaj.emaj_relation
        WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
          AND grpdef_group = ANY (v_groupNames) AND NOT rel_group = ANY (v_groupNames)
        ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table or sequence %.% belongs to another group (%).', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq), r.rel_group;
      v_nbError = v_nbError + 1;
    END LOOP;
-- check that several tables of the group have not the same emaj names prefix
    FOR r IN
      SELECT coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) AS prefix, count(*)
        FROM emaj.emaj_group_def
        WHERE grpdef_group = ANY (v_groupNames)
        GROUP BY 1 HAVING count(*) > 1
        ORDER BY 1
    LOOP
      RAISE WARNING '_check_groups_content: Error, the emaj prefix "%" is configured for several tables in the groups.', r.prefix;
      v_nbError = v_nbError + 1;
    END LOOP;
-- check that emaj names prefix that will be generared will not generate conflict with objects from existing groups
    FOR r IN
      SELECT coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) AS prefix
        FROM emaj.emaj_group_def, emaj.emaj_relation
        WHERE coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) || '_log' = rel_log_table
          AND grpdef_group = ANY (v_groupNames) AND NOT rel_group = ANY (v_groupNames)
      ORDER BY 1
    LOOP
      RAISE WARNING '_check_groups_content: Error, the emaj prefix "%" is already used.', r.prefix;
      v_nbError = v_nbError + 1;
    END LOOP;
-- check no table is a TEMP table
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relpersistence = 't'
        ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table %.% is a TEMPORARY table.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- check no table is an unlogged table
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relpersistence = 'u'
        ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table %.% is an UNLOGGED table.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- check no table is a WITH OIDS table
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relhasoids
        ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table %.% is declared WITH OIDS.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
    FOR r IN
-- only 0 or 1 SELECT is really executed, depending on the v_isRollbackable value
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE v_isRollbackable                                                 -- true (or false) for tables groups creation
          AND grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                            AND contype = 'p' AND nspname = grpdef_schema AND relname = grpdef_tblseq)
        UNION ALL
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
        WHERE v_isRollbackable IS NULL                                         -- NULL for alter groups function call
          AND grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r'
          AND group_name = grpdef_group AND group_is_rollbackable              -- the is_rollbackable attribute is read from the emaj_group table
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                            AND contype = 'p' AND nspname = grpdef_schema AND relname = grpdef_tblseq)
      ORDER BY grpdef_schema, grpdef_tblseq
    LOOP
    RAISE WARNING '_check_groups_content: Error, the table %.% has no PRIMARY KEY.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- all sequences described in emaj_group_def have their log schema suffix attribute set to NULL
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_schema_suffix IS NOT NULL
    LOOP
      RAISE WARNING '_check_groups_content: Error, for the sequence %.%, the secondary log schema suffix is not NULL.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- all sequences described in emaj_group_def have their emaj names prefix attribute set to NULL
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_emaj_names_prefix IS NOT NULL
    LOOP
      RAISE WARNING '_check_groups_content: Error, for the sequence %.%, the emaj names prefix is not NULL.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- all sequences described in emaj_group_def have their data log tablespaces attributes set to NULL
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_dat_tsp IS NOT NULL
    LOOP
      RAISE WARNING '_check_groups_content: Error, for the sequence %.%, the data log tablespace is not NULL.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
-- all sequences described in emaj_group_def have their index log tablespaces attributes set to NULL
    FOR r IN
      SELECT grpdef_schema, grpdef_tblseq
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_idx_tsp IS NOT NULL
    LOOP
      RAISE WARNING '_check_groups_content: Error, for the sequence %.%, the index log tablespace is not NULL.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
      v_nbError = v_nbError + 1;
    END LOOP;
    IF v_nbError > 0 THEN
      RAISE EXCEPTION '_check_groups_content: One or several errors have been detected in the emaj_group_def table content.';
    END IF;
--
    RETURN;
  END;
$_check_groups_content$;

CREATE OR REPLACE FUNCTION emaj._check_new_mark(v_mark TEXT, v_groupNames TEXT[])
RETURNS TEXT LANGUAGE plpgsql AS
$_check_new_mark$
-- This function verifies that a new mark name supplied the user is valid.
-- It processes the possible NULL mark value and the replacement of % wild characters.
-- It also checks that the mark name do not already exist for any group.
-- Input: name of the mark to set, array of group names
--        The array of group names may be NULL to avoid the check against groups
-- Output: internal name of the mark
  DECLARE
    v_markName               TEXT = v_mark;
    v_aGroupName             TEXT;
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
    v_markName = replace(v_markName, '%', to_char(current_timestamp, 'HH24.MI.SS.MS'));
-- if requested, check the existence of the mark in groups
    IF v_groupNames IS NOT NULL THEN
-- for each group of the array,
      FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
-- ... if a mark with the same name already exists for the group, stop
        PERFORM 0 FROM emaj.emaj_mark
          WHERE mark_group = v_aGroupName AND mark_name = v_markName;
        IF FOUND THEN
           RAISE EXCEPTION '_check_new_mark: The group "%" already contains a mark named "%".', v_aGroupName, v_markName;
        END IF;
      END LOOP;
    END IF;
    RETURN v_markName;
  END;
$_check_new_mark$;

CREATE OR REPLACE FUNCTION emaj._create_log_schema(v_logSchemaName TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_create_log_schema$
-- The function creates a log schema and gives the appropriate rights to emaj users
-- Input: log schema name
-- The function is created as SECURITY DEFINER so that secondary schemas can be owned by superuser
  DECLARE
  BEGIN
-- check that the schema doesn't already exist
    PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchemaName;
    IF FOUND THEN
      RAISE EXCEPTION '_create_log_schema: The schema "%" should not exist. Drop it manually, or modify emaj_group_def table''s content.',v_logSchemaName;
    END IF;
-- create the schema and give the appropriate rights
    EXECUTE 'CREATE SCHEMA ' || quote_ident(v_logSchemaName);
    EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(v_logSchemaName) || ' TO emaj_adm';
    EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(v_logSchemaName) || ' TO emaj_viewer';
    RETURN;
  END;
$_create_log_schema$;

CREATE OR REPLACE FUNCTION emaj._drop_log_schema(v_logSchemaName TEXT, v_isForced BOOLEAN)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_log_schema$
-- The function drops a log schema
-- Input: log schema name, boolean telling whether the schema to drop may contain residual objects
-- The function is created as SECURITY DEFINER so that secondary schemas can be dropped in any case
  DECLARE
  BEGIN
-- check that the schema exists
    PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchemaName;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_log_schema: Internal error (the schema "%" does not exist).',v_logSchemaName;
    END IF;
    IF v_isForced THEN
-- drop cascade when called by emaj_force_xxx_group()
      EXECUTE 'DROP SCHEMA ' || quote_ident(v_logSchemaName) || ' CASCADE';
    ELSE
-- otherwise, drop restrict with a trap on the potential error
      BEGIN
        EXECUTE 'DROP SCHEMA ' || quote_ident(v_logSchemaName);
        EXCEPTION
-- trap the 2BP01 exception to generate a more understandable error message
          WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
            RAISE EXCEPTION '_drop_log_schema: Cannot drop the schema "%". It probably owns unattended objects. Use the emaj_verify_all() function to get details.', v_logSchemaName;
      END;
    END IF;
    RETURN;
  END;
$_drop_log_schema$;

CREATE OR REPLACE FUNCTION emaj._create_tbl(r_grpdef emaj.emaj_group_def, v_isRollbackable BOOLEAN)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: the emaj_group_def row related to the application table to process, the group name, a boolean indicating whether the group is rollbackable
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
          AND attnum > 0 AND attisdropped = false
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
                rel_log_index, rel_log_sequence, rel_log_function,
                rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions)
        VALUES (r_grpdef.grpdef_schema, r_grpdef.grpdef_tblseq, r_grpdef.grpdef_group, r_grpdef.grpdef_priority, v_logSchema,
                r_grpdef.grpdef_log_dat_tsp, r_grpdef.grpdef_log_idx_tsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName,
                v_colList, v_pkColList, v_pkCondList);
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    SELECT string_agg(tgname, ', ') INTO v_triggerList FROM (
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

CREATE OR REPLACE FUNCTION emaj._change_log_schema_tbl(r_rel emaj.emaj_relation, v_newLogSchemaSuffix TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_change_log_schema_tbl$
-- This function processes the change of log schema for an application table
-- Input: the existing emaj_relation row for the table, and the new log schema suffix
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_newLogSchema           TEXT;
  BEGIN
-- build the name of new log schema
    v_newLogSchema = coalesce(v_schemaPrefix || v_newLogSchemaSuffix, v_emajSchema);
-- process the log schema change
    EXECUTE 'ALTER TABLE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table)|| ' SET SCHEMA ' || quote_ident(v_newLogSchema);
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)|| ' SET SCHEMA ' || quote_ident(v_newLogSchema);
    EXECUTE 'ALTER FUNCTION ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() SET SCHEMA ' || quote_ident(v_newLogSchema);
-- adjust sequences schema names in emaj_sequence tables
    UPDATE emaj.emaj_sequence SET sequ_schema = v_newLogSchema WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation SET rel_log_schema = v_newLogSchema
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'LOG SCHEMA CHANGED', quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              r_rel.rel_log_schema || ' => ' || v_newLogSchema);
    RETURN;
  END;
$_change_log_schema_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_emaj_names_prefix(r_rel emaj.emaj_relation, v_newNamesPrefix TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_change_emaj_names_prefix$
-- This function processes the change of emaj names prefix for an application table
-- Input: the existing emaj_relation row for the table and the new emaj names prefix
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table
  DECLARE
    v_newEmajNamesPrefix     TEXT;
    v_newLogTableName        TEXT;
    v_newLogFunctionName     TEXT;
    v_newLogSequenceName     TEXT;
    v_newLogIndexName        TEXT;
  BEGIN
-- build the name of new emaj components associated to the application table (non schema qualified and not quoted)
    v_newEmajNamesPrefix = coalesce(v_newNamesPrefix, r_rel.rel_schema || '_' || r_rel.rel_tblseq);
    v_newLogTableName    = v_newEmajNamesPrefix || '_log';
    v_newLogIndexName    = v_newEmajNamesPrefix || '_log_idx';
    v_newLogFunctionName = v_newEmajNamesPrefix || '_log_fnct';
    v_newLogSequenceName = v_newEmajNamesPrefix || '_log_seq';
-- process the emaj names prefix change
    EXECUTE 'ALTER TABLE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table)|| ' RENAME TO ' || quote_ident(v_newLogTableName);
    EXECUTE 'ALTER INDEX ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_index)|| ' RENAME TO ' || quote_ident(v_newLogIndexName);
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)|| ' RENAME TO ' || quote_ident(v_newLogSequenceName);
    EXECUTE 'ALTER FUNCTION ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() RENAME TO ' || quote_ident(v_newLogFunctionName);
-- adjust sequences schema names in emaj_sequence tables
    UPDATE emaj.emaj_sequence SET sequ_name = v_newLogSequenceName WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation
      SET rel_log_table = v_newLogTableName, rel_log_index = v_newLogIndexName,
          rel_log_sequence = v_newLogSequenceName, rel_log_function = v_newLogFunctionName
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'NAMES PREFIX CHANGED', quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              coalesce(substring(r_rel.rel_log_table FROM '(.*)_log$'),r_rel.rel_log_table,'Internal error in _change_emaj_names_prefix') || ' => ' || v_newEmajNamesPrefix);
    RETURN;
  END;
$_change_emaj_names_prefix$;

CREATE OR REPLACE FUNCTION emaj._change_log_data_tsp_tbl(r_rel emaj.emaj_relation, v_newLogDatTsp TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_change_log_data_tsp_tbl$
-- This function changes the log data tablespace for an application table
-- Input: the existing emaj_relation row for the table and the new log data tablespace
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = v_newLogDatTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- process the log data tablespace change
    EXECUTE 'ALTER TABLE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' SET TABLESPACE ' || quote_ident(v_newTsp);
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation SET rel_log_dat_tsp = v_newLogDatTsp
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'LOG DATA TABLESPACE CHANGED', quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              coalesce(r_rel.rel_log_dat_tsp, 'Default tablespace') || ' => ' || coalesce(v_newLogDatTsp, 'Default tablespace'));
    RETURN;
  END;
$_change_log_data_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_log_index_tsp_tbl(r_rel emaj.emaj_relation, v_newLogIdxTsp TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_change_log_index_tsp_tbl$
-- This function changes the log index tablespace for an application table
-- Input: the existing emaj_relation row for the table and the new log index tablespace
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = v_newLogIdxTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- process the log index tablespace change
    EXECUTE 'ALTER INDEX ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_index) || ' SET TABLESPACE ' || quote_ident(v_newTsp);
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation SET rel_log_idx_tsp = v_newLogIdxTsp
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'LOG INDEX TABLESPACE CHANGED', quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
              coalesce(r_rel.rel_log_idx_tsp, 'Default tablespace') || ' => ' || coalesce(v_newLogIdxTsp, 'Default tablespace'));
    RETURN;
  END;
$_change_log_index_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._create_seq(grpdef emaj.emaj_group_def)
RETURNS VOID LANGUAGE plpgsql AS
$_create_seq$
-- The function checks whether the sequence is related to a serial column of an application table.
-- If yes, it verifies that this table also belong to the same group
-- Required inputs: the emaj_group_def row related to the application sequence to process, the group name
  DECLARE
    v_tableSchema            TEXT;
    v_tableName              TEXT;
    v_tableGroup             TEXT;
  BEGIN
-- the checks on the sequence properties are performed by the calling functions
-- get the schema and the name of the table that contains a serial or a "generated as identity" column this sequence is linked to, if one exists
    SELECT nt.nspname, ct.relname INTO v_tableSchema, v_tableName
      FROM pg_catalog.pg_class cs, pg_catalog.pg_namespace ns, pg_depend,
           pg_catalog.pg_class ct, pg_catalog.pg_namespace nt
      WHERE cs.relname = grpdef.grpdef_tblseq AND ns.nspname = grpdef.grpdef_schema -- the selected sequence
        AND cs.relnamespace = ns.oid                             -- join condition for sequence schema name
        AND ct.relnamespace = nt.oid                             -- join condition for linked table schema name
        AND pg_depend.objid = cs.oid                             -- join condition for the pg_depend table
        AND pg_depend.refobjid = ct.oid                          -- join conditions for depended table schema name
        AND pg_depend.classid = pg_depend.refclassid             -- the classid et refclassid must be 'pg_class'
        AND pg_depend.classid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_class');
    IF FOUND THEN
      SELECT grpdef_group INTO v_tableGroup FROM emaj.emaj_group_def
        WHERE grpdef_schema = v_tableSchema AND grpdef_tblseq = v_tableName;
      IF NOT FOUND THEN
        RAISE WARNING '_create_seq: The sequence %.% is linked to table %.% but this table does not belong to any tables group.', grpdef.grpdef_schema, grpdef.grpdef_tblseq, v_tableSchema, v_tableName;
      ELSE
        IF v_tableGroup <> grpdef.grpdef_group THEN
          RAISE WARNING '_create_seq: The sequence %.% is linked to table %.% but this table belong to another tables group (%).', grpdef.grpdef_schema, grpdef.grpdef_tblseq, v_tableSchema, v_tableName, v_tableGroup;
        END IF;
      END IF;
    END IF;
-- record the sequence in the emaj_relation table
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_kind)
          VALUES (grpdef.grpdef_schema, grpdef.grpdef_tblseq, grpdef.grpdef_group, grpdef.grpdef_priority, 'S');
    RETURN;
  END;
$_create_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_minGlobalSeq BIGINT, v_maxGlobalSeq BIGINT, v_nbSession INT, v_isLoggedRlbk BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence
-- The function is called by emaj._rlbk_session_exec()
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence (non inclusive) lower and (inclusive) upper limits covering the rollback time frame
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
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'All log rows with emaj_gid > ' || v_minGlobalSeq || ' and <= ' || v_maxGlobalSeq);
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
    EXECUTE 'CREATE ' || v_tableType || ' TABLE ' || v_tmpTable || ' AS '
         || '  SELECT ' || r_rel.rel_sql_pk_columns || ', min(emaj_gid) as emaj_gid'
         || '    FROM ' || v_logTableName
         || '    WHERE emaj_gid > ' || v_minGlobalSeq || 'AND emaj_gid <= ' || v_maxGlobalSeq
         || '    GROUP BY ' || r_rel.rel_sql_pk_columns;
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- delete all rows from the application table corresponding to each touched primary key
--   this deletes rows inserted or updated during the rolled back period
    EXECUTE 'DELETE FROM ONLY ' || v_fullTableName || ' tbl USING ' || v_tmpTable || ' keys '
         || '  WHERE ' || r_rel.rel_sql_pk_eq_conditions;
-- for logged rollbacks, if the number of pkey to process is greater than 1.000, ANALYZE the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement
    IF v_isLoggedRlbk AND v_nbPk > 1000 THEN
      EXECUTE 'ANALYZE ' || v_logTableName;
    END IF;
-- insert into the application table rows that were deleted or updated during the rolled back period
    IF emaj._pg_version_num() >= 100000 THEN
      v_insertClause = ' OVERRIDING SYSTEM VALUE';
    END IF;
    EXECUTE 'INSERT INTO ' || v_fullTableName || v_insertClause
         || '  SELECT ' || r_rel.rel_sql_columns
         || '    FROM ' || v_logTableName || ' tbl, ' || v_tmpTable || ' keys '
         || '    WHERE ' || r_rel.rel_sql_pk_eq_conditions || ' AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
         || '      AND tbl.emaj_gid > ' || v_minGlobalSeq || 'AND tbl.emaj_gid <= ' || v_maxGlobalSeq;
-- drop the now useless temporary table
    EXECUTE 'DROP TABLE ' || v_tmpTable;
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
    IF emaj._pg_version_num() < 100000 THEN
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
    ELSE
      EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size) VALUES ('
        || quote_literal(r_rel.rel_schema) || ',' || quote_literal(r_rel.rel_tblseq) || ',' || v_beginTimeId || ',' || v_endTimeId || ', ('
        || ' SELECT CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END FROM '
        || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence) || ' rel, pg_sequences'
        || ' WHERE schemaname = '|| quote_literal(r_rel.rel_log_schema) || ' AND sequencename = ' || quote_literal(r_rel.rel_log_sequence)
        || ')-('
        || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
        || ' emaj.emaj_sequence WHERE'
        || ' sequ_schema = ' || quote_literal(r_rel.rel_log_schema)
        || ' AND sequ_name = ' || quote_literal(r_rel.rel_log_sequence)
        || ' AND sequ_time_id = ' || v_beginTimeId || '))';
    END IF;
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

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
-- Input: schema name and table name, log schema, the time stamp ids defining the time range to examine
--   a end time stamp id set to NULL indicates the current situation
-- Output: number of log rows between both marks for the table
  DECLARE
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- get the log table id at begin time id
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_beginLastValue
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
-- check the postgres version: E-Maj needs postgres 9.1+
    IF emaj._pg_version_num() < 90100 THEN
      RAISE EXCEPTION '_verify_groups : The current postgres version (%) is not compatible with E-Maj.', version();
    END IF;
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
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
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
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
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
      WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
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
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'),
           cte_log_tables_columns AS (                -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
              AND relname = rel_log_table
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r')
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
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND rel_group = group_name AND group_is_rollbackable
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
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
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
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND relhasoids
        ORDER BY 1,2,3
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj._check_fk_groups(v_groupNames TEXT[])
RETURNS VOID LANGUAGE plpgsql AS
$_check_fk_groups$
-- this function checks foreign key constraints for tables of a groups array.
-- tables from audit_only groups are ignored in this check because they will never be rolled back.
-- Input: group names array
  DECLARE
    r_fk                     RECORD;
  BEGIN
-- issue a warning if a table of the groups has a foreign key that references a table outside the groups
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace nf, pg_catalog.pg_class tf, emaj.emaj_relation r, emaj.emaj_group g
        WHERE contype = 'f'                                         -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid      -- join for table and namespace
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid     -- join for referenced table and namespace
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq -- join on emaj_relation table
          AND r.rel_group = g.group_name                            -- join on emaj_group table
          AND r.rel_group = ANY (v_groupNames)                      -- only tables of the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND NOT EXISTS                                            -- referenced table outside the groups
              (SELECT NULL FROM emaj.emaj_relation
                 WHERE rel_schema = nf.nspname AND rel_tblseq = tf.relname AND rel_group = ANY (v_groupNames))
        ORDER BY 1,2,3
      LOOP
      RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" references the table "%.%" that is outside the groups (%).',
                r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
-- issue a warning if a table of the groups is referenced by a table outside the groups
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace nf, pg_catalog.pg_class tf, emaj.emaj_relation r, emaj.emaj_group g
        WHERE contype = 'f'                                           -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid        -- join for table and namespace
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid       -- join for referenced table and namespace
          AND nf.nspname = r.rel_schema AND tf.relname = r.rel_tblseq -- join with emaj_relation table
          AND r.rel_group = g.group_name                              -- join on emaj_group table
          AND r.rel_group = ANY (v_groupNames)                        -- only tables of the selected groups
          AND g.group_is_rollbackable                                 -- only tables from rollbackable groups
          AND NOT EXISTS                                              -- referenced table outside the groups
              (SELECT NULL FROM emaj.emaj_relation
                 WHERE rel_schema = n.nspname AND rel_tblseq = t.relname AND rel_group = ANY (v_groupNames))
        ORDER BY 1,2,3
      LOOP
      RAISE WARNING '_check_fk_groups: The table "%.%" is referenced by the foreign key "%" on the table "%.%" that is outside the groups (%).',
                r_fk.rel_schema,r_fk.rel_tblseq,r_fk.conname,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
    RETURN;
  END;
$_check_fk_groups$;

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
    v_ok                     BOOLEAN = false;
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
-- scan all existing tables of the groups
        v_nbTbl = 0;
        FOR r_tblsq IN
            SELECT rel_priority, rel_schema, rel_tblseq
               FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
               WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
                 AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
               ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
-- lock the table
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE 'LOCK TABLE ' || v_fullTableName || ' IN ' || v_lockMode || ' MODE';
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = true;
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
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_lock_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN DEFAULT true, v_is_empty BOOLEAN DEFAULT false)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- It also creates the secondary E-Maj schemas when needed
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
--        boolean explicitely indicating whether the group is empty or not
-- Output: number of processed tables and sequences
  DECLARE
    v_timeId                 BIGINT;
    v_nbTbl                  INT = 0;
    v_nbSeq                  INT = 0;
    v_schemaPrefix           TEXT = 'emaj';
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
    r_schema                 RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName, CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- check that the group name is valid
    IF v_groupName IS NULL OR v_groupName = ''THEN
      RAISE EXCEPTION 'emaj_create_group: The group name can''t be NULL or empty.';
    END IF;
-- check that the group is not yet recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: The group "%" already exists.', v_groupName;
    END IF;
-- check the consistency between the emaj_group_def table content and the v_is_empty input parameter
    PERFORM 0 FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName LIMIT 1;
    IF NOT v_is_empty AND NOT FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: The group "%" is unknown in the emaj_group_def table. To create an empty group, explicitely set the third parameter to true.', v_groupName;
    END IF;
    IF v_is_empty AND FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: The group "%" is referenced into the emaj_group_def table. This is not consistent with the <is_empty> parameter set to true.', v_groupName;
    END IF;
-- performs various checks on the group's content described in the emaj_group_def table
    PERFORM emaj._check_groups_content(ARRAY[v_groupName],v_isRollbackable);
-- OK
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('C') INTO v_timeId;
-- insert the row describing the group into the emaj_group table
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable)
    INSERT INTO emaj.emaj_group (group_name, group_is_logging, group_is_rollbackable, group_is_rlbk_protected, group_creation_time_id)
      VALUES (v_groupName, FALSE, v_isRollbackable, NOT v_isRollbackable, v_timeId);
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
      PERFORM emaj._create_tbl(r_grpdef, v_isRollbackable);
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
      PERFORM emaj._create_seq(r_grpdef);
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
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN,BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_group$
-- This function sets or modifies a comment on a group by updating the group_comment of the emaj_group table.
-- Input: group name, comment
--   To reset an existing comment for a group, the supplied comment can be NULL.
  DECLARE
  BEGIN
-- attempt to update the group_comment column from emaj_group table
    UPDATE emaj.emaj_group SET group_comment = v_comment WHERE group_name = v_groupName;
-- check that the group has been found
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_group: The group "%" does not exist.', v_groupName;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_GROUP', v_groupName);
    RETURN;
  END;
$emaj_comment_group$;
COMMENT ON FUNCTION emaj.emaj_comment_group(TEXT,TEXT) IS
$$Sets a comment on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- It also drops secondary schemas that are not useful any more
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that secondary schemas can be dropped
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_schemasToDrop          TEXT[];
    v_nbTb                   INT = 0;
    v_schemaPrefix           TEXT = 'emaj';
    v_logSchema              TEXT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_group: The group "%" does not exist.', v_groupName;
    END IF;
-- if the state of the group has to be checked,
    IF NOT v_isForced THEN
--   check that the group is not in LOGGING state
      IF v_groupIsLogging THEN
        RAISE EXCEPTION '_drop_group: The group "%" cannot be dropped because it is in LOGGING state.', v_groupName;
      END IF;
    END IF;
-- OK
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- build the list of secondary schemas to drop later
    SELECT coalesce(array_agg(rel_log_schema),'{}') INTO v_schemasToDrop FROM (
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_log_schema <>  v_schemaPrefix
      EXCEPT
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
        WHERE rel_group <> v_groupName AND rel_log_schema <>  v_schemaPrefix
      ORDER BY 1
    ) AS t;
-- delete the emaj objets for each table of the group
    FOR r_rel IN
        SELECT * FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_rel.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
        PERFORM emaj._drop_tbl(r_rel);
        ELSEIF r_rel.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
          PERFORM emaj._drop_seq(r_rel);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- drop the E-Maj secondary schemas previously identified as useless (i.e. not used by any other created group)
    FOREACH v_logSchema IN ARRAY v_schemasToDrop
      LOOP
-- drop the schema
      PERFORM emaj._drop_log_schema(v_logSchema, v_isForced);
-- and record the schema suppression in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (CASE WHEN v_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END,'SCHEMA DROPPED',quote_ident(v_logSchema));
    END LOOP;
-- delete group row from the emaj_group table.
--   By cascade, it also deletes rows from emaj_mark
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_alter_group(v_groupName TEXT, v_mark TEXT DEFAULT 'ALTER_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_alter_group$
-- This function alters a tables group.
-- Input: group name
-- Output: number of tables and sequences belonging to the group after the operation
  BEGIN
    RETURN emaj._alter_groups(ARRAY[v_groupName], false, v_mark);
  END;
$emaj_alter_group$;
COMMENT ON FUNCTION emaj.emaj_alter_group(TEXT, TEXT) IS
$$Alter an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_alter_groups(v_groupNames TEXT[], v_mark TEXT DEFAULT 'ALTER_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_alter_groups$
-- This function alters several tables groups.
-- Input: group names array
-- Output: number of tables and sequences belonging to the groups after the operation
  BEGIN
    RETURN emaj._alter_groups(v_groupNames, true, v_mark);
  END;
$emaj_alter_groups$;
COMMENT ON FUNCTION emaj.emaj_alter_groups(TEXT[], TEXT) IS
$$Alter several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._alter_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_alter_groups$
-- This function effectively alters a tables groups array.
-- It takes into account the changes recorded in the emaj_group_def table since the groups have been created.
-- Input: group names array, flag indicating whether the function is called by the multi-group function or not
-- Output: number of tables and sequences belonging to the groups after the operation
  DECLARE
    v_aGroupName             TEXT;
    v_loggingGroups          TEXT[];
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_eventTriggers          TEXT[];
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','));
-- check that each group is recorded in emaj_group table, and take a lock on it to avoid other actions on these groups
    FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
      PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_aGroupName FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_alter_groups: The group "%" does not exist.', v_aGroupName;
      END IF;
    END LOOP;
-- performs various checks on the groups content described in the emaj_group_def table
    PERFORM emaj._check_groups_content(v_groupNames, NULL);
-- build the list of groups that are in logging state
    SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames) AND group_is_logging;
-- check and process the supplied mark name, if it is worth to be done
    IF v_loggingGroups IS NOT NULL THEN
      SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
    END IF;
-- OK
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- for LOGGING groups, lock all tables to get a stable point
    IF v_loggingGroups IS NOT NULL THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
       PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', v_multiGroup);
-- and set the mark, using the same time identifier
       PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, v_multiGroup, true, NULL, v_timeId);
    END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- we can now plan all the steps needed to perform the operation
    PERFORM emaj._alter_plan(v_groupNames, v_timeId);
-- and then execute the plan
    PERFORM emaj._alter_exec(v_timeId);
-- update tables and sequences counters and the last alter timestamp in the emaj_group table
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = v_timeId,
          group_nb_table = (SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = group_name AND rel_kind = 'r'),
          group_nb_sequence = (SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = group_name AND rel_kind = 'S')
      WHERE group_name = ANY (v_groupNames);
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- check foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(v_groupNames);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'END', array_to_string(v_groupNames,','),
              'Timestamp Id : ' || v_timeId );
-- and return
    RETURN sum(group_nb_table) + sum(group_nb_sequence) FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames);
  END;
$_alter_groups$;

CREATE OR REPLACE FUNCTION emaj._alter_plan(v_groupNames TEXT[], v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_alter_plan$
-- This function build the elementary steps that will be needed to perform an alter_groups operation.
-- Looking at emaj_relation and emaj_group_def tables, it populates the emaj_alter_plan table that will be used by the _alter_exec() function.
-- Input: group names array, timestamp id of the operation (it will be used to identify rows in the emaj_alter_plan table)
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_groups                 TEXT;
  BEGIN
-- determine the relations that do not belong to the groups anymore
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REMOVE_TBL' ELSE 'REMOVE_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, rel_priority
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames)
          AND NOT EXISTS (
              SELECT NULL FROM emaj.emaj_group_def
                WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
                  AND grpdef_group = ANY (v_groupNames));
-- determine the secondary log schemas that need to be created before new log tables
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CREATE_LOG_SCHEMA', rel_log_schema, '', '', NULL FROM (
        SELECT DISTINCT v_schemaPrefix || grpdef_log_schema_suffix AS rel_log_schema FROM emaj.emaj_group_def
          WHERE grpdef_group = ANY (v_groupNames)
            AND grpdef_log_schema_suffix IS NOT NULL AND grpdef_log_schema_suffix <> ''   -- secondary log schemas needed for the groups
        EXCEPT
        SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation                            -- minus those already created
        ) AS t;
-- determine the tables that need to be "repaired" (damaged or out of sync E-Maj components)
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REPAIR_TBL' ELSE 'REPAIR_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, rel_priority
        FROM (                                   -- all damaged or out of sync tables
          SELECT DISTINCT ver_schema, ver_tblseq FROM emaj._verify_groups(v_groupNames, false)
             ) AS t, emaj.emaj_relation
        WHERE rel_schema = ver_schema AND rel_tblseq = ver_tblseq
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
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
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
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
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
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
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
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
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
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
        AND rel_group = ANY (v_groupNames)
        AND grpdef_group = ANY (v_groupNames)
        AND rel_group <> grpdef_group;
-- determine the relation that change their priority level
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_priority)
      SELECT v_timeId, 'CHANGE_REL_PRIORITY', rel_schema, rel_tblseq, rel_group, rel_priority, grpdef_priority
      FROM emaj.emaj_relation, emaj.emaj_group_def
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
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
                WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
                  AND rel_group = ANY (v_groupNames))
          AND relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq;
-- determine the secondary log schemas that will need to be dropped once obsolete log tables will be dropped
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'DROP_LOG_SCHEMA', rel_log_schema, '', '', NULL FROM (
        SELECT rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_log_schema <> v_emajSchema         -- secondary log schemas that currently exist for the groups
        EXCEPT
        SELECT rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group <> ALL (v_groupNames)                                           -- minus those that exist for other groups
        EXCEPT
        SELECT v_schemaPrefix || grpdef_log_schema_suffix FROM emaj.emaj_group_def
          WHERE grpdef_group = ANY (v_groupNames)
            AND grpdef_log_schema_suffix IS NOT NULL AND grpdef_log_schema_suffix <> ''   -- minus those that will remain for the groups
        ) AS t;
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
    SELECT string_agg(DISTINCT altr_group, ', ') INTO v_groups
      FROM emaj.emaj_alter_plan
      WHERE altr_time_id = v_timeId
        AND altr_step IN ('REMOVE_TBL', 'REMOVE_SEQ', 'RESET_GROUP', 'REPAIR_TBL', 'ASSIGN_REL', 'ADD_TBL', 'ADD_SEQ')
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
    r_plan                   RECORD;
    r_rel                    emaj.emaj_relation%ROWTYPE;
    r_grpdef                 emaj.emaj_group_def%ROWTYPE;
  BEGIN
-- scan the emaj_alter_plan table and execute each elementary item in the proper order
    FOR r_plan IN
      SELECT altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging, altr_new_group, altr_new_priority
        FROM emaj.emaj_alter_plan
        WHERE altr_time_id = v_timeId
        ORDER BY altr_step, altr_priority, altr_schema, altr_tblseq, altr_group
      LOOP
      CASE r_plan.altr_step
        WHEN 'REMOVE_TBL' THEN
-- remove a table from its group
          PERFORM emaj._drop_tbl(emaj.emaj_relation.*) FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'REMOVE_SEQ' THEN
-- remove a sequence from its group
          PERFORM emaj._drop_seq(emaj.emaj_relation.*) FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'RESET_GROUP' THEN
-- reset a group
          PERFORM emaj._reset_groups(ARRAY[r_plan.altr_group]);
--
        WHEN 'CREATE_LOG_SCHEMA' THEN
-- create a log schema
          PERFORM emaj._create_log_schema(r_plan.altr_schema);
--   and record the schema creation in emaj_hist table
          INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
            VALUES ('ALTER_GROUP','SCHEMA CREATED',quote_ident(r_plan.altr_schema));
--
        WHEN 'REPAIR_TBL' THEN
          IF r_plan.altr_group_is_logging THEN
            RAISE EXCEPTION 'alter_exec: Cannot repair the table %.%. Its group % is in LOGGING state', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
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
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- and recreate it
            PERFORM emaj._create_tbl(r_grpdef, v_isRollbackable);
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
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- and recreate it
            PERFORM emaj._create_seq(r_grpdef);
          END IF;
--
        WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- get the table description from emaj_group_def
          SELECT grpdef_log_schema_suffix INTO v_logSchemaSuffix FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_schema_tbl(r_rel, v_logSchemaSuffix);
--
        WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- get the table description from emaj_group_def
          SELECT grpdef_emaj_names_prefix INTO v_emajNamesPrefix FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_emaj_names_prefix(r_rel, v_emajNamesPrefix);
--
        WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- get the table description from emaj_group_def
          SELECT grpdef_log_dat_tsp INTO v_logDatTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_data_tsp_tbl(r_rel, v_logDatTsp);
--
        WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- get the table description from emaj_group_def
          SELECT grpdef_log_idx_tsp INTO v_logIdxTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_index_tsp_tbl(r_rel, v_logIdxTsp);
--
        WHEN 'ASSIGN_REL' THEN
-- update the emaj_relation table to report the group ownership change
          UPDATE emaj.emaj_relation SET rel_group = r_plan.altr_new_group
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'CHANGE_REL_PRIORITY' THEN
-- update the emaj_relation table to report the priority change
          UPDATE emaj.emaj_relation SET rel_priority = r_plan.altr_new_priority
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'ADD_TBL' THEN
-- get the is_rollbackable status of the related group
          SELECT group_is_rollbackable INTO v_isRollbackable
            FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- get the table description from emaj_group_def
          SELECT * INTO r_grpdef
            FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- create the table
          PERFORM emaj._create_tbl(r_grpdef, v_isRollbackable);
--
        WHEN 'ADD_SEQ' THEN
-- create the sequence
          PERFORM emaj._create_seq(emaj.emaj_group_def.*) FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
--
        WHEN 'DROP_LOG_SCHEMA' THEN
-- drop the log schema
          PERFORM emaj._drop_log_schema(r_plan.altr_schema, false);
-- and record the schema drop in emaj_hist table
          INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
            VALUES ('ALTER_GROUP','SCHEMA DROPPED',quote_ident(r_plan.altr_schema));
--
      END CASE;
    END LOOP;
    RETURN;
  END;
$_alter_exec$;

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_aGroupName             TEXT;
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
    FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_aGroupName FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_start_groups: The group "%" does not exist.', v_aGroupName;
      END IF;
-- ... and is not in LOGGING state
      IF v_groupIsLogging THEN
        RAISE EXCEPTION '_start_groups: The group "%" cannot be started because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_aGroupName;
      END IF;
    END LOOP;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_groups(v_groupNames);
-- if requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs
    if v_resetLog THEN
      PERFORM emaj._reset_groups(v_groupNames);
    END IF;
-- check and process the supplied mark name
    IF v_mark IS NULL OR v_mark = '' THEN
      v_mark = 'START_%';
    END IF;
    SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
-- OK, lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
--   the requested lock level is based on the lock level of the future ALTER TABLE, which depends on the postgres version.
    IF emaj._pg_version_num() >= 90500 THEN
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
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
          v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg';
        WHEN 'S' THEN
-- if it is a sequence, nothing to do
      END CASE;
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
    v_validGroupNames        TEXT[];
    v_aGroupName             TEXT;
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    r_schema                 RECORD;
    r_tblsq                  RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- for each group of the array,
    FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
-- ... check that the group is recorded in emaj_group table
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_aGroupName FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_stop_groups: The group "%" does not exist.', v_aGroupName;
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE WARNING '_stop_groups: The group "%" cannot be stopped because it is not in LOGGING state.', v_aGroupName;
      ELSE
-- ... if OK, add the group into the array of groups to process
        v_validGroupNames = v_validGroupNames || array[v_aGroupName];
      END IF;
    END LOOP;
-- check and process the supplied mark name (except if the function is called by emaj_force_stop_group())
    IF v_mark IS NULL OR v_mark = '' THEN
      v_mark = 'STOP_%';
    END IF;
    IF NOT v_isForced THEN
      SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
    END IF;
--
    IF v_validGroupNames IS NOT NULL THEN
-- OK (no error detected and at least one group in logging state)
-- lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
--   the requested lock level is based on the lock level of the future ALTER TABLE, which depends on the postgres version.
      IF emaj._pg_version_num() >= 90500 THEN
        PERFORM emaj._lock_groups(v_validGroupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
      ELSE
        PERFORM emaj._lock_groups(v_validGroupNames,'ACCESS EXCLUSIVE',v_multiGroup);
      END IF;
-- verify that all application schemas for the groups still exists
      FOR r_schema IN
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_validGroupNames)
              AND NOT EXISTS (SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = rel_schema)
            ORDER BY rel_schema
        LOOP
        IF v_isForced THEN
          RAISE WARNING '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        END IF;
      END LOOP;
-- for each relation of the groups to process,
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_validGroupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
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
                EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_log_trg';
              EXCEPTION
                WHEN undefined_object THEN
                  IF v_isForced THEN
                    RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  END IF;
              END;
              BEGIN
                EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_trunc_trg';
              EXCEPTION
                WHEN undefined_object THEN
                  IF v_isForced THEN
                    RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  END IF;
              END;
            END IF;
          WHEN 'S' THEN
-- if it is a sequence, nothing to do
        END CASE;
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
      RAISE EXCEPTION 'emaj_protect_group: The group "%" does not exist.', v_groupName;
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

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_group$
-- This function unsets a protection on a group against accidental rollback.
-- Input: group name
-- Output: 1 if successful, 0 if the group was not already in protected state
  DECLARE
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsProtected       BOOLEAN;
    v_status                 INT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_rollbackable, group_is_rlbk_protected INTO v_groupIsRollbackable, v_groupIsProtected
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_unprotect_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_unprotect_group: The group "%" cannot be unprotected because it is an AUDIT_ONLY group.', v_groupName;
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

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group
-- Input: group name, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
-- Output: number of processed tables and sequences
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_markName               TEXT;
    v_nbTb                   INT;
  BEGIN
-- insert begin into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'BEGIN', v_groupName, v_markName);
-- check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_set_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_set_mark_group: A mark cannot be set for group "%" because it is not in LOGGING state. An emaj_start_group function must be previously executed.', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM 0 FROM emaj._verify_groups(array[v_groupName], true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, array[v_groupName]) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(array[v_groupName],'ROW EXCLUSIVE',false);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(array[v_groupName], v_markName, false, false) INTO v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'END', v_groupName, v_markName);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time
-- Input: array of group names, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
-- Output: number of processed tables and sequences
  DECLARE
    v_validGroupNames        TEXT[];
    v_aGroupName             TEXT;
    v_groupIsLogging         BOOLEAN;
    v_markName               TEXT;
    v_nbTb                   INT;
  BEGIN
-- validate the group names array
    v_validGroupNames=emaj._check_names_array(v_groupNames,'group');
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
    FOREACH v_aGroupName IN ARRAY v_validGroupNames LOOP
-- ... check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_aGroupName FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: The group "%" does not exist.', v_aGroupName;
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: A mark cannot be set for group "%" because it is not in LOGGING state. An emaj_start_group function must be previously executed.', v_aGroupName;
      END IF;
    END LOOP;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_validGroupNames, true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_validGroupNames) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(v_validGroupNames,'ROW EXCLUSIVE',true);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(v_validGroupNames, v_markName, true, false) INTO v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(v_groupNames,','), v_mark);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT) IS
$$Sets a mark on several E-Maj groups.$$;

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
    v_nbTb                   INT = 0;
    v_timestamp              TIMESTAMPTZ;
    r_tblsq                  RECORD;
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
-- look at the clock to get the 'official' timestamp representing the mark
    SELECT time_clock_timestamp INTO v_timestamp FROM emaj.emaj_time_stamp WHERE time_id = v_timeId;
-- process sequences as early as possible (no lock protects them from other transactions activity)
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- for each sequence of the groups, record the sequence parameters into the emaj_sequence table
      IF emaj._pg_version_num() < 100000 THEN
        EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
                'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
                'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
                quote_literal(r_tblsq.rel_tblseq) || ', ' || v_timeId ||
                ', last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                'FROM ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      ELSE
        EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
                'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
                'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                ') SELECT schemaname, sequencename, ' || v_timeId ||
                ', rel.last_value, start_value, increment_by, max_value, min_value, cache_size, cycle, rel.is_called ' ||
                'FROM ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq) ||
                ' rel, pg_catalog.pg_sequences ' ||
                ' WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_tblseq);
      END IF;
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
      IF emaj._pg_version_num() < 100000 THEN
        EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
                'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
                'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                ') SELECT '|| quote_literal(r_tblsq.rel_log_schema) || ', ' || quote_literal(r_tblsq.rel_log_sequence) || ', ' ||
                v_timeId || ', last_value, start_value, ' ||
                'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                'FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_sequence);
      ELSE
        EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
                'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
                'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                ') SELECT schemaname, sequencename, ' || v_timeId ||
                ', rel.last_value, start_value, increment_by, max_value, min_value, cache_size, cycle, rel.is_called ' ||
                'FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_sequence) ||
                ' rel, pg_catalog.pg_sequences ' ||
                ' WHERE schemaname = '|| quote_literal(r_tblsq.rel_log_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_log_sequence);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the mark for each group into the emaj_mark table
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, v_mark, v_timeId, FALSE, FALSE, v_loggedRlbkTargetMark
        FROM emaj.emaj_group WHERE group_name = ANY(v_groupNames) ORDER BY group_name;
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

CREATE OR REPLACE FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_mark_group$
-- This function sets or modifies a comment on a mark by updating the mark_comment of the emaj_mark table.
-- Input: group name, mark to comment, comment
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
--   To reset an existing comment for a mark, the supplied comment can be NULL.
  DECLARE
    v_realMark               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_comment_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- OK, update the mark_comment from emaj_mark table
    UPDATE emaj.emaj_mark SET mark_comment = v_comment WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('COMMENT_MARK_GROUP', v_groupName, 'Mark ' || v_realMark);
    RETURN;
  END;
$emaj_comment_mark_group$;
COMMENT ON FUNCTION emaj.emaj_comment_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a comment on a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given date and time.
-- It may return unpredictable result in case of system date or time change.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, date and time
-- Output: mark name, or NULL if there is no mark before the given date and time
  DECLARE
    v_markName               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group (1): The group "%" does not exist.', v_groupName;
    END IF;
-- find the requested mark
    SELECT mark_name INTO v_markName FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND time_clock_timestamp < v_datetime
      ORDER BY time_clock_timestamp DESC LIMIT 1;
    IF NOT FOUND THEN
      RETURN NULL;
    ELSE
      RETURN v_markName;
    END IF;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TIMESTAMPTZ) IS
$$Returns the latest mark name preceeding a point in time.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given mark for a group.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, mark name
--   The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark.
-- Output: mark name, or NULL if there is no mark before the given mark
  DECLARE
    v_realMark               TEXT;
    v_markName               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group (2): The group "%" does not exist.', v_groupName;
    END IF;
-- retrieve and check the given mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- find the requested mark
    SELECT mark_name INTO v_markName FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_time_id <
        (SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark)
      ORDER BY mark_time_id DESC LIMIT 1;
    IF NOT FOUND THEN
      RETURN NULL;
    ELSE
      RETURN v_markName;
    END IF;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TEXT) IS
$$Returns the latest mark name preceeding a given mark for a group.$$;

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
    v_realMark               TEXT;
    v_markId                 BIGINT;
    v_MarkTimeId             BIGINT;
    v_idNewMin               BIGINT;
    v_markNewMin             TEXT;
    v_cpt                    INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- count the number of marks in the group
    SELECT count(*) INTO v_cpt FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- and check there are at least 2 marks for the group
    IF v_cpt < 2 THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: "%" is the only mark of the group. It cannot be deleted.', v_mark;
    END IF;
-- OK, now get the id and time stamp id of the mark to delete
    SELECT mark_id, mark_time_id INTO v_markId, v_MarkTimeId
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- ... and the id and timestamp of the future first mark
    SELECT mark_id, mark_name INTO v_idNewMin, v_markNewMin
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name <> v_realMark ORDER BY mark_id LIMIT 1;
    IF v_markId < v_idNewMin THEN
-- if the mark to delete is the first one, process its deletion with _delete_between_marks_group(), as the first rows of log tables become useless
      PERFORM emaj._delete_between_marks_group(v_groupName, NULL, v_markNewMin);
    ELSE
-- otherwise, process its deletion with _delete_intermediate_mark_group()
      PERFORM emaj._delete_intermediate_mark_group(v_groupName, v_realMark, v_markId, v_MarkTimeId);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_realMark);
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
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
    v_realMark               TEXT;
    v_nbMark                 INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- return NULL if mark name is NULL
    IF v_mark IS NULL THEN
      RETURN NULL;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- effectively delete all marks before the supplied mark
    SELECT (emaj._delete_between_marks_group(v_groupName, NULL, v_realMark)).v_nbMark INTO v_nbMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', v_groupName,  v_nbMark || ' marks deleted ; ' || v_realMark || ' is now the initial mark' );
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be in LOGGING or not.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly.
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  DECLARE
    v_realMark               TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- check the new mark name is not 'EMAJ_LAST_MARK' or NULL
    IF v_newName = 'EMAJ_LAST_MARK' OR v_newName IS NULL THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: "%" is not an allowed name for a new mark.', v_newName;
    END IF;
-- check if the new mark name doesn't exist for the group
    PERFORM 0 FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_newName;
    IF FOUND THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: A mark "%" already exists for the group "%".', v_newName, v_groupName;
    END IF;
-- OK, update the emaj_mark table
    UPDATE emaj.emaj_mark SET mark_name = v_newName
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- also rename mark names recorded in the mark_logged_rlbk_target_mark column if needed
    UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = v_newName
      WHERE mark_group = v_groupName AND mark_logged_rlbk_target_mark = v_realMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'END', v_groupName, v_realMark || ' renamed ' || v_newName);
    RETURN;
  END;
$emaj_rename_mark_group$;
COMMENT ON FUNCTION emaj.emaj_rename_mark_group(TEXT,TEXT,TEXT) IS
$$Renames a mark for an E-Maj group.$$;

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
    v_markIsDeleted          BOOLEAN;
    v_status                 INT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: A mark on the group "%" cannot be protected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- retrieve and check that the mark name exists
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
    END IF;
-- check that the mark is not logicaly deleted
    SELECT mark_is_deleted, mark_is_rlbk_protected INTO v_markIsDeleted, v_markIsProtected FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
    IF v_markIsDeleted THEN
      RAISE EXCEPTION 'emaj_protect_mark_group: The mark "%" for the group "%" is logicaly deleted and thus cannot be protected.', v_mark, v_groupName;
    END IF;
-- OK, set the protection, if not already set
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
    v_realMark               TEXT;
    v_markIsProtected        BOOLEAN;
    v_status                 INT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_rollbackable INTO v_groupIsRollbackable
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_unprotect_mark_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check that the group is ROLLBACKABLE
    IF NOT v_groupIsRollbackable THEN
      RAISE EXCEPTION 'emaj_unprotect_mark_group: A mark on the group "%" cannot be unprotected because it is an AUDIT_ONLY group.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_unprotect_mark_group: The mark "%" does not exist for the group "%".', v_mark, v_groupName;
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

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- Input: group name, mark to rollback to
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the group (with boolean: isLoggedRlbk = false, multiGroup = false, v_isAlterGroupAllowed = null)
    RETURN rlbk_message::INT FROM emaj._rlbk_groups(array[v_groupName], v_mark, FALSE, FALSE, NULL) WHERE rlbk_severity = 'Notice';
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT) IS
$$Rollbacks an E-Maj group to a given mark (deprecated).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history
-- Input: array of group names, mark to rollback to
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the groups (with boolean: isLoggedRlbk = false, multiGroup = true, v_isAlterGroupAllowed = null)
    RETURN rlbk_message::INT FROM emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, FALSE, TRUE, NULL) WHERE rlbk_severity = 'Notice';
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT) IS
$$Rollbacks an set of E-Maj groups to a given mark (deprecated).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark to rollback to
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the group (with boolean: isLoggedRlbk = true, multiGroup = false, v_isAlterGroupAllowed = null)
    RETURN rlbk_message::INT FROM emaj._rlbk_groups(array[v_groupName], v_mark, TRUE, FALSE, NULL) WHERE rlbk_severity = 'Notice';
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark (deprecated).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true, v_isAlterGroupAllowed = null)
    RETURN rlbk_message::INT FROM emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, TRUE, TRUE, NULL) WHERE rlbk_severity = 'Notice';
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark (deprecated).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just (unlogged) rollback the group (with boolean: isLoggedRlbk = false, multiGroup = false)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(array[v_groupName], v_mark, FALSE, FALSE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT,BOOLEAN) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just (unlogged) rollback the groups (with boolean: isLoggedRlbk = false, multiGroup = true)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, FALSE, TRUE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just "logged-rollback" the group (with boolean: isLoggedRlbk = true, multiGroup = false)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(array[v_groupName], v_mark, TRUE, FALSE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT,BOOLEAN) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, TRUE, TRUE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_groups$
-- The function rollbacks all tables and sequences of a groups array up to a mark in the history.
-- It is called by emaj_rollback_group.
-- It effectively manages the rollback operation for each table or sequence, deleting rows from log tables
-- only when asked by the calling functions.
-- Its activity is split into smaller functions that are also called by the parallel restore php function
-- Input: group name, mark to rollback to, a boolean indicating whether the rollback is a logged rollback, a boolean indicating whether the function
--        is a multi_group function and a boolean saying whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
    v_rlbkId                 INT;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
       rlbk_severity = 'Notice'; rlbk_message = 0;
       RETURN NEXT;
      RETURN;
    END IF;
-- check supplied parameter and prepare the rollback operation
    SELECT emaj._rlbk_init(v_groupNames, v_mark, v_isLoggedRlbk, 1, v_multiGroup, v_isAlterGroupAllowed) INTO v_rlbkId;
-- lock all tables
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
-- set a rollback start mark if logged rollback
    PERFORM emaj._rlbk_start_mark(v_rlbkId, v_multiGroup);
-- execute the rollback planning
    PERFORM emaj._rlbk_session_exec(v_rlbkId, 1);
-- process sequences, complete the rollback operation and return the execution report
    RETURN QUERY SELECT * FROM emaj._rlbk_end(v_rlbkId, v_multiGroup, v_isAlterGroupAllowed);
  END;
$_rlbk_groups$;

CREATE OR REPLACE FUNCTION emaj._rlbk_async(v_rlbkId INT, v_multiGroup BOOLEAN, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_async$
-- The function calls the main rollback functions following the initialisation phase.
-- It is only called by the phpPgAdmin plugin, in an asynchronous way, so that the rollback can be then monitored by the client.
-- Input: rollback identifier, a boolean saying if the rollback is a logged rollback
--        and a boolean saying whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
  BEGIN
-- simply chain the internal functions
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
    PERFORM emaj._rlbk_start_mark(v_rlbkId, v_multiGroup);
    PERFORM emaj._rlbk_session_exec(v_rlbkId, 1);
    RETURN QUERY SELECT * FROM emaj._rlbk_end(v_rlbkId, v_multiGroup, v_isAlterGroupAllowed);
  END;
$_rlbk_async$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps.
  DECLARE
    v_markName               TEXT;
    v_markTimeId             BIGINT;
    v_markTimestamp          TIMESTAMPTZ;
    v_msg                    TEXT;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsable         BOOLEAN = false;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_stmt                   TEXT;
    v_rlbkId                 INT;
  BEGIN
-- lock the groups to rollback
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = ANY(v_groupNames) FOR UPDATE;
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames, v_mark, v_isAlterGroupAllowed, FALSE) INTO v_markName;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- get the time stamp id and its clock timestamp for the 1st group (as we know this time stamp is the same for all groups of the array)
    SELECT emaj._get_mark_time_id(v_groupNames[1], v_mark) INTO v_markTimeId;
    SELECT time_clock_timestamp INTO v_markTimestamp
      FROM emaj.emaj_time_stamp WHERE time_id = v_markTimeId;
-- insert begin in the history
    IF v_isLoggedRlbk THEN
      v_msg = 'Logged';
    ELSE
      v_msg = 'Unlogged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
              array_to_string(v_groupNames,','),
              v_msg || ' rollback to mark ' || v_markName || ' [' || v_markTimestamp || ']'
             ) RETURNING hist_id INTO v_histId;
-- get the total number of tables for these groups
    SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
      FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- first try to open a dblink connection
    SELECT emaj._dblink_open_cnx('rlbk#1') INTO v_dbLinkCnxStatus;
    v_isDblinkUsable = (v_dbLinkCnxStatus >= 0);
-- for parallel rollback (nb sessions > 1) the dblink connection must be ok
    IF v_nbSession > 1 AND NOT v_isDblinkUsable THEN
      RAISE EXCEPTION '_rlbk_init: Cannot use several sessions without dblink connection capability. (Status of the dblink connection attempt = % - see E-Maj documentation)', v_dbLinkCnxStatus;
    END IF;
-- create the row representing the rollback event in the emaj_rlbk table and get the rollback id back
    v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, ' ||
             'rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
             'rlbk_is_dblink_used) ' ||
             'VALUES (' || quote_literal(v_groupNames) || ',' || quote_literal(v_markName) || ',' ||
             v_markTimeId || ',' || v_isLoggedRlbk || ',' || coalesce(v_isAlterGroupAllowed, false) || ',' ||
             v_nbSession || ',' || v_nbTblInGroups || ',' || v_nbSeqInGroups || ', ''PLANNING'',' || v_histId || ',' ||
             v_isDblinkUsable || ') RETURNING rlbk_id';
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

CREATE OR REPLACE FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, isRollbackSimulation BOOLEAN)
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
    v_markTimeId             BIGINT;
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
        RAISE EXCEPTION '_rlbk_check: The group "%" does not exist.', v_aGroupName;
      END IF;
-- ... is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION '_rlbk_check: The group "%" is not in LOGGING state.', v_aGroupName;
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_groupIsRollbackable THEN
        RAISE EXCEPTION '_rlbk_check: The group "%" has been created for audit only purpose.', v_aGroupName;
      END IF;
-- ... is not protected against rollback (check disabled for rollback simulation)
      IF v_groupIsProtected AND NOT isRollbackSimulation THEN
        RAISE EXCEPTION '_rlbk_check: The group "%" is currently protected against rollback.', v_aGroupName;
      END IF;
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_aGroupName,v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_check: The mark "%" does not exist for the group "%".', v_mark, v_aGroupName;
      END IF;
-- ... and this mark can be used as target for a rollback
      SELECT mark_id, mark_time_id, mark_is_deleted INTO v_markId, v_markTimeId, v_markIsDeleted FROM emaj.emaj_mark
        WHERE mark_group = v_aGroupName AND mark_name = v_markName;
      IF v_markIsDeleted THEN
        RAISE EXCEPTION '_rlbk_check: The mark "%" for the group "%" is not usable for rollback.', v_markName, v_aGroupName;
      END IF;
-- ... and the rollback wouldn't delete protected marks (check disabled for rollback simulation)
      IF NOT isRollbackSimulation THEN
        SELECT string_agg(mark_name,', ') INTO v_protectedMarkList FROM (
          SELECT mark_name FROM emaj.emaj_mark
            WHERE mark_group = v_aGroupName AND mark_id > v_markId AND mark_is_rlbk_protected
            ORDER BY mark_id) AS t;
        IF v_protectedMarkList IS NOT NULL THEN
          RAISE EXCEPTION '_rlbk_check: Protected marks (%) for group "%" block the rollback to the mark "%".', v_protectedMarkList, v_aGroupName, v_markName;
        END IF;
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_time_id(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_check: The mark "%" does not represent the same point in time for all groups.', v_mark;
    END IF;
-- if the isAlterGroupAllowed flag is set to true, check that the rollback would not cross any alter group operation for the groups
    IF NOT v_isAlterGroupAllowed THEN
       PERFORM 0 FROM emaj.emaj_alter_plan WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames);
       IF FOUND THEN
         RAISE EXCEPTION '_rlbk_check: This rollback operation would cross some previously executed alter group operations. You can remove this protection by using a less strict setting for this function.';
       END IF;
    END IF;
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_session_lock$
-- It creates the session row in the emaj_rlbk_session table and then locks all the application tables for the session.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_nbRetry                SMALLINT = 0;
    v_lockmode               TEXT;
    v_ok                     BOOLEAN = false;
    v_nbTbl                  INT;
    r_tbl                    RECORD;
  BEGIN
-- try to open a dblink connection for #session > 1 (the attempt for session 1 has already been done)
    IF v_session > 1 THEN
      PERFORM emaj._dblink_open_cnx('rlbk#'||v_session);
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups INTO v_groupNames FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- create the session row the emaj_rlbk_session table.
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
             'VALUES (' || v_rlbkId || ',' || v_session || ',' || txid_current() || ',' ||
              quote_literal(clock_timestamp()) || ') RETURNING 1';
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
--    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      v_isDblinkUsable = true;
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
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
            WHERE rel_schema = rlbp_schema AND rel_tblseq = rlbp_table
            AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_session = v_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
--   lock each table
--     The locking level is EXCLUSIVE mode, except for tables whose log trigger needs to be disabled/enables when postgres version is prior 9.5.
--     In this later case, the ACCESS EXCLUSIVE mode is used, blocking all concurrent accesses to these tables.
--     In the former case, the EXCLUSIVE mode blocks all concurrent update capabilities of all tables of the groups (including tables with no logged
--     update to rollback), in order to ensure a stable state of the group at the end of the rollback operation). But these tables can be accessed
--     by SELECT statements during the E-Maj rollback.
          IF emaj._pg_version_num() < 90500 AND r_tbl.disLogTrg THEN
            v_lockmode = 'ACCESS EXCLUSIVE';
          ELSE
            v_lockmode = 'EXCLUSIVE';
          END IF;
          EXECUTE 'LOCK TABLE ' || r_tbl.fullName || ' IN ' || v_lockmode || ' MODE';
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_session_lock: A deadlock has been trapped while locking tables for groups "%".', array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      PERFORM emaj._rlbk_error(v_rlbkId, '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables', v_session);
      RAISE EXCEPTION '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables for groups "%".',array_to_string(v_groupNames,',');
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'END', array_to_string(v_groupNames,','), 'Rollback session #' || v_session || ': ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
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
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
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
      v_isDblinkUsable = true;
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
                      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
                        AND NOT EXISTS
                            (SELECT NULL FROM emaj.emaj_rlbk_plan
                              WHERE rlbp_schema = rel_schema AND rlbp_table = rel_tblseq
                                AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE')
                    ) AS t
      WHERE emaj._log_stat_tbl(t, v_markTimeId, NULL) > 0;
    IF FOUND THEN
      v_errorMsg = 'the rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables to process.';
      PERFORM emaj._rlbk_error(v_rlbkId, v_errorMsg, 1);
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If rollback is "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbkDatetime, 'HH24.MI.SS.MS') || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true, NULL, v_timeId);
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
    v_isDblinkUsable         BOOLEAN = false;
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
      v_isDblinkUsable = true;
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
          SELECT emaj._rlbk_tbl(emaj_relation.*, v_lastGlobalSeq, v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
        WHEN 'DELETE_LOG' THEN
-- process the deletion of log rows
          SELECT emaj._delete_log_tbl(emaj_relation.*, v_rlbkMarkTimeId, v_rlbkTimeId, v_lastGlobalSeq)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
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

CREATE OR REPLACE FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
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
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_ctrlDuration           INTERVAL;
    v_markId                 BIGINT;
    v_markTimeId             BIGINT;
    v_nbSeq                  INT;
    v_markName               TEXT;
    v_histDateTime           TIMESTAMPTZ;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = true;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_table, rlbk_eff_nb_table, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbTbl, v_effNbTbl, v_rlbkDatetime
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
              (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
               WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
-- the sequences related to the deleted marks can be also suppressed
--   delete first application sequences related data for the groups
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
          AND sequ_schema = rel_schema AND sequ_name = rel_tblseq
          AND sequ_time_id > v_markTimeId;
--   delete then emaj sequences related data for the groups
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence
          AND sequ_time_id > v_markTimeId;
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
    SELECT coalesce(sum(ctrl_duration),'0'::interval) INTO v_ctrlDuration FROM (
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
    PERFORM emaj._rlbk_seq(t.*, v_markTimeId)
      FROM (SELECT * FROM emaj.emaj_relation
              WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automaticaly set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbkDatetime, 'HH24.MI.SS.MS') || '_DONE';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true, v_mark);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END',
              array_to_string(v_groupNames,','),
              'Rollback_id ' || v_rlbkId || ', ' || v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed'
             ) RETURNING hist_datetime INTO v_histDateTime;
-- update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the result message
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMPLETED'', rlbk_end_datetime = ' ||
               quote_literal(v_histDateTime) || ', rlbk_msg = ''Completed: ' ||
               v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed''' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
--     and then close the connection
      PERFORM emaj._dblink_close_cnx('rlbk#1');
    ELSE
-- ... or directly (the status can be directly set to committed, the update being in the same transaction)
      EXECUTE 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMMITTED'', rlbk_end_datetime = ' ||
               quote_literal(v_histDateTime) || ', rlbk_msg = ''Completed: ' ||
               v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed''' ||
               ' WHERE rlbk_id = ' || v_rlbkId;
    END IF;
-- build and return the execution report
    IF v_isAlterGroupAllowed IS NULL THEN
-- return the number of processed tables and sequences to old style calling functions
       rlbk_severity = 'Notice'; rlbk_message = (v_effNbTbl + v_nbSeq)::TEXT;
       RETURN NEXT;
    ELSE
-- return the execution report to new style calling functions
-- ... the general notice messages with counters
       rlbk_severity = 'Notice';
       rlbk_message = format ('%s / %s tables effectively processed.', v_effNbTbl::TEXT, v_nbTbl::TEXT);
       RETURN NEXT;
       IF v_nbSeq > 0 THEN
         rlbk_message = format ('%s sequences processed.', v_nbSeq::TEXT);
         RETURN NEXT;
       END IF;
-- ... and warning messages for any elementary action from alter group operations that has not been rolled back
--TODO add missing cases
       RETURN QUERY
         SELECT 'Warning'::TEXT AS rlbk_severity,
               ('Tables group change not rolled back: ' || CASE altr_step
                   WHEN 'CHANGE_REL_PRIORITY' THEN 'E-Maj priority for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN 'E-Maj log schema for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN 'E-Maj names prefix for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN 'log data tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN 'log index tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   ELSE altr_step::TEXT || ' / ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                   END)::TEXT AS rlbk_message
           FROM emaj.emaj_alter_plan
           WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> ''
        ORDER BY altr_time_id, altr_step, altr_schema, altr_tblseq;
-- for unlogged rollbacks, delete from alter group operations the elementary actions that have not been automatically rolled back
--TODO add missing cases
      IF NOT v_isLoggedRlbk THEN
         DELETE FROM emaj.emaj_alter_plan
           WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_tblseq <> ''
             AND altr_step IN ('CHANGE_REL_PRIORITY', 'CHANGE_TBL_LOG_SCHEMA', 'CHANGE_TBL_NAMES_PREFIX', 'CHANGE_TBL_LOG_DATA_TSP', 'CHANGE_TBL_LOG_INDEX_TSP');
      END IF;
    END IF;
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

CREATE OR REPLACE FUNCTION emaj.emaj_consolidate_rollback_group(v_groupName TEXT, v_endRlbkMark TEXT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_consolidate_rollback_group$
-- This function "consolidates" a rollback for a group. It transforms an already completed logged rollback into an unlogged rollback.
-- All marks and update logs between a mark used as reference by an unlogged rollback operation and the final mark set by this rollback are suppressed.
-- The group may be in any state (logging or idle).
-- Input: group name, name of the final mark set by the rollback operation to consolidate
-- Output: number of sequences and tables effectively processed
  DECLARE
    v_firstMark              TEXT;
    v_lastMark               TEXT;
    v_nbMark                 INT;
    v_nbSeq                  INT;
    v_nbTbl                  INT;
  BEGIN
-- check and lock the group to process
    SELECT group_nb_sequence INTO v_nbSeq FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The group "%" does not exist.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_endRlbkMark) INTO v_lastMark;
    IF v_lastMark IS NULL THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The mark "%" does not exist for the group "%".', v_endRlbkMark, v_groupName;
    END IF;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(ARRAY[v_groupName], true);
-- check the supplied mark is known as an end rollback mark
    SELECT mark_logged_rlbk_target_mark INTO v_firstMark FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_lastMark;
    IF v_firstMark IS NULL THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The mark "%" for the group "%" is not an end rollback mark.', v_lastMark, v_groupName;
    END IF;
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'BEGIN', v_groupName, 'Erase all between marks ' || v_firstMark || ' and ' || v_lastMark);
-- check the first mark really exists (it should, because deleting or renaming a mark must update the mark_logged_rlbk_mark_name column)
    PERFORM 1 FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_firstMark;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The rollback target mark "%" for the group "%" has not been found.', v_firstMark, v_groupName;
    END IF;
-- perform the consolidation operation
    SELECT * FROM emaj._delete_between_marks_group(v_groupName, v_firstMark, v_lastMark) INTO v_nbMark,v_nbTbl;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'END', v_groupName, v_nbTbl || ' tables and ' || v_nbSeq || ' sequences processed ; ' || v_nbMark || ' marks deleted');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_consolidate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_consolidate_rollback_group(TEXT,TEXT) IS
$$Consolidate a rollback for a group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_reset_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves
-- It calls the emaj_rst_group function to do the job
-- Input: group name
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('RESET_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_reset_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_reset_group: The group "%" cannot be reset because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_groupName;
    END IF;
-- perform the reset operation
    SELECT emaj._reset_groups(ARRAY[v_groupName]) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RESET_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._reset_groups(v_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_reset_groups$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences saves
-- It is called by emaj_reset_group(), emaj_start_group() and emaj_alter_group() functions
-- Input: group names array
-- Output: number of processed tables and sequences
-- There is no check of the groups state (this is done by callers)
-- The function is defined as SECURITY DEFINER so that an emaj_adm role can truncate log tables
  DECLARE
    v_nbTb                   INT;
    r_rel                    RECORD;
  BEGIN
-- delete all marks for the groups from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames);
-- delete emaj_sequence rows related to the tables of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence;
-- delete all sequence holes for the tables of the groups
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table;
-- initialize the return value with the number of sequences
    SELECT count(*) INTO v_nbTb FROM emaj.emaj_relation
      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S';
-- delete emaj_sequence rows related to the sequences of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_schema = sequ_schema AND rel_tblseq = sequ_name AND
            rel_group = ANY (v_groupNames) AND rel_kind = 'S';
-- then, truncate log tables for application tables
    FOR r_rel IN
        SELECT rel_log_schema, rel_log_table, rel_log_sequence FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
--   truncate the log table
      EXECUTE 'TRUNCATE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
--   and reset the log sequence
      PERFORM setval(quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence), 1, false);
      v_nbTb = v_nbTb + 1;
    END LOOP;
    RETURN v_nbTb;
  END;
$_reset_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing)
-- It is also used to estimate the cost of a rollback to a specified mark
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: group name, the 2 mark names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of log rows by table (including tables with 0 rows to rollback)
  DECLARE
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_log_stat_group: The group "%" does not exist.', v_groupName;
    END IF;
-- if first mark is NULL or empty, retrieve the name, timestamp and last sequ_hole id of the first recorded mark for the group
    IF v_firstMark IS NULL OR v_firstMark = '' THEN
--   if no mark exists for the group (just after emaj_create_group() or emaj_reset_group() functions call),
--     v_realFirstMark remains NULL
      SELECT mark_name, mark_id, mark_time_id, time_clock_timestamp INTO v_realFirstMark, v_firstMarkId, v_firstMarkTimeId, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName
        ORDER BY mark_id LIMIT 1;
    ELSE
-- else, check and retrieve the name, timestamp and last sequ_hole id of the supplied first mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: The start mark "%" is unknown for the group "%".', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_time_id, time_clock_timestamp INTO v_firstMarkId, v_firstMarkTimeId, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- if a last mark name is supplied, check and retrieve the name, timestamp and last sequ_hole id of the supplied end mark for the group
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: The end mark "%" is unknown for the group "%".', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_time_id, time_clock_timestamp INTO v_lastMarkId, v_lastMarkTimeId, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName AND mark_name = v_realLastMark;
-- if last mark is null or empty, v_realLastMark, v_lastMarkTimeId, v_lastMarkTs and v_lastLastSeqHoleId remain NULL
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkTimeId IS NOT NULL AND v_firstMarkTimeId > v_lastMarkTimeId THEN
      RAISE EXCEPTION 'emaj_log_stat_group: The start mark "%" (%) has been set after the end mark "%" (%).', v_realFirstMark, v_firstMarkTs, v_realLastMark, v_lastMarkTs;
    END IF;
-- for each table of the emaj_relation table, get the number of log rows and return the statistic
    RETURN QUERY
      SELECT rel_group, rel_schema, rel_tblseq,
             CASE WHEN v_firstMarkTimeId IS NULL THEN 0
                  ELSE emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId, v_lastMarkTimeId) END AS nb_rows
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_priority, rel_schema, rel_tblseq;
  END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged events for an E-Maj group between 2 marks.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
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
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_logTableName           TEXT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: The group "%" does not exist.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the global sequence value and the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: The start mark "%" is unknown for the group "%".', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, time_last_emaj_gid, time_clock_timestamp INTO v_firstMarkId, v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the global sequence value and the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_detailed_log_stat_group: The end mark "%" is unknown for the group "%".', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, time_last_emaj_gid, time_clock_timestamp INTO v_lastMarkId, v_lastEmajGid, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: The start mark "%" (%) has been set after the end mark "%" (%).', v_realFirstMark, v_firstMarkTs, v_realLastMark, v_lastMarkTs;
    END IF;
-- for each table of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_kind, rel_log_table FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the log table name and its sequence name for this table
      v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table);
-- prepare and execute the statement
      v_stmt= 'SELECT ' || quote_literal(v_groupName) || '::TEXT as emaj_group,'
           || ' ' || quote_literal(r_tblsq.rel_schema) || '::TEXT as emaj_schema,'
           || ' ' || quote_literal(r_tblsq.rel_tblseq) || '::TEXT as emaj_table,'
           || ' emaj_user,'
           || ' CASE emaj_verb WHEN ''INS'' THEN ''INSERT'''
           ||                ' WHEN ''UPD'' THEN ''UPDATE'''
           ||                ' WHEN ''DEL'' THEN ''DELETE'''
           ||                             ' ELSE ''?'' END::VARCHAR(6) as emaj_verb,'
           || ' count(*) as emaj_rows'
           || ' FROM ' || v_logTableName
           || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')';
      IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN v_stmt = v_stmt
           || ' AND emaj_gid > '|| v_firstEmajGid ;
      END IF;
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN v_stmt = v_stmt
           || ' AND emaj_gid <= '|| v_lastEmajGid ;
      END IF;
      v_stmt = v_stmt
           || ' GROUP BY emaj_group, emaj_schema, emaj_table, emaj_user, emaj_verb'
           || ' ORDER BY emaj_user, emaj_verb';
      FOR r_stat IN EXECUTE v_stmt LOOP
        RETURN NEXT r_stat;
      END LOOP;
    END LOOP;
    RETURN;
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks.$$;

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
-- check supplied group names and mark parameters with the isAlterGroupAllowed and isRollbackSimulation flags set to true
    SELECT emaj._rlbk_check(v_groupNames, v_mark, TRUE, TRUE) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::int INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session)
        VALUES (v_rlbkId, v_groupNames, v_mark, emaj._get_mark_time_id(v_groupNames[1], v_markName), v_isLoggedRlbk, false, 1);
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

CREATE OR REPLACE FUNCTION emaj._rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$_rollback_activity$
-- This function effectively builds the list of rollback operations currently in execution.
-- It is called by the emaj_rollback_activity() function.
-- This is a separate function to help in testing the feature (avoiding the effects of emaj_cleanup_rollback_state()).
-- The number of parallel rollback sessions is not taken into account here,
--   as it is difficult to estimate the benefit brought by several parallel sessions.
-- The times and progression indicators reported are based on the transaction timestamp (allowing stable results in regression tests).
  DECLARE
    v_ipsDuration            INTERVAL;           -- In Progress Steps Duration
    v_nyssDuration           INTERVAL;           -- Not Yes Started Steps Duration
    v_nbNyss                 INT;                -- Number of Net Yes Started Steps
    v_ctrlDuration           INTERVAL;
    v_currentTotalEstimate   INTERVAL;
    r_rlbk                   emaj.emaj_rollback_activity_type;
  BEGIN
-- retrieve all not completed rollback operations (ie in 'PLANNING', 'LOCKING' or 'EXECUTING' state)
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_groups, rlbk_mark, t1.time_clock_timestamp, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, t2.time_tx_timestamp,
             transaction_timestamp() - t2.time_tx_timestamp AS "elapse", NULL, 0
        FROM emaj.emaj_rlbk
             JOIN emaj.emaj_time_stamp t1 ON (rlbk_mark_time_id = t1.time_id)
             LEFT OUTER JOIN emaj.emaj_time_stamp t2 ON (rlbk_time_id = t2.time_id)
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING')
        ORDER BY rlbk_id
        LOOP
-- compute the estimated remaining duration
--   for rollback operations in 'PLANNING' state, the remaining duration is NULL
      IF r_rlbk.rlbk_status IN ('LOCKING', 'EXECUTING') THEN
--     estimated duration of remaining work of in progress steps
        SELECT coalesce(
               sum(CASE WHEN rlbp_start_datetime + rlbp_estimated_duration - transaction_timestamp() > '0'::interval
                        THEN rlbp_start_datetime + rlbp_estimated_duration - transaction_timestamp()
                        ELSE '0'::interval END),'0'::interval) INTO v_ipsDuration
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_start_datetime IS NOT NULL AND rlbp_duration IS NULL;
--     estimated duration and number of not yet started steps
        SELECT coalesce(sum(rlbp_estimated_duration),'0'::interval), count(*) INTO v_nyssDuration, v_nbNyss
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_start_datetime IS NULL
           AND rlbp_step NOT IN ('CTRL-DBLINK','CTRL+DBLINK');
--     estimated duration of inter-step duration for not yet started steps
        SELECT coalesce(sum(rlbp_estimated_duration) * v_nbNyss / sum(rlbp_estimated_quantity),'0'::interval)
          INTO v_ctrlDuration
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_step IN ('CTRL-DBLINK','CTRL+DBLINK');
--     update the global remaining duration estimate
        r_rlbk.rlbk_remaining = v_ipsDuration + v_nyssDuration + v_ctrlDuration;
      END IF;
-- compute the completion pct
--   for rollback operations in 'PLANNING' or 'LOCKING' state, the completion_pct = 0
      IF r_rlbk.rlbk_status = 'EXECUTING' THEN
--   first compute the new total duration estimate, using the estimate of the remaining work
        SELECT transaction_timestamp() - time_tx_timestamp + r_rlbk.rlbk_remaining INTO v_currentTotalEstimate
          FROM emaj.emaj_rlbk, emaj.emaj_time_stamp
          WHERE rlbk_time_id = time_id AND rlbk_id = r_rlbk.rlbk_id;
--   and then the completion pct
        IF v_currentTotalEstimate <> '0'::interval THEN
          SELECT 100 - (extract(epoch FROM r_rlbk.rlbk_remaining) * 100
                      / extract(epoch FROM v_currentTotalEstimate))::smallint
            INTO r_rlbk.rlbk_completion_pct;
        END IF;
      END IF;
      RETURN NEXT r_rlbk;
    END LOOP;
    RETURN;
  END;
$_rollback_activity$;

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
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: The group "%" does not exist.', v_groupName;
    END IF;
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
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
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
                  AND attnum > 0 AND attisdropped = false) AS t;
          ELSE
--   the table has no pkey
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList FROM (
              SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = false) AS t;
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
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no
--   end mark is specified. They contain one row per sequence.
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
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it.
  DECLARE
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_firstMarkTsId          BIGINT;
    v_lastMarkTsId           BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_logTableName           TEXT;
    v_fileName               TEXT;
    v_conditions             TEXT;
    v_stmt                   TEXT;
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
      RAISE EXCEPTION 'emaj_snap_log_group: The group "%" does not exist.', v_groupName;
    END IF;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_log_group: The directory parameter cannot be NULL.';
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%'  THEN
      RAISE EXCEPTION 'emaj_snap_log_group: The COPY options parameter format is invalid.';
    END IF;
-- catch the global sequence value and the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the global sequence value and the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_snap_log_group: The start mark "%" is unknown for the group "%".', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, time_id, time_last_emaj_gid, time_clock_timestamp
        INTO v_firstMarkId, v_firstMarkTsId, v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realFirstMark;
    ELSE
      SELECT mark_name, mark_id, time_id, time_last_emaj_gid, time_clock_timestamp
        INTO v_realFirstMark, v_firstMarkId, v_firstMarkTsId, v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    END IF;
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- the end mark is supplied
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_snap_log_group: The end mark "%" is unknown for the group "%".', v_lastMark, v_groupName;
      END IF;
    ELSE
-- the end mark is not supplied (look for the current state)
-- temporarily create a mark, without locking tables
      SELECT emaj._check_new_mark('TEMP_%', ARRAY[v_groupName]) INTO v_realLastMark;
      PERFORM emaj._set_mark_groups(ARRAY[v_groupName], v_realLastMark, false, false);
    END IF;
-- catch the global sequence value and timestamp of the last mark
    SELECT mark_id, time_id, time_last_emaj_gid, time_clock_timestamp INTO v_lastMarkId, v_lastMarkTsId, v_lastEmajGid, v_lastMarkTs
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realLastMark;
-- check that the first_mark < end_mark
    IF v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_snap_log_group: The start mark "%" (%) has been set after the end mark "%" (%).', v_realFirstMark, v_firstMarkTs, v_realLastMark, v_lastMarkTs;
    END IF;
-- build the conditions on emaj_gid corresponding to this marks frame, used for the COPY statements dumping the tables
    v_conditions = 'TRUE';
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
      v_conditions = v_conditions || ' AND emaj_gid > '|| v_firstEmajGid;
    END IF;
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      v_conditions = v_conditions || ' AND emaj_gid <= '|| v_lastEmajGid;
    END IF;
-- process all log tables of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_log_schema, rel_log_table FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- process tables
--   build names
        v_fileName = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log.snap';
        v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table);
--   prepare the execute the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE ' || v_conditions
             || ' ORDER BY emaj_gid ASC) TO ' || quote_literal(v_fileName)
             || ' ' || coalesce (v_copyOptions, '');
        EXECUTE v_stmt;
      END IF;
-- for sequences, just adjust the counter
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realFirstMark;
-- and execute the COPY statement
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
            ' WHERE sequ_time_id = ' || quote_literal(v_firstMarkTsId) || ' AND ' ||
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
            coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
-- generate the full file name for sequences state at end mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realLastMark;
    ELSE
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || to_char(v_lastMarkTs,'HH24.MI.SS.MS');
    END IF;
-- and execute the COPY statement
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
            ' WHERE sequ_time_id = ' || quote_literal(v_lastMarkTsId) || ' AND ' ||
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
            coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
    IF v_lastMark IS NULL OR v_lastMark = '' THEN
-- no last mark has been supplied, suppress the just created mark
      PERFORM emaj._delete_intermediate_mark_group(v_groupName, v_realLastMark, mark_id, mark_time_id)
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj log tables snap of group ' || v_groupName ||
            ' between marks ' || v_realFirstMark || ' and ' ||
            coalesce(v_realLastMark,'current state') || ' at ' || statement_timestamp()) ||
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
    v_aGroupName             TEXT;
    v_tblList                TEXT;
    v_cpt                    INT;
    v_firstMarkCopy          TEXT = v_firstMark;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkTimeId        BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_lastMarkTimeId         BIGINT;
    v_tblseqErr              TEXT;
    v_nbSQL                  BIGINT;
    v_nbSeq                  INT;
    v_cumNbSQL               BIGINT = 0;
    v_fullSeqName            TEXT;
    v_endComment             TEXT;
    v_conditions             TEXT;
    v_rqSeq                  TEXT;
    r_tblsq                  RECORD;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- check group names array and stop the processing if it is null
    v_groupNames = emaj._check_names_array(v_groupNames,'group');
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- if table/sequence names are supplied, check them
    IF v_tblseqs IS NOT NULL THEN
      IF v_tblseqs = array[''] THEN
        RAISE EXCEPTION '_gen_sql_groups: The filtered table/sequence names array cannot be empty.';
      END IF;
      v_tblseqs = emaj._check_names_array(v_tblseqs,'table/sequence');
    END IF;
-- check that each group ...
    FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
-- ...is recorded into the emaj_group table
      PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_aGroupName;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_gen_sql_groups: The group "%" does not exist.', v_aGroupName;
      END IF;
-- ... has no tables without pkey
      SELECT string_agg(rel_schema || '.' || rel_tblseq,',') INTO v_tblList
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_relation
        WHERE relnamespace = pg_namespace.oid
          AND nspname = rel_schema AND relname = rel_tblseq
          AND rel_group = v_aGroupName AND rel_kind = 'r'
          AND relhaspkey = false;
      IF v_tblList IS NOT NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: The tables group "%" contains tables without pkey (%).', v_aGroupName, v_tblList;
      END IF;
-- If the first mark supplied is NULL or empty, get the first mark for the current processed group
--   (in fact the first one) and override the supplied first mark
      IF v_firstMarkCopy IS NULL OR v_firstMarkCopy = '' THEN
        SELECT mark_name INTO v_firstMarkCopy
          FROM emaj.emaj_mark WHERE mark_group = v_aGroupName ORDER BY mark_id LIMIT 1;
        IF NOT FOUND THEN
           RAISE EXCEPTION '_gen_sql_groups: No initial mark can be found for the group "%".', v_aGroupName;
        END IF;
      END IF;
-- ... owns the requested first mark
      SELECT emaj._get_mark_name(v_aGroupName,v_firstMarkCopy) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: The start mark "%" does not exist for the group "%".', v_firstMarkCopy, v_aGroupName;
      END IF;
-- ... and owns the requested last mark, if supplied
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
        SELECT emaj._get_mark_name(v_aGroupName,v_lastMark) INTO v_realLastMark;
        IF v_realLastMark IS NULL THEN
          RAISE EXCEPTION '_gen_sql_groups: The end mark "%" does not exist for the group "%".', v_lastMark, v_aGroupName;
        END IF;
      END IF;
    END LOOP;
-- check that the first mark timestamp is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_time_id(group_name,v_firstMarkCopy)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_gen_sql_groups: The start mark "%" does not represent the same point in time for all groups.', v_firstMarkCopy;
    END IF;
-- check that the last mark timestamp, if supplied, is the same for all groups of the array
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT count(DISTINCT emaj._get_mark_time_id(group_name,v_lastMark)) INTO v_cpt FROM emaj.emaj_group
        WHERE group_name = ANY (v_groupNames);
      IF v_cpt > 1 THEN
        RAISE EXCEPTION '_gen_sql_groups: The end mark "%" does not represent the same point in time for all groups.', v_lastMark;
      END IF;
    END IF;
-- retrieve the name, the global sequence value and the timestamp of the supplied first mark for the 1st group
--   (the global sequence value and the timestamp are the same for all groups of the array)
    SELECT mark_time_id, time_last_emaj_gid, time_clock_timestamp INTO v_firstMarkTimeId, v_firstEmajGid, v_firstMarkTs
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupNames[1] AND mark_name = v_realFirstMark;
-- if last mark is NULL or empty, there is no timestamp to register
    IF v_lastMark IS NULL OR v_lastMark = '' THEN
      v_lastEmajGid = NULL;
      v_lastMarkTs = NULL;
      v_lastMarkTimeId = NULL;
    ELSE
-- else, retrieve the name, timestamp and last global sequence id of the supplied end mark for the 1st group
      SELECT mark_time_id, time_last_emaj_gid, time_clock_timestamp INTO v_lastMarkTimeId, v_lastEmajGid, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupNames[1] AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkTimeId IS NOT NULL AND v_firstMarkTimeId > v_lastMarkTimeId THEN
      RAISE EXCEPTION '_gen_sql_groups: The start mark "%" (%) has been set after the end mark "%" (%).', v_firstMarkCopy, v_firstMarkTs, v_lastMark, v_lastMarkTs;
    END IF;
-- check the array of tables and sequences to filter, if supplied.
-- each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups
    IF v_tblseqs IS NOT NULL THEN
      SELECT string_agg(t,', ') INTO v_tblseqErr FROM (
        SELECT t FROM unnest(v_tblseqs) AS t
          EXCEPT
        SELECT rel_schema || '.' || rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames)
        ) AS t2;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: Some tables and/or sequences (%) do not belong to any of the selected tables groups.', v_tblseqErr;
      END IF;
    END IF;
-- test the supplied output file name by inserting a temporary line (trap NULL or bad file name)
    BEGIN
      EXECUTE 'COPY (SELECT ''-- _gen_sql_groups() function in progress - started at '
                     || statement_timestamp() || ''') TO ' || quote_literal(v_location);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION '_gen_sql_groups: The file "%" cannot be used as script output file.', v_location;
    END;
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
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                        -- tables of the groups
            AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs)) -- filtered or not by the user
                                                                               -- only tables having updates to process
            AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId, v_lastMarkTimeId) > 0
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
      IF v_lastMarkTimeId IS NULL THEN
-- no supplied last mark, so get current sequence characteritics
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
-- a last mark is supplied, so get sequence characteristics from emaj_sequence table
        EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_time_id = ' || v_lastMarkTimeId INTO v_rqSeq;
      END IF;
-- insert into temp table
      v_nbSeq = v_nbSeq + 1;
      EXECUTE 'INSERT INTO emaj_temp_script '
           || 'SELECT NULL, -1 * ' || v_nbSeq || ', txid_current(), ' || quote_literal(v_rqSeq);
    END LOOP;
-- add initial comments
    IF v_lastMarkTimeId IS NOT NULL THEN
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
-- drop temporary table
    DROP TABLE IF EXISTS emaj_temp_script;
-- return the number of sql verbs generated into the output file
    v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  DECLARE
  BEGIN
-- check the postgres version at groups creation time is compatible (i.e. >= 9.1)
    RETURN QUERY
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                   to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 901
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
--   start with log triggers
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
--   then truncate triggers
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
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM (
        WITH cte_app_tables_columns AS (                -- application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
                AND rel_kind = 'r'),
             cte_log_tables_columns AS (                -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_log_table
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
                AND rel_kind = 'r')
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
        WHERE rel_kind = 'r' AND rel_group = group_name AND group_is_rollbackable
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
        WHERE rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND relpersistence <> 'p'
        ORDER BY rel_schema, rel_tblseq, 1;
-- check all tables are WITHOUT OIDS (i.e. have not been altered as WITH OIDS after their tables group creation)
    RETURN QUERY
      SELECT 'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is WITH OIDS.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND relhasoids
        ORDER BY rel_schema, rel_tblseq, 1;
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
    v_errorFound             BOOLEAN = FALSE;
    v_nbMissingEventTrigger  INT;
    r_object                 RECORD;
  BEGIN
-- Global checks
-- detect if the current postgres version is at least 9.1
    IF emaj._pg_version_num() < 90100 THEN
      RETURN NEXT 'The current postgres version (' || version() || ') is not compatible with E-Maj.';
      v_errorFound = TRUE;
    END IF;
    IF emaj._pg_version_num() >= 90300 THEN
-- With postgres 9.3+, report a warning if some E-Maj event triggers are missing
      SELECT (CASE WHEN emaj._pg_version_num() >= 90500 THEN 3 WHEN emaj._pg_version_num() >= 90300 THEN 2 END) - count(*)
        INTO v_nbMissingEventTrigger FROM pg_catalog.pg_event_trigger
        WHERE evtname IN ('emaj_protection_trg','emaj_sql_drop_trg','emaj_table_rewrite_trg');
      IF v_nbMissingEventTrigger > 0 THEN
        RETURN NEXT 'Warning: Some E-Maj event triggers are missing. Your database administrator may (re)create them using the emaj_upgrade_after_postgres_upgrade.sql script.';
      END IF;
-- With postgres 9.3+, report a warning if some E-Maj event triggers exist but are not enabled
      PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
      IF FOUND THEN
        RETURN NEXT 'Warning: Some E-Maj event triggers exist but are disabled. You may enable them using the emaj_enable_protection_by_event_triggers() function.';
      END IF;
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

--<end_functions>                                pattern used by the tool that extracts and insert the functions definition
------------------------------------------
--                                      --
-- event triggers and related functions --
--                                      --
------------------------------------------
DO
$do$
BEGIN

-- beginning of 9.3+ specific code
IF emaj._pg_version_num() >= 90300 THEN
-- sql_drop event trigger are only possible with postgres 9.3+

CREATE OR REPLACE FUNCTION public._emaj_protection_event_trigger_fnct()
 RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_emaj_protection_event_trigger_fnct$
-- This function is called by the emaj_protection_trg event trigger
-- The function only blocks any attempt to drop the emaj schema or the emaj extension
-- It is located into the public schema to be able to detect the emaj schema removal attempt
-- It is also unlinked from the emaj extension to be able to detect the emaj extension removal attempt
-- Another pair of function and event trigger handles all other drop attempts
  DECLARE
    r_dropped                RECORD;
  BEGIN
-- scan all dropped objects
    FOR r_dropped IN
      SELECT object_type, object_name FROM pg_event_trigger_dropped_objects()
    LOOP
      IF r_dropped.object_type = 'schema' AND r_dropped.object_name = 'emaj' THEN
-- detecting an attempt to drop the emaj object
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "emaj". Please use the emaj_uninstall.sql script if you really want to remove all E-Maj components.';
      END IF;
      IF r_dropped.object_type = 'extension' AND r_dropped.object_name = 'emaj' THEN
-- detecting an attempt to drop the emaj extension
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the emaj extension. Please use the emaj_uninstall.sql script if you really want to remove all E-Maj components.';
      END IF;
    END LOOP;
  END;
$_emaj_protection_event_trigger_fnct$;
COMMENT ON FUNCTION public._emaj_protection_event_trigger_fnct() IS
$$E-Maj extension: support of the emaj_protection_trg event trigger.$$;

-- remove the function trigger from the extension, so that it can fire the "DROP EXTENSION emaj"
ALTER EXTENSION emaj DROP FUNCTION public._emaj_protection_event_trigger_fnct();

CREATE OR REPLACE FUNCTION emaj._event_trigger_sql_drop_fnct()
 RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_event_trigger_sql_drop_fnct$
-- This function is called by the emaj_sql_drop_trg event trigger
-- The function blocks any ddl operation that leads to a drop of
--   - an application table or a sequence registered into an active (not stopped) E-Maj group, or a schema containing such tables/sequence
--   - an E-Maj schema, a log table, a log sequence, a log function or a log trigger
-- The drop of emaj schema or extension is managed by another event trigger
  DECLARE
    v_groupName              TEXT;
    r_dropped                RECORD;
  BEGIN
-- scan all dropped objects
    FOR r_dropped IN
      SELECT * FROM pg_event_trigger_dropped_objects()
-- TODO: when postgres 9.4 will not be supported any more, replace the statement by:
--      SELECT object_type, schema_name, object_name, object_identity, original FROM pg_event_trigger_dropped_objects()
-- (the 'original' column is not known in pg9.4- versions)
    LOOP
      CASE
        WHEN r_dropped.object_type = 'schema' THEN
-- the object is a schema
--   look at the emaj_relation table to verify that the schema being dropped does not belong to any active (not stopped) group
          SELECT string_agg(DISTINCT rel_group, ', ') INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.object_name
              AND group_name = rel_group AND group_is_logging;
          IF v_groupName IS NOT NULL THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application schema "%". But it belongs to the active tables groups "%".', r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the schema being dropped is not an E-Maj schema containing log tables
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "%". But dropping an E-Maj schema is not allowed.', r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'table' THEN
-- the object is a table
--   look at the emaj_relation table to verify that the table being dropped does not belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name
              AND group_name = rel_group AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application table "%.%". But it belongs to the active tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the table being dropped is not a log table
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.schema_name AND rel_log_table = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log table "%.%". But dropping an E-Maj log table is not allowed.',
                            r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'sequence' THEN
-- the object is a sequence
--   look at the emaj_relation table to verify that the sequence being dropped does not belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name
              AND group_name = rel_group AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application sequence "%.%". But it belongs to the active tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the sequence being dropped is not a log sequence
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.schema_name AND rel_log_sequence = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log sequence "%.%". But dropping an E-Maj sequence is not allowed.', r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'function' THEN
-- the object is a function
--   look at the emaj_relation table to verify that the function being dropped is not a log function
          PERFORM 1 FROM emaj.emaj_relation
            WHERE  r_dropped.object_identity = quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_function) || '()';
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log function "%". But dropping an E-Maj log function is not allowed.', r_dropped.object_identity;
          END IF;
        WHEN r_dropped.object_type = 'trigger' THEN
-- the object is a trigger
--   if postgres version is 9.5+ (to see the 'original' column of the pg_event_trigger_dropped_objects() function),
--   look at the trigger name pattern to identify emaj trigger
--   and do not raise an exception if the triggers drop is derived from a drop of a table or a function
          IF emaj._pg_version_num() >= 90500 THEN
            IF r_dropped.original AND
               (r_dropped.object_identity LIKE 'emaj_log_trg%' OR r_dropped.object_identity LIKE 'emaj_trunc_trg%') THEN
              RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the "%" E-Maj trigger. But dropping an E-Maj trigger is not allowed.', r_dropped.object_identity;
            END IF;
          END IF;
        ELSE
          CONTINUE;
      END CASE;
    END LOOP;
  END;
$_event_trigger_sql_drop_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_sql_drop_fnct() IS
$$E-Maj extension: support of the emaj_sql_drop_trg event trigger.$$;

-- end of 9.3+ specific code
END IF;

-- beginning of 9.5+ specific code
IF emaj._pg_version_num() >= 90500 THEN
-- table_rewrite event trigger are only possible with postgres 9.5+

CREATE OR REPLACE FUNCTION emaj._emaj_event_trigger_table_rewrite_fnct()
 RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_emaj_event_trigger_table_rewrite_fnct$
-- This function is called by the emaj_table_rewrite_trg event trigger
-- The function blocks any ddl operation that leads to a table rewrite for:
--   - an application table registered into an active (not stopped) E-Maj group
--   - an E-Maj log table
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
      WHERE rel_schema = v_tableSchema AND rel_tblseq = v_tableName
              AND group_name = rel_group AND group_is_logging;
    IF FOUND THEN
-- the table is an application table that belongs to a group, so raise an exception
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the application table "%.%" structure. But the table belongs to the active tables group "%".',
                      v_tableSchema, v_tableName , v_groupName;
    END IF;
-- look at the emaj_relation table to verify that the table being rewritten is not a known log table
    SELECT rel_group INTO v_groupName FROM emaj.emaj_relation
      WHERE rel_log_schema = v_tableSchema AND rel_log_table = v_tableName;
    IF FOUND THEN
-- the table is an E-Maj log table, so raise an exception
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the log table "%.%" structure. But the table belongs to the tables group "%".',
                      v_tableSchema, v_tableName , v_groupName;
    END IF;
  END;
$_emaj_event_trigger_table_rewrite_fnct$;
COMMENT ON FUNCTION emaj._emaj_event_trigger_table_rewrite_fnct() IS
$$E-Maj extension: support of the emaj_table_rewrite_trg event trigger.$$;

-- end of 9.5+ specific code
END IF;
END $do$;

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

GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rollback_activity() TO emaj_viewer;

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
UPDATE emaj.emaj_param SET param_value_text = '2.1.0' WHERE param_key = 'emaj_version';

-- insert the upgrade record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 2.1.0', 'Upgrade from 2.0.1 completed');

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

