--
-- E-Maj: migration from 2.0.1 to <NEXT_VERSION>
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
  'ATTRIBUTE_TBL',           -- change the attributes for a table (log schema suffix, emaj names prefix, log data or index tablespace)
  'ASSIGN_REL',              -- move a table or a sequence from one group to another
  'PRIORITY_REL',            -- change the priority level for a table or a sequence
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

-- In the emaj_mark table, rename and adjust the content of the mark_is_deleted column
UPDATE emaj.emaj_mark SET mark_is_deleted = NOT mark_is_deleted;
ALTER TABLE emaj.emaj_mark RENAME mark_is_deleted TO mark_is_targetable;

-- table containing the elementary steps to perform alter_groups operations
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

SELECT pg_catalog.pg_extension_config_dump('emaj_alter_plan','');

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
DROP FUNCTION emaj._check_group_content(V_GROUPNAME TEXT);
DROP FUNCTION emaj._create_tbl(R_GRPDEF EMAJ.EMAJ_GROUP_DEF,V_GROUPNAME TEXT,V_ISROLLBACKABLE BOOLEAN,V_DEFTSP TEXT);
DROP FUNCTION emaj._create_seq(GRPDEF EMAJ.EMAJ_GROUP_DEF,V_GROUPNAME TEXT);
DROP FUNCTION emaj.emaj_create_group(V_GROUPNAME TEXT,V_ISROLLBACKABLE BOOLEAN);
DROP FUNCTION emaj.emaj_alter_group(V_GROUPNAME TEXT);
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
      UNION ALL                                   -- get the transaction timestamp of the oldest targetable mark for all groups
        (SELECT MIN(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_mark
           WHERE time_id = mark_time_id AND mark_is_targetable)
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
          WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
        ORDER BY 1,2) AS t
    LOOP
      RAISE WARNING '_check_groups_content: Error, the table or sequence %.% does not exist.', quote_ident(r.grpdef_schema), quote_ident(r.grpdef_tblseq);
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
      RAISE EXCEPTION '_check_groups_content: one or several errors have been detected in the emaj_group_def table content.';
    END IF;
--
    RETURN;
  END;
$_check_groups_content$;

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

CREATE OR REPLACE FUNCTION emaj._change_attr_tbl(r_rel emaj.emaj_relation, v_newLogSchemaSuffix TEXT, v_newNamesPrefix TEXT, v_newLogDatTsp TEXT, v_newLogIdxTsp TEXT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
$_change_attr_tbl$
-- This function processes the attributes changes registered in the emaj_group_def table for an application table (all but the priority level)
-- Input: the existing emaj_relation row for the table, and the parameters from emaj_group_def that may by changed
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_changeMsg              TEXT = '';
    v_newLogSchema           TEXT;
    v_newEmajNamesPrefix     TEXT;
    v_newLogTableName        TEXT;
    v_newLogFunctionName     TEXT;
    v_newLogSequenceName     TEXT;
    v_newLogIndexName        TEXT;
    v_newTsp                 TEXT;
  BEGIN
-- build the name of new emaj components associated to the application table (non schema qualified and not quoted)
    v_newLogSchema = coalesce(v_schemaPrefix || v_newLogSchemaSuffix, v_emajSchema);
    v_newEmajNamesPrefix = coalesce(v_newNamesPrefix, r_rel.rel_schema || '_' || r_rel.rel_tblseq);
    v_newLogTableName    = v_newEmajNamesPrefix || '_log';
    v_newLogIndexName    = v_newEmajNamesPrefix || '_log_idx';
    v_newLogFunctionName = v_newEmajNamesPrefix || '_log_fnct';
    v_newLogSequenceName = v_newEmajNamesPrefix || '_log_seq';
-- if the log schema in emaj_group_def has changed, process the change
    IF r_rel.rel_log_schema <> v_newLogSchema THEN
      EXECUTE 'ALTER TABLE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table)|| ' SET SCHEMA ' || quote_ident(v_newLogSchema);
      EXECUTE 'ALTER SEQUENCE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)|| ' SET SCHEMA ' || quote_ident(v_newLogSchema);
      EXECUTE 'ALTER FUNCTION ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() SET SCHEMA ' || quote_ident(v_newLogSchema);
-- adjust sequences schema names in emaj_sequence tables
      UPDATE emaj.emaj_sequence SET sequ_schema = v_newLogSchema WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
--
      v_changeMsg = v_changeMsg || ', log schema';
    END IF;
-- if the emaj names prefix in emaj_group_def has changed, process the change
    IF r_rel.rel_log_table <> v_newLogTableName THEN
      EXECUTE 'ALTER TABLE ' || quote_ident(v_newLogSchema) || '.' || quote_ident(r_rel.rel_log_table)|| ' RENAME TO ' || quote_ident(v_newLogTableName);
      EXECUTE 'ALTER INDEX ' || quote_ident(v_newLogSchema) || '.' || quote_ident(r_rel.rel_log_index)|| ' RENAME TO ' || quote_ident(v_newLogIndexName);
      EXECUTE 'ALTER SEQUENCE ' || quote_ident(v_newLogSchema) || '.' || quote_ident(r_rel.rel_log_sequence)|| ' RENAME TO ' || quote_ident(v_newLogSequenceName);
      EXECUTE 'ALTER FUNCTION ' || quote_ident(v_newLogSchema) || '.' || quote_ident(r_rel.rel_log_function) || '() RENAME TO ' || quote_ident(v_newLogFunctionName);
-- adjust sequences schema names in emaj_sequence tables
      UPDATE emaj.emaj_sequence SET sequ_name = v_newLogSequenceName WHERE sequ_schema = v_newLogSchema AND sequ_name = r_rel.rel_log_sequence;
--
      v_changeMsg = v_changeMsg || ', emaj names prefix';
    END IF;
-- if the log data tablespace in emaj_group_def has changed, process the change
    IF coalesce(r_rel.rel_log_dat_tsp,'') <> coalesce(v_newLogDatTsp,'') THEN
-- build the new data tablespace name. If needed, get the name of the current default tablespace.
      v_newTsp = v_newLogDatTsp;
      IF v_newTsp IS NULL OR v_newTsp = '' THEN
        v_newTsp = emaj._get_default_tablespace();
      END IF;
      EXECUTE 'ALTER TABLE ' || quote_ident(v_newLogSchema) || '.' || quote_ident(v_newLogTableName) || ' SET TABLESPACE ' || quote_ident(v_newTsp);
      v_changeMsg = v_changeMsg || ', log data tablespace';
    END IF;
-- if the log index tablespace in emaj_group_def has changed, process the change
    IF coalesce(r_rel.rel_log_idx_tsp,'') <> coalesce(v_newLogIdxTsp,'') THEN
-- build the new index tablespace name. If needed, get the name of the current default tablespace.
      v_newTsp = v_newLogIdxTsp;
      IF v_newTsp IS NULL OR v_newTsp = '' THEN
        v_newTsp = emaj._get_default_tablespace();
      END IF;
      EXECUTE 'ALTER TABLE ' || quote_ident(v_newLogSchema) || '.' || quote_ident(v_newLogIndexName) || ' SET TABLESPACE ' || quote_ident(v_newTsp);
      v_changeMsg = v_changeMsg || ', log index tablespace';
    END IF;
-- update the table attributes into emaj_relation
    UPDATE emaj.emaj_relation
      SET rel_log_schema = v_newLogSchema, rel_log_table = v_newLogTableName, rel_log_dat_tsp = v_newLogDatTsp,
          rel_log_index = v_newLogIndexName, rel_log_idx_tsp = v_newLogIdxTsp, rel_log_sequence = v_newLogSequenceName,
          rel_log_function = v_newLogFunctionName
      WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'TABLE ATTR CHANGED', quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq), 'Applied changes: ' || substr(v_changeMsg, 3));
--
    RETURN;
  END;
$_change_attr_tbl$;

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
-- get the schema and the name of the table that contains a serial column this sequence is linked to, if one exists
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
        RAISE WARNING '_create_seq: Sequence %.% is linked to table %.% but this table does not belong to any tables group.', grpdef.grpdef_schema, grpdef.grpdef_tblseq, v_tableSchema, v_tableName;
      ELSE
        IF v_tableGroup <> grpdef.grpdef_group THEN
          RAISE WARNING '_create_seq: Sequence %.% is linked to table %.% but this table belong to another tables group (%).', grpdef.grpdef_schema, grpdef.grpdef_tblseq, v_tableSchema, v_tableName, v_tableGroup;
        END IF;
      END IF;
    END IF;
-- record the sequence in the emaj_relation table
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_kind)
          VALUES (grpdef.grpdef_schema, grpdef.grpdef_tblseq, grpdef.grpdef_group, grpdef.grpdef_priority, 'S');
    RETURN;
  END;
$_create_seq$;

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
      RAISE EXCEPTION 'The current postgres version (%) is not compatible with E-Maj.', version();
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
      RAISE EXCEPTION 'emaj_create_group: group name can''t be NULL or empty.';
    END IF;
-- check that the group is not yet recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: group "%" is already created.', v_groupName;
    END IF;
-- check the consistency between the emaj_group_def table content and the v_is_empty input parameter
    PERFORM 0 FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName LIMIT 1;
    IF NOT v_is_empty AND NOT FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: group "%" is unknown in the emaj_group_def table. To create an empty group, explicitely set the third parameter to true.', v_groupName;
    END IF;
    IF v_is_empty AND FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: group "%" is referenced into the emaj_group_def table. This is not consistent with the <is_empty> parameter set to true.', v_groupName;
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
      RAISE EXCEPTION '_drop_group: group "%" has not been created.', v_groupName;
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
        RAISE EXCEPTION '_alter_groups: the group "%" has not been created.', v_aGroupName;
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
-- Looking at emaj_relation and emaj_group_def tables, it populate the emaj_alter_plan table that will be used by the _alter_exec() function.
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
-- determine the tables whose attributes in emaj_group_def have changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'ATTRIBUTE_TBL', rel_schema, rel_tblseq, rel_group, grpdef_priority
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND (
--   detect if the log data tablespace in emaj_group_def has changed
               coalesce(rel_log_dat_tsp,'') <> coalesce(grpdef_log_dat_tsp,'')
--   or if the log index tablespace in emaj_group_def has changed
            OR coalesce(rel_log_idx_tsp,'') <> coalesce(grpdef_log_idx_tsp,'')
--   or if the log schema in emaj_group_def has changed
            OR rel_log_schema <> (v_schemaPrefix || coalesce(grpdef_log_schema_suffix, ''))
--   or if the emaj names prefix in emaj_group_def has changed (detected with the log table name)
            OR rel_log_table <> (coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) || '_log'))
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
      SELECT v_timeId, 'PRIORITY_REL', rel_schema, rel_tblseq, rel_group, rel_priority, grpdef_priority
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
    SELECT string_agg(altr_group, ', ') INTO v_groups
      FROM emaj.emaj_alter_plan
      WHERE altr_time_id = v_timeId
        AND altr_step IN ('REMOVE_TBL', 'REMOVE_SEQ', 'RESET_GROUP', 'REPAIR_TBL', 'ASSIGN_REL', 'ADD_TBL', 'ADD_SEQ')
        AND altr_group_is_logging;
    IF v_groups IS NOT NULL THEN
      RAISE EXCEPTION '_alter_plan: the groups "%" cannot be altered because they are in LOGGING state.', v_groups;
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
            RAISE EXCEPTION 'alter_exec: cannot repair the table %.%. Its group % is in LOGGING state', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
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
            RAISE EXCEPTION 'alter_exec: cannot repair the sequence %.%. Its group % is in LOGGING state', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
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
        WHEN 'ATTRIBUTE_TBL' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel
            FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
-- get the table description from emaj_group_def
          SELECT grpdef_log_schema_suffix, grpdef_emaj_names_prefix, grpdef_log_dat_tsp, grpdef_log_idx_tsp
            INTO v_logSchemaSuffix, v_emajNamesPrefix, v_logDatTsp, v_logIdxTsp
            FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation
          PERFORM emaj._change_attr_tbl(r_rel, v_logSchemaSuffix, v_emajNamesPrefix, v_logDatTsp, v_logIdxTsp);
--
        WHEN 'ASSIGN_REL' THEN
-- update the emaj_relation table to report the group ownership change
          UPDATE emaj.emaj_relation SET rel_group = r_plan.altr_new_group
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq;
--
        WHEN 'PRIORITY_REL' THEN
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
        RAISE EXCEPTION '_start_groups: group "%" has not been created.', v_aGroupName;
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
        RAISE EXCEPTION '_stop_groups: group "%" has not been created.', v_aGroupName;
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE WARNING '_stop_groups: Group "%" cannot be stopped because it is not in LOGGING state.', v_aGroupName;
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
          RAISE WARNING '_stop_groups: Schema "%" does not exist any more.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: Schema "%" does not exist any more.', r_schema.rel_schema;
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
                RAISE WARNING '_stop_groups: Table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
              ELSE
                RAISE EXCEPTION '_stop_groups: Table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
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
                    RAISE WARNING '_stop_groups: Log trigger "emaj_log_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: Log trigger "emaj_log_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  END IF;
              END;
              BEGIN
                EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_trunc_trg';
              EXCEPTION
                WHEN undefined_object THEN
                  IF v_isForced THEN
                    RAISE WARNING '_stop_groups: Truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                  ELSE
                    RAISE EXCEPTION '_stop_groups: Truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
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
                 WHERE mark_group = ANY (v_validGroupNames) AND mark_is_targetable GROUP BY mark_group);
      END IF;
-- set all marks for the groups from the emaj_mark table as un-targetable to avoid any further rollback and remove protection if any
      UPDATE emaj.emaj_mark SET mark_is_targetable = FALSE, mark_is_rlbk_protected = FALSE
        WHERE mark_group = ANY (v_validGroupNames) AND mark_is_targetable;
-- update the state of the groups rows from the emaj_group table (the rollback protection of rollbackable groups is reset)
      UPDATE emaj.emaj_group SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (v_validGroupNames);
    END IF;
    RETURN v_nbTb;
  END;
$_stop_groups$;

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
      EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
               quote_literal(r_tblsq.rel_tblseq) || ', ' || v_timeId ||
               ', last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the number of log rows for the old last mark of each group
--   the statement returns no row in case of emaj_start_group(s)
    UPDATE emaj.emaj_mark m SET mark_log_rows_before_next =
      coalesce( (SELECT sum(stat_rows) FROM emaj.emaj_log_stat_group(m.mark_group,'EMAJ_LAST_MARK',NULL)) ,0)
      WHERE mark_group = ANY (v_groupNames)
        AND (mark_group, mark_id) IN                   -- select only the last targetable mark of each concerned group
            (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
             WHERE mark_group = ANY (v_groupNames) AND mark_is_targetable GROUP BY mark_group);
-- for each table of the groups, ...
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_log_sequence FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- ... record the associated sequence parameters in the emaj sequence table
      EXECUTE 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT '|| quote_literal(r_tblsq.rel_log_schema) || ', ' || quote_literal(r_tblsq.rel_log_sequence) || ', ' ||
               v_timeId || ', last_value, start_value, ' ||
               'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_sequence);
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the mark for each group into the emaj_mark table
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_targetable, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, v_mark, v_timeId, group_is_rollbackable, FALSE, v_loggedRlbkTargetMark
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
    v_markIsTargetable       BOOLEAN;
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
-- ... and this mark can be used as target for a rollback
      SELECT mark_id, mark_is_targetable INTO v_markId, v_markIsTargetable FROM emaj.emaj_mark
        WHERE mark_group = v_aGroupName AND mark_name = v_markName;
      IF NOT v_markIsTargetable THEN
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

CREATE OR REPLACE FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_end$
-- This is the last step of a rollback group processing. It :
--    - deletes the marks that are no longer available,
--    - deletes the recorded sequences values for these deleted marks
--    - copy data into the emaj_rlbk_stat table,
--    - rollbacks all sequences of the groups,
--    - set the end rollback mark if logged rollback,
--    - and finaly set the operation as COMPLETED or COMMITED.
-- It returns the number of processed sequences.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_effNbTbl               INT;
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
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_eff_nb_table, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_effNbTbl, v_rlbkDatetime
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
          AND (mark_group, mark_id) IN                -- select only the last targetable mark of each concerned group
              (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
               WHERE mark_group = ANY (v_groupNames) AND mark_is_targetable GROUP BY mark_group);
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
    RETURN v_nbSeq;
-- trap and record exception during the rollback operation
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(v_rlbkId, 'In _rlbk_end(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_end$;

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
      RAISE EXCEPTION 'emaj_reset_group: group "%" has not been created.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_reset_group: Group "%" cannot be reset because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_groupName;
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
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <NEXT_VERSION>', 'Upgrade from 2.0.1 completed');

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

