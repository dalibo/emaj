--
-- E-Maj: migration from 3.0.0 to <devel>
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
-- the emaj version registered in emaj_param must be '3.0.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '3.0.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 3.0.0',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.5
    IF current_setting('server_version_num')::int < 90500 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.5.', current_setting('server_version');
    END IF;
-- no existing group must have been created with a postgres version prior 8.4
    SELECT string_agg(group_name, ', ') INTO v_groupList FROM emaj.emaj_group
      WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                 to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804;
    IF v_groupList IS NOT NULL THEN
      RAISE EXCEPTION 'E-Maj upgrade: groups "%" have been created with a too old postgres version (< 8.4). Drop these groups before upgrading. ',v_groupList;
    END IF;
-- specific test for this version: verify the emaj environment integrity
    PERFORM 0 FROM emaj.emaj_verify_all() AS t(msg) WHERE msg = 'No error detected';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj upgrade: The E-Maj environment looks unsane. Please execute the "SELECT * FROM emaj.emaj_verify_all()" statement and solve the reported issues before upgrading.';
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- insert the upgrade begin record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 3.0.0 started');

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- change the log schema location to fit    --
-- the new rules                            --
--                                          --
----------------------------------------------

-- The procedure
-- * creates the new log schemas
-- * moves the log objects into their new schema, and rename them
-- * drops the useless log schemas
DO
$do$
  DECLARE
    v_newLogSchema           TEXT;
    v_newName                TEXT;
    v_emajNamesPrefix        TEXT;
    v_logTableVersion        TEXT;
    r_schema                 RECORD;
    r_rel                    RECORD;
  BEGIN
--
-- create the new log schemas
--
    FOR r_schema IN
        SELECT DISTINCT 'emaj_' || rel_schema AS log_schema FROM emaj.emaj_relation
          WHERE NOT EXISTS
              (SELECT 0 FROM emaj.emaj_schema WHERE sch_name = 'emaj_' || rel_schema)
        ORDER BY 1
      LOOP
-- check that the schema doesn't already exist
      PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = r_schema.log_schema;
      IF FOUND THEN
        RAISE EXCEPTION 'E-Maj upgrade: the schema "%" should not exist. Drop it manually before rerunning the upgrade.',r_schema.log_schema;
      END IF;
-- create the schema and give the appropriate rights
      EXECUTE 'CREATE SCHEMA ' || quote_ident(r_schema.log_schema);
      EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(r_schema.log_schema) || ' TO emaj_adm';
      EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(r_schema.log_schema) || ' TO emaj_viewer';
      EXECUTE 'ALTER EXTENSION emaj DROP SCHEMA ' || quote_ident(r_schema.log_schema);
-- and record the schema creation into the emaj_schema and the emaj_hist tables
      INSERT INTO emaj.emaj_schema (sch_name) VALUES (r_schema.log_schema);
    END LOOP;
--
-- moves the log objects into their new schema, and rename them
--
-- prepare the changes using a clone of the emaj_relation table
    CREATE TEMP TABLE tmp_relation AS SELECT * FROM emaj.emaj_relation;
    ALTER TABLE tmp_relation
      ADD COLUMN new_log_schema   TEXT,
      ADD COLUMN new_log_table    TEXT,
      ADD COLUMN new_log_index    TEXT,
      ADD COLUMN new_log_sequence TEXT,
      ADD COLUMN new_log_function TEXT;
-- compute the new log objects name
    FOR r_rel IN
        SELECT * FROM tmp_relation
          WHERE rel_kind = 'r'
          ORDER BY rel_schema, rel_tblseq, rel_time_range
        LOOP
-- for each table described in the tmp_relation table,
-- compute the log table version
      IF r_rel.rel_log_table ~ '_\d+$' THEN
        v_logTableVersion = unnest(regexp_matches(r_rel.rel_log_table,'(_\d+)$'));
      ELSE
        v_logTableVersion = '';
      END IF;
-- compute the names prefix
      IF length(r_rel.rel_tblseq) <= 50 THEN
        v_emajNamesPrefix = r_rel.rel_tblseq;
      ELSE
        SELECT substr(r_rel.rel_tblseq, 1, 50) || '#' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_emajNamesPrefix
          FROM
            (SELECT unnest(regexp_matches(substr(new_log_table, 51),'#(\d+)'))::INT AS suffix
               FROM tmp_relation
               WHERE new_log_table IS NOT NULL AND substr(new_log_table, 1, 50) = substr(r_rel.rel_tblseq, 1, 50)
            ) AS t;
      END IF;
-- update the tmp_relation accordingly
      UPDATE tmp_relation
        SET new_log_schema   = 'emaj_' || rel_schema,
            new_log_table    = v_emajNamesPrefix || '_log' || v_logTableVersion,
            new_log_index    = v_emajNamesPrefix || '_log_idx'|| v_logTableVersion,
            new_log_sequence = CASE WHEN rel_log_sequence IS NULL THEN NULL ELSE v_emajNamesPrefix || '_log_seq' END,
            new_log_function = CASE WHEN rel_log_function IS NULL THEN NULL ELSE v_emajNamesPrefix || '_log_fnct' END
        WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq AND rel_time_range = r_rel.rel_time_range;
    END LOOP;

-- process the log tables, indexes and sequences
    FOR r_rel IN
        SELECT * FROM tmp_relation
          WHERE rel_kind = 'r'
          ORDER BY rel_schema, rel_tblseq, rel_time_range
        LOOP
-- process the log schema change for the log table (IF EXISTS because several rows may share the same log table)
      EXECUTE 'ALTER TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table)
           || ' SET SCHEMA ' || quote_ident(r_rel.new_log_schema);
-- rename the log table if needed
      IF r_rel.rel_log_table <> r_rel.new_log_table THEN
        EXECUTE 'ALTER TABLE IF EXISTS ' || quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.rel_log_table)
             || ' RENAME TO ' || quote_ident(r_rel.new_log_table);
      END IF;
-- rename the log index, if needed
      IF r_rel.rel_log_index <> r_rel.new_log_index THEN
        EXECUTE 'ALTER INDEX IF EXISTS ' || quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.rel_log_index)
             || ' RENAME TO ' || quote_ident(r_rel.new_log_index);
      END IF;
-- move the log sequence and rename, if needed
      EXECUTE 'ALTER SEQUENCE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)
           || ' SET SCHEMA ' || quote_ident(r_rel.new_log_schema);
      IF r_rel.rel_log_sequence <> r_rel.new_log_sequence THEN
        EXECUTE 'ALTER SEQUENCE IF EXISTS ' || quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence)
             || ' RENAME TO ' || quote_ident(r_rel.new_log_sequence);
      END IF;
    END LOOP;

-- process the log functions
-- (we need to drop them with their related log trigger and recreate them, because the function content changes)
    FOR r_rel IN
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_log_table, rel_log_function, rel_log_sequence,
                                       new_log_schema, new_log_table, new_log_function, new_log_sequence,
               group_is_logging
          FROM tmp_relation, emaj.emaj_group
          WHERE rel_kind = 'r' AND upper_inf(rel_time_range)
            AND rel_group = group_name
          ORDER BY 1,2
      LOOP
-- drop the log function and trigger, and recreate them
      EXECUTE 'DROP FUNCTION ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() CASCADE';
      PERFORM emaj._create_log_trigger(quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq),
                                       quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.new_log_table),
                                       quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.new_log_sequence),
                                       quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.new_log_function));
-- remove the log function from the extension
      EXECUTE 'ALTER EXTENSION emaj DROP FUNCTION ' || quote_ident(r_rel.new_log_schema) || '.' || quote_ident(r_rel.new_log_function) || '()';
-- if the group is in IDLE state, disable the just recreated trigger
      IF NOT r_rel.group_is_logging THEN
        EXECUTE 'ALTER TABLE ' || quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq) || ' DISABLE TRIGGER emaj_log_trg';
      END IF;
    END LOOP;

-- adjust the sequences schema and name in the emaj_sequence table
    WITH tmp_seq AS (
      SELECT DISTINCT rel_log_schema, rel_log_sequence, new_log_schema, new_log_sequence
        FROM tmp_relation
        WHERE rel_log_sequence IS NOT NULL)
    UPDATE emaj.emaj_sequence SET sequ_schema = new_log_schema, sequ_name = new_log_sequence
      FROM tmp_seq
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence;

-- update the attributes into emaj_relation
    UPDATE emaj.emaj_relation r
      SET rel_log_schema   = new_log_schema,
          rel_log_table    = new_log_table,
          rel_log_index    = new_log_index,
          rel_log_sequence = new_log_sequence,
          rel_log_function = new_log_function
      FROM tmp_relation t
      WHERE r.rel_schema = t.rel_schema AND r.rel_tblseq = t.rel_tblseq AND r.rel_time_range = t.rel_time_range;

    DROP TABLE tmp_relation;
--
-- drop the useless log schemas
--
-- For each log schema to drop,
    FOR r_schema IN
        SELECT sch_name AS log_schema FROM emaj.emaj_schema                           -- the existing schemas
          WHERE sch_name <> 'emaj'
          EXCEPT
        SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation                        -- the currently needed schemas
          WHERE rel_kind = 'r'
        ORDER BY 1
        LOOP
-- check that the schema really exists
      PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = r_schema.log_schema;
      IF NOT FOUND THEN
        RAISE WARNING 'E-Maj upgrade: the schema "%" does not exist ! Let''s bypass its suppression', r_schema.log_schema;
      ELSE
-- otherwise, drop restrict with a trap on the potential error
        BEGIN
          EXECUTE 'DROP SCHEMA ' || quote_ident(r_schema.log_schema);
          EXCEPTION
-- trap the 2BP01 exception to generate a more understandable error message
            WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
              RAISE EXCEPTION 'E-Maj upgrade: cannot drop the schema "%". It probably owns unattended objects. Use the emaj_verify_all() function to get details.', r_schema.log_schema;
        END;
      END IF;
-- remove the schema from the emaj_schema table
      DELETE FROM emaj.emaj_schema WHERE sch_name = r_schema.log_schema;
    END LOOP;
    RETURN;
  END;
$do$;

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------

--
-- drop both grpdef_log_schema_suffix and grpdef_emaj_names_prefix columns from the emaj_group_def table
--
ALTER TABLE emaj.emaj_group_def
  DROP COLUMN grpdef_log_schema_suffix,
  DROP COLUMN grpdef_emaj_names_prefix;

--
-- Add the table emaj_rel_hist
--
-- table containing the history of relations - groups relationship
CREATE TABLE emaj.emaj_rel_hist (
  relh_schema                  TEXT        NOT NULL,       -- schema name containing the relation
  relh_tblseq                  TEXT        NOT NULL,       -- application table or sequence name
  relh_time_range              INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
                                           CHECK (NOT upper_inf(relh_time_range)),
  relh_group                   TEXT        NOT NULL,       -- name of the group that owned the relation
                                                           --   (the group may not exist in emaj_group anymore)
  relh_kind                    TEXT,                       -- similar to the relkind column of pg_class table
                                                           --   ('r' = table, 'S' = sequence)
  PRIMARY KEY (relh_schema, relh_tblseq, relh_time_range),
  EXCLUDE USING gist (relh_schema WITH =, relh_tblseq WITH =, relh_time_range WITH &&)
  );
-- functional index on emaj_rel_hist used to speedup the history purge function
CREATE INDEX emaj_rel_hist_idx1 ON emaj.emaj_rel_hist ((upper(relh_time_range)));
COMMENT ON TABLE emaj.emaj_rel_hist IS
$$Contains the history of groups content.$$;

--
-- Add both values to the _rlbk_step_enum enum type
--
ALTER TYPE emaj._rlbk_step_enum
  RENAME TO _rlbk_step_enum_old;
-- enum of the possible values for the rollback steps
CREATE TYPE emaj._rlbk_step_enum AS ENUM (
  'LOCK_TABLE',              -- set a lock on a table
  'DIS_APP_TRG',             -- disable an application trigger
  'DIS_LOG_TRG',             -- disable a log trigger
  'DROP_FK',                 -- drop a foreign key
  'SET_FK_DEF',              -- set a foreign key deferred
  'RLBK_TABLE',              -- rollback a table
  'DELETE_LOG',              -- delete rows from a log table
  'SET_FK_IMM',              -- set a foreign key immediate
  'ADD_FK',                  -- recreate a foreign key
  'ENA_APP_TRG',             -- enable an application trigger
  'ENA_LOG_TRG',             -- enable a log trigger
  'CTRL+DBLINK',             -- pseudo step representing the periods between 2 steps execution, when dblink is used
  'CTRL-DBLINK'              -- pseudo step representing the periods between 2 steps execution, when dblink is not used
  );

--
-- change the emaj_rlbk_plan table structure
--
ALTER TABLE emaj.emaj_rlbk_plan
  RENAME COLUMN rlbp_fkey TO rlbp_object;
ALTER TABLE emaj.emaj_rlbk_plan
  RENAME COLUMN rlbp_fkey_def TO rlbp_object_def;
ALTER TABLE emaj.emaj_rlbk_plan
  ALTER COLUMN rlbp_step TYPE emaj._rlbk_step_enum USING rlbp_step::TEXT::emaj._rlbk_step_enum;

--
-- change the emaj_rlbk_plan table structure
--
ALTER TABLE emaj.emaj_rlbk_stat
  RENAME COLUMN rlbt_fkey TO rlbt_object;
ALTER TABLE emaj.emaj_rlbk_stat
  ALTER COLUMN rlbt_step TYPE emaj._rlbk_step_enum USING rlbt_step::TEXT::emaj._rlbk_step_enum;

-- 
-- Create the new emaj_ignored_app_trigger table
--

-- table containing the list of appliction triggers on tables that should not be automatically disabled when launching a rollback operation
-- it is the administrator's responsibility to setup its content, using the emaj_set_disabled_triggers function
CREATE TABLE emaj.emaj_ignored_app_trigger (
  trg_schema                   TEXT        NOT NULL,       -- application schema
  trg_table                    TEXT        NOT NULL,       -- application table
  trg_name                     TEXT        NOT NULL,       -- trigger name
  PRIMARY KEY (trg_schema, trg_table, trg_name)
  );
COMMENT ON TABLE emaj.emaj_ignored_app_trigger IS
$$Contains the triggers on application tables that do not need to be disabled when rollbacking.$$;

--
-- add created or recreated tables and sequences to the list of content to save by pg_dump
--
SELECT pg_catalog.pg_extension_config_dump('emaj_rel_hist','');
SELECT pg_catalog.pg_extension_config_dump('emaj_ignored_app_trigger','');

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
DROP FUNCTION IF EXISTS emaj._create_tbl(R_GRPDEF EMAJ.EMAJ_GROUP_DEF,V_TIMEID BIGINT,V_ISROLLBACKABLE BOOLEAN);
DROP FUNCTION IF EXISTS emaj._change_log_schema_tbl(R_REL EMAJ.EMAJ_RELATION,V_NEWLOGSCHEMASUFFIX TEXT,V_MULTIGROUP BOOLEAN);
DROP FUNCTION IF EXISTS emaj._change_emaj_names_prefix(R_REL EMAJ.EMAJ_RELATION,V_NEWNAMESPREFIX TEXT,V_MULTIGROUP BOOLEAN);
DROP FUNCTION IF EXISTS emaj._drop_tbl(R_REL EMAJ.EMAJ_RELATION);
DROP FUNCTION IF EXISTS emaj._create_seq(GRPDEF EMAJ.EMAJ_GROUP_DEF,V_TIMEID BIGINT);
DROP FUNCTION IF EXISTS emaj._drop_seq(R_REL EMAJ.EMAJ_RELATION);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._purge_hist()
RETURNS VOID LANGUAGE plpgsql AS
$_purge_hist$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter, but
--   not deleting event traces neither after the oldest active mark or after the oldest not committed or aborted rollback operation.
-- It also purges oldest rows from the maj_exec_plan, emaj_rlbk_session and emaj_rlbk_plan tables, using the same rules.
-- The function is called at start group time and when oldest marks are deleted.
  DECLARE
    v_datetimeLimit          TIMESTAMPTZ;
    v_maxTimeId              BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbPurgedHist           BIGINT;
    v_nbPurgedRelHist        BIGINT;
    v_nbPurgedRlbk           BIGINT;
    v_nbPurgedAlter          BIGINT;
    v_wording                TEXT = '';
  BEGIN
-- compute the timestamp limit
    SELECT min(datetime) INTO v_datetimeLimit FROM
      (                                           -- compute the timestamp limit from the history_retention parameter
        (SELECT current_timestamp -
           coalesce((SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'),'1 YEAR'))
      UNION ALL                                   -- get the transaction timestamp of the oldest non deleted mark for all groups
        (SELECT min(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_mark
           WHERE time_id = mark_time_id AND NOT mark_is_deleted)
      UNION ALL                                   -- get the transaction timestamp of the oldest non committed or aborted rollback
        (SELECT min(time_tx_timestamp) FROM emaj.emaj_time_stamp, emaj.emaj_rlbk
           WHERE time_id = rlbk_time_id AND rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED'))
      ) AS t(datetime);
-- get the greatest timestamp identifier corresponding to the timeframe to purge, if any
    SELECT max(time_id) INTO v_maxTimeId FROM emaj.emaj_time_stamp
      WHERE time_tx_timestamp < v_datetimeLimit;
-- delete oldest rows from emaj_hist
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < v_datetimeLimit;
    GET DIAGNOSTICS v_nbPurgedHist = ROW_COUNT;
    IF v_nbPurgedHist > 0 THEN
      v_wording = v_nbPurgedHist || ' emaj_hist rows deleted';
    END IF;
-- delete oldest rows from emaj_rel_hist
    DELETE FROM emaj.emaj_rel_hist WHERE upper(relh_time_range) < v_maxTimeId;
    GET DIAGNOSTICS v_nbPurgedRelHist = ROW_COUNT;
    IF v_nbPurgedRelHist > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedRelHist || ' relation history rows deleted';
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
    SELECT max(rlbk_id) INTO v_maxRlbkId FROM emaj.emaj_rlbk
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

CREATE OR REPLACE FUNCTION emaj._check_conf_groups(v_groupNames TEXT[])
RETURNS SETOF emaj._check_conf_groups_type LANGUAGE plpgsql AS
$_check_conf_groups$
-- This function verifies that the content of tables group as defined into the emaj_group_def table is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them, depending on the tables group type.
-- It is called by the emaj_create_group() and _alter_groups() functions.
-- This function checks that the referenced application tables and sequences:
--  - exist,
--  - is not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
--  - will not generate conflicts on emaj objects to create (when emaj names prefix is not the default one)
-- It also checks that:
--  - tables are not TEMPORARY
--  - for rollbackable groups, tables are not UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for sequences, the tablespaces, emaj log schema and emaj object name prefix are all set to NULL
--  - for tables, configured tablespaces exist
-- Input: name array of the tables groups to check
  BEGIN
-- check that all application tables and sequences listed for the group really exist
    RETURN QUERY
      SELECT 1, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table or sequence %s.%s does not exist.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def
        WHERE grpdef_group = ANY(v_groupNames)
          AND (grpdef_schema, grpdef_tblseq) NOT IN (
            SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S','p'));
---- check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj)
    RETURN QUERY
      SELECT 2, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq
          AND grpdef_group = ANY(v_groupNames)
          AND relkind = 'p';
---- check no application schema listed for the group in the emaj_group_def table is an E-Maj schema
    RETURN QUERY
      SELECT 3, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table or sequence %s.%s belongs to an E-Maj schema.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, emaj.emaj_schema
        WHERE grpdef_group = ANY(v_groupNames)
          AND grpdef_schema = sch_name;
---- check that no table or sequence of the checked groups already belongs to other created groups
    RETURN QUERY
      SELECT 4, 1, grpdef_group, grpdef_schema, grpdef_tblseq, rel_group,
             format('in the group %s, the table or sequence %s.%s already belongs to the group %s.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(rel_group))
        FROM emaj.emaj_group_def, emaj.emaj_relation
        WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
          AND upper_inf(rel_time_range) AND grpdef_group = ANY (v_groupNames) AND NOT rel_group = ANY (v_groupNames);
---- check no table is a TEMP table
    RETURN QUERY
      SELECT 5, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is a TEMPORARY table.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relpersistence = 't';
---- check that a table is not assigned several time in the groups
    RETURN QUERY
      WITH dupl AS (
        SELECT grpdef_schema, grpdef_tblseq, count(*)
          FROM emaj.emaj_group_def
          WHERE grpdef_group = ANY (v_groupNames)
          GROUP BY 1,2 HAVING count(*) > 1)
      SELECT 10, 1, v_groupNames[1], grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('the table %s.%s is assigned several times.', quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM dupl;
---- check that the log data tablespaces for tables exist
    RETURN QUERY
      SELECT 12, 1, grpdef_group, grpdef_schema, grpdef_tblseq, grpdef_log_dat_tsp,
             format('in the group %s, for the table %s.%s, the data log tablespace %s does not exist.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(grpdef_log_dat_tsp))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND grpdef_log_dat_tsp IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = grpdef_log_dat_tsp);
---- check that the log index tablespaces for tables exist
    RETURN QUERY
      SELECT 13, 1, grpdef_group, grpdef_schema, grpdef_tblseq, grpdef_log_idx_tsp,
             format('in the group %s, for the table %s.%s, the index log tablespace %s does not exist.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(grpdef_log_idx_tsp))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND grpdef_log_idx_tsp IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = grpdef_log_idx_tsp);
---- check no table is an unlogged table (blocking rollbackable groups only)
    RETURN QUERY
      SELECT 20, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is an UNLOGGED table.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relpersistence = 'u';
---- check no table is a WITH OIDS table (blocking rollbackable groups only)
    RETURN QUERY
      SELECT 21, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is declared WITH OIDS.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relhasoids;
---- check every table has a primary key (blocking rollbackable groups only)
    RETURN QUERY
      SELECT 22, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s has no PRIMARY KEY.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                            AND contype = 'p' AND nspname = grpdef_schema AND relname = grpdef_tblseq);
---- all sequences described in emaj_group_def have their data log tablespaces attributes set to NULL
    RETURN QUERY
      SELECT 32, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the data log tablespace is not NULL.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_dat_tsp IS NOT NULL;
---- all sequences described in emaj_group_def have their index log tablespaces attributes set to NULL
    RETURN QUERY
      SELECT 33, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the index log tablespace is not NULL.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_idx_tsp IS NOT NULL;
--
    RETURN;
  END;
$_check_conf_groups$;

CREATE OR REPLACE FUNCTION emaj._forbid_truncate_fnct()
RETURNS TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_forbid_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of a rollbackable group
-- in logging mode.
-- It can only be called with postgresql in a version greater or equal 8.4
  BEGIN
    IF (TG_OP = 'TRUNCATE') THEN
      RAISE EXCEPTION 'emaj._forbid_truncate_fnct: TRUNCATE is not allowed while updates on this table (%.%) are currently protected by E-Maj. Consider stopping the group before issuing a TRUNCATE.', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    RETURN NULL;
  END;
$_forbid_truncate_fnct$;

CREATE OR REPLACE FUNCTION emaj._create_log_schemas(v_function TEXT, v_groupNames TEXT[])
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_schemas$
-- The function creates all log schemas that will be needed to create new log tables. It gives the appropriate rights to emaj users on these schemas.
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
      EXECUTE 'CREATE SCHEMA ' || quote_ident(r_schema.log_schema);
      EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(r_schema.log_schema) || ' TO emaj_adm';
      EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(r_schema.log_schema) || ' TO emaj_viewer';
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
-- The function is created as SECURITY DEFINER so that log schemas can be dropped in any case
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
        EXECUTE 'DROP SCHEMA ' || quote_ident(r_schema.log_schema) || ' CASCADE';
      ELSE
-- otherwise, drop restrict with a trap on the potential error
        BEGIN
          EXECUTE 'DROP SCHEMA ' || quote_ident(r_schema.log_schema);
          EXCEPTION
-- trap the 2BP01 exception to generate a more understandable error message
            WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
              RAISE EXCEPTION '_drop_log_schemas: Cannot drop the schema "%". It probably owns unattended objects. Use the emaj_verify_all() function to get details.', r_schema.log_schema;
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

CREATE OR REPLACE FUNCTION emaj._create_tbl(v_schema TEXT, v_tbl TEXT, v_groupName TEXT, v_priority INT, v_logDatTsp TEXT, v_logIdxTsp TEXT, v_timeId BIGINT, v_groupIsRollbackable BOOLEAN, v_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: the application table to process, the group to add it into, the priority and tablespaces attributes, the time id of the operation,
--        2 booleans indicating whether the group is rollbackable and whether the group is currently in logging state
-- Are created in the log schema:
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
-- for long table names (over 50 char long), compute the suffix to add to the first 50 characters (#1, #2, ...), by looking at the existing names
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
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName;
    EXECUTE 'CREATE TABLE ' || v_logTableName
         || ' (LIKE ' || v_fullTableName || ') ' || v_dataTblSpace;
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_verb      VARCHAR(3),'
         || ' ADD COLUMN emaj_tuple     VARCHAR(3),'
         || ' ADD COLUMN emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
         || ' ADD COLUMN emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
         || ' ADD COLUMN emaj_txid      BIGINT      DEFAULT txid_current(),'
         || ' ADD COLUMN emaj_user      VARCHAR(32) DEFAULT session_user';
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
      EXECUTE 'ALTER TABLE ' || v_logTableName || ' ' || v_alter_log_table_param;
    END IF;
-- create the index on the log table
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
          AND attnum > 0 AND attnum < v_attnum AND attisdropped = FALSE AND attnotnull) AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE 'ALTER TABLE ' || v_logTableName || v_stmt;
    END IF;
-- create the sequence associated to the log table
    EXECUTE 'CREATE SEQUENCE ' || v_sequenceName;
-- create the log function and the log trigger
    PERFORM emaj._create_log_trigger(v_fullTableName, v_logTableName, v_sequenceName, v_logFnctName);
-- If the group is idle, deactivate the log trigger (it will be enabled at emaj_start_group time)
    IF NOT v_groupIsLogging THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_log_trg';
    END IF;
-- creation of the trigger that manage any TRUNCATE on the application table
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
    IF v_groupIsRollbackable THEN
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
    IF NOT v_groupIsLogging THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER emaj_trunc_trg';
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
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers (%). They will be automatically disabled during E-Maj rollback operations, unless they have been recorded into the list of triggers that may be kept enabled, with the emaj_ignore_app_trigger() function.', v_fullTableName, v_triggerList;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
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
    EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
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
    EXECUTE 'CREATE TRIGGER emaj_log_trg'
         || ' AFTER INSERT OR UPDATE OR DELETE ON ' || v_fullTableName
         || '  FOR EACH ROW EXECUTE PROCEDURE ' || v_logFnctName || '()';
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
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group operation
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
    PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp, v_timeId, v_groupIsRollbackable, r_plan.altr_group_is_logging)
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
        EXECUTE 'ALTER SEQUENCE ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logSequence) || ' RESTART ' || v_nextVal;
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
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group time for instance).
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group operation
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
      EXECUTE 'ALTER TABLE IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_currentLogTable) ||
              ' RENAME TO '|| quote_ident(v_currentLogTable || v_namesSuffix);
      EXECUTE 'ALTER INDEX IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_currentLogIndex) ||
              ' RENAME TO '|| quote_ident(v_currentLogIndex || v_namesSuffix);
--TODO: share some code with _drop_tbl() ?
-- ... drop the log and truncate triggers
--     (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
      v_fullTableName  = quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq);
      PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = r_plan.altr_schema AND relname = r_plan.altr_tblseq AND relkind = 'r';
      IF FOUND THEN
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
      END IF;
-- ... drop the log function and the log sequence
-- (but we keep the sequence related data in the emaj_sequence and the emaj_seq_hole tables)
      EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logFunction) || '() CASCADE';
      EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(v_logSchema) || '.' || quote_ident(v_logSequence);
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

CREATE OR REPLACE FUNCTION emaj._move_tbl(r_plan emaj.emaj_alter_plan, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_tbl$
-- The function change the group ownership of a table. It is called during an alter group operation.
-- Required inputs: row from emaj_alter_plan corresponding to the appplication table to proccess, time stamp id of the alter group operation
  BEGIN
    IF NOT r_plan.altr_group_is_logging AND NOT r_plan.altr_new_group_is_logging THEN
-- no group is logging, so just adapt the last emaj_relation row related to the table
      UPDATE emaj.emaj_relation
        SET rel_group = r_plan.altr_new_group, rel_time_range = int8range(v_timeId, NULL, '[)')
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    ELSE
-- register the end of the previous relation time frame and create a new relation time frame with the new group
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range),v_timeId,'[)')
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema,
                                      rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
                                      rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions,rel_log_seq_last_value, rel_emaj_verb_attnum)
        SELECT rel_schema, rel_tblseq, int8range(v_timeId, NULL, '[)'), r_plan.altr_new_group, rel_kind, rel_priority, rel_log_schema,
               rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
               rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions, rel_log_seq_last_value, rel_emaj_verb_attnum
          FROM emaj.emaj_relation
          WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper(rel_time_range) = v_timeId;
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUPS', 'TABLE MOVED', quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq),
              'From group ' || r_plan.altr_group || ' to group ' || r_plan.altr_new_group);
    RETURN;
  END;
$_move_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess
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
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_log_trg ON ' || v_fullTableName;
        EXECUTE 'DROP TRIGGER IF EXISTS emaj_trunc_trg ON ' || v_fullTableName;
      END IF;
-- drop the log function
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_function) || '() CASCADE';
      END IF;
-- drop the sequence associated to the log table
      EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_sequence);
    END IF;
-- drop the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
-- delete rows related to the log sequence from emaj_sequence table (it my delete rows for other not yet processed time_ranges for the same table)
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- delete rows related to the table from emaj_seq_hole table (it may delete rows for other not yet processed time_ranges for the same table)
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

CREATE OR REPLACE FUNCTION emaj._create_seq(v_schema TEXT, v_seq TEXT, v_groupName TEXT, v_priority INT, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_create_seq$
-- The function checks whether the sequence is related to a serial column of an application table.
-- If yes, it verifies that this table also belong to the same group
-- Required inputs: the application sequence to process, the group to add it into, the priority attribute, the time id of the operation
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
      WHERE cs.relname = v_seq AND ns.nspname = v_schema         -- the selected sequence
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
        RAISE WARNING '_create_seq: The sequence %.% is linked to table %.% but this table does not belong to any tables group.', v_schema, v_seq, v_tableSchema, v_tableName;
      ELSE
        IF v_tableGroup <> v_groupName THEN
          RAISE WARNING '_create_seq: The sequence %.% is linked to table %.% but this table belong to another tables group (%).', v_schema, v_seq, v_tableSchema, v_tableName, v_tableGroup;
        END IF;
      END IF;
    END IF;
-- record the sequence in the emaj_relation table
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority, rel_kind)
      VALUES (v_schema, v_seq, int8range(v_timeId, NULL, '[)'), v_groupName, v_priority, 'S');
    RETURN;
  END;
$_create_seq$;

CREATE OR REPLACE FUNCTION emaj._add_seq(r_plan emaj.emaj_alter_plan, v_timeId BIGINT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_add_seq$
-- The function adds a sequence to a group. It is called during an alter group operation.
-- If the group is in idle state, it simply calls the _create_seq() function.
-- Otherwise, it calls the _create_seql() function, and record the current state of the sequence
-- Required inputs: row from emaj_alter_plan corresponding to the appplication sequence to proccess, time stamp id of the alter group operation
  BEGIN
-- create the sequence
    PERFORM emaj._create_seq(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, v_timeId)
      FROM emaj.emaj_group_def
      WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- if the group is in logging state, perform additional tasks
    IF r_plan.altr_group_is_logging THEN
-- ... record the new sequence state in the emaj_sequence table for the current alter_group mark
      INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                  sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
        SELECT * FROM emaj._get_current_sequence_state(r_plan.altr_schema, r_plan.altr_tblseq, v_timeId);
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)       VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'SEQUENCE ADDED',
              quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq), 'To the group ' || r_plan.altr_group);
    RETURN;
  END;
$_add_seq$;

CREATE OR REPLACE FUNCTION emaj._remove_seq(r_plan emaj.emaj_alter_plan, v_timeId BIGINT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_remove_seq$
-- The function removes a sequence from a group. It is called during an alter group operation.
-- Required inputs: row from emaj_alter_plan corresponding to the appplication sequence to proccess, time stamp id of the alter group operation
  BEGIN
    IF r_plan.altr_group_is_logging THEN
-- if the group is in logging state, just register the end of the relation time frame
      UPDATE emaj.emaj_relation SET rel_time_range = int8range(lower(rel_time_range),v_timeId, '[)')
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    ELSE
-- if the group is in idle state, drop the sequence immediately
      PERFORM emaj._drop_seq(emaj.emaj_relation.*, v_timeId) FROM emaj.emaj_relation
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'SEQUENCE REMOVED',
              quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq), 'From the group ' || r_plan.altr_group);
    RETURN;
  END;
$_remove_seq$;

CREATE OR REPLACE FUNCTION emaj._move_seq(r_plan emaj.emaj_alter_plan, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_seq$
-- The function change the group ownership of a sequence. It is called during an alter group operation.
-- Required inputs: row from emaj_alter_plan corresponding to the appplication sequence to proccess, time stamp id of the alter group operation
  BEGIN
    IF NOT r_plan.altr_group_is_logging AND NOT r_plan.altr_new_group_is_logging THEN
-- no group is logging, so just adapt the last emaj_relation row related to the sequence
      UPDATE emaj.emaj_relation
        SET rel_group = r_plan.altr_new_group, rel_time_range = int8range(v_timeId, NULL, '[)')
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
    ELSE
-- register the end of the previous relation time frame and create a new relation time frame with the new group
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range),v_timeId,'[)')
        WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority, rel_kind)
        SELECT rel_schema, rel_tblseq, int8range(v_timeId, NULL, '[)'), r_plan.altr_new_group, rel_priority, rel_kind
          FROM emaj.emaj_relation
          WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper(rel_time_range) = v_timeId;
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUPS', 'SEQUENCE MOVED', quote_ident(r_plan.altr_schema) || '.' || quote_ident(r_plan.altr_tblseq),
              'From the group ' || r_plan.altr_group || ' to the group ' || r_plan.altr_new_group);
    RETURN;
  END;
$_move_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: row from emaj_relation corresponding to the appplication sequence to proccess
  BEGIN
-- delete rows from emaj_sequence
-- if several rows exist in emaj_relation for the same sequence, due to removals and additions while the group was in logging state,
--   the first function call deletes all emaj_sequence rows for the sequence
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ' || quote_literal(r_rel.rel_schema) || ' AND sequ_name = ' || quote_literal(r_rel.rel_tblseq);
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

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_minGlobalSeq BIGINT, v_maxGlobalSeq BIGINT, v_nbSession INT, v_isLoggedRlbk BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence
-- The function is called by emaj._rlbk_session_exec()
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

CREATE OR REPLACE FUNCTION emaj.emaj_ignore_app_trigger(v_action TEXT, v_schema TEXT, v_table TEXT, v_trigger TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_ignore_app_trigger$
-- This function records the list of application table triggers that must not be automatically disabled when launching a rollback operation.
-- Input: the action to perform, either ADD or REMOVE,
--        the schema and table names of the table that owns the trigger
--        and the trigger to record into or remove from the emaj_ignored_app_trigger table
-- Output: number of recorded or removed triggers
-- A trigger to add must exist. E-Maj triggers are not processed.
-- The trigger parameter may contain '%' and/or '_' characters, these characters having the same meaning as in LIKE clauses.
  DECLARE
    v_nbRows                 INT;
    v_tableOid               OID;
    v_trgList                TEXT;
  BEGIN
-- check the action parameter
    IF upper(v_action) NOT IN ('ADD','REMOVE') THEN
      RAISE EXCEPTION 'emaj_ignore_app_trigger: the action "%" must be either ''ADD'' or ''REMOVE''.', v_action;
    END IF;
-- process the REMOVE action
    IF upper(v_action) = 'REMOVE' THEN
      DELETE FROM emaj.emaj_ignored_app_trigger
        WHERE trg_schema = v_schema AND trg_table = v_table AND trg_name LIKE v_trigger;
      GET DIAGNOSTICS v_nbRows = ROW_COUNT;
      RETURN v_nbRows;
    END IF;
-- process the ADD action
-- check that the supplied schema qualified table name exists
    SELECT pg_class.oid INTO v_tableOid
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schema AND relname = v_table
        AND relkind = 'r';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_ignore_app_trigger: the table "%.%" does not exist.', v_schema, v_table;
    END IF;
-- check that the trigger exists for the table
    PERFORM 1 FROM pg_catalog.pg_trigger
      WHERE tgrelid = v_tableOid
        AND tgname LIKE v_trigger AND NOT tgisinternal;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_ignore_app_trigger: no trigger like "%" found for the table "%.%".', v_trigger, v_schema, v_table;
    END IF;
-- issue a warning if there is at least 1 emaj trigger selected
    SELECT string_agg(tgname,', ') INTO v_trgList
      FROM pg_catalog.pg_trigger
      WHERE tgrelid = v_tableOid
        AND tgname LIKE v_trigger AND tgname IN ('emaj_trunc_trg', 'emaj_log_trg') AND NOT tgisinternal;
    IF v_trgList IS NOT NULL THEN
      RAISE WARNING 'emaj_ignore_app_trigger: the triggers "%" are E-Maj triggers and are not processed by the function.', v_trgList;
    END IF;
-- insert into the emaj_ignored_app_trigger table the not yet recorded triggers
    INSERT INTO emaj.emaj_ignored_app_trigger
      SELECT v_schema, v_table, tgname
        FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_tableOid
          AND tgname LIKE v_trigger AND tgname NOT IN ('emaj_trunc_trg', 'emaj_log_trg') AND NOT tgisinternal
      ON CONFLICT DO NOTHING;
-- return the number of effectively added triggers
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
    RETURN v_nbRows;
  END;
$emaj_ignore_app_trigger$;
COMMENT ON FUNCTION emaj.emaj_ignore_app_trigger(TEXT,TEXT,TEXT,TEXT) IS
$$Records application tables triggers that are not automatically disabled at rollback time.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE 'SELECT rel.last_value, start_value, increment_by, max_value, min_value, cache_size as cache_value, cycle as is_cycled, rel.is_called FROM '
               || v_fullSeqName || ' rel, pg_catalog.pg_sequences '
               || 'WHERE schemaname = '|| quote_literal(r_rel.rel_schema) || ' AND sequencename = ' || quote_literal(r_rel.rel_tblseq)
              INTO STRICT curr_seq_rec;
    ELSE
      EXECUTE 'SELECT last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM '
               || v_fullSeqName
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

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN DEFAULT TRUE, v_is_empty BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- It also creates the log E-Maj schemas when needed
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
--        boolean explicitely indicating whether the group is empty or not
-- Output: number of processed tables and sequences
  DECLARE
    v_timeId                 BIGINT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    r                        RECORD;
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
    FOR r IN
      SELECT chk_message FROM emaj._check_conf_groups(ARRAY[v_groupName])
        WHERE (v_isRollbackable AND chk_severity <= 2)
           OR (NOT v_isRollbackable AND chk_severity <= 1)
        ORDER BY chk_msg_type, chk_group, chk_schema, chk_tblseq
    LOOP
      RAISE WARNING 'emaj_create_group: %', r.chk_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: One or several errors have been detected in the emaj_group_def table content.';
    END IF;
-- OK
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('C') INTO v_timeId;
-- insert the row describing the group into the emaj_group table
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable)
    INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_creation_time_id, group_has_waiting_changes,
                                 group_is_logging, group_is_rlbk_protected)
      VALUES (v_groupName, v_isRollbackable, v_timeId, FALSE, FALSE, NOT v_isRollbackable);
-- create new E-Maj log schemas, if needed
    PERFORM emaj._create_log_schemas('CREATE_GROUP', ARRAY[v_groupName]);
-- get and process all tables of the group (in priority order, NULLS being processed last)
    PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp, v_timeId, v_isRollbackable, FALSE)
      FROM (
        SELECT grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_group = v_groupName
            AND relnamespace = pg_namespace.oid
            AND nspname = grpdef_schema AND relname = grpdef_tblseq
            AND relkind = 'r'
          ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
           ) AS t;
    SELECT count(*) INTO v_nbTbl
      FROM emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND upper_inf(rel_time_range);
-- get and process all sequences of the group (in priority order, NULLS being processed last)
    PERFORM emaj._create_seq(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, v_timeId)
      FROM (
        SELECT grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_group = v_groupName
            AND relnamespace = pg_namespace.oid
            AND nspname = grpdef_schema AND relname = grpdef_tblseq
            AND relkind = 'S'
          ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
           ) AS t;
    SELECT count(*) INTO v_nbSeq
      FROM emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'S' AND upper_inf(rel_time_range);
-- update tables and sequences counters in the emaj_group table
    UPDATE emaj.emaj_group SET group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
      WHERE group_name = v_groupName;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_groups(array[v_groupName]);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'END', v_groupName, v_nbTbl + v_nbSeq || ' tables/sequences processed');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN,BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- It also drops log schemas that are not useful any more
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that log schemas can be dropped
  DECLARE
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbTb                   INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('D') INTO v_timeId;
-- delete the emaj objets for each table of the group
    FOR r_rel IN
        SELECT * FROM emaj.emaj_relation
          WHERE rel_group = v_groupName
          ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
      LOOP
        PERFORM CASE WHEN r_rel.rel_kind = 'r' THEN emaj._drop_tbl(r_rel, v_timeId)
                     WHEN r_rel.rel_kind = 'S' THEN emaj._drop_seq(r_rel, v_timeId) END;
    END LOOP;
-- drop the E-Maj log schemas that are now useless (i.e. not used by any other created group)
    PERFORM emaj._drop_log_schemas(CASE WHEN v_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END, v_isForced);
-- delete group row from the emaj_group table.
--   By cascade, it also deletes rows from emaj_mark
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName
      RETURNING group_nb_table + group_nb_sequence INTO v_nbTb;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj._alter_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_alter_groups$
-- This function effectively alters a tables groups array.
-- It takes into account the changes recorded in the emaj_group_def table since the groups have been created.
-- Input: group names array, flag indicating whether the function is called by the multi-group function or not
-- Output: number of tables and sequences belonging to the groups after the operation
  DECLARE
    v_loggingGroups          TEXT[];
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_eventTriggers          TEXT[];
    r                        RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','));
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := '') INTO v_groupNames;
    IF v_groupNames IS NOT NULL THEN
-- performs various checks on the groups content described in the emaj_group_def table
    FOR r IN
      SELECT chk_message FROM emaj._check_conf_groups(v_groupNames), emaj.emaj_group
        WHERE chk_group = group_name
          AND ((group_is_rollbackable AND chk_severity <= 2)
            OR (NOT group_is_rollbackable AND chk_severity <= 1))
        ORDER BY chk_msg_type, chk_group, chk_schema, chk_tblseq
    LOOP
      RAISE WARNING '_alter_groups: %', r.chk_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_alter_groups: One or several errors have been detected in the emaj_group_def table content.';
    END IF;
-- build the list of groups that are in logging state
      SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups FROM emaj.emaj_group
        WHERE group_name = ANY(v_groupNames) AND group_is_logging;
-- check and process the supplied mark name, if it is worth to be done
      IF v_loggingGroups IS NOT NULL THEN
        SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
      END IF;
-- OK
-- get the time stamp of the operation
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- for LOGGING groups, lock all tables to get a stable point
      IF v_loggingGroups IS NOT NULL THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', v_multiGroup);
-- and set the mark, using the same time identifier
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, v_multiGroup, TRUE, NULL, v_timeId);
      END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- we can now plan all the steps needed to perform the operation
      PERFORM emaj._alter_plan(v_groupNames, v_timeId);
-- create the needed log schemas
      PERFORM emaj._create_log_schemas(CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, v_groupNames);
-- execute the plan
      PERFORM emaj._alter_exec(v_timeId, v_multiGroup);
-- drop the E-Maj log schemas that are now useless (i.e. not used by any created group)
      PERFORM emaj._drop_log_schemas(CASE WHEN v_multiGroup THEN 'ALTER_GROUPS' ELSE 'ALTER_GROUP' END, FALSE);
-- update some attributes in the emaj_group table
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId, group_has_waiting_changes = FALSE,
            group_nb_table = (SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r'),
            group_nb_sequence = (SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'S')
        WHERE group_name = ANY (v_groupNames);
-- enable previously disabled event triggers
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- check foreign keys with tables outside the groups
      PERFORM emaj._check_fk_groups(v_groupNames);
    END IF;
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
  BEGIN
-- the plan is built using the same steps order than the coming execution
-- determine the relations that do not belong to the groups anymore
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REMOVE_TBL' ELSE 'REMOVE_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, rel_priority
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groupNames) AND upper_inf(rel_time_range)
          AND NOT EXISTS (
              SELECT NULL FROM emaj.emaj_group_def
                WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
                  AND grpdef_group = ANY (v_groupNames));
-- determine the tables that need to be "repaired" (damaged or out of sync E-Maj components)
-- (normally, there should not be any REPAIR_SEQ - if any, the _alter_exec() function will produce an exception)
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_group)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'REPAIR_TBL' ELSE 'REPAIR_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, grpdef_priority,
             CASE WHEN rel_group <> grpdef_group THEN grpdef_group ELSE NULL END
        FROM (                                   -- all damaged or out of sync tables
          SELECT DISTINCT ver_schema, ver_tblseq FROM emaj._verify_groups(v_groupNames, FALSE)
             ) AS t, emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = ver_schema AND rel_tblseq = ver_tblseq AND upper_inf(rel_time_range)
          AND rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
--   exclude relations that will have been removed in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step IN ('REMOVE_TBL', 'REMOVE_SEQ'));
-- determine the groups that will be reset (i.e. those in IDLE state)
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group)
      SELECT v_timeId, 'RESET_GROUP', '', '', group_name
        FROM emaj.emaj_group
        WHERE group_name = ANY (v_groupNames)
          AND NOT group_is_logging;
-- determine the tables whose log data tablespace in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_group)
      SELECT v_timeId, 'CHANGE_TBL_LOG_DATA_TSP', rel_schema, rel_tblseq, rel_group, grpdef_priority,
             CASE WHEN rel_group <> grpdef_group THEN grpdef_group ELSE NULL END
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND coalesce(rel_log_dat_tsp,'') <> coalesce(grpdef_log_dat_tsp,'')
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the tables whose log data tablespace in emaj_group_def has changed
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_group)
      SELECT v_timeId, 'CHANGE_TBL_LOG_INDEX_TSP', rel_schema, rel_tblseq, rel_group, grpdef_priority,
             CASE WHEN rel_group <> grpdef_group THEN grpdef_group ELSE NULL END
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND grpdef_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND coalesce(rel_log_idx_tsp,'') <> coalesce(grpdef_log_idx_tsp,'')
--   exclude tables that will have been repaired in a previous step
          AND (rel_schema, rel_tblseq) NOT IN (
            SELECT altr_schema, altr_tblseq FROM emaj.emaj_alter_plan WHERE altr_time_id = v_timeId AND altr_step = 'REPAIR_TBL');
-- determine the tables or sequences that change their group ownership
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_new_group)
      SELECT v_timeId, CAST(CASE WHEN rel_kind = 'r' THEN 'MOVE_TBL' ELSE 'MOVE_SEQ' END AS emaj._alter_step_enum),
             rel_schema, rel_tblseq, rel_group, grpdef_priority, grpdef_group
      FROM emaj.emaj_relation, emaj.emaj_group_def
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
        AND rel_group = ANY (v_groupNames)
        AND grpdef_group = ANY (v_groupNames)
        AND rel_group <> grpdef_group;
-- determine the relation that change their priority level
    INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority)
      SELECT v_timeId, 'CHANGE_REL_PRIORITY', rel_schema, rel_tblseq, rel_group, grpdef_priority
      FROM emaj.emaj_relation, emaj.emaj_group_def
      WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
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
                WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range)
                  AND rel_group = ANY (v_groupNames))
          AND relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq;
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
-- and return
    RETURN;
  END;
$_alter_plan$;

CREATE OR REPLACE FUNCTION emaj._alter_exec(v_timeId BIGINT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_alter_exec$
-- This function executes the alter groups operation that has been planned by the _alter_plan() function.
-- It looks at the emaj_alter_plan table and executes elementary step in proper order.
-- Input: timestamp id of the operation
  DECLARE
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_isRollbackable         BOOLEAN;
    r_plan                   emaj.emaj_alter_plan%ROWTYPE;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- scan the emaj_alter_plan table and execute each elementary item in the proper order
    FOR r_plan IN
      SELECT *
        FROM emaj.emaj_alter_plan
        WHERE altr_time_id = v_timeId
        ORDER BY altr_step, altr_priority, altr_schema, altr_tblseq, altr_group
      LOOP
      CASE r_plan.altr_step
        WHEN 'REMOVE_TBL' THEN
-- remove a table from its group
          PERFORM emaj._remove_tbl(r_plan, v_timeId, v_multiGroup);
--
        WHEN 'REMOVE_SEQ' THEN
-- remove a sequence from its group
          PERFORM emaj._remove_seq(r_plan, v_timeId, v_multiGroup);
--
        WHEN 'RESET_GROUP' THEN
-- reset a group
          PERFORM emaj._reset_groups(ARRAY[r_plan.altr_group]);
--
        WHEN 'REPAIR_TBL' THEN
          IF r_plan.altr_group_is_logging THEN
            RAISE EXCEPTION 'alter_exec: Cannot repair the table %.%. Its group % is in LOGGING state.', r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
          ELSE
-- remove the table from its group
            PERFORM emaj._drop_tbl(emaj.emaj_relation.*, v_timeId) FROM emaj.emaj_relation
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the is_rollbackable status of the related group
            SELECT group_is_rollbackable INTO v_isRollbackable
              FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- and recreate it
            PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp, v_timeId, v_isRollbackable, r_plan.altr_group_is_logging)
              FROM emaj.emaj_group_def
              WHERE grpdef_group = coalesce (r_plan.altr_new_group, r_plan.altr_group)
                AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
          END IF;
--
        WHEN 'REPAIR_SEQ' THEN
          RAISE EXCEPTION 'alter_exec: Internal error, trying to repair a sequence (%.%) is abnormal.', r_plan.altr_schema, r_plan.altr_tblseq;
--
        WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_log_dat_tsp INTO v_logDatTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = coalesce (r_plan.altr_new_group, r_plan.altr_group)
              AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_data_tsp_tbl(r_rel, v_logDatTsp, v_multiGroup);
--
        WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the table description from emaj_group_def
          SELECT grpdef_log_idx_tsp INTO v_logIdxTsp FROM emaj.emaj_group_def
            WHERE grpdef_group = coalesce (r_plan.altr_new_group, r_plan.altr_group)
              AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
-- then alter the relation, depending on the changes
          PERFORM emaj._change_log_index_tsp_tbl(r_rel, v_logIdxTsp, v_multiGroup);
--
        WHEN 'MOVE_TBL' THEN
-- move a table from one group to another group
          PERFORM emaj._move_tbl(r_plan, v_timeId);
--
        WHEN 'MOVE_SEQ' THEN
-- move a sequence from one group to another group
          PERFORM emaj._move_seq(r_plan, v_timeId);
--
        WHEN 'CHANGE_REL_PRIORITY' THEN
-- update the emaj_relation table to report the priority change
          UPDATE emaj.emaj_relation SET rel_priority = r_plan.altr_priority
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
--
        WHEN 'ADD_TBL' THEN
-- add a table to a group
          PERFORM emaj._add_tbl(r_plan, v_timeId, v_multiGroup);
--
        WHEN 'ADD_SEQ' THEN
-- add a sequence to a group
          PERFORM emaj._add_seq(r_plan, v_timeId, v_multiGroup);
--
      END CASE;
    END LOOP;
    RETURN;
  END;
$_alter_exec$;

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
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
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := 'IDLE') INTO v_groupNames;
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
-- check the supplied mark name (the check must be performed after the _reset_groups() call to allow to reuse an old mark name that is being deleted
      IF v_mark IS NULL OR v_mark = '' THEN
        v_mark = 'START_%';
      END IF;
      SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
-- OK, lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
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
            EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg';
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
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
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
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS' WHEN NOT v_multiGroup AND NOT v_isForced THEN 'STOP_GROUP' ELSE 'FORCE_STOP_GROUP' END,
              'BEGIN', array_to_string(v_groupNames,','));
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := '') INTO v_groupNames;
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
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the risk of deadlock.
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
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS' WHEN NOT v_multiGroup AND NOT v_isForced THEN 'STOP_GROUP' ELSE 'FORCE_STOP_GROUP' END,
              'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
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
      EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
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
      EXECUTE 'DELETE FROM ' || r_rel.log_table_name || ' WHERE emaj_gid <= ' || v_markGlobalSeq;
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

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
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
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true, v_isAlterGroupAllowed = null)
    RETURN rlbk_message::INT FROM emaj._rlbk_groups(v_groupNames, v_mark, TRUE, TRUE, NULL) WHERE rlbk_severity = 'Notice';
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark (deprecated).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
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
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(v_groupNames, v_mark, TRUE, TRUE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps (or NULL if there are some NULL input).
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_markName               TEXT;
    v_markTimeId             BIGINT;
    v_markTimestamp          TIMESTAMPTZ;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsable         BOOLEAN = FALSE;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_stmt                   TEXT;
    v_rlbkId                 INT;
  BEGIN
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames, v_mark, v_isAlterGroupAllowed, FALSE) INTO v_markName;
    IF v_markName IS NOT NULL THEN
-- check that no group is damaged
      PERFORM 0 FROM emaj._verify_groups(v_groupNames, TRUE);
-- get the time stamp id and its clock timestamp for the first group (as we know this time stamp is the same for all groups of the array)
      SELECT time_id, time_clock_timestamp INTO v_markTimeId, v_markTimestamp
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupNames[1] AND mark_name = v_markName;
-- insert begin in the history
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
                array_to_string(v_groupNames,','),
                CASE WHEN v_isLoggedRlbk THEN 'Logged' ELSE 'Unlogged' END || ' rollback to mark ' || v_markName || ' [' || v_markTimestamp || ']'
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
               v_markTimeId || ',' || v_isLoggedRlbk || ',' || quote_nullable(v_isAlterGroupAllowed) || ',' ||
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
    END IF;
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN, isRollbackSimulation BOOLEAN)
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
-- check the group names and states
    IF isRollbackSimulation THEN
      SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := 'LOGGING,ROLLBACKABLE') INTO v_groupNames;
    ELSE
      SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := 'LOGGING,ROLLBACKABLE,UNPROTECTED') INTO v_groupNames;
    END IF;
    IF v_groupNames IS NOT NULL THEN
-- check the mark name
      SELECT emaj._check_mark_name(v_groupNames := v_groupNames, v_mark := v_mark, v_checkList := 'ACTIVE') INTO v_markName;
      IF NOT isRollbackSimulation THEN
-- check that for each group that the rollback wouldn't delete protected marks (check disabled for rollback simulation)
        FOREACH v_aGroupName IN ARRAY v_groupNames LOOP
--   get the target mark time id
          SELECT mark_time_id INTO v_markTimeId FROM emaj.emaj_mark
            WHERE mark_group = v_aGroupName AND mark_name = v_markName;
--   and look at the protected mark
          SELECT string_agg(mark_name,', ' ORDER BY mark_name) INTO v_protectedMarksList FROM (
            SELECT mark_name FROM emaj.emaj_mark
              WHERE mark_group = v_aGroupName AND mark_time_id > v_markTimeId AND mark_is_rlbk_protected
              ORDER BY mark_time_id) AS t;
          IF v_protectedMarksList IS NOT NULL THEN
            RAISE EXCEPTION '_rlbk_check: Protected marks (%) for the group "%" block the rollback to the mark "%".', v_protectedMarksList, v_aGroupName, v_markName;
          END IF;
        END LOOP;
      END IF;
-- if the isAlterGroupAllowed flag is not explicitely set to true, check that the rollback would not cross any alter group operation for the groups
      IF v_isAlterGroupAllowed IS NULL OR NOT v_isAlterGroupAllowed THEN
        SELECT mark_time_id INTO v_markTimeId
          FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_markName;
        PERFORM 0 FROM emaj.emaj_alter_plan
          WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_rlbk_id IS NULL;
        IF FOUND THEN
          RAISE EXCEPTION '_rlbk_check: This rollback operation would cross some previously executed alter group operations, which is not allowed by the current function parameters.';
        END IF;
      END IF;
    END IF;
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(v_rlbkId INT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viwer role can write into rollback tables without having specific privileges to do it.
  DECLARE
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_markTimeId             BIGINT;
    v_nbSession              INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_effNbTable             INT;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- get the rollack characteristics for the emaj_rlbk event
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_ctrlStepName
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
if v_markTimeId is null then raise notice '§§§ attention pour mark % v_markTimeId NULL', v_mark; end if;
-- get all duration parameters that will be needed later from the emaj_param table,
--   or get default values for rows that are not present in emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_fkey_check_duration'),'5 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_step_rollback_duration'),'2.5 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::INTERVAL)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk;
-- insert into emaj_rlbk_plan a row per table currently belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object)
      SELECT v_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, ''
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_group = ANY(v_groupNames) AND rel_kind = 'r';
-- insert into emaj_rlbk_plan a row per table to effectively rollback.
-- the numbers of log rows is computed using the _log_stat_tbl() function.
-- a final check will be performed after tables will be locked to be sure no new table will have been updated
     INSERT INTO emaj.emaj_rlbk_plan
            (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_estimated_quantity)
      SELECT v_rlbkId, 'RLBK_TABLE', rel_schema, rel_tblseq, '', greatest(v_markTimeId, lower(rel_time_range)),
             emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL)
        FROM (SELECT * FROM emaj.emaj_relation
                WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'r') AS t
        WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0;
     GET DIAGNOSTICS v_effNbTable = ROW_COUNT;
--
-- group tables into batchs to process all tables linked by foreign keys as a batch
--
    v_batchNumber = 1;
--   allocate tables with rows to rollback to batch number starting with the heaviest to rollback tables
--     as reported by emaj_log_stat_group() function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE' ORDER BY rlbp_estimated_quantity DESC
      LOOP
--   is the table already allocated to a batch number (it may have been already allocated because of a fkey link) ?
      PERFORM 0 FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table AND rlbp_batch_number IS NULL;
--   no,
      IF FOUND THEN
--   allocate the table to the batch number, with all other tables linked by foreign key constraints
        PERFORM emaj._rlbk_set_batch_number(v_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table);
        v_batchNumber = v_batchNumber + 1;
      END IF;
    END LOOP;
--
-- if unlogged rollback, register into emaj_rlbk_plan "disable log triggers", "deletes from log tables"
-- and "enable log trigger" steps
--
    IF NOT v_isLoggedRlbk THEN
-- compute the cost for each DIS_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DIS_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all DIS_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'DIS_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- insert all DELETE_LOG steps. But the duration estimates will be computed later
-- the estimated number of log rows to delete is set to the the estimated number of updates. This is underestimated
--   in particular when SQL UPDATES are logged. But the collected statistics used for duration estimates are also
--   based on the estimated number of updates.
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
        ) SELECT v_rlbkId, 'DELETE_LOG', rlbp_schema, rlbp_table, '', rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- compute the cost for each ENA_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'ENA_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all ENA_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'ENA_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
    END IF;
--
-- process application triggers
--
-- compute the cost for each DIS_APP_TRG step
--   if DIS_APP_TRG statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'DIS_APP_TRG';
    v_estimMethod = 2;
    IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
      v_estimDuration = v_fixed_step_rlbk;
      v_estimMethod = 3;
    END IF;
-- insert all DIS_APP_TRG steps
    INSERT INTO emaj.emaj_rlbk_plan (
        rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
        rlbp_estimated_duration, rlbp_estimate_method
      ) SELECT v_rlbkId, 'DIS_APP_TRG', rlbp_schema, rlbp_table, tgname, rlbp_batch_number,
               v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan, pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_trigger
        WHERE nspname = rlbp_schema AND relname = rlbp_table AND relnamespace = pg_namespace.oid
          AND tgrelid = pg_class.oid
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND NOT tgisinternal AND NOT tgenabled = 'D'
          AND tgname NOT IN ('emaj_trunc_trg','emaj_log_trg')
          AND NOT EXISTS (SELECT trg_name FROM emaj.emaj_ignored_app_trigger
                            WHERE trg_schema = rlbp_schema AND trg_table = rlbp_table AND trg_name = tgname);
-- compute the cost for each ENA_APP_TRG step
--   if ENA_APP_TRG statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'ENA_APP_TRG';
    v_estimMethod = 2;
    IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
      v_estimDuration = v_fixed_step_rlbk;
      v_estimMethod = 3;
    END IF;
-- insert all ENA_APP_TRG steps
    INSERT INTO emaj.emaj_rlbk_plan (
        rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_object_def, rlbp_batch_number,
        rlbp_estimated_duration, rlbp_estimate_method
      ) SELECT v_rlbkId, 'ENA_APP_TRG', rlbp_schema, rlbp_table, rlbp_object,
               CASE WHEN tgenabled = 'A' THEN 'ALWAYS' WHEN tgenabled = 'R' THEN 'REPLICA' ELSE '' END,
               rlbp_batch_number, v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan, pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_trigger
        WHERE nspname = rlbp_schema AND relname = rlbp_table AND relnamespace = pg_namespace.oid
          AND tgrelid = pg_class.oid AND tgname = rlbp_object
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DIS_APP_TRG';
--
-- process foreign key to define which action to perform on them
--
-- First compute the fixed duration estimates for each 'DROP_FK' and 'SET_FK_DEF' steps
--   if DROP_FK statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDropFkDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'DROP_FK';
    v_estimDropFkMethod = 2;
    IF v_estimDropFkDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimDropFkDuration = v_fixed_step_rlbk;
      v_estimDropFkMethod = 3;
    END IF;
--   if SET_FK_DEF statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimSetFkDefDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'SET_FK_DEF';
    v_estimSetFkDefMethod = 2;
    IF v_estimSetFkDefDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimSetFkDefDuration = v_fixed_step_rlbk;
      v_estimSetFkDefMethod = 3;
    END IF;
-- select all foreign keys belonging to or referencing the tables to process
    FOR r_fk IN
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND n.nspname = r.rlbp_schema AND t.relname = r.rlbp_table     -- join on emaj_rlbk_plan table
      UNION
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace rn, pg_catalog.pg_class rt, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace
          AND rn.nspname = r.rlbp_schema AND rt.relname = r.rlbp_table   -- join on emaj_rlbk_plan table
      ORDER BY nspname, relname, conname
      LOOP
-- depending on the foreign key characteristics, record as 'to be dropped' or 'to be set deffered' or 'to just be reset immediate'
      IF NOT r_fk.condeferrable OR r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
-- non deferrable fkeys and deferrable fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
          ) VALUES (
          v_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
          v_estimDropFkDuration, v_estimDropFkMethod
          );
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_object_def, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, r_fk.def, r_fk.reltuples
          );
      ELSE
-- other deferrable but not deferred fkeys need to be set deferred
        IF NOT r_fk.condeferred THEN
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            v_rlbkId, 'SET_FK_DEF', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimSetFkDefDuration, v_estimSetFkDefMethod
            );
        END IF;
-- deferrable fkeys are recorded as 'to be set immediate at the end of the rollback operation'
-- compute the number of fkey values to check at set immediate time
        SELECT (coalesce(
--   get the number of rolled back rows in the referencing table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND rlbp_schema = r_fk.nspname AND rlbp_table = r_fk.relname)  -- referencing schema.table
            , 0)) + (coalesce(
--   get the number of rolled back rows in the referenced table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan, pg_catalog.pg_constraint c, pg_catalog.pg_namespace rn, pg_catalog.pg_class rt
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND c.oid = r_fk.conoid                                        -- constraint id
                AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced schema.table
                AND rn.nspname = rlbp_schema AND rt.relname = rlbp_table)      -- join on emaj_rlbk_plan
            , 0)) INTO v_checks;
-- and record the SET_FK_IMM step
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'SET_FK_IMM', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, v_checks
          );
      END IF;
    END LOOP;
--
-- Now compute the estimation duration for each complex step ('RLBK_TABLE', 'DELETE_LOG', 'ADD_FK', 'SET_FK_IMM')
--
-- Compute the rollback duration estimates for the tables
-- for each table with content to rollback
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_rlbk * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the log rows delete duration for the tables
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_del_log * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the fkey recreation duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
      LOOP
      IF r_fk.rlbp_estimated_quantity = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estimDuration = '0 SECONDS'::INTERVAL;
        v_estimMethod = 3;
      ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'ADD_FK' AND rlbt_quantity > 0
            AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_object = r_fk.rlbp_object;
        v_estimMethod = 1;
        IF v_estimDuration IS NULL THEN
-- non empty table, but no statistics with at least one row are available => take the last duration for this fkey, if any
          SELECT rlbt_duration INTO v_estimDuration FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = 'ADD_FK'
              AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_tbl.rlbp_table AND rlbt_object = r_fk.rlbp_object
              AND rlbt_rlbk_id =
               (SELECT max(rlbt_rlbk_id) FROM emaj.emaj_rlbk_stat WHERE rlbt_step = 'ADD_FK'
                  AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_object = r_fk.rlbp_object);
          v_estimMethod = 2;
          IF v_estimDuration IS NULL THEN
-- definitely no statistics available, compute with the avg_fkey_check_duration parameter
            v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
            v_estimMethod = 3;
          END IF;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_object = r_fk.rlbp_object;
    END LOOP;
-- Compute the fkey checks duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
      LOOP
-- if fkey checks statistics are available for this fkey, compute an average cost
      SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'SET_FK_IMM' AND rlbt_quantity > 0
          AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_object = r_fk.rlbp_object;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
-- if no statistics are available for this fkey, use the avg_fkey_check parameter
        v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_object = r_fk.rlbp_object;
    END LOOP;
--
-- Allocate batch number to sessions to spread the load on sessions as best as possible
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys
--
--   initialisation
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = '0 SECONDS'::INTERVAL;
    END LOOP;
--   allocate tables batch to sessions, starting with the heaviest to rollback batch
    FOR r_batch IN
        SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number IS NOT NULL
          GROUP BY rlbp_batch_number
          ORDER BY sum(rlbp_estimated_duration) DESC
      LOOP
--   compute the least loaded session
      v_minSession=1; v_minDuration = v_sessionLoad [1];
      FOR v_session IN 2 .. v_nbSession LOOP
        IF v_sessionLoad [v_session] < v_minDuration THEN
          v_minSession = v_session;
          v_minDuration = v_sessionLoad [v_session];
        END IF;
      END LOOP;
--   allocate the batch to the session
      UPDATE emaj.emaj_rlbk_plan SET rlbp_session = v_minSession
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number = r_batch.rlbp_batch_number;
      v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + r_batch.batch_duration;
    END LOOP;
-- assign session 1 to all 'LOCK_TABLE' steps not yet affected
    UPDATE emaj.emaj_rlbk_plan SET rlbp_session = 1
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_session IS NULL;
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate
-- get the number of recorded steps (except LOCK_TABLE)
    SELECT count(*) INTO v_nbStep FROM emaj.emaj_rlbk_plan
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step <> 'LOCK_TABLE';
    IF v_nbStep > 0 THEN
-- if CTRLxDBLINK statistics are available, compute an average cost
      SELECT sum(rlbt_duration) * v_nbStep / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat WHERE rlbt_step = v_ctrlStepName AND rlbt_quantity > 0;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_dblink_rlbk * v_nbStep;
        v_estimMethod = 3;
      END IF;
-- insert the 'CTRLxDBLINK' pseudo step
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_estimated_quantity,
          rlbp_estimated_duration, rlbp_estimate_method
        ) VALUES (
          v_rlbkId, v_ctrlStepName, '', '', '', v_nbStep, v_estimDuration, v_estimMethod
        );
    END IF;
-- return the number of tables to effectively rollback
    RETURN v_effNbTable;
  END;
$_rlbk_planning$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS
----SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it doesn't own the application tables.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = FALSE;
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
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
      v_isDblinkUsable = TRUE;
    END IF;
-- get the rollback characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_nb_session, time_last_emaj_gid
      INTO v_groupNames, v_mark, v_rlbkTimeId, v_isLoggedRlbk, v_nbSession, v_maxGlobalSeq
      FROM emaj.emaj_rlbk, emaj.emaj_time_stamp WHERE rlbk_id = v_rlbkId AND rlbk_time_id = time_id;
-- fetch the mark_time_id, the last global sequence at set_mark time for the first group of the groups array (they all share the same values)
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
      IF v_isDblinkUsable THEN
-- ... either through dblink if possible
        PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      ELSE
-- ... or directly
        EXECUTE v_stmt;
      END IF;
-- process the step depending on its type
      CASE r_step.rlbp_step
        WHEN 'DIS_APP_TRG' THEN
-- process an application trigger disable
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' DISABLE TRIGGER ' || quote_ident(r_step.rlbp_object);
        WHEN 'DIS_LOG_TRG' THEN
-- process a log trigger disable
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' DISABLE TRIGGER emaj_log_trg';
        WHEN 'DROP_FK' THEN
-- process a foreign key deletion
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' DROP CONSTRAINT ' || quote_ident(r_step.rlbp_object);
        WHEN 'SET_FK_DEF' THEN
-- set a foreign key deferred
          EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_object) ||
                  ' DEFERRED';
        WHEN 'RLBK_TABLE' THEN
-- process a table rollback
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._rlbk_tbl(emaj_relation.*,
                                CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                     ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id) END,
                                v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'DELETE_LOG' THEN
-- process the deletion of log rows
--  for tables added to the group after the rollback target mark, get the last sequence value specific to each table
          SELECT emaj._delete_log_tbl(emaj_relation.*, r_step.rlbp_target_time_id, v_rlbkTimeId,
                                      CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                           ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id) END)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table AND upper_inf(rel_time_range);
        WHEN 'SET_FK_IMM' THEN
-- set a foreign key immediate
          EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_object) ||
                  ' IMMEDIATE';
        WHEN 'ADD_FK' THEN
-- process a foreign key creation
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' ADD CONSTRAINT ' || quote_ident(r_step.rlbp_object) || ' ' || r_step.rlbp_object_def;
        WHEN 'ENA_APP_TRG' THEN
-- process an application trigger enable
          EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                  ' ENABLE ' || r_step.rlbp_object_def || 'TRIGGER ' || quote_ident(r_step.rlbp_object);
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
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
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

CREATE OR REPLACE FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
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
    v_isDblinkUsable         BOOLEAN = FALSE;
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
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = TRUE;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl, v_rlbkDatetime
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
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
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
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 6 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''DIS_APP_TRG'',''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_APP_TRG'',''ENA_LOG_TRG'') ' ||
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
-- if the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time
    PERFORM emaj._rlbk_seq(t.*, greatest(v_markTimeId, lower(t.rel_time_range)))
      FROM (SELECT * FROM emaj.emaj_relation
              WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automatically set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbkDatetime, 'HH24.MI.SS.MS') || '_DONE';
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
        VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'NOTICE', 'Rollback id ' || v_rlbkId, rlbk_message);
      v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
      IF v_isAlterGroupAllowed IS NOT NULL THEN
        RETURN NEXT;
      END IF;
    END IF;
-- then, for new style calling functions, return the WARNING messages for any elementary action from alter group operations that has not been rolled back
    IF v_isAlterGroupAllowed IS NOT NULL THEN
      rlbk_severity = 'Warning';
      FOR r_msg IN
-- steps are splitted into 2 groups to filter them differently
        SELECT altr_time_id, altr_step, altr_schema, altr_tblseq,
               (CASE altr_step
                  WHEN 'ADD_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment state (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'ADD_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_SEQ' THEN
                    'The sequence ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment state (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_TBL' THEN
                    'The table ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq) ||
                    ' has only been rolled back to its latest group attachment (' || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
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
                    'Tables group change not rolled back: E-Maj priority for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_SCHEMA' THEN
                    'Tables group change not rolled back: E-Maj log schema for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_NAMES_PREFIX' THEN
                    'Tables group change not rolled back: E-Maj names prefix for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_DATA_TSP' THEN
                    'Tables group change not rolled back: log data tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
                  WHEN 'CHANGE_TBL_LOG_INDEX_TSP' THEN
                    'Tables group change not rolled back: log index tablespace for ' || quote_ident(altr_schema) || '.' || quote_ident(altr_tblseq)
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
            VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'WARNING', 'Rollback id ' || v_rlbkId, r_msg.message);
          rlbk_message = r_msg.message;
          v_messages = concat(v_messages, ',', quote_literal(rlbk_severity || ': ' || rlbk_message));
          RETURN NEXT;
      END LOOP;
    END IF;
-- update the alter steps that have been covered by the rollback
    UPDATE emaj.emaj_alter_plan SET altr_rlbk_id = v_rlbkId
      WHERE altr_time_id > v_markTimeId AND altr_group = ANY (v_groupNames) AND altr_rlbk_id IS NULL;
-- update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the result message
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMPLETED'', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_messages || ']' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
--     and then close the connection
      PERFORM emaj._dblink_close_cnx('rlbk#1');
    ELSE
-- ... or directly (the status can be directly set to committed, the update being in the same transaction)
      EXECUTE 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMMITTED'', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_messages || ']' ||
               ' WHERE rlbk_id = ' || v_rlbkId;
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

CREATE OR REPLACE FUNCTION emaj._cleanup_rollback_state()
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_cleanup_rollback_state$
-- This function effectively cleans the rollback states up. It is called by the emaj_cleanup_rollback_state()
-- and by other emaj functions.
-- The rollbacks whose transaction(s) is/are active are left as is.
-- Among the others, those which are also visible in the emaj_hist table are set "COMMITTED",
--   while those which are not visible in the emaj_hist table are set "ABORTED".
-- Input: no parameter
-- Output: number of updated rollback events
  DECLARE
    v_nbRlbk                 INT = 0;
    v_newStatus              emaj._rlbk_status_enum;
    r_rlbk                   RECORD;
  BEGIN
-- scan all pending rollback events having all their session transactions completed (either committed or rolled back)
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session, count(rlbs_txid) AS nbVisibleTx
        FROM emaj.emaj_rlbk
             LEFT OUTER JOIN emaj.emaj_rlbk_session ON
               (    rlbk_id = rlbs_rlbk_id                                      -- main join condition
                AND txid_visible_in_snapshot(rlbs_txid,txid_current_snapshot()) -- only visible tx
                AND rlbs_txid <> txid_current()                                 -- exclude the current tx
               )
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED')  -- only pending rollback events
        GROUP BY rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session
        HAVING count(rlbs_txid) = rlbk_nb_session                               -- all sessions tx must be visible
        ORDER BY rlbk_id
      LOOP
-- look at the emaj_hist to find the trace of the rollback begin event
      PERFORM 0 FROM emaj.emaj_hist WHERE hist_id = r_rlbk.rlbk_begin_hist_id;
      IF FOUND THEN
-- if the emaj_hist rollback_begin event is visible, the rollback transaction has been committed.
-- then set the rollback event in emaj_rlbk as "COMMITTED"
        v_newStatus = 'COMMITTED';
      ELSE
-- otherwise, set the rollback event in emaj_rlbk as "ABORTED"
        v_newStatus = 'ABORTED';
      END IF;
      UPDATE emaj.emaj_rlbk SET rlbk_status = v_newStatus WHERE rlbk_id = r_rlbk.rlbk_id;
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('CLEANUP_RLBK_STATE', 'Rollback id ' || r_rlbk.rlbk_id, 'set to ' || v_newStatus);
      v_nbRlbk = v_nbRlbk + 1;
    END LOOP;
    RETURN v_nbRlbk;
  END;
$_cleanup_rollback_state$;

CREATE OR REPLACE FUNCTION emaj.emaj_consolidate_rollback_group(v_groupName TEXT, v_endRlbkMark TEXT)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := '');
-- check the supplied end rollback mark name
    SELECT emaj._check_mark_name(v_groupNames := ARRAY[v_groupName], v_mark := v_endRlbkMark, v_checkList := '') INTO v_lastMark;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(ARRAY[v_groupName], TRUE);
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
    SELECT * FROM emaj._delete_between_marks_group(v_groupName, v_firstMark, v_lastMark) INTO v_nbMark, v_nbTbl;
-- get the number of sequences belonging to the group
    SELECT group_nb_sequence INTO v_nbSeq FROM emaj.emaj_group WHERE group_name = v_groupName;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'END', v_groupName, v_nbTbl || ' tables and ' || v_nbSeq || ' sequences processed ; ' || v_nbMark || ' marks deleted');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_consolidate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_consolidate_rollback_group(TEXT,TEXT) IS
$$Consolidate a rollback for a group.$$;

CREATE OR REPLACE FUNCTION emaj._delete_between_marks_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, OUT v_nbMark INT, OUT v_nbTbl INT)
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
        EXECUTE 'DELETE FROM ' || r_rel.log_table_name || ' WHERE emaj_gid > ' || v_firstMarkGlobalSeq || ' AND emaj_gid <= ' || v_lastMarkGlobalSeq;
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

CREATE OR REPLACE FUNCTION emaj.emaj_reset_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves
-- It calls the emaj_rst_group function to do the job
-- Input: group name
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_nbTb                   INT = 0;
    v_eventTriggers          TEXT[];
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('RESET_GROUP', 'BEGIN', v_groupName);
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := 'IDLE');
-- perform the reset operation
    SELECT emaj._reset_groups(ARRAY[v_groupName]) INTO v_nbTb;
-- drop the log schemas that would have been emptied by the _reset_groups() call
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
    PERFORM emaj._drop_log_schemas('RESET_GROUP', FALSE);
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RESET_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._reset_groups(v_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_reset_groups$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences images
-- It is called by emaj_reset_group(), emaj_start_group() and emaj_alter_group() functions
-- Input: group names array
-- Output: number of processed tables and sequences
-- There is no check of the groups state (this is done by callers)
-- The function is defined as SECURITY DEFINER so that an emaj_adm role can truncate log tables
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
      EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table) || ' CASCADE';
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
      EXECUTE 'TRUNCATE ' || quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
    END LOOP;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN sum(group_nb_table)+sum(group_nb_sequence) FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames);
  END;
$_reset_groups$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: group names array, a boolean indicating whether the groups array may contain several groups,
--        the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
    v_nbTblseq               INT;
  BEGIN
-- check the group names (the groups state checks are delayed for later)
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '') INTO v_groupNames;
-- if the group names array is null, immediately return NULL
    IF v_groupNames IS NULL THEN
      RETURN NULL;
    END IF;
-- check supplied group names and mark parameters with the isAlterGroupAllowed and isRollbackSimulation flags set to true
    SELECT emaj._rlbk_check(v_groupNames, v_mark, TRUE, TRUE) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::INT INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session)
        SELECT v_rlbkId, v_groupNames, v_markName, mark_time_id, v_isLoggedRlbk, FALSE, 1
          FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_markName;
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
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::INTERVAL)
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
-- check the group name
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_groupName], v_mayBeNull := FALSE, v_lockGroups := FALSE, v_checkList := '');
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
          WHERE upper_inf(rel_time_range) AND rel_group = v_groupName
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName = v_dir || '/' || translate(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap', E' /\\$<>*', '_______');
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
                  AND attnum > 0 AND attisdropped = FALSE) AS t;
          ELSE
--   the table has no pkey
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList FROM (
              SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = FALSE) AS t;
          END IF;
--   prepare the COPY statement
          v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO ' ||
                  quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
        WHEN 'S' THEN
-- if it is a sequence, the statement has no order by
          IF emaj._pg_version_num() >= 100000 THEN
            v_stmt= 'COPY (SELECT sequencename, rel.last_value, start_value, increment_by, max_value, ' ||
                    'min_value, cache_size, cycle, rel.is_called ' ||
                    'FROM ' || v_fullTableName || ' rel, pg_sequences ' ||
                    'WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_tblseq) ||
                    ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
          ELSE
            v_stmt= 'COPY (SELECT sequence_name, last_value, start_value, increment_by, max_value, ' ||
                    'min_value, cache_value, is_cycled, is_called FROM ' || v_fullTableName ||
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
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark
-- For log tables, files contain all rows related to the time frame, sorted on emaj_gid.
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no end mark is specified.
--   They contain one row per sequence belonging to the group at the related time
--   (a sequence may belong to a group at the start mark time and not at the end mark time for instance).
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
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || to_char(v_lastMarkTs,'HH24.MI.SS.MS'), E' /\\$<>*', '_______');
      v_stmt = 'SELECT seq.* FROM emaj.emaj_relation, LATERAL emaj._get_current_sequence_state(rel_schema, rel_tblseq, ' ||
                                                                                               v_lastMarkTimeId || ') AS seq' ||
               '  WHERE upper_inf(rel_time_range) AND rel_group = ' || quote_literal(v_groupName) || ' AND rel_kind = ''S''';
    ELSE
      v_fileName = v_dir || '/' || translate(v_groupName || '_sequences_at_' || v_lastMark, E' /\\$<>*', '_______');
      v_stmt = 'SELECT emaj_sequence.*' ||
               ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
               ' WHERE sequ_time_id = ' || v_lastMarkTimeId ||
               '   AND rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) ||
               '   AND (rel_time_range @> ' || v_lastMarkTimeId || '::BIGINT OR upper(rel_time_range) = ' || v_lastMarkTimeId || '::BIGINT)'
               '   AND sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
               ' ORDER BY sequ_schema, sequ_name';
    END IF;
-- and create the file
    EXECUTE 'COPY (' || v_stmt || ') TO ' || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj log tables snap of group ' || v_groupName ||
            ' between marks ' || v_firstMark || ' and ' ||
            CASE WHEN v_noSuppliedLastMark THEN 'current state' ELSE v_lastMark END || ' at ' || statement_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO') || ' ' || coalesce (v_copyOptions, '');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbFile || ' generated files');
    RETURN v_nbFile;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET standard_conforming_strings = ON SET search_path = pg_catalog, pg_temp AS
$emaj_gen_sql_group$
-- This function generates a SQL script representing all updates performed on a tables group between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables group
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- call the _gen_sql_groups() function that effectively processes the request
    RETURN emaj._gen_sql_groups(array[v_groupName], FALSE, v_firstMark, v_lastMark, v_location, v_tblseqs);
  END;
$emaj_gen_sql_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_group(TEXT,TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a tables group between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET standard_conforming_strings = ON SET search_path = pg_catalog, pg_temp AS
$emaj_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a set of tables groups between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables groups array
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- call the _gen_sql_groups() function that effectively processes the request
    RETURN emaj._gen_sql_groups(v_groupNames, TRUE, v_firstMark, v_lastMark, v_location, v_tblseqs);
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(TEXT[],TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a set of tables groups between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
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
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '') INTO v_groupNames;
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
            WHERE rel_time_range @> v_firstMarkTimeId AND rel_group = ANY (v_groupNames)    -- tables/sequences that belong to their group at the start mark time
          ) AS t2;
        IF v_count > 0 THEN
          IF v_count = 1 THEN
            RAISE EXCEPTION '_gen_sql_groups: 1 table/sequence (%) did not belong to any of the selected tables groups at % mark time.', v_tblseqErr, v_firstMark;
          ELSE
            RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) did not belong to any of the selected tables groups at % mark time.', v_count, v_tblseqErr, v_firstMark;
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
        EXECUTE 'COPY (SELECT ''-- _gen_sql_groups() function in progress - started at '
                       || statement_timestamp() || ''') TO ' || quote_literal(v_location);
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
              AND rel_time_range @> v_firstMarkTimeId                                              -- sequences belonging to the groups at the start mark
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))         -- filtered or not by the user
            ORDER BY rel_priority DESC, rel_schema DESC, rel_tblseq DESC
          LOOP
        v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- no supplied last mark and the sequence currently belongs to its group, so get current sequence characteritics
          IF emaj._pg_version_num() >= 100000 THEN
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                    || ''' || '' RESTART '' || CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_size || CASE WHEN NOT cycle THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                    || 'FROM ' || v_fullSeqName  || ' rel, pg_catalog.pg_sequences ' ||
                    ' WHERE schemaname = ' || quote_literal(r_tblsq.rel_schema) || ' AND sequencename = ' || quote_literal(r_tblsq.rel_tblseq)
                    INTO v_rqSeq;
          ELSE
            EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                    || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                    || 'FROM ' || v_fullSeqName INTO v_rqSeq;
          END IF;
        ELSE
-- a last mark is supplied, or the sequence does not belong to its groupe anymore, so get sequence characteristics from the emaj_sequence table
          v_endTimeId = CASE WHEN upper_inf(r_tblsq.rel_time_range) OR v_lastMarkTimeId < upper(r_tblsq.rel_time_range) THEN v_lastMarkTimeId
                             ELSE upper(r_tblsq.rel_time_range) END;
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
                 || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
                 || 'FROM emaj.emaj_sequence '
                 || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
                 || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
                 || '  AND sequ_time_id = ' || v_endTimeId INTO v_rqSeq;
        END IF;
-- insert into temp table
        v_nbSeq = v_nbSeq + 1;
        EXECUTE 'INSERT INTO emaj_temp_script '
             || 'SELECT NULL, -1 * ' || v_nbSeq || ', txid_current(), ' || quote_literal(v_rqSeq);
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
      EXECUTE 'COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid ) TO '
            || quote_literal(v_location);
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

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in the emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_errorFound             BOOLEAN = FALSE;
    v_nbMissingEventTrigger  INT;
    v_nbGroupNeedAlter       INT;
    r_object                 RECORD;
  BEGIN
-- Global checks
-- detect if the current postgres version is at least 9.5
    IF emaj._pg_version_num() < 90500 THEN
      RETURN NEXT 'The current postgres version (' || version() || ') is not compatible with this E-Maj version. It should be at least 9.5.';
      v_errorFound = TRUE;
    END IF;
-- report a warning if some E-Maj event triggers are missing
    SELECT 3 - count(*)
      INTO v_nbMissingEventTrigger FROM pg_catalog.pg_event_trigger
      WHERE evtname IN ('emaj_protection_trg','emaj_sql_drop_trg','emaj_table_rewrite_trg');
    IF v_nbMissingEventTrigger > 0 THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers are missing. Your database administrator may (re)create them using the emaj_upgrade_after_postgres_upgrade.sql script.';
    END IF;
-- report a warning if some E-Maj event triggers exist but are not enabled
    PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
    IF FOUND THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers exist but are disabled. You may enable them using the emaj_enable_protection_by_event_triggers() function.';
    END IF;
-- check all E-Maj schemas
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
-- check the emaj_ignored_app_trigger table content
    FOR r_object IN
      SELECT 'No trigger "' || trg_name || '" found for table "' || trg_schema || '"."' || trg_table || '". Use the emaj_ignore_app_trigger() function to adjust the list of application triggers that should not be automatically disabled at rollback time.' AS msg
        FROM (
          SELECT trg_schema, trg_table, trg_name FROM emaj.emaj_ignored_app_trigger
            EXCEPT
          SELECT nspname, relname, tgname
            FROM pg_catalog.pg_namespace, pg_catalog.pg_class, pg_catalog.pg_trigger
            WHERE relnamespace = pg_namespace.oid AND tgrelid = pg_class.oid
        ) AS t
    LOOP
      RETURN NEXT r_object.msg;
      v_errorFound = TRUE;
    END LOOP;

-- final message if no error has been yet detected
    IF NOT v_errorFound THEN
      RETURN NEXT 'No error detected';
    END IF;
-- check the value of the group_has_waiting_changes column of the emaj_group table, and reset it at the right value if needed
    PERFORM emaj._adjust_group_properties();
-- if tables groups are out of sync with emaj_group_def, report it
    SELECT count(*) INTO v_nbGroupNeedAlter FROM emaj.emaj_group
      WHERE group_has_waiting_changes;
    IF v_nbGroupNeedAlter > 0 THEN
      RETURN NEXT v_nbGroupNeedAlter || ' tables groups need to be altered to match their configuration definition or to be repaired';
    END IF;
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

CREATE OR REPLACE FUNCTION emaj._adjust_group_properties()
RETURNS INTEGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_adjust_group_properties$
-- The function adjusts the content of the emaj_group table.
-- It actualy only adjusts the group_has_waiting_changes column.
-- This group_has_waiting_changes column is normally set by a trigger on emaj_group_def.
-- But in some cases, its value may not correspond to the real situation. This function sets its value to the proper value.
-- It mainly joins the content of the emaj_group_def and the emaj_relation table to detect differences.
-- It also calls the _verify_groups() function to detect potential corrupted groups that would need to be altered.
-- If needed, the emaj_group table is updated.
-- The function is declared SECURITY DEFINER so that emaj_viewer roles can execute it when calling the emaj_verify_all() function
-- It returns the number of groups that have been updated.
  DECLARE
    v_nbAdjustedGroups       INT = 0;
  BEGIN
-- process the group_has_waiting_changes column using one big SQL statement
    WITH
      tblseq_with_changes AS (
        -- tables and sequences modified or deleted from emaj_group_def
        SELECT rel_group, rel_schema, rel_tblseq
          FROM emaj.emaj_relation
               LEFT OUTER JOIN emaj.emaj_group_def ON (rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND rel_group = grpdef_group)
          WHERE upper_inf(rel_time_range)
            AND (
              -- the relations that do not belong to the groups anymore
                  grpdef_group IS NULL
              -- the tables whose log data tablespace in emaj_group_def has changed
              --         or whose log index tablespace in emaj_group_def has changed
               OR (rel_kind = 'r'
                  AND (coalesce(rel_log_dat_tsp,'') <> coalesce(grpdef_log_dat_tsp,'')
                    OR coalesce(rel_log_idx_tsp,'') <> coalesce(grpdef_log_idx_tsp,'')
                      ))
              -- the tables or sequences that change their group ownership
               OR (rel_group <> grpdef_group)
              -- the relation that change their priority level
               OR (rel_priority IS NULL AND grpdef_priority IS NOT NULL) OR
                  (rel_priority IS NOT NULL AND grpdef_priority IS NULL) OR
                  (rel_priority <> grpdef_priority)
                )
      UNION
        -- new tables or sequences in emaj_group_def
        SELECT grpdef_group, grpdef_schema, grpdef_tblseq
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
          WHERE NOT EXISTS (
                SELECT NULL FROM emaj.emaj_relation
                  WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq AND upper_inf(rel_time_range))
            AND relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq
            AND group_name = grpdef_group
      UNION
        -- dammaged tables
        SELECT ver_group, ver_schema, ver_tblseq
          FROM emaj._verify_groups(
                 (SELECT array_agg(group_name) FROM emaj.emaj_group)
                 , false)
          WHERE ver_group IS NOT NULL
      ),
      -- get the list of groups that would need to be altered
      group_with_changes AS (
        SELECT DISTINCT rel_group as group_name FROM tblseq_with_changes
      ),
      -- adjust the group_has_waiting_changes column, only when needed
      modified_group AS (
        UPDATE emaj.emaj_group SET group_has_waiting_changes = NOT group_has_waiting_changes
          WHERE group_has_waiting_changes = FALSE AND group_name IN (SELECT group_name FROM group_with_changes)
             OR group_has_waiting_changes = TRUE AND group_name NOT IN (SELECT group_name FROM group_with_changes)
          RETURNING group_name, group_has_waiting_changes
      ),
      -- insert a row in the history for each flag change
      hist_insert AS (
        INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
          SELECT 'ADJUST_GROUP_PROPERTIES', group_name, 'Set the group_has_waiting_changes column to ' || group_has_waiting_changes
            FROM modified_group
      )
      SELECT count(*) INTO v_nbAdjustedGroups FROM modified_group;
    RETURN v_nbAdjustedGroups;
  END;
$_adjust_group_properties$;

CREATE OR REPLACE FUNCTION emaj._event_trigger_sql_drop_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_sql_drop_fnct$
-- This function is called by the emaj_sql_drop_trg event trigger
-- The function blocks any ddl operation that leads to a drop of
--   - an application table or a sequence registered into an active (not stopped) E-Maj group, or a schema containing such tables/sequence
--   - an E-Maj schema, a log table, a log sequence, a log function or a log trigger
-- The drop of emaj schema or extension is managed by another event trigger
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when dropping their objects
  DECLARE
    v_groupName              TEXT;
    r_dropped                RECORD;
  BEGIN
-- scan all dropped objects
    FOR r_dropped IN
      SELECT object_type, schema_name, object_name, object_identity, original FROM pg_event_trigger_dropped_objects()
    LOOP
      CASE
        WHEN r_dropped.object_type = 'schema' THEN
-- the object is a schema
--   look at the emaj_relation table to verify that the schema being dropped does not belong to any active (not stopped) group
          SELECT string_agg(DISTINCT rel_group, ', ' ORDER BY rel_group) INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.object_name AND upper_inf(rel_time_range)
              AND group_name = rel_group AND group_is_logging;
          IF v_groupName IS NOT NULL THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application schema "%". But it belongs to the active tables groups "%".', r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_schema table to verify that the schema being dropped is not an E-Maj schema containing log tables
          PERFORM 1 FROM emaj.emaj_schema
            WHERE sch_name = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "%". But dropping an E-Maj schema is not allowed.', r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'table' THEN
-- the object is a table
--   look at the emaj_relation table to verify that the table being dropped does not currently belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name AND upper_inf(rel_time_range)
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
--   look at the emaj_relation table to verify that the sequence being dropped does not currently belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name AND upper_inf(rel_time_range)
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
--   look at the trigger name pattern to identify emaj trigger
--   and do not raise an exception if the triggers drop is derived from a drop of a table or a function
          IF r_dropped.original AND
             (r_dropped.object_identity LIKE 'emaj_log_trg%' OR r_dropped.object_identity LIKE 'emaj_trunc_trg%') THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the "%" E-Maj trigger. But dropping an E-Maj trigger is not allowed.', r_dropped.object_identity;
          END IF;
        ELSE
          CONTINUE;
      END CASE;
    END LOOP;
  END;
$_event_trigger_sql_drop_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_sql_drop_fnct() IS
$$E-Maj extension: support of the emaj_sql_drop_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj._event_trigger_table_rewrite_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_table_rewrite_fnct$
-- This function is called by the emaj_table_rewrite_trg event trigger
-- The function blocks any ddl operation that leads to a table rewrite for:
--   - an application table registered into an active (not stopped) E-Maj group
--   - an E-Maj log table
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when altering their tables
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
$_event_trigger_table_rewrite_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_table_rewrite_fnct() IS
$$E-Maj extension: support of the emaj_table_rewrite_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_disable_protection_by_event_triggers()
RETURNS INT LANGUAGE plpgsql AS
$emaj_disable_protection_by_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively disabled event triggers
  DECLARE
    v_eventTriggers          TEXT[];
  BEGIN
-- call the _disable_event_triggers() function and get the disabled event trigger names array
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- insert a row into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('DISABLE_PROTECTION', 'EVENT TRIGGERS DISABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- return the number of disabled event triggers
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
-- Output: number of effectively enabled event triggers
  DECLARE
    v_eventTriggers          TEXT[];
  BEGIN
-- build the event trigger names array from the pg_event_trigger table
    SELECT coalesce(array_agg(evtname  ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- call the _enable_event_triggers() function
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- insert a row into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS ENABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- return the number of disabled event triggers
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
-- Output: array of effectively disabled event trigger names. It can be reused as input when calling _enable_event_triggers()
  DECLARE
    v_eventTrigger           TEXT;
    v_eventTriggers          TEXT[] = ARRAY[]::TEXT[];
  BEGIN
-- build the event trigger names array from the pg_event_trigger table
-- (pg_event_trigger table doesn't exists in 9.2- postgres versions)
-- A single operation like emaj_alter_groups() may call the function several times. But this is not an issue as only enabled triggers are disabled.
    SELECT coalesce(array_agg(evtname ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled <> 'D';
-- disable each event trigger
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE 'ALTER EVENT TRIGGER ' || v_eventTrigger || ' DISABLE';
    END LOOP;
    RETURN v_eventTriggers;
  END;
$_disable_event_triggers$;

CREATE OR REPLACE FUNCTION emaj._enable_event_triggers(v_eventTriggers TEXT[])
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_enable_event_triggers$
-- This function enables all event triggers supplied as parameter
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_enable_event_triggers_protection() function.
-- Input: array of event trigger names to enable
-- Output: same array
  DECLARE
    v_eventTrigger           TEXT;
  BEGIN
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE 'ALTER EVENT TRIGGER ' || v_eventTrigger || ' ENABLE';
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
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 3.0.0 completed');

-- post installation checks
DO
$tmp$
  DECLARE
  BEGIN
-- check the max_prepared_transactions GUC value
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback is possible.', current_setting('max_prepared_transactions');
    END IF;
-- warning about the new automatic application triggers disabling at rollbacks
      RAISE WARNING 'E-Maj upgrade: with this new E-Maj version, application triggers are by default automatically disabled during E-Maj rollback operations. This may require some changes in your rollback procedures. Refer to the E-Maj documentation for additional information.';
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
