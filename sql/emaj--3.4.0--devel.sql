--
-- E-Maj: migration from 3.4.0 to <devel>
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
-- the emaj version registered in emaj_param must be '3.4.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '3.4.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 3.4.0',v_emajVersion;
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 3.4.0 started');

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------

-- drop the triggers on the emaj_group_def table
DROP TRIGGER emaj_group_def_change_trg ON emaj.emaj_group_def;
DROP TRIGGER emaj_group_def_truncate_trg ON emaj.emaj_group_def;

-- drop the group_has_waiting_changes column of the emaj_group table
ALTER TABLE emaj.emaj_group DROP COLUMN group_has_waiting_changes;

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
DROP FUNCTION IF EXISTS emaj._emaj_group_def_change_fnct();
DROP FUNCTION IF EXISTS emaj.emaj_alter_group(V_GROUPNAME TEXT,V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_alter_groups(V_GROUPNAMES TEXT[],V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_sync_def_group(V_GROUP TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_rollback_group(V_GROUPNAME TEXT,V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_rollback_groups(V_GROUPNAMES TEXT[],V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_logged_rollback_group(V_GROUPNAME TEXT,V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_logged_rollback_groups(V_GROUPNAMES TEXT[],V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._adjust_group_properties();

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN DEFAULT TRUE,
                                                  v_is_empty BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates emaj objects for all tables of a group.
-- It also creates the log E-Maj schemas when needed.
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
--        boolean explicitely indicating whether the group is empty or not
-- Output: number of processed tables and sequences
  DECLARE
    v_timeId                 BIGINT;
    v_nbTbl                  INT = 0;
    v_nbSeq                  INT = 0;
    r                        RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName, CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- check that the group name is valid
    IF v_groupName IS NULL OR v_groupName = '' THEN
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
       RAISE EXCEPTION 'emaj_create_group: The group "%" is unknown in the emaj_group_def table. To create an empty group,'
                       ' explicitely set the third parameter to true.', v_groupName;
    END IF;
    IF v_is_empty AND FOUND THEN
       RAISE WARNING 'emaj_create_group: Although the group "%" is referenced into the emaj_group_def table, it is left empty.',
                     v_groupName;
    END IF;
-- performs various checks on the group's content described in the emaj_group_def table
    IF NOT v_is_empty THEN
      FOR r IN
        SELECT rpt_message FROM emaj._check_conf_groups(ARRAY[v_groupName])
          WHERE (v_isRollbackable AND rpt_severity <= 2)
             OR (NOT v_isRollbackable AND rpt_severity <= 1)
          ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3
      LOOP
        RAISE WARNING 'emaj_create_group: error, %', r.rpt_message;
      END LOOP;
      IF FOUND THEN
        RAISE EXCEPTION 'emaj_create_group: One or several errors have been detected in the emaj_group_def table content.';
      END IF;
    END IF;
-- OK
-- get the time stamp of the operation
    SELECT emaj._set_time_stamp('C') INTO v_timeId;
-- insert the row describing the group into the emaj_group table
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable)
    INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_creation_time_id,
                                 group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence)
      VALUES (v_groupName, v_isRollbackable, v_timeId, FALSE, NOT v_isRollbackable, 0, 0);
-- populate the group
    IF NOT v_is_empty THEN
-- create new E-Maj log schemas, if needed
      PERFORM emaj._create_log_schemas('CREATE_GROUP', ARRAY[v_groupName]);
-- get and process all tables of the group (in priority order, NULLS being processed last)
      PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp,
                               v_timeId, v_isRollbackable, FALSE)
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
-- get and process all sequences of the group (in alphabetical order)
      PERFORM emaj._create_seq(grpdef_schema, grpdef_tblseq, grpdef_group, v_timeId)
        FROM (
          SELECT grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority
            FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE grpdef_group = v_groupName
              AND relnamespace = pg_namespace.oid
              AND nspname = grpdef_schema AND relname = grpdef_tblseq
              AND relkind = 'S'
            ORDER BY grpdef_schema, grpdef_tblseq
             ) AS t;
      SELECT count(*) INTO v_nbSeq
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'S' AND upper_inf(rel_time_range);
-- update tables and sequences counters in the emaj_group table
      UPDATE emaj.emaj_group SET group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
        WHERE group_name = v_groupName;
-- check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(array[v_groupName]);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'END', v_groupName, v_nbTbl + v_nbSeq || ' tables/sequences processed');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN,BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._alter_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT, v_callingFunction TEXT,
                                              INOUT v_timeId BIGINT, OUT v_nbRel INT)
RETURNS RECORD LANGUAGE plpgsql AS
$_alter_groups$
-- This function effectively alters a tables groups array.
-- It takes into account the changes recorded in the emaj_group_def table since the groups have been created.
-- Input: group names array,
--        flag indicating whether the function is called by the multi-group function or not
--        a mark name to set on groups in logging state
--        the calling function
--        an optional timestamp id (used for groups configuration import)
-- Output: number of tables and sequences belonging to the groups after the operation
  DECLARE
    v_loggingGroups          TEXT[];
    v_markName               TEXT;
    v_eventTriggers          TEXT[];
    r                        RECORD;
  BEGIN
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := '')
      INTO v_groupNames;
    IF v_groupNames IS NOT NULL THEN
-- build the list of groups that are in logging state
      SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups FROM emaj.emaj_group
        WHERE group_name = ANY(v_groupNames) AND group_is_logging;
-- check and process the supplied mark name, if it is worth to be done
      IF v_loggingGroups IS NOT NULL THEN
        SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
      END IF;
-- OK
-- get the time stamp of the operation, if not supplied by the caller
      IF v_timeId IS NULL THEN
        SELECT emaj._set_time_stamp('A') INTO v_timeId;
      END IF;
-- for LOGGING groups, lock all tables to get a stable point
      IF v_loggingGroups IS NOT NULL THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', v_multiGroup);
-- and set the mark, using the same time identifier
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, v_multiGroup, TRUE, NULL, v_timeId);
      END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- we can now plan all the steps needed to perform the operation
      PERFORM emaj._alter_plan(v_groupNames, v_timeId);
-- create the needed log schemas
      PERFORM emaj._create_log_schemas(v_callingFunction, v_groupNames);
-- execute the plan
      PERFORM emaj._alter_exec(v_timeId, v_callingFunction);
-- drop the E-Maj log schemas that are now useless (i.e. not used by any created group)
      PERFORM emaj._drop_log_schemas(v_callingFunction, FALSE);
-- update some attributes in the emaj_group table
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (SELECT count(*) FROM emaj.emaj_relation
                                WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r'),
            group_nb_sequence = (SELECT count(*) FROM emaj.emaj_relation
                                   WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'S')
        WHERE group_name = ANY (v_groupNames);
-- enable previously disabled event triggers
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- check foreign keys with tables outside the groups in logging state
      PERFORM emaj._check_fk_groups(v_loggingGroups);
    END IF;
-- and return
    SELECT sum(group_nb_table) + sum(group_nb_sequence) INTO v_nbRel
      FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames);
    RETURN;
  END;
$_alter_groups$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(v_groupsJson JSON, v_groups TEXT[],
                                                    v_allowGroupsUpdate BOOLEAN, v_location TEXT)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepares the effective tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- At the end of the function, the emaj_group_def table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
-- Output: diagnostic records
  DECLARE
    v_rollbackableGroups     TEXT[];
    r_group                  RECORD;
    r_table                  RECORD;
    r_sequence               RECORD;
  BEGIN
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('IMPORT_GROUPS', 'BEGIN', array_to_string(v_groups, ', '), 'Input file: ' || quote_literal(v_location));
-- extract the "tables_groups" json path
    v_groupsJson = v_groupsJson #> '{"tables_groups"}';
-- check that all tables groups listed in the v_groups array exist in the JSON structure
    RETURN QUERY
      SELECT 250, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                   format('The tables group "%s" to import is not referenced in the JSON structure.',
                          group_name)
        FROM (
          SELECT group_name FROM unnest(v_groups) AS g(group_name)
            EXCEPT
          SELECT "group" FROM json_to_recordset(v_groupsJson) AS x("group" TEXT)
          ) AS t;
    IF FOUND THEN
      RETURN;
    END IF;
-- if the v_allowGroupsUpdate flag is FALSE, check that no tables group already exists
    IF NOT v_allowGroupsUpdate THEN
      RETURN QUERY
        SELECT 251, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('The tables group "%s" already exists.',
                            group_name)
          FROM (
            SELECT "group" AS group_name FROM json_to_recordset(v_groupsJson) AS x("group" TEXT), emaj.emaj_group
              WHERE group_name = "group"
                AND "group" = ANY (v_groups)
               ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    ELSE
-- if the v_allowGroupsUpdate flag is TRUE, check that existing tables groups have the same type than in the JSON structure
      RETURN QUERY
        SELECT 252, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('Changing the type of the tables group "%s" is not allowed.',
                            group_name)
          FROM (
            SELECT "group" AS group_name
              FROM json_to_recordset(v_groupsJson) AS x("group" TEXT, "is_rollbackable" BOOLEAN), emaj.emaj_group
              WHERE group_name = "group"
                AND "group" = ANY (v_groups)
                AND group_is_rollbackable <> coalesce(is_rollbackable, true)
               ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    END IF;
-- Create a temporary table to hold the application triggers referenced in the JSON structure
    CREATE TEMP TABLE tmp_app_trigger (
      tmp_group    TEXT,
      tmp_schema   TEXT,
      tmp_table    TEXT,
      tmp_trigger  TEXT
    );
-- In a second pass over the JSON structure:
--   - replace the emaj_group_def content by the JSON content for the imported tables groups
--   - populate the emaj_group_def table and the tmp_app_trigger temporary table
    v_rollbackableGroups = '{}';
    FOR r_group IN
      SELECT value AS groupJson FROM json_array_elements(v_groupsJson)
        WHERE value ->> 'group' = ANY (v_groups)
    LOOP
--   build the array of rollbackable groups
      IF coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE) THEN
        v_rollbackableGroups = array_append(v_rollbackableGroups, r_group.groupJson ->> 'group');
      END IF;
--   delete the emaj_group_def rows for that group
      DELETE FROM emaj.emaj_group_def
        WHERE grpdef_group = r_group.groupJson ->> 'group';
--   insert tables into emaj_group_def, and application triggers into the tmp_app_trigger temporary table
      FOR r_table IN
        SELECT value AS tableJson FROM json_array_elements(r_group.groupJson -> 'tables')
      LOOP
        INSERT INTO emaj.emaj_group_def(grpdef_group, grpdef_schema, grpdef_tblseq,
                                        grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp)
          VALUES (r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table',
                  (r_table.tableJson ->> 'priority')::INT, r_table.tableJson ->> 'log_data_tablespace',
                  r_table.tableJson ->> 'log_index_tablespace');
        INSERT INTO tmp_app_trigger
          SELECT r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table', "trigger"
            FROM json_to_recordset(r_table.tableJson -> 'ignored_triggers') AS x("trigger" TEXT);
      END LOOP;
--   insert sequences into emaj_group_def
      FOR r_sequence IN
        SELECT value AS sequenceJson FROM json_array_elements(r_group.groupJson -> 'sequences')
      LOOP
        INSERT INTO emaj.emaj_group_def(grpdef_group, grpdef_schema, grpdef_tblseq)
          VALUES (r_group.groupJson ->> 'group', r_sequence.sequenceJson ->> 'schema', r_sequence.sequenceJson ->> 'sequence');
      END LOOP;
    END LOOP;
-- check the just imported emaj_group_def content is ok for the groups
    RETURN QUERY
      SELECT * FROM emaj._check_conf_groups(v_groups)
        WHERE ((rpt_text_var_1 = ANY (v_groups) AND rpt_severity = 1)
            OR (rpt_text_var_1 = ANY (v_rollbackableGroups) AND rpt_severity = 2))
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3;
-- check that all listed triggers exist
    RETURN QUERY
      SELECT 260, 1, tmp_group, tmp_schema, tmp_table, tmp_trigger, NULL::INT,
                   format('In the group "%s" and for the table %I.%I, the trigger %s does not exist.',
                          tmp_group, quote_ident(tmp_schema), quote_ident(tmp_table), quote_ident(tmp_trigger))
        FROM (
          SELECT tmp_group, tmp_schema, tmp_table, tmp_trigger
            FROM tmp_app_trigger
            WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_trigger
                                 WHERE relnamespace = pg_namespace.oid
                                   AND tgrelid = pg_class.oid
                                   AND nspname = tmp_schema AND relname = tmp_table AND tgname = tmp_trigger
                                   AND NOT tgisinternal )
             ) AS t;
-- ... and are not emaj triggers
    RETURN QUERY
      SELECT 261, 1, tmp_group, tmp_schema, tmp_table, tmp_trigger, NULL::INT,
                   format('In the group "%s" and for the table %I.%I, the trigger %I is an E-Maj trigger.',
                          tmp_group, quote_ident(tmp_schema), quote_ident(tmp_table), quote_ident(tmp_trigger))
        FROM (
          SELECT tmp_group, tmp_schema, tmp_table, tmp_trigger
            FROM tmp_app_trigger
            WHERE tmp_trigger IN ('emaj_trunc_trg', 'emaj_log_trg')
             ) AS t;
    RETURN;
  END;
$_import_groups_conf_prepare$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_exec(v_json JSON, v_groups TEXT[])
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf_exec$
-- This function completes a tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- Non existing groups are created empty.
-- The _alter_groups() function is used to process the assignement, the move, the removal or the attributes change for tables and
-- sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process
--        - a boolean indicating whether tables groups to import may already exist
-- Output: the number of created or altered tables groups
  DECLARE
    v_timeId                 BIGINT;
    v_groupsJson             JSON;
    v_nbGroup                INT;
    v_comment                TEXT;
    v_isRollbackable         BOOLEAN;
--    r_msg                    RECORD;
    r_group                  RECORD;
  BEGIN
-- Get a time stamp id of type 'I' for the operation
    SELECT emaj._set_time_stamp('I') INTO v_timeId;
-- extract the "tables_groups" json path
    v_groupsJson = v_json #> '{"tables_groups"}';
-- In a third pass over the JSON structure:
--   - create empty groups for those which does not exist yet
--   - adjust the comment on the groups, if needed
    v_nbGroup = 0;
    FOR r_group IN
      SELECT value AS groupJson FROM json_array_elements(v_groupsJson)
        WHERE value ->> 'group' = ANY (v_groups)
    LOOP
      v_nbGroup = v_nbGroup + 1;
--   create the tables group if it does not exist yet
      SELECT group_comment INTO v_comment
        FROM emaj.emaj_group
        WHERE group_name = r_group.groupJson ->> 'group';
      IF NOT FOUND THEN
        v_isRollbackable = coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE);
        INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_creation_time_id,
                                     group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment)
          VALUES (r_group.groupJson ->> 'group', v_isRollbackable, v_timeId,
                                     FALSE, NOT v_isRollbackable, 0, 0, r_group.groupJson ->> 'comment');
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
          VALUES ('IMPORT_GROUPS', 'GROUP CREATED', r_group.groupJson ->> 'group',
                  CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
      ELSE
--   if the group exists, adjust the comment if needed
        IF coalesce(v_comment, '') <> coalesce(r_group.groupJson ->> 'comment', '') THEN
          UPDATE emaj.emaj_group SET group_comment = r_group.groupJson ->> 'comment'
            WHERE group_name = r_group.groupJson ->> 'group';
        END IF;
      END IF;
    END LOOP;
-- process the emaj_group_def content change, if any, by calling the _alter_groups() function
    PERFORM v_nbRel FROM emaj._alter_groups(v_groups, TRUE, 'IMPORT_%', 'IMPORT_GROUPS', v_timeId);
-- adjust the application triggers that need to be set as "not automatically disabled at rollback time"
--   delete from the emaj_ignored_app_trigger table triggers that are not listed anymore
    DELETE FROM emaj.emaj_ignored_app_trigger
      USING emaj.emaj_group_def
      WHERE trg_schema = grpdef_schema
        AND trg_table = grpdef_tblseq
        AND grpdef_group = ANY (v_groups)
        AND NOT EXISTS (
          SELECT 1 FROM tmp_app_trigger
            WHERE tmp_schema = trg_schema
              AND tmp_table = trg_table
              AND tmp_trigger = trg_name);
--   insert into the emaj_ignored_app_trigger table the missing triggers
    INSERT INTO emaj.emaj_ignored_app_trigger
      SELECT tmp_schema, tmp_table, tmp_trigger
        FROM tmp_app_trigger
        WHERE tmp_trigger NOT IN ('emaj_trunc_trg', 'emaj_log_trg')
      ON CONFLICT DO NOTHING;
-- the temporary table is not needed anymore
    DROP TABLE tmp_app_trigger;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_GROUPS', 'END', v_nbGroup || ' created or altered tables groups');
--
    RETURN v_nbGroup;
  END;
$_import_groups_conf_exec$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                    OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just (unlogged) rollback the group (with boolean: isLoggedRlbk = false, multiGroup = false)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(array[v_groupName], v_mark, FALSE, FALSE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT,BOOLEAN) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                     OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
-- operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just (unlogged) rollback the groups (with boolean: isLoggedRlbk = false, multiGroup = true)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(v_groupNames, v_mark, FALSE, TRUE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                           OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
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

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                            OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter
--          group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true)
    RETURN QUERY SELECT * FROM emaj._rlbk_groups(v_groupNames, v_mark, TRUE, TRUE, coalesce(v_isAlterGroupAllowed, FALSE));
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN,
                                             v_isAlterGroupAllowed BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
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
-- check the group names (the groups lock and the state checks are delayed for the later - needed for rollbacks generated by the web
-- application)
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '')
      INTO v_groupNames;
-- if the group names array is null, immediately return
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
    RETURN QUERY SELECT * FROM emaj._rlbk_end(v_rlbkId, v_multiGroup);
  END;
$_rlbk_groups$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(v_rlbkId INT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viwer role can write into rollback tables without having specific privileges
-- to do it.
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
-- get the rollback characteristics for the emaj_rlbk event
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_ctrlStepName
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
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
-- the estimated number of log rows to delete is set to the estimated number of updates. This is underestimated
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
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable,
             c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND n.nspname = r.rlbp_schema AND t.relname = r.rlbp_table     -- join on emaj_rlbk_plan table
      UNION
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable,
             c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
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
-- empty table (or table not yet analyzed)
        v_estimDuration = v_fixed_step_rlbk;
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

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_mark TEXT, v_isLoggedRlbk BOOLEAN)
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
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
    v_nbTblseq               INT;
  BEGIN
-- check the group names (the groups state checks are delayed for later)
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '')
      INTO v_groupNames;
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
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
                                  rlbk_nb_session)
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
-- get the number of tables to lock and sequences to rollback
    SELECT sum(group_nb_table)+sum(group_nb_sequence) INTO v_nbTblseq
      FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames);
-- compute the final estimated duration
    v_estimDuration = v_estimDuration + (v_nbTblseq * v_fixed_table_rlbk);
    RETURN v_estimDuration;
  END;
$_estimate_rollback_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of error or warning messages for discovered discrepancies.
-- If no error is detected, no row is returned.
  BEGIN
--
-- Errors detection
--
-- check the postgres version at groups creation time is compatible (i.e. >= 8.4)
    RETURN QUERY
      SELECT 'Error: The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                   to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804
        ORDER BY msg;
-- check all application schemas referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'Error: The application schema "' || rel_schema || '" does not exist any more.' AS msg
        FROM (
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range)
            EXCEPT
          SELECT nspname FROM pg_catalog.pg_namespace
             ) AS t
        ORDER BY msg;
-- check all application relations referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'Error: In the group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                        -- all expected application relations
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE upper_inf(rel_time_range)
            EXCEPT                                    -- minus relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r             -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range)
        ORDER BY t.rel_schema, t.rel_tblseq, 1;
-- check the log table for all tables referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'Error: In the group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_log_schema AND relname = rel_log_table
                   AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check the log sequence for all tables referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'Error: In the group "' || rel_group || '", the log sequence "' ||
               rel_log_schema || '"."' || rel_log_sequence || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE nspname = rel_log_schema AND relname = rel_log_sequence
                   AND relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check the log function for each table referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'Error: In the group "' || rel_group || '", the log function "' ||
               rel_log_schema || '"."' || rel_log_function || '" is not found.'
             AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
                 WHERE nspname = rel_log_schema AND proname = rel_log_function
                   AND pronamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log triggers
    RETURN QUERY
      SELECT 'Error: In the group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
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
      SELECT 'Error: In the group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
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
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
                AND upper_inf(rel_time_range) AND rel_kind = 'r'),
             cte_log_tables_columns AS (                -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_log_table
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE AND attnum < rel_emaj_verb_attnum
                AND upper_inf(rel_time_range) AND rel_kind = 'r')
        SELECT DISTINCT rel_schema, rel_tblseq,
               'Error: In the group "' || rel_group || '", the structure of the application table "' ||
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
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
        FROM emaj.emaj_relation, emaj.emaj_group
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r' AND rel_group = group_name AND group_is_rollbackable
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
-- for rollbackable groups, check no table has been altered as UNLOGGED or dropped and recreated as TEMP table after tables groups creation
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
          AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND group_name = rel_group AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY rel_schema, rel_tblseq, 1;
-- with PG 11-, check all tables are WITHOUT OIDS (i.e. have not been altered as WITH OIDS after their tables group creation)
    IF emaj._pg_version_num() < 120000 THEN
      RETURN QUERY
        SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
               rel_schema || '"."' || rel_tblseq || '" is WITH OIDS.' AS msg
          FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
          WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
            AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
            AND group_name = rel_group AND group_is_rollbackable
            AND relhasoids
          ORDER BY rel_schema, rel_tblseq, 1;
    END IF;
-- check the primary key structure of all tables belonging to rollbackable groups is unchanged
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             rel_sql_rlbk_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM (
          SELECT rel_schema, rel_tblseq, rel_group, rel_sql_rlbk_pk_columns,
                 string_agg(quote_ident(attname), ',' ORDER BY attnum) AS current_pk_columns
            FROM emaj.emaj_relation, emaj.emaj_group, pg_catalog.pg_attribute, pg_catalog.pg_index, pg_catalog.pg_class,
                 pg_catalog.pg_namespace
            WHERE -- join conditions
                  rel_group = group_name
              AND relname = rel_tblseq AND nspname = rel_schema
              AND pg_attribute.attrelid = pg_index.indrelid
              AND indrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                  -- filter conditions
              AND rel_kind = 'r' AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND attnum = ANY (indkey)
              AND indisprimary
              AND attnum > 0 AND attisdropped = FALSE
            GROUP BY rel_schema, rel_tblseq, rel_group, rel_sql_rlbk_pk_columns
          ) AS t
          WHERE rel_sql_rlbk_pk_columns <> current_pk_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- check all log tables have the 6 required technical columns.
    RETURN QUERY
      SELECT msg FROM (
        SELECT DISTINCT rel_schema, rel_tblseq,
               'Error: In the group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
               string_agg(attname,', ') || ').' AS msg
          FROM (
              SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                FROM emaj.emaj_relation,
                     (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
                WHERE rel_kind = 'r' AND upper_inf(rel_time_range)
                  AND EXISTS
                    (SELECT NULL FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                       WHERE nspname = rel_log_schema AND relname = rel_log_table
                         AND relnamespace = pg_namespace.oid)
            EXCEPT
              SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
                WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                  AND relname = rel_log_table
                  AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
                  AND attname IN ('emaj_verb', 'emaj_tuple', 'emaj_gid', 'emaj_changed', 'emaj_txid', 'emaj_user')
                  AND rel_kind = 'r' AND upper_inf(rel_time_range)
             ) AS t2
          GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
          ORDER BY 1,2,3
        ) AS t;
--
-- Warnings detection
--
-- detect all sequences associated to a serial or a "generated as identity" column have their related table in the same group
    RETURN QUERY
      SELECT msg FROM (
        WITH serial_dependencies AS (
          SELECT rs.rel_group AS seq_group, rs.rel_schema AS seq_schema, rs.rel_tblseq AS seq_name,
                 rt.rel_group AS tbl_group, nt.nspname AS tbl_schema, ct.relname AS tbl_name
            FROM emaj.emaj_relation rs
                 JOIN pg_catalog.pg_class cs ON cs.relname = rel_tblseq
                 JOIN pg_catalog.pg_namespace ns ON cs.relnamespace = ns.oid AND ns.nspname = rel_schema
                 JOIN pg_depend ON pg_depend.objid = cs.oid
                 JOIN pg_catalog.pg_class ct ON pg_depend.refobjid = ct.oid
                 JOIN pg_catalog.pg_namespace nt ON ct.relnamespace = nt.oid
                 LEFT OUTER JOIN emaj.emaj_relation rt ON rt.rel_schema = nt.nspname AND rt.rel_tblseq = ct.relname
            WHERE rs.rel_kind = 'S' AND upper_inf(rs.rel_time_range)
              AND (rt.rel_time_range IS NULL OR upper_inf(rt.rel_time_range))
              AND pg_depend.classid = pg_depend.refclassid             -- the classid et refclassid must be 'pg_class'
              AND pg_depend.classid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_class')
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
-- detect tables linked by a foreign key but not belonging to the same tables group
    RETURN QUERY
      SELECT msg FROM (
        WITH fk_dependencies AS (           -- all foreign keys that link 2 tables at least one of both belongs to a tables group
          SELECT n.nspname AS tbl_schema, t.relname AS tbl_name, c.conname, nf.nspname AS reftbl_schema, tf.relname AS reftbl_name,
                 r.rel_group AS tbl_group, g.group_is_rollbackable AS tbl_group_is_rollbackable,
                 rf.rel_group AS reftbl_group, gf.group_is_rollbackable AS reftbl_group_is_rollbackable
            FROM pg_catalog.pg_constraint c
                 JOIN pg_catalog.pg_class t      ON t.oid = c.conrelid
                 JOIN pg_catalog.pg_namespace n  ON n.oid = t.relnamespace
                 JOIN pg_catalog.pg_class tf     ON tf.oid = c.confrelid
                 JOIN pg_catalog.pg_namespace nf ON nf.oid = tf.relnamespace
                 LEFT OUTER JOIN emaj.emaj_relation r ON r.rel_schema = n.nspname AND r.rel_tblseq = t.relname
                                                     AND upper_inf(r.rel_time_range)
                 LEFT OUTER JOIN emaj.emaj_group g ON g.group_name = r.rel_group
                 LEFT OUTER JOIN emaj.emaj_relation rf ON rf.rel_schema = nf.nspname AND rf.rel_tblseq = tf.relname
                                                     AND upper_inf(rf.rel_time_range)
                 LEFT OUTER JOIN emaj.emaj_group gf ON gf.group_name = rf.rel_group
            WHERE contype = 'f'                                         -- FK constraints only
              AND (r.rel_group IS NOT NULL OR rf.rel_group IS NOT NULL) -- at least the table or the referenced table belongs to
                                                                        -- a tables group
        )
        SELECT tbl_schema, tbl_name,
               'Warning: In the group "' || tbl_group || '", the foreign key "' || conname ||
               '" on the table "' || tbl_schema || '"."' || tbl_name ||
               '" references the table "' || reftbl_schema || '"."' || reftbl_name || '" that does not belong to any group.' AS msg
          FROM fk_dependencies
          WHERE tbl_group IS NOT NULL AND tbl_group_is_rollbackable
            AND reftbl_group IS NULL
        UNION ALL
        SELECT tbl_schema, tbl_name,
               'Warning: In the group "' || reftbl_group || '", the table "' || reftbl_schema || '"."' || reftbl_name ||
               '" is referenced by the foreign key "' || conname ||
               '" of the table "' || tbl_schema || '"."' || tbl_name || '" that does not belong to any group.' AS msg
          FROM fk_dependencies
          WHERE reftbl_group IS NOT NULL AND reftbl_group_is_rollbackable
            AND tbl_group IS NULL
        UNION ALL
        SELECT tbl_schema, tbl_name,
               'Warning: In the group "' || tbl_group || '", the foreign key "' || conname ||
               '" on the table "' || tbl_schema || '"."' || tbl_name ||
               '" references the table "' || reftbl_schema || '"."' || reftbl_name || '" that belongs to another group ("' ||
               reftbl_group || '")' AS msg
          FROM fk_dependencies
          WHERE tbl_group IS NOT NULL AND reftbl_group IS NOT NULL
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
    v_nbMissingEventTrigger  INT;
    r_object                 RECORD;
  BEGIN
-- Global checks
-- detect if the current postgres version is at least 9.5
    IF emaj._pg_version_num() < 90500 THEN
      RETURN NEXT 'Error: The current postgres version (' || version()
               || ') is not compatible with this E-Maj version. It should be at least 9.5.';
      v_errorFound = TRUE;
    END IF;
-- check all E-Maj schemas
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_schemas() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- check all groups components
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_groups() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- check the emaj_ignored_app_trigger table content
    FOR r_object IN
      SELECT 'Error: No trigger "' || trg_name || '" found for table "' || trg_schema || '"."' || trg_table
          || '". Use the emaj_ignore_app_trigger() function to adjust the list of application triggers that should not be'
          || ' automatically disabled at rollback time.'
             AS msg
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
-- report a warning if some E-Maj event triggers are missing
    SELECT 3 - count(*)
      INTO v_nbMissingEventTrigger FROM pg_catalog.pg_event_trigger
      WHERE evtname IN ('emaj_protection_trg','emaj_sql_drop_trg','emaj_table_rewrite_trg');
    IF v_nbMissingEventTrigger > 0 THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers are missing. Your database administrator may (re)create them using the'
               || ' emaj_upgrade_after_postgres_upgrade.sql script.';
    END IF;
-- report a warning if some E-Maj event triggers exist but are not enabled
    PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
    IF FOUND THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers exist but are disabled. You may enable them using the'
               || ' emaj_enable_protection_by_event_triggers() function.';
    END IF;
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
ALTER TABLE emaj.emaj_param DISABLE TRIGGER emaj_param_change_trg;
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';
ALTER TABLE emaj.emaj_param ENABLE TRIGGER emaj_param_change_trg;

-- insert the upgrade end record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 3.4.0 completed');

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
