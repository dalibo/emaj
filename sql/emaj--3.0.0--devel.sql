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
---- check that several tables of the group have not the same emaj names prefix
    RETURN QUERY
      WITH dupl_prefix AS (
        SELECT coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) AS prefix, count(*)
          FROM emaj.emaj_group_def
          WHERE grpdef_group = ANY (v_groupNames)
          GROUP BY 1 HAVING count(*) > 1)
      SELECT 10, 1, grpdef_group, grpdef_schema, grpdef_tblseq, prefix,
             format('in the group %s, the table %s.%s would have a duplicate emaj prefix "%s".', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), prefix)
        FROM emaj.emaj_group_def, dupl_prefix
        WHERE coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) = prefix
          AND grpdef_group = ANY (v_groupNames);
---- check that emaj names prefix that will be generared will not generate conflict with objects from existing groups
    RETURN QUERY
      WITH dupl_prefix AS (
        SELECT coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) AS prefix
          FROM emaj.emaj_group_def, emaj.emaj_relation
          WHERE coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) || '_log' = rel_log_table
            AND grpdef_group = ANY (v_groupNames)
            AND NOT rel_group = ANY (v_groupNames) AND upper_inf(rel_time_range)
      )
      SELECT 11, 1, grpdef_group, grpdef_schema, grpdef_tblseq, prefix,
             format('in the group %s, the table %s.%s would have an already used emaj prefix "%s".', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), prefix)
        FROM emaj.emaj_group_def, dupl_prefix
        WHERE coalesce(grpdef_emaj_names_prefix, grpdef_schema || '_' || grpdef_tblseq) = prefix
          AND grpdef_group = ANY (v_groupNames);
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
---- all sequences described in emaj_group_def have their log schema suffix attribute set to NULL
    RETURN QUERY
      SELECT 30, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the secondary log schema suffix is not NULL.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_schema_suffix IS NOT NULL;
---- all sequences described in emaj_group_def have their emaj names prefix attribute set to NULL
    RETURN QUERY
      SELECT 31, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the emaj names prefix is not NULL.', quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_emaj_names_prefix IS NOT NULL;
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
-- delete obsolete emaj_sequence and emaj_relation rows (those corresponding to the just dropped log tables)
-- (the related emaj_seq_hole rows will be deleted just later ; they are not directly linked to a emaj_relation row)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper(rel_time_range) <= v_markTimeId
        AND sequ_time_id < v_markTimeId;
    DELETE FROM emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND upper(rel_time_range) <= v_markTimeId;
-- drop the E-Maj secondary schemas that are now useless (i.e. not used by any created group)
    PERFORM emaj._drop_log_schemas('DELETE_BEFORE_MARK_GROUP', FALSE);
-- delete rows from all other log tables
    FOR r_rel IN
        SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r' AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
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

--TODO: drop useless application tables (when a feature will need it)

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
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
