--
-- E-Maj: migration from 4.7.0 to <devel>
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
-- The current emaj version must be '4.7.0'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.7.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.7.0',v_emajVersion;
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

----------------------------------------------------------------
--                                                            --
--                       Upgrade start                        --
--                                                            --
----------------------------------------------------------------

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.7.0 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------------------------
--                                                            --
--       Enumerated types, tables, sequences and views        --
--                                                            --
----------------------------------------------------------------


SET client_min_messages TO WARNING;
DROP TYPE IF EXISTS emaj.emaj_log_stat_type;
DROP TYPE IF EXISTS emaj.emaj_detailed_log_stat_type;
DROP TYPE IF EXISTS emaj.emaj_sequence_stat_type;
SET client_min_messages TO NOTICE;

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
DROP FUNCTION IF EXISTS emaj._check_tables_for_rollbackable_group(P_SCHEMA TEXT,P_TABLES TEXT[],P_ARRAYFROMREGEX BOOLEAN,P_CALLINGFUNCTION TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._clean_array(p_array TEXT[])
RETURNS TEXT[] LANGUAGE SQL AS
$$
-- This function cleans up a text array by removing duplicates, NULL and empty strings.
  SELECT array_agg(DISTINCT element)
    FROM unnest(p_array) AS element
    WHERE element IS NOT NULL AND element <> '';
$$;

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
    p_groupNames = emaj._clean_array(p_groupNames);
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

CREATE OR REPLACE FUNCTION emaj._check_tables_for_rollbackable_group(p_schema TEXT, p_tables TEXT[], p_arrayFromRegex BOOLEAN)
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
    SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
      INTO v_list, v_array
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
        RAISE EXCEPTION '_check_tables_for_rollbackable_group: In schema %, some tables (%) have no PRIMARY KEY.',
                        quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_check_tables_for_rollbackable_group: Some tables without PRIMARY KEY (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- Check or discard UNLOGGED tables.
    SELECT string_agg(quote_ident(relname), ', ' ORDER BY relname), array_agg(relname)
      INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'r'
        AND relpersistence = 'u';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_check_tables_for_rollbackable_group: In schema %, some tables (%) are UNLOGGED tables.',
                        quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_check_tables_for_rollbackable_group: Some UNLOGGED tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
--
    RETURN p_tables;
  END;
$_check_tables_for_rollbackable_group$;

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
    v_tblseqs = emaj._clean_array(p_tblseqs);
-- If the schema exists, check that all relations exist.
    IF v_schemaExists THEN
      SELECT string_agg(quote_ident(tblseq), ', ' ORDER BY tblseq)
        INTO v_list
        FROM unnest(v_tblseqs) AS tblseq
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = p_schema
                     AND relname = tblseq
                     AND relkind = p_relkind
                );
      IF v_list IS NOT NULL THEN
        IF p_exceptionIfMissing THEN
          RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not exist!', p_schema, v_relationKind, v_list;
        ELSE
          RAISE WARNING '_check_tblseqs_array: In schema "%", some % (%) do not exist.', p_schema, v_relationKind, v_list;
        END IF;
      END IF;
    END IF;
-- Check that the relations currently belong to a tables group (not necessarily the same for all relations).
    SELECT string_agg(quote_ident(tblseq), ', ' ORDER BY tblseq)
      INTO v_list
      FROM
        (  SELECT tblseq
             FROM unnest(v_tblseqs) AS tblseq
         EXCEPT
           SELECT rel_tblseq
             FROM emaj.emaj_relation
             WHERE rel_schema = p_schema
               AND rel_tblseq = ANY(v_tblseqs)
               AND rel_kind = p_relkind
               AND upper_inf(rel_time_range)
        ) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not currently belong to any tables group.',
                      p_schema, v_relationKind, v_list;
    END IF;
--
    RETURN v_tblseqs;
  END;
$_check_tblseqs_array$;

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
    p_tblseqs = coalesce(emaj._clean_array(p_tblseqs), ARRAY[]::TEXT[]);
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
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex);
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
            EXECUTE format('SELECT array_agg(tgname ORDER BY tgname)'
                           '  FROM pg_catalog.pg_trigger'
                           '       JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)'
                           '       JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)'
                           '  WHERE nspname = %L'
                           '    AND relname = %L'
                           '    AND tgconstraint = 0'
                           '    AND tgname NOT IN (''emaj_log_trg'',''emaj_trunc_trg'')'
                           '    AND (%s)',
                           p_schema, r_rel.rel_tblseq, v_selectConditions)
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
                JOIN pg_catalog.pg_constraint ON (conrelid = attrelid)
           WHERE attnum = ANY (conkey)
             AND conrelid = p_fullTableName::regclass
             AND contype = 'p'
           ORDER BY array_position(conkey, attnum)
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

CREATE OR REPLACE FUNCTION emaj.emaj_does_exist_group(p_groupName TEXT)
RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$$
-- This function returns TRUE if a tables group already exists, otherwise FALSE.
SELECT EXISTS(SELECT 1 FROM emaj.emaj_group WHERE group_name = p_groupName);
$$;
COMMENT ON FUNCTION emaj.emaj_does_exist_group(TEXT) IS
$$Returns a boolean indicating whether a tables group exists.$$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_is_logging_group(p_groupName TEXT)
RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$$
-- This function returns TRUE if a tables group is in LOGGING state, otherwise FALSE (including when the tables group doesn't exist).
SELECT EXISTS(SELECT 1 FROM emaj.emaj_group WHERE group_name = p_groupName AND group_is_logging);
$$;
COMMENT ON FUNCTION emaj.emaj_is_logging_group(TEXT) IS
$$Returns a boolean indicating whether a tables group is in LOGGING state.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_does_exist_mark_group(p_groupName TEXT, p_markName TEXT)
RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$$
-- This function returns TRUE if a mark already exists for a tables group, otherwise FALSE.
SELECT EXISTS(SELECT 1 FROM emaj.emaj_mark WHERE mark_group = p_groupName AND mark_name = p_markName);
$$;
COMMENT ON FUNCTION emaj.emaj_does_exist_mark_group(TEXT, TEXT) IS
$$Returns a boolean indicating whether a mark exists for a tables group.$$;

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
      SELECT string_agg(rlbp_schema || '.' || rlbp_table || '.' || rlbp_object, ', ' ORDER BY rlbp_schema, rlbp_table, rlbp_object)
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
          FROM emaj._estimate_rlbk_step_duration('ADD_FK', r_fk.rlbp_schema, r_fk.rlbp_table, r_fk.rlbp_object,
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
          FROM emaj._estimate_rlbk_step_duration('SET_FK_IMM', r_fk.rlbp_schema, r_fk.rlbp_table, r_fk.rlbp_object,
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

CREATE OR REPLACE FUNCTION emaj._estimate_rlbk_step_duration(p_step emaj._rlbk_step_enum, p_schema TEXT, p_table TEXT,
                                                             p_object TEXT, p_estimatedQuantity BIGINT,
                                                             p_defaultFixedCost INTERVAL, p_defaultVariableCost INTERVAL,
                                                             OUT p_estimateMethod INT, OUT p_estimatedDuration INTERVAL)
LANGUAGE plpgsql AS
$_estimate_rlbk_step_duration$
-- This function reads the rollback statistics in order to compute the duration estimate for elementary steps.
-- The function is called by _rlbk_planning().
-- The cost model depends on the step.
-- Input: step name, the schema, table and object names when it is relevant, the expected volume for the step,
--        the default fixed cost and the default variable cost from the emaj parameters
-- Output: the estimate method (1, 2 or 3), the duration estimate
  BEGIN
-- Initialize the output data.
    p_estimatedDuration = NULL;
-- Compute the duration estimate depending on the step.
    CASE
      WHEN p_step IN ('RLBK_TABLE', 'DELETE_LOG') THEN
-- For RLBK_TBL and DELETE_LOG, the estimate takes into account the estimated number of log rows to revert.
-- First look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude).
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 1
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step
            AND rlbt_quantity > 0
            AND rlbt_schema = p_schema
            AND rlbt_table = p_table
            AND rlbt_quantity / p_estimatedQuantity < 10
            AND p_estimatedQuantity / rlbt_quantity < 10;
        IF p_estimatedDuration IS NULL THEN
-- If there is no previous rollback operation with similar volume, take statistics for the table with all available volumes.
          SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
            INTO p_estimatedDuration, p_estimateMethod
            FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = p_step
              AND rlbt_quantity > 0
              AND rlbt_schema = p_schema
              AND rlbt_table = p_table;
          IF p_estimatedDuration IS NULL THEN
-- No statistics found for the step, so use supplied E-Maj parameters.
            p_estimatedDuration = p_defaultVariableCost * p_estimatedQuantity + p_defaultFixedCost;
            p_estimateMethod = 3;
          END IF;
        END IF;
--
      WHEN p_step = 'ADD_FK' THEN
        IF p_estimatedQuantity <= 0 THEN
-- Empty table (or table not yet analyzed).
          p_estimatedDuration = p_defaultFixedCost;
          p_estimateMethod = 3;
        ELSE
-- Non empty table and statistics (with at least one row) are available.
          SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 1
            INTO p_estimatedDuration, p_estimateMethod
            FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = p_step
              AND rlbt_quantity > 0
              AND rlbt_schema = p_schema
              AND rlbt_table = p_table
              AND rlbt_object = p_object;
          IF p_estimatedDuration IS NULL THEN
-- Non empty table, but no statistic with at least one row is available => take the last duration for this fkey, if any.
            SELECT rlbt_duration, 2
              INTO p_estimatedDuration, p_estimateMethod
              FROM emaj.emaj_rlbk_stat
              WHERE rlbt_step = p_step
                AND rlbt_schema = p_schema
                AND rlbt_table = p_table
                AND rlbt_object = p_object
                AND rlbt_rlbk_id =
                      (SELECT max(rlbt_rlbk_id)
                         FROM emaj.emaj_rlbk_stat
                         WHERE rlbt_step = p_step
                           AND rlbt_schema = p_schema
                           AND rlbt_table = p_table
                           AND rlbt_object = p_object
                      );
            IF p_estimatedDuration IS NULL THEN
-- Definitely no statistics available, compute with the supplied default parameters.
              p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
              p_estimateMethod = 3;
            END IF;
          END IF;
        END IF;
--
      WHEN p_step = 'SET_FK_IMM' THEN
-- If fkey checks statistics are available for this fkey, compute an average cost.
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step
            AND rlbt_quantity > 0
            AND rlbt_schema = p_schema
            AND rlbt_table = p_table
            AND rlbt_object = p_object;
        IF p_estimatedDuration IS NULL THEN
-- No statistics are available for this fkey, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
--
      WHEN p_step = 'RLBK_SEQUENCES' THEN
-- If sequences rollback statistics are available, compute an average cost.
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step;
        IF p_estimatedDuration IS NULL THEN
-- No statistics are available for sequences rollbacks, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
--
      ELSE
-- For other steps, there is no volume to consider.
-- Read statistics, if any, and compute an average cost.
        SELECT sum(rlbt_duration) / sum(rlbt_quantity), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step;
        IF p_estimatedDuration IS NULL THEN
-- No statistics found for the step, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
    END CASE;
--
    RETURN;
  END;
$_estimate_rlbk_step_duration$;

CREATE OR REPLACE FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_group_type LANGUAGE plpgsql AS
$_detailed_log_stat_groups$
-- This function effectively returns statistics on logged data changes executed between 2 marks as viewed through the log tables for one
-- or several groups.
-- It provides much precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
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
             || ' emaj_user::TEXT AS stat_role,'
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
             || ' GROUP BY stat_role, stat_verb'
             || ' ORDER BY stat_role, stat_verb';
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

CREATE OR REPLACE FUNCTION emaj._log_stat_table(p_schema TEXT, p_table TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
RETURNS SETOF emaj.emaj_log_stat_table_type LANGUAGE plpgsql AS
$_log_stat_table$
-- This function returns statistics about a single table, for the time period framed by a supplied time_id slice.
-- It is called by the various emaj_log_stat_table() functions.
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
-- Compute and return the statistics by scanning the table history in the emaj_table table.
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
               rel_group, rel_time_range, clock_timestamp(), '', '[current state]', FALSE, FALSE
          FROM emaj.emaj_relation
          WHERE v_needCurrentState
            AND rel_schema = p_schema
            AND rel_tblseq = p_table
            AND upper_inf(rel_time_range)
        ORDER BY 1, 2
        ), time_slice AS (
-- Transform elementary time events into time slices
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
-- Filter time slices and compute statistics aggregates
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
-- When one or several options need PRIMARY KEYS, check that all selected tables have a PK during the selected time frame.
      IF v_consolidation IN ('PARTIAL', 'FULL') OR v_colsOrder = 'PK' OR v_orderBy = 'PK' THEN
        SELECT string_agg(DISTINCT table_name, ', ' ORDER BY table_name)
          INTO v_tableWithoutPkList
          FROM (
            SELECT rel_schema || '.' || rel_tblseq AS table_name
              FROM emaj.emaj_relation
              WHERE rel_group = p_groupName
                AND rel_kind = 'r' AND NOT v_sequencesOnly
                AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))
                AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
                AND rel_pk_cols IS NULL
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
    v_errorFound             BOOLEAN = FALSE;
    v_status                 INT;
    v_schema                 TEXT;
    v_paramList              TEXT;
    r_object                 RECORD;
  BEGIN
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
-- Report a warning if the max_prepared_transaction GUC setting is not appropriate for parallel rollbacks.
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

ALTER EXTENSION emaj ADD FUNCTION public._emaj_protection_event_trigger_fnct();
CREATE OR REPLACE FUNCTION public._emaj_protection_event_trigger_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_emaj_protection_event_trigger_fnct$
-- This function is called by the emaj_protection_trg event trigger.
-- The function only blocks any attempt to drop the emaj schema or the emaj extension.
-- It is located into the public schema to be able to detect the emaj schema removal attempt.
-- It is also unlinked from the emaj extension to be able to detect the emaj extension removal attempt.
-- Another pair of function and event trigger handles all other drop attempts.
  DECLARE
    r_dropped                RECORD;
  BEGIN
-- Scan all dropped objects.
    FOR r_dropped IN
      SELECT object_type, object_name
        FROM pg_event_trigger_dropped_objects()
    LOOP
      IF r_dropped.object_type = 'schema' AND r_dropped.object_name = 'emaj' THEN
-- Detecting an attempt to drop the emaj object.
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "emaj".'
                        ' Please execute the emaj.emaj_drop_extension() function if you really want to remove all E-Maj components.';
      END IF;
      IF r_dropped.object_type = 'extension' AND r_dropped.object_name = 'emaj' THEN
-- Detecting an attempt to drop the emaj extension.
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the emaj extension.'
                        ' Please execute the emaj.emaj_drop_extension() function if you really want to remove all E-Maj components.';
      END IF;
    END LOOP;
  END;
$_emaj_protection_event_trigger_fnct$;
COMMENT ON FUNCTION public._emaj_protection_event_trigger_fnct() IS
$$E-Maj extension: support of the emaj_protection_trg event trigger.$$;
ALTER EXTENSION emaj DROP FUNCTION public._emaj_protection_event_trigger_fnct();

CREATE OR REPLACE FUNCTION emaj.emaj_drop_extension()
RETURNS VOID LANGUAGE plpgsql AS
$emaj_drop_extension$
-- This function drops emaj from the current database, with both installation kinds,
-- - either as EXTENSION (i.e. with a CREATE EXTENSION SQL statement),
-- - or with the alternate psql script.
  DECLARE
    v_nbObject               INTEGER;
    v_roleToDrop             BOOLEAN;
    v_dbList                 TEXT;
    v_granteeRoleList        TEXT;
    v_granteeClassList       TEXT;
    v_granteeFunctionList    TEXT;
    v_tspList                TEXT;
    r_object                 RECORD;
  BEGIN
-- Perform some checks to verify that the conditions to execute the function are met.
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
-- Revoke also the grants given to emaj_adm on the dblink_connect_u function at install time.
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
    v_roleToDrop = TRUE;
--
-- Are emaj roles also used in other databases of the cluster ?
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
--
-- Are emaj roles granted to other roles ?
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
--
-- Are emaj roles granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
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
--
-- Are emaj roles granted to functions (other than just dropped emaj ones) ?
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
--
-- If emaj roles can be dropped, drop them.
    IF v_roleToDrop THEN
-- Revoke the remaining grants set on tablespaces
      SELECT string_agg(spcname, ', ')
        INTO v_tspList
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

GRANT EXECUTE ON FUNCTION emaj._clean_array(p_array TEXT[]) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_does_exist_group(p_groupName TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_is_logging_group(p_groupName TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_does_exist_mark_group(p_groupName TEXT, p_markName TEXT) TO emaj_viewer;

----------------------------------------------------------------
--                                                            --
--                  Fix emaj tables content                   --
--                                                            --
----------------------------------------------------------------

-- Rebuild the emaj_relation.rel_pk_cols content for tables having a multi-column PK.
DO
$do$
  DECLARE
    v_newPkCols              TEXT[];
    r_rel                    RECORD;
  BEGIN
-- Select tables currently assigned to a tables group and having a multi-columnn PK.
    FOR r_rel IN
      SELECT rel_schema, rel_tblseq, rel_pk_cols
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND array_length(rel_pk_cols, 1) > 1
    LOOP
-- Rebuild the PK columns array with the right columns order.
      SELECT array_agg(attname ORDER BY array_position(conkey, attnum))
        INTO v_newPkCols
        FROM pg_catalog.pg_attribute
             JOIN pg_catalog.pg_constraint ON (conrelid = attrelid)
        WHERE attnum = ANY (conkey)
          AND conrelid = (quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq))::regclass
          AND contype = 'p';
-- If the PK columns array has changed, update the emaj_relation row
      IF v_newPkCols IS NULL OR v_newPkCols <> r_rel.rel_pk_cols THEN
        UPDATE emaj.emaj_relation
          SET rel_pk_cols = v_newPkCols
          WHERE rel_schema = r_rel.rel_schema
            AND rel_tblseq = r_rel.rel_tblseq
            AND upper_inf(rel_time_range);
      END IF;
    END LOOP;
  END;
$do$;

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
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.7.0 completed');

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
