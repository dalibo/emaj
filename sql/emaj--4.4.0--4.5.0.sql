--
-- E-Maj: migration from 4.4.0 to 4.5.0
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
-- The current emaj version must be '4.4.0'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.4.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.4.0',v_emajVersion;
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj 4.5.0', 'Upgrade from 4.4.0 started');

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

-- Re-attach the _emaj_protection_event_trigger_fnct() function to the extension, in order to change its code.
ALTER EXTENSION emaj ADD FUNCTION public._emaj_protection_event_trigger_fnct();

--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
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
-- If the output file is a psql script, the '\\' are transformed into '\', using a sed command.
-- The caller is responsible for checking:
--  - the output file location (directory, permissions),
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
-- The file is removed by calling a rm shell command through a COPY TO PROGRAM statement.
        EXECUTE format ('COPY (SELECT NULL) TO PROGRAM ''rm %s''',
                        p_location);
        RETURN 0;
       END IF;
    END IF;
--
    RETURN 1;
  END;
$_copy_to_file$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(p_groupsJson JSON, p_groups TEXT[],
                                                    p_allowGroupsUpdate BOOLEAN, p_location TEXT)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepares the effective tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- At the end of the function, the tmp_app_table table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
-- Output: diagnostic records
  DECLARE
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
    DROP TABLE IF EXISTS tmp_app_table, tmp_app_sequence;
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
        SELECT rel_schema || '.' || rel_tblseq, coalesce(last_value, start_value - increment_by)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
            JOIN pg_catalog.pg_sequences ON (schemaname = rel_log_schema AND sequencename = rel_log_sequence)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'r'
           AND (rel_schema || '.' || rel_tblseq) ~ $3
           AND ($4 = '' OR (rel_schema || '.' || rel_tblseq) !~ $4)
      $$;
    END IF;
    IF p_sequencesExcludeFilter != '.*' THEN
      v_stmt = v_stmt || $$
      UNION ALL
        SELECT rel_schema || '.' || rel_tblseq, coalesce(last_value, start_value - increment_by)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
            JOIN pg_catalog.pg_sequences ON (schemaname = rel_schema AND sequencename = rel_tblseq)
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
  DECLARE
    v_copyOptions            TEXT;
    v_noEmptyFiles           BOOLEAN;
    v_nbFile                 INT = 1;
    v_pathName               TEXT;
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
      INTO v_copyOptions, v_noEmptyFiles, p_lastMark
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, FALSE) g;
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
        IF r_sql.sql_text ~ '^(SET|RESET)' THEN
-- The SET or RESET statements are executed as is.
          EXECUTE r_sql.sql_text;
        ELSE
-- Otherwise, dump the log table or the sequence states.
          v_pathName = emaj._build_path_name(p_dir, r_sql.sql_schema || '_' || r_sql.sql_tblseq || r_sql.sql_file_name_suffix);
          v_copyResult = emaj._copy_to_file('(' || r_sql.sql_text || ')', v_pathName, v_copyOptions, v_noEmptyFiles);
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
    PERFORM 1 FROM pg_namespace WHERE nspname = 'emaj';
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
        WHERE msg NOT LIKE 'Error: The E-Maj schema % does not exist any more.'
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

GRANT EXECUTE ON FUNCTION emaj._get_sequences_last_value(p_groupsIncludeFilter TEXT, p_groupsExcludeFilter TEXT,
                                                         p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                         p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                         OUT p_key TEXT, OUT p_value TEXT) TO emaj_viewer;

------------------------------------
--                                --
-- Complete the upgrade           --
--                                --
------------------------------------
-- Re-detach the _emaj_protection_event_trigger_fnct() function from the extension.
ALTER EXTENSION emaj DROP FUNCTION public._emaj_protection_event_trigger_fnct();

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
    SELECT '4.5.0', TSTZRANGE(clock_timestamp(), null, '[]'), duration
      FROM start_time_data;

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj 4.5.0', 'Upgrade from 4.4.0 completed');

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
-- If the adminpack extension is installed, warn that emaj don't need it anymore.
    PERFORM 1 FROM pg_catalog.pg_extension WHERE extname = 'adminpack';
    IF FOUND THEN
      RAISE WARNING 'E-Maj upgrade: the adminpack extension is not used by E-Maj anymore. You may drop it.';
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
