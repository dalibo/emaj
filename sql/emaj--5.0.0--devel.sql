--
-- E-Maj: migration from 5.0.0 to <devel>
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
-- The current emaj version must be '5.0.0'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '5.0.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 5.0.0',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 14.
    IF current_setting('server_version_num')::int < 140000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 14.', current_setting('server_version');
    END IF;
-- If we are in the first upgrade step of the transaction, ...
    SELECT current_setting('emaj.upgrade_verify_txid', TRUE) INTO v_txid;
    IF v_txid IS NULL OR v_txid <> txid_current()::TEXT THEN
-- ... check the E-Maj environment state,
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
-- ... and lock the emaj_group table to ensure that no other E-Maj operation is currently in progress.
      LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 5.0.0 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------------------------
--                                                            --
--       Enumerated types, tables, sequences and views        --
--                                                            --
----------------------------------------------------------------

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

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_groups_configuration$
-- This function returns a JSON formatted structure representing some or all configured table groups
-- The function can be called by clients like Emaj_web.
-- This is just a wrapper of the internal _export_groups_conf() function.
-- Input: an optional group names array, NULL means all table groups
-- Output: the table groups content in JSON format
  BEGIN
    RETURN emaj._export_groups_conf(p_groups);
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT[]) IS
$$Generates a json structure describing configured table groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_groups_configuration$
-- This function stores some or all configured table groups configuration into a file on the server.
-- The JSON structure is built by the _export_groups_conf() function.
-- Input: output file path name,
--        an optional group names array, NULL means all table groups
-- Output: the number of table groups recorded in the file.
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
-- Return the number of recorded table groups.
    RETURN json_array_length(v_groupsJson->'tables_groups');
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT, TEXT[]) IS
$$Generates and stores in a file a json structure describing configured table groups.$$;

CREATE OR REPLACE FUNCTION emaj._export_groups_conf(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$_export_groups_conf$
-- This function generates a JSON formatted structure representing the current configuration of some or all table groups.
-- Input: an optional group names array, NULL means all table groups
-- Output: the table groups configuration in JSON format
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
      v_groupsText = v_groupsText || E', including all table groups",\n';
    ELSE
      v_groupsText = v_groupsText || E', including a table groups subset",\n';
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
        RAISE EXCEPTION '_export_groups_conf: The table groups % are unknown.', v_unknownGroupsList;
      END IF;
    END IF;
-- Build the table groups description.
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

CREATE OR REPLACE FUNCTION emaj._import_groups_conf(p_json JSON, p_groups TEXT[], p_allowGroupsUpdate BOOLEAN,
                                                    p_location TEXT, p_mark TEXT, p_dropOtherGroups BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf$
-- This function processes a JSON formatted structure representing the table groups to create or update.
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
-- For table groups, "group_is_rollbackable" and "group_comment" attributes are optional.
-- For tables, "priority", "log_data_tablespace" and "log_index_tablespace" attributes are optional.
-- A table group may have no "tables" or "sequences" arrays.
-- A table may have no "ignored_triggers" array.
-- Non existing groups are created empty.
-- Input: - the table groups configuration structure in JSON format
--        - the array of group names to process (a NULL value process all table groups described in the JSON structure)
--        - a boolean indicating whether table groups to import may already exist
--        - the input file name, if any, to record in the emaj_hist table (NULL if direct import)
--        - the mark name to set for table groups in logging state
--        - the request for the drop of all existing groups that are not in the imported configuration
-- Output: the number of created or altered table groups
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
-- This function prepares the effective table groups configuration import.
-- It is called by _import_groups_conf() and by Emaj_web
-- At the end of the function, the tmp_app_table table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management.
-- Input: - the table groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all table groups described in the JSON structure)
--        - an optional boolean indicating whether table groups to import may already exist (FALSE by default)
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
-- Check that all table groups listed in the p_groups array exist in the JSON structure.
    RETURN QUERY
      SELECT 250, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                   format('The table group "%s" to import is not referenced in the JSON structure.',
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
-- If the p_allowGroupsUpdate flag is FALSE, check that no table group already exists.
    IF NOT p_allowGroupsUpdate THEN
      RETURN QUERY
        SELECT 251, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('The table group "%s" already exists.',
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
-- If the p_allowGroupsUpdate flag is TRUE, check that existing table groups have the same type than in the JSON structure.
      RETURN QUERY
        SELECT 252, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('Changing the type of the table group "%s" is not allowed. '
                            'You may drop this table group before importing the configuration.',
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
             format('The table %s.%s is referenced in %s different table groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_tbl_name), nb_group)
        FROM (SELECT tmp_schema, tmp_tbl_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_table
                GROUP BY tmp_schema, tmp_tbl_name
                HAVING count(DISTINCT tmp_group) > 1) AS t1
      UNION
      SELECT 271, 1, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::TEXT, nb_group::INT,
             format('The sequence %s.%s is referenced in %s different table groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_seq_name), nb_group)
        FROM (SELECT tmp_schema, tmp_seq_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_sequence
                GROUP BY tmp_schema, tmp_seq_name
                HAVING count(DISTINCT tmp_group) > 1) AS t2
      UNION
      SELECT 272, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In table group "%s", the table %s.%s is referenced several times.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM (SELECT tmp_group, tmp_schema, tmp_tbl_name, count(*)
                FROM tmp_app_table
                GROUP BY tmp_group, tmp_schema, tmp_tbl_name
                HAVING count(*) > 1) AS t3
      UNION
      SELECT 273, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In table group "%s", the sequence %s.%s is referenced several times.',
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

CREATE OR REPLACE FUNCTION emaj._export_param_conf(p_includeDefault BOOLEAN)
RETURNS JSON LANGUAGE plpgsql AS
$_export_param_conf$
-- This function generates a JSON formatted structure representing the parameters.
-- Input: boolean indicating whether keys which current value equals their default value must be exported.
-- Output: the parameters content in JSON format
  DECLARE
    v_paramHelp              TEXT;
    v_params                 TEXT;
    v_paramsJson             JSON;
    r_param                  RECORD;
  BEGIN
-- Build the _help attribute content.
    SELECT string_agg(param_key || CASE WHEN param_default <> '' THEN ' (default = ' || param_default || ')' ELSE '' END,
                      ', ' ORDER BY param_rank)
      INTO v_paramHelp
      FROM emaj.emaj_default_param;
-- Build the JSON structure header.
    v_params = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || statement_timestamp() || E'",\n' ||
                '  "_help": "Known parameter keys: ' || v_paramHelp || E'",\n';
-- Build the parameters description.
    v_params = v_params || E'  "parameters": [\n';
    FOR r_param IN
      SELECT to_json(param_key) AS key,
             to_json(param_value) AS value
        FROM emaj.emaj_all_param
        WHERE p_includeDefault OR
              CASE
                WHEN param_cast IS NULL THEN (param_value <> param_default)
                WHEN param_cast = 'INTERVAL' THEN (param_value::INTERVAL <> param_default::INTERVAL)
                ELSE TRUE
              END
        ORDER BY param_rank
    LOOP
      v_params = v_params || E'    {\n'
                          ||  '      "key": ' || r_param.key || E',\n'
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

CREATE OR REPLACE FUNCTION emaj._disable_event_triggers()
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_disable_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- The function is called by functions that alter or drop E-Maj components.
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
-- A single operation may call the function several times. But this is not an issue as only enabled triggers are disabled.
    SELECT coalesce(array_agg(evtname ORDER BY evtname), ARRAY[]::TEXT[]) INTO v_eventTriggers
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
-- The function is called by functions that alter or drop E-Maj components.
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
        WHEN TAG IN ('DROP EXTENSION', 'DROP SCHEMA')
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
REVOKE SELECT ON TABLE emaj.emaj_all_param FROM emaj_viewer;

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
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 5.0.0 completed');

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

SET client_min_messages TO default;
