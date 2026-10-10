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
CREATE OR REPLACE FUNCTION emaj._check_fk_groups(p_groups TEXT[])
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
      SELECT c.conname, r.rel_schema, r.rel_tblseq, nf.nspname, tf.relname
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class t ON (t.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid AND n.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid  = t.oid)
             JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groups)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND tf.relkind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table currently outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = nf.nspname
                     AND rel_tblseq = tf.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groups)
                )
        ORDER BY 1, 2, 3
    LOOP
      RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" references the table "%.%" that is outside the groups (%).',
        r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq, r_fk.nspname, r_fk.relname, array_to_string(p_groups, ', ');
    END LOOP;
-- Issue a warning if a table of the groups is referenced by a table outside the groups.
    FOR r_fk IN
      SELECT c.conname, n.nspname, t.relname, r.rel_schema, r.rel_tblseq
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class tf ON (tf.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace AND nf.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.confrelid  = tf.oid)
             JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
             JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groups)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND t.relkind = 'r'                                       -- only constraints referenced by true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = n.nspname
                     AND rel_tblseq = t.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groups)
                )
        ORDER BY 1, 2, 3
    LOOP
      RAISE WARNING '_check_fk_groups: The table "%.%" is referenced by the foreign key "%" on the table "%.%" that is outside'
                    ' the groups (%).', r_fk.rel_schema, r_fk.rel_tblseq, r_fk.conname, r_fk.nspname, r_fk.relname,
                    array_to_string(p_groups, ', ');
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
          AND r.rel_group = ANY (p_groups)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND r.rel_kind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
        ORDER BY 1, 2, 3
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

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(p_group TEXT, p_mark TEXT)
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
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Retrieve the emaj_gid value and the time stamp id of the target new first mark.
    SELECT time_last_emaj_gid, mark_time_id INTO v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_group
        AND mark_name = p_mark;
--
-- First process all obsolete time ranges for the group.
--
-- Drop obsolete old log tables.
    FOR r_rel IN
        -- log tables for the group, whose end time stamp is older than the new first mark time stamp
        SELECT DISTINCT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND rel_group = p_group
            AND upper(rel_time_range) <= v_markTimeId
      EXCEPT
        -- unless they are also used for more recent time range, or are also linked to other groups
        SELECT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND (upper(rel_time_range) > v_markTimeId
                 OR upper_inf(rel_time_range)
                 OR rel_group <> p_group)
        ORDER BY 1, 2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- Delete emaj_table rows corresponding to obsolete relation time range that will be deleted just later.
-- The related emaj_seq_hole rows will be deleted just later ; they are not directly linked to an emaj_relation row.
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation r1
      WHERE rel_group = p_group
        AND rel_kind = 'r'
        AND tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND upper(rel_time_range) <= v_markTimeId
        AND (tbl_time_id < v_markTimeId                   -- all tables states prior the mark time
             OR (tbl_time_id = v_markTimeId               -- and the tables state of the mark time
                 AND NOT EXISTS                           --   if it is not the lower bound of an adjacent time range
                       (SELECT 0
                          FROM emaj.emaj_relation r2
                          WHERE r2.rel_schema = r1.rel_schema
                            AND r2.rel_tblseq = r1.rel_tblseq
                            AND lower(r2.rel_time_range) = v_markTimeId
                       )));
-- Keep a trace of the relation group ownership history and finaly delete from the emaj_relation table the relation that ended before
-- the new first mark.
    WITH deleted AS
      (DELETE FROM emaj.emaj_relation
         WHERE rel_group = p_group
           AND upper(rel_time_range) <= v_markTimeId
         RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
        FROM deleted;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any created group).
    PERFORM emaj._drop_log_schemas('DELETE_BEFORE_MARK_GROUP', FALSE);
--
-- Then process the current relation time range for the group.
--
-- Delete rows from all log tables.
    FOR r_rel IN
      SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name
        FROM emaj.emaj_relation
        WHERE rel_group = p_group
          AND rel_kind = 'r'
          AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
-- Delete log rows prior to the new first mark.
      EXECUTE format('DELETE FROM %s WHERE emaj_gid <= $1',
                     r_rel.log_table_name)
        USING v_markGlobalSeq;
    END LOOP;
-- Process emaj_seq_hole content.
-- Delete all existing holes, if any, before the mark.
-- It may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
-- but is safe enough for rollbacks.
    DELETE FROM emaj.emaj_seq_hole
      USING emaj.emaj_relation
      WHERE rel_group = p_group
        AND rel_kind = 'r'
        AND rel_schema = sqhl_schema
        AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id < v_markTimeId;
-- Now the sequences related to the mark to delete can be suppressed.
-- Delete first application sequences related data for the group.
-- The sequence state at time range bounds are kept (if the mark comes from a logging group alter operation).
    DELETE FROM emaj.emaj_sequence
      USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema
        AND sequ_name = rel_tblseq
        AND rel_time_range @> sequ_time_id
        AND rel_group = p_group
        AND rel_kind = 'S'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
-- Delete then tables related data for the group.
-- The tables state at time range bounds are kept.
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation
      WHERE tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND rel_time_range @> tbl_time_id
        AND rel_group = p_group
        AND rel_kind = 'r'
        AND tbl_time_id < v_markTimeId
        AND lower(rel_time_range) <> tbl_time_id;
-- And that may have one of the deleted marks as target mark from a previous logged rollback operation.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = p_group
        AND mark_time_id >= v_markTimeId
        AND mark_logged_rlbk_target_mark IN
             (SELECT mark_name
                FROM emaj.emaj_mark
                WHERE mark_group = p_group
                  AND mark_time_id < v_markTimeId
             );
-- Delete oldest marks.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = p_group
        AND mark_time_id < v_markTimeId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Purge the history tables, if needed (even if no mark as been really dropped).
    PERFORM emaj._purge_histories();
--
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_drop_extension()
RETURNS VOID LANGUAGE plpgsql AS
$emaj_drop_extension$
-- This function drops emaj from the current database, with both installation kinds,
-- - either as EXTENSION (i.e. with a CREATE EXTENSION SQL statement),
-- - or with the alternate psql script.
  DECLARE
    v_isEmajExtension        BOOLEAN;
    v_supportEmajAdm         BOOLEAN;
    v_supportEmajViewer      BOOLEAN;
    v_isSuperuser            BOOLEAN;
    v_isRoleCreaterole       BOOLEAN;
    v_isRoleEmajAdm          BOOLEAN;
    v_emajSchemaOwner        TEXT;
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
-- Read some installation and current user characteristics.
    v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
    SELECT inst_with_emaj_adm, inst_with_emaj_viewer
      INTO STRICT v_supportEmajAdm, v_supportEmajViewer
      FROM emaj.emaj_install_conf;
    SELECT rolname
      INTO v_emajSchemaOwner
      FROM pg_catalog.pg_namespace
           JOIN pg_catalog.pg_roles ON (pg_roles.oid = nspowner)
      WHERE nspname = 'emaj';
    SELECT rolsuper, rolcreaterole,
           CASE WHEN NOT v_supportEmajAdm THEN FALSE
                ELSE pg_catalog.pg_has_role(rolname, 'emaj_adm', 'MEMBER') END
      INTO v_isSuperuser, v_isRoleCreaterole, v_isRoleEmajAdm
      FROM pg_catalog.pg_roles
      WHERE rolname = current_user;
-- Check current user rights.
    IF NOT v_isSuperuser THEN
      IF v_isEmajExtension THEN
-- For extensions, the current role must be superuser.
        RAISE EXCEPTION 'emaj_drop_extension: The current role must be a superuser to be allowed to drop the EXTENSION';
      ELSE
        IF v_emajSchemaOwner <> current_user THEN
          IF v_supportEmajAdm AND v_emajSchemaOwner = 'emaj_adm' THEN
---- For emaj installed by psql script with emaj_adm capabilities, the current role must have emaj_adm rights or be a superuser.
            IF NOT v_isRoleEmajAdm THEN
              RAISE EXCEPTION 'emaj_drop_extension: The current role must be an emaj adminstrator or a superuser';
            END IF;
          ELSE
---- For emaj installed by psql script without emaj_adm capabilities, the current role must either be the owner of the emaj schema
----   or be a superuser.
            RAISE EXCEPTION 'emaj_drop_extension: The current role must be the owner of the emaj schema or a superuser';
          END IF;
        END IF;
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
      RAISE WARNING 'emaj_drop_extension - schema consistency checks: %', r_object.msg;
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
    PERFORM emaj._disable_event_triggers();
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
-- Determine whether the emaj_adm role can be dropped.
-- If emaj_adm is not supported in this emaj instance, just warn.
    IF NOT v_supportEmajAdm THEN
      RAISE WARNING 'emaj_drop_extension: The emaj_adm role is not supported in this database.';
    ELSIF NOT v_isRoleCreaterole THEN
      RAISE WARNING 'emaj_drop_extension: The current role has not the CREATEROLE rights needed to drop emaj_adm.';
    ELSE
      v_roleToDrop = TRUE;
-- Is emaj_adm also used in other databases of the cluster ?
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
-- Is emaj_adm granted to other roles ?
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
-- Is emaj_adm granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
      v_granteeClassList = NULL;
      SELECT string_agg(nspname || '.' || relname, ', ' ORDER BY nspname, relname)
        INTO v_granteeClassList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
        WHERE array_to_string (relacl, ';') LIKE '%emaj_adm=%';
      IF v_granteeClassList IS NOT NULL THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList, 1, 200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_adm role has some remaining grants on tables, views or sequences (%).',
                      v_granteeClassList;
        v_roleToDrop = FALSE;
      END IF;
-- Is emaj_adm granted to functions (other than just dropped emaj ones) ?
      v_granteeFunctionList = NULL;
      SELECT string_agg(nspname || '.' || proname || '()', ', ' ORDER BY nspname, proname)
        INTO v_granteeFunctionList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_proc ON (pronamespace = pg_namespace.oid)
        WHERE array_to_string (proacl, ';') LIKE '%emaj_adm=%';
      IF v_granteeFunctionList IS NOT NULL THEN
        IF length(v_granteeFunctionList) > 200 THEN
          v_granteeClassList = substr(v_granteeFunctionList, 1, 200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_adm role has some remaining grants on functions (%).',
                      v_granteeFunctionList;
        v_roleToDrop = FALSE;
      END IF;
-- If emaj_adm can be dropped, do it.
      IF v_roleToDrop THEN
-- Revoke first the remaining grants set on tablespaces
        SELECT string_agg(spcname, ', ')
          INTO v_tspList
          FROM pg_catalog.pg_tablespace
          WHERE array_to_string (spcacl, ';') LIKE '%emaj_adm=%';
        IF v_tspList IS NOT NULL THEN
          EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_adm';
        END IF;
-- ... and drop the role.
        DROP ROLE emaj_adm;
        RAISE WARNING 'emaj_drop_extension: the emaj_adm role has been dropped.';
      ELSE
        RAISE WARNING 'emaj_drop_extension: For these reasons, emaj_adm has not been dropped by this script.';
      END IF;
    END IF;
--
-- Determine whether the emaj_viewer role can be dropped.
-- If emaj_viewer is not supported in this emaj instance, just warn.
    IF NOT v_supportEmajViewer THEN
      RAISE WARNING 'emaj_drop_extension: The emaj_viewer role is not supported in this database.';
    ELSIF NOT v_isRoleCreaterole THEN
      RAISE WARNING 'emaj_drop_extension: The current role has not the CREATEROLE rights needed to drop emaj_viewer.';
    ELSE
--
-- Is emaj_viewer also used in other databases of the cluster ?
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
-- Is emaj_viewer granted to other roles ?
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
-- Is emaj_viewer granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
      v_granteeClassList = NULL;
      SELECT string_agg(nspname || '.' || relname, ', ' ORDER BY nspname, relname)
        INTO v_granteeClassList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
        WHERE array_to_string (relacl, ';') LIKE '%emaj_viewer=%';
      IF v_granteeClassList IS NOT NULL THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList, 1, 200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has some remaining grants on tables, views or sequences (%).',
                      v_granteeClassList;
        v_roleToDrop = FALSE;
      END IF;
--
-- Is emaj_viewer granted to functions (other than just dropped emaj ones) ?
      v_granteeFunctionList = NULL;
      SELECT string_agg(nspname || '.' || proname || '()', ', ' ORDER BY nspname, proname)
        INTO v_granteeFunctionList
        FROM pg_catalog.pg_namespace
             JOIN pg_catalog.pg_proc ON (pronamespace = pg_namespace.oid)
        WHERE array_to_string (proacl, ';') LIKE '%emaj_viewer=%';
      IF v_granteeFunctionList IS NOT NULL THEN
        IF length(v_granteeFunctionList) > 200 THEN
          v_granteeFunctionList = substr(v_granteeFunctionList, 1, 200) || '...';
        END IF;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has some remaining grants on functions (%).',
                      v_granteeFunctionList;
        v_roleToDrop = FALSE;
      END IF;
--
-- If emaj_viewer can be dropped, do it.
      IF v_roleToDrop THEN
-- Revoke first the remaining grants set on tablespaces
        SELECT string_agg(spcname, ', ')
          INTO v_tspList
          FROM pg_catalog.pg_tablespace
          WHERE array_to_string (spcacl, ';') LIKE '%emaj_viewer=%';
        IF v_tspList IS NOT NULL THEN
          EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_viewer';
        END IF;
-- ... and drop the role.
        DROP ROLE emaj_viewer;
        RAISE WARNING 'emaj_drop_extension: The emaj_viewer role has been dropped.';
      ELSE
        RAISE WARNING 'emaj_drop_extension: For these reasons, the emaj_viewer role has not been dropped by this script.';
      END IF;
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

-- Set the ownership of all emaj objects to emaj_adm.
ALTER SCHEMA emaj OWNER TO emaj_adm;
ALTER SEQUENCE emaj.emaj_global_seq OWNER TO emaj_adm;
-- Process tables, views and types.
DO LANGUAGE plpgsql
$do$
  DECLARE
    r_object                 RECORD;
  BEGIN
-- Process tables, views and composite types.
    FOR r_object IN
      SELECT relname, CASE relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' WHEN 'c' THEN 'TYPE' END AS object
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
        WHERE nspname = 'emaj'
          AND relkind IN ('r', 'v', 'c')
    LOOP
      EXECUTE format('ALTER %s emaj.%s OWNER TO emaj_adm',
                         r_object.object, r_object.relname);
    END LOOP;
-- Process enum types.
    FOR r_object IN
      SELECT typname
        FROM pg_catalog.pg_type
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = typnamespace)
        WHERE nspname = 'emaj'
          AND typtype = 'e'
    LOOP
      EXECUTE format('ALTER type emaj.%s OWNER TO emaj_adm',
                         r_object.typname);
    END LOOP;
  END;
$do$;

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
