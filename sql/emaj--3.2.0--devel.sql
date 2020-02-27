--
-- E-Maj: migration from 3.2.0 to <devel>
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
-- the emaj version registered in emaj_param must be '3.2.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '3.2.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 3.2.0',v_emajVersion;
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
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 3.2.0 started');

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------

ALTER TYPE emaj._check_conf_groups_type RENAME TO _check_groups_conf_type;

--
-- process the emaj_relation table
--
-- create a temporary table with the old structure and copy the source content
CREATE TEMP TABLE emaj_relation_old (LIKE emaj.emaj_relation);

INSERT INTO emaj_relation_old SELECT * FROM emaj.emaj_relation;

-- drop the old table
-- removing linked objects from the extension is a workaround for a bug in postgres extensions management (now fixed in latest versions)

ALTER EXTENSION emaj DROP FUNCTION _drop_tbl(emaj_relation,bigint);
ALTER EXTENSION emaj DROP FUNCTION _drop_seq(emaj_relation,bigint);
ALTER EXTENSION emaj DROP FUNCTION _rlbk_tbl(emaj_relation,bigint,bigint,integer,boolean);
ALTER EXTENSION emaj DROP FUNCTION _delete_log_tbl(emaj_relation,bigint,bigint,bigint);
ALTER EXTENSION emaj DROP FUNCTION _rlbk_seq(emaj_relation,bigint);
ALTER EXTENSION emaj DROP FUNCTION _log_stat_tbl(emaj_relation,bigint,bigint);
ALTER EXTENSION emaj DROP FUNCTION _gen_sql_tbl(emaj_relation,bigint,bigint);
ALTER EXTENSION emaj DROP FUNCTION _gen_sql_seq(emaj_relation,bigint,bigint,bigint);

DROP TABLE emaj.emaj_relation CASCADE;

-- create the new table, with its indexes, comment, constraints (except foreign key)...
CREATE TABLE emaj.emaj_relation (
  rel_schema                   TEXT        NOT NULL,       -- schema name containing the relation
  rel_tblseq                   TEXT        NOT NULL,       -- application table or sequence name
  rel_time_range               INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
  rel_group                    TEXT        NOT NULL,       -- name of the group that owns the relation
  rel_kind                     TEXT,                       -- similar to the relkind column of pg_class table
                                                           --   ('r' = table, 'S' = sequence)
-- next columns are specific for tables and remain NULL for sequences
  rel_priority                 INTEGER,                    -- priority level of processing inside the group
  rel_log_schema               TEXT,                       -- schema for the log table, functions and sequence
  rel_log_table                TEXT,                       -- name of the log table associated
  rel_log_dat_tsp              TEXT,                       -- tablespace for the log table
  rel_log_index                TEXT,                       -- name of the index of the log table
  rel_log_idx_tsp              TEXT,                       -- tablespace for the log index
  rel_log_sequence             TEXT,                       -- name of the log sequence
  rel_log_function             TEXT,                       -- name of the function associated to the log trigger
                                                           -- created on the application table
  rel_emaj_verb_attnum         SMALLINT,                   -- column number (attnum) of the log table's emaj_verb column in the
                                                           --  pg_attribute table
  rel_has_always_ident_col     BOOLEAN,                    -- are there any "generated always as identity" column ?
  rel_sql_rlbk_columns         TEXT,                       -- piece of sql used to rollback: list of the columns
  rel_sql_rlbk_pk_columns      TEXT,                       -- piece of sql used to rollback: list of the pk columns
  rel_sql_rlbk_pk_conditions   TEXT,                       -- piece of sql used to rollback: equality conditions on the pk columns
  rel_sql_gen_ins_col          TEXT,                       -- piece of sql used for SQL generation: list of columns to insert
  rel_sql_gen_ins_val          TEXT,                       -- piece of sql used for SQL generation: list of column values to insert
  rel_sql_gen_upd_set          TEXT,                       -- piece of sql used for SQL generation: set clause for updates
  rel_sql_gen_pk_conditions    TEXT,                       -- piece of sql used for SQL generation: equality conditions on the pk columns
  rel_log_seq_last_value       BIGINT,                     -- last value of the log sequence when the table is removed from the group
                                                           -- (NULL otherwise)
  PRIMARY KEY (rel_schema, rel_tblseq, rel_time_range),
  FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name),
  FOREIGN KEY (rel_log_schema) REFERENCES emaj.emaj_schema (sch_name),
  EXCLUDE USING gist (rel_schema WITH =, rel_tblseq WITH =, rel_time_range WITH &&)
  );
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;

-- populate the new table
INSERT INTO emaj.emaj_relation (
             rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
             rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_emaj_verb_attnum, rel_has_always_ident_col,
             rel_sql_rlbk_columns, rel_sql_rlbk_pk_columns, rel_sql_rlbk_pk_conditions,
             rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
             rel_log_seq_last_value
             )
  SELECT     rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema, rel_log_table, rel_log_dat_tsp,
             rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function, rel_emaj_verb_attnum, FALSE /*rel_has_always_ident_col*/,
             rel_sql_columns, rel_sql_pk_columns, rel_sql_pk_eq_conditions,
             NULL /*rel_sql_gen_ins_col*/, NULL /*rel_sql_gen_ins_val*/, NULL /*rel_sql_gen_upd_set*/, NULL /*rel_sql_gen_pk_conditions*/,
             rel_log_seq_last_value
    FROM emaj_relation_old;

-- Adjust the emaj_relation content
DO
$do$
  DECLARE
    v_genColList             TEXT;
    v_isColListNeeded        BOOLEAN;
    v_unquotedType           TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                             'int2','int4','int8','serial','bigserial',
                                             'real','double precision','float','float4','float8','oid'];
    v_genValList             TEXT;
    v_genSetList             TEXT;
    v_genPkConditions        TEXT;
    r_col                    RECORD;
    r_rel                    RECORD;
  BEGIN
--
-- Set the rel_has_always_ident_col column content
--
    IF emaj._pg_version_num() >= 100000 THEN
      FOR r_col IN
        SELECT DISTINCT rel_schema, rel_tblseq                 -- "generated always as identity" application table's columns
          FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
            AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
            AND upper_inf(rel_time_range) AND rel_kind = 'r'
            AND attidentity = 'a'
      LOOP
-- suppress the generated column name in the list of columns to insert at E-Maj rollback time
        UPDATE emaj.emaj_relation
          SET rel_has_always_ident_col = TRUE
          WHERE rel_schema = r_col.rel_schema AND rel_tblseq = r_col.rel_tblseq;     -- update all time ranges of this relation
      END LOOP;
    END IF;
--
-- Fix the emaj_relation.rel_sql_columns column content for cases when the application table has columns defined as
-- "GENERATED ALWAYS AS (expr)"
--
    IF emaj._pg_version_num() >= 120000 THEN
      FOR r_col IN
        SELECT rel_schema, rel_tblseq, attname, attgenerated                 -- generated application table's columns
          FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
            AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
            AND upper_inf(rel_time_range) AND rel_kind = 'r'
            AND attgenerated <> ''
      LOOP
-- suppress the generated column name in the list of columns to insert at E-Maj rollback time
        UPDATE emaj.emaj_relation
          SET rel_sql_rlbk_columns = regexp_replace(rel_sql_rlbk_columns,'tbl\.' || r_col.attname || '(,)?', '')
          WHERE rel_schema = r_col.rel_schema AND rel_tblseq = r_col.rel_tblseq AND upper_inf(rel_time_range);
      END LOOP;
    END IF;
--
-- Set the four new columns used by the sql script generation functions
--
    FOR r_rel IN
      SELECT rel_schema, rel_tblseq
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range) AND rel_kind = 'r'
      LOOP
-- reset variables
        v_genColList = '';
        v_isColListNeeded = FALSE;
        v_genValList = '';
        v_genSetList = '';
        v_genPkConditions = '';
-- get each column and their attributes
        FOR r_col IN EXECUTE format(
          ' SELECT attname, format_type(atttypid,atttypmod), %s AS attidentity, %s AS attgenerated'
          ' FROM pg_catalog.pg_attribute'
          ' WHERE attrelid = %s::regclass'
          '   AND attnum > 0 AND NOT attisdropped'
          ' ORDER BY attnum',
          CASE WHEN emaj._pg_version_num() >= 100000 THEN 'attidentity' ELSE '''''::TEXT' END,
          CASE WHEN emaj._pg_version_num() >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
          quote_literal(quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq)))
        LOOP
-- build the INSERT column list, excluding the GENERATED ALWAYS AS (expression) columns
          IF r_col.attgenerated = '' THEN
            v_genColList = v_genColList || quote_ident(replace(r_col.attname,'''','''''')) || ', ';
          ELSE
            v_isColListNeeded = TRUE;
          END IF;
-- test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric
-- data types)
          IF regexp_replace(r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
            IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
              v_genValList = v_genValList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::TEXT,''NULL'') || '', ';
            END IF;
            IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
              v_genSetList = v_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.'
                                          || quote_ident(r_col.attname) || ' ::TEXT,''NULL'') || '', ';
            END IF;
          ELSE
-- literal for this column must be quoted
            IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
              v_genValList = v_genValList || ''' || quote_nullable(o.' || quote_ident(r_col.attname) || ') || '', ';
            END IF;
            IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
              v_genSetList = v_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_nullable(n.'
                                          || quote_ident(r_col.attname) || ') || '', ';
            END IF;
          END IF;
        END LOOP;
-- suppress the final separators
        IF v_isColListNeeded THEN
          v_genColList = substring(v_genColList FROM 1 FOR char_length(v_genColList) - 2);
        ELSE
          v_genColList = '';
        END IF;
        v_genValList = substring(v_genValList FROM 1 FOR char_length(v_genValList) - 2);
        v_genSetList = substring(v_genSetList FROM 1 FOR char_length(v_genSetList) - 2);
-- retrieve all columns that represents the pkey and build the "pkey equal" conditions set that will be used in UPDATE and DELETE
--  statements (taking column names in pg_attribute from the table's definition instead of index definition is mandatory
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
        FOR r_col IN
          SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute, pg_catalog.pg_index
            WHERE pg_attribute.attrelid = pg_index.indrelid
              AND attnum = ANY (indkey)
              AND indrelid = (quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq))::regclass
              AND indisprimary
              AND attnum > 0 AND NOT attisdropped
        LOOP
-- test if the column format (at least up to the parenthesis) belongs to the list of formats that do not require any quotation
-- (like numeric data types)
          IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
            v_genPkConditions = v_genPkConditions || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.'
                                                  || quote_ident(r_col.attname) || ' || '' AND ';
          ELSE
-- literal for this column must be quoted
            v_genPkConditions = v_genPkConditions || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.'
                                                  || quote_ident(r_col.attname) || ') || '' AND ';
          END IF;
        END LOOP;
-- if the table has PK,
        IF v_genPkConditions <> '' THEN
-- ... suppress the final separator and update the emaj_relation row
          v_genPkConditions = substring(v_genPkConditions FROM 1 FOR char_length(v_genPkConditions) - 5);
          UPDATE emaj.emaj_relation
            SET rel_sql_gen_ins_col = v_genColList, rel_sql_gen_ins_val = v_genValList,
                rel_sql_gen_upd_set = v_genSetList, rel_sql_gen_pk_conditions = v_genPkConditions
            WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;     -- update all time ranges of this relation
        ELSE
-- ... otherwise, set all pieces of sql to NULL
          UPDATE emaj.emaj_relation
            SET rel_sql_rlbk_columns = NULL, rel_sql_rlbk_pk_columns = NULL, rel_sql_rlbk_pk_conditions = NULL
            WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq;     -- update all time ranges of this relation
        END IF;
    END LOOP;
  END;
$do$;

-- create indexes
-- index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- index on emaj_relation used to speedup _verify_schema() with large E-Maj configuration
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- recreate the foreign keys that point on this table
--   there is no fkey for this table

--
-- add created or recreated tables and sequences to the list of content to save by pg_dump
--
SELECT pg_catalog.pg_extension_config_dump('emaj_relation','');

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

------------------------------------
--                                --
-- emaj triggers                  --
--                                --
------------------------------------
-- Triggers for changes and truncate on the emaj_param table.

CREATE OR REPLACE FUNCTION emaj._emaj_param_change_fnct()
RETURNS TRIGGER LANGUAGE plpgsql AS
$_emaj_param_change_fnct$
  BEGIN
    IF (TG_OP = 'DELETE' AND OLD.param_key = 'emaj_version') OR
       ((TG_OP = 'UPDATE' OR TG_OP = 'INSERT') AND NEW.param_key = 'emaj_version') THEN
      RAISE EXCEPTION '_emaj_param_change_fnct: modifying the emaj_version key is not allowed.';
    END IF;
    IF TG_OP = 'DELETE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('', 'DELETED PARAMETER', OLD.param_key);
      RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'UPDATED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'INSERTED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'TRUNCATE' THEN
      RAISE EXCEPTION '_emaj_param_change_fnct: TRUNCATE the emaj_param table is not allowed.';
    END IF;
    RETURN NULL;
  END;
$_emaj_param_change_fnct$;

CREATE TRIGGER emaj_param_change_trg
  AFTER INSERT OR UPDATE OR DELETE  ON emaj.emaj_param
  FOR EACH ROW EXECUTE PROCEDURE emaj._emaj_param_change_fnct();

CREATE TRIGGER emaj_param_truncate_trg
  BEFORE TRUNCATE ON emaj.emaj_param
  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._emaj_param_change_fnct();

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
DROP FUNCTION IF EXISTS emaj._check_conf_groups(V_GROUPNAMES TEXT[]);
DROP FUNCTION IF EXISTS emaj._create_log_trigger(V_FULLTABLENAME TEXT,V_LOGTABLENAME TEXT,V_SEQUENCENAME TEXT,V_LOGFNCTNAME TEXT);
DROP FUNCTION IF EXISTS emaj._alter_groups(V_GROUPNAMES TEXT[],V_MULTIGROUP BOOLEAN,V_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._alter_exec(V_TIMEID BIGINT,V_MULTIGROUP BOOLEAN);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._emaj_group_def_change_fnct()
RETURNS TRIGGER LANGUAGE plpgsql AS
$_emaj_group_def_change_fnct$
-- This function is associated to the emaj_emaj_group_def_change_trg trigger set on the emaj_group_def table.
-- It sets the group_has_waiting_changes boolean column of the emaj_group table to TRUE when a change is recorded into the emaj_group_def
--   table.
-- If the group doesn't exists (yet), the update statements will silently not update any row
  BEGIN
    IF TG_OP = 'DELETE' THEN
      UPDATE emaj.emaj_group SET group_has_waiting_changes = TRUE
        WHERE group_name = OLD.grpdef_group;
      RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      UPDATE emaj.emaj_group SET group_has_waiting_changes = TRUE
        WHERE group_name = OLD.grpdef_group OR group_name = NEW.grpdef_group;
      RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
      UPDATE emaj.emaj_group SET group_has_waiting_changes = TRUE
        WHERE group_name = NEW.grpdef_group;
      RETURN NEW;
    ELSIF TG_OP = 'TRUNCATE' THEN
      UPDATE emaj.emaj_group SET group_has_waiting_changes = TRUE;
      RETURN NULL;
    END IF;
    RETURN NULL;
  END;
$_emaj_group_def_change_fnct$;

CREATE OR REPLACE FUNCTION emaj._emaj_param_change_fnct()
RETURNS TRIGGER LANGUAGE plpgsql AS
$_emaj_param_change_fnct$
  BEGIN
    IF (TG_OP = 'DELETE' AND OLD.param_key = 'emaj_version') OR
       ((TG_OP = 'UPDATE' OR TG_OP = 'INSERT') AND NEW.param_key = 'emaj_version') THEN
      RAISE EXCEPTION '_emaj_param_change_fnct: modifying the emaj_version key is not allowed.';
    END IF;
    IF TG_OP = 'DELETE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('', 'DELETED PARAMETER', OLD.param_key);
      RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'UPDATED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'INSERTED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'TRUNCATE' THEN
      RAISE EXCEPTION '_emaj_param_change_fnct: TRUNCATE the emaj_param table is not allowed.';
    END IF;
    RETURN NULL;
  END;
$_emaj_param_change_fnct$;

CREATE OR REPLACE FUNCTION emaj._check_json_groups_conf(v_groupsJson JSON)
RETURNS SETOF emaj._check_groups_conf_type LANGUAGE plpgsql AS
$_check_json_groups_conf$
-- This function verifies that the JSON structure that contains a tables groups configuration is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them.
-- It is called by the _import_groups_conf() function.
-- The function is also directly called by Emaj_web.
-- This function checks that:
--   - the "tables_groups" attribute exists
--   - "groups_name" attribute are defined
--   - no unknow attribute are listed in the group level
--   - the "is_rollbackable" attributes are boolean
--   - the "priority" attributes are numeric
--   - groups are not described several times
-- Input: the JSON structure to check
  DECLARE
    v_groupNumber            INT;
    v_group                  TEXT;
    v_tblseqNumber           INT;
    v_schema                 TEXT;
    v_tblseq                 TEXT;
    v_triggerNumber          INT;
    v_trigger                TEXT;
    r_group                  RECORD;
    r_table                  RECORD;
    r_trigger                RECORD;
    r_sequence               RECORD;
  BEGIN
-- extract the "tables_groups" json path and check that the attribute exists
    v_groupsJson = v_groupsJson #> '{"tables_groups"}';
    IF v_groupsJson IS NULL THEN
      RETURN QUERY
        VALUES (1, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT,
                'The JSON structure does not contain any "tables_groups" array.');
    ELSE
-- check that all keywords of the JSON structure are valid
-- process groups attributes
      v_groupNumber = 0;
      FOR r_group IN
        SELECT value AS groupJson FROM json_array_elements(v_groupsJson)
      LOOP
--   the group_name must be defined
        v_groupNumber = v_groupNumber + 1;
        v_group = r_group.groupJson ->> 'group';
        IF v_group IS NULL OR v_group = '' THEN
          RETURN QUERY
            VALUES (10, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_groupNumber::TEXT,
                    format('The tables group #%s has no "group" attribute.',
                           v_groupNumber::text));
        ELSE
--   other attributes of the group level must be known
          RETURN QUERY
            SELECT 11, 1, v_group, NULL::TEXT, NULL::TEXT, key,
                 format('For the tables group "%s", the keyword "%s" is unknown.',
                        v_group, key)
              FROM (
                SELECT key FROM json_object_keys(r_group.groupJson) AS x(key)
                  WHERE key NOT IN ('group', 'is_rollbackable', 'comment', 'tables', 'sequences')
                ) AS t;
--   if it exists, the "is_rollbackable" attribute must be boolean
          IF r_group.groupJson -> 'is_rollbackable' IS NOT NULL AND
             json_typeof(r_group.groupJson -> 'is_rollbackable') <> 'boolean' THEN
            RETURN QUERY
              VALUES (12, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT,
                      format('For the tables group "%s", the "is_rollbackable" attribute is not a boolean.',
                             v_group));
          END IF;
-- process tables attributes
          v_tblseqNumber = 0;
          FOR r_table IN
            SELECT value AS tableJson FROM json_array_elements(r_group.groupJson -> 'tables')
          LOOP
            v_tblseqNumber = v_tblseqNumber + 1;
            v_schema = r_table.tableJson ->> 'schema';
            v_tblseq = r_table.tableJson ->> 'table';
--   the schema and table attributes must exists
            IF v_schema IS NULL OR v_schema = '' THEN
              RETURN QUERY
                VALUES (20, 1, v_group, NULL::TEXT, NULL::TEXT, v_tblseqNumber::TEXT,
                        format('In the tables group "%s", the table #%s has no "schema" attribute.',
                               v_group, v_tblseqNumber::text));
            ELSIF v_tblseq IS NULL OR v_tblseq = '' THEN
              RETURN QUERY
                VALUES (21, 1, v_group, NULL::TEXT, NULL::TEXT, v_tblseqNumber::TEXT,
                        format('In the tables group "%s", the table #%s has no "table" attribute.',
                               v_group, v_tblseqNumber::text));
            ELSE
--   attributes of the tables level must exist
              RETURN QUERY
                SELECT 22, 1, v_group, v_schema, v_tblseq, key,
                     format('In the tables group "%s" and for the table %I.%I, the keyword "%s" is unknown.',
                            v_group, quote_ident(v_schema), quote_ident(v_tblseq), key)
                  FROM (
                    SELECT key FROM json_object_keys(r_table.tableJson) AS x(key)
                      WHERE key NOT IN ('schema', 'table', 'priority', 'log_data_tablespace',
                                        'log_index_tablespace', 'ignored_triggers')
                    ) AS t;
---- if it exists, the "priority" attribute must be a number
              IF r_table.tableJson -> 'priority' IS NOT NULL AND
                 json_typeof(r_table.tableJson -> 'priority') <> 'number' THEN
                RETURN QUERY
                  VALUES (23, 1, v_group, v_schema, v_tblseq, NULL::TEXT,
                          format('In the tables group "%s" and for the table %I.%I, the "priority" attribute is not a number.',
                                 v_group, quote_ident(v_schema), quote_ident(v_tblseq)));
              END IF;
-- process triggers attributes
              v_triggerNumber = 0;
              FOR r_trigger IN
                SELECT value AS triggerJson FROM json_array_elements(r_table.tableJson -> 'ignored_triggers')
              LOOP
                v_triggerNumber = v_triggerNumber + 1;
                v_trigger = r_trigger.triggerJson ->> 'trigger';
--   the "trigger" attribute must exists
                IF v_trigger IS NULL OR v_trigger = '' THEN
                  RETURN QUERY
                    VALUES (24, 1, v_group, v_schema, v_tblseq, v_triggerNumber::TEXT,
                            format('In the tables group "%s" and for the table %I.%I, the trigger #%s has no "trigger" attribute.',
                                   v_group, quote_ident(v_schema), quote_ident(v_tblseq), v_triggerNumber));
                ELSE
--   attributes of the ignored_triggers level must exist
                  RETURN QUERY
                    SELECT 25, 1, v_group, v_schema, v_tblseq, key,
                         format('In the tables group "%s" and for a trigger of the table %I.%I, the keyword "%s" is unknown.',
                                v_group, quote_ident(v_schema), quote_ident(v_tblseq), key)
                      FROM (
                        SELECT key FROM json_object_keys(r_trigger.triggerJson) AS x(key)
                          WHERE key NOT IN ('trigger')
                        ) AS t;
                END IF;
              END LOOP;
            END IF;
          END LOOP;
-- process sequences attributes
          v_tblseqNumber = 0;
          FOR r_sequence IN
            SELECT value AS sequenceJson FROM json_array_elements(r_group.groupJson -> 'sequences')
          LOOP
            v_tblseqNumber = v_tblseqNumber + 1;
            v_schema = r_sequence.sequenceJson ->> 'schema';
            v_tblseq = r_sequence.sequenceJson ->> 'sequence';
--   the schema and table attributes must exists
            IF v_schema IS NULL OR v_schema = '' THEN
              RETURN QUERY
                VALUES (30, 1, v_group, NULL::TEXT, NULL::TEXT, v_tblseqNumber::TEXT,
                        format('In the tables group "%s", the sequence #%s has no "schema" attribute.',
                               v_group, v_tblseqNumber::text));
            ELSIF v_tblseq IS NULL OR v_tblseq = '' THEN
              RETURN QUERY
                VALUES (31, 1, v_group, NULL::TEXT, NULL::TEXT, v_tblseqNumber::TEXT,
                        format('In the tables group "%s", the sequence #%s has no "sequence" attribute.',
                               v_group, v_tblseqNumber::text));
            ELSE
--   no other attributes of the sequences level must exist
              RETURN QUERY
                SELECT 32, 1, v_group, v_schema, v_tblseq, key,
                     format('In the tables group "%s" and for the sequence %I.%I, the keyword "%s" is unknown.',
                            v_group, quote_ident(v_schema), quote_ident(v_tblseq), key)
                  FROM (
                    SELECT key FROM json_object_keys(r_sequence.sequenceJson) AS x(key)
                      WHERE key NOT IN ('schema', 'sequence')
                    ) AS t;
            END IF;
          END LOOP;
        END IF;
      END LOOP;
-- check that tables groups are not configured more than once in the JSON structure
      RETURN QUERY
        SELECT 2, 1, "group", NULL::TEXT, NULL::TEXT, NULL::TEXT,
             format('The JSON structure references several times the tables group "%s".',
                    "group")
          FROM (
            SELECT "group", count(*)
              FROM json_to_recordset(v_groupsJson) AS x("group" TEXT)
              GROUP BY "group" HAVING count(*) > 1
            ) AS t;
    END IF;
--
    RETURN;
  END;
$_check_json_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._check_conf_groups(v_groupNames TEXT[])
RETURNS SETOF emaj._check_groups_conf_type LANGUAGE plpgsql AS
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
--  - for sequences, the tablespaces and emaj priority are all set to NULL
--  - for tables, configured tablespaces exist
-- The function is directly called by Emaj_web.
-- Input: name array of the tables groups to check
  BEGIN
-- check that all application tables and sequences listed for the group really exist
    RETURN QUERY
      SELECT 1, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table or sequence %s.%s does not exist.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def
        WHERE grpdef_group = ANY(v_groupNames)
          AND NOT EXISTS (
            SELECT 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid
                AND grpdef_schema = nspname AND grpdef_tblseq = relname
                AND relkind IN ('r','S','p'));
---- check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj)
    RETURN QUERY
      SELECT 2, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq
          AND grpdef_group = ANY(v_groupNames)
          AND relkind = 'p';
---- check no application schema listed for the group in the emaj_group_def table is an E-Maj schema
    RETURN QUERY
      SELECT 3, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table or sequence %s.%s belongs to an E-Maj schema.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, emaj.emaj_schema
        WHERE grpdef_group = ANY(v_groupNames)
          AND grpdef_schema = sch_name;
---- check that no table or sequence of the checked groups already belongs to other created groups
    RETURN QUERY
      SELECT 4, 1, grpdef_group, grpdef_schema, grpdef_tblseq, rel_group,
             format('in the group %s, the table or sequence %s.%s already belongs to the group %s.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(rel_group))
        FROM emaj.emaj_group_def, emaj.emaj_relation
        WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
          AND upper_inf(rel_time_range) AND grpdef_group = ANY (v_groupNames) AND NOT rel_group = ANY (v_groupNames);
---- check no table is a TEMP table
    RETURN QUERY
      SELECT 5, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is a TEMPORARY table.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
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
             format('the table %s.%s is assigned several times.',
                    quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM dupl;
---- check that the log data tablespaces for tables exist
    RETURN QUERY
      SELECT 12, 1, grpdef_group, grpdef_schema, grpdef_tblseq, grpdef_log_dat_tsp,
             format('in the group %s, for the table %s.%s, the data log tablespace %s does not exist.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(grpdef_log_dat_tsp))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND grpdef_log_dat_tsp IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = grpdef_log_dat_tsp);
---- check that the log index tablespaces for tables exist
    RETURN QUERY
      SELECT 13, 1, grpdef_group, grpdef_schema, grpdef_tblseq, grpdef_log_idx_tsp,
             format('in the group %s, for the table %s.%s, the index log tablespace %s does not exist.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq), quote_ident(grpdef_log_idx_tsp))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND grpdef_log_idx_tsp IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = grpdef_log_idx_tsp);
---- check no table is an unlogged table (blocking rollbackable groups only)
    RETURN QUERY
      SELECT 20, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s is an UNLOGGED table.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relpersistence = 'u';
---- with PG11- check no table is a WITH OIDS table (blocking rollbackable groups only)
    IF emaj._pg_version_num() < 120000 THEN
      RETURN QUERY
        SELECT 21, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
               format('in the group %s, the table %s.%s is declared WITH OIDS.',
                      quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
            AND grpdef_group = ANY (v_groupNames) AND relkind = 'r' AND relhasoids;
    END IF;
---- check every table has a primary key (blocking rollbackable groups only)
    RETURN QUERY
      SELECT 22, 2, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, the table %s.%s has no PRIMARY KEY.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                            AND contype = 'p' AND nspname = grpdef_schema AND relname = grpdef_tblseq);
---- all sequences described in emaj_group_def have their priority attribute set to NULL
    RETURN QUERY
      SELECT 31, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the priority is not NULL.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_priority IS NOT NULL;
---- all sequences described in emaj_group_def have their data log tablespace attribute set to NULL
    RETURN QUERY
      SELECT 32, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the data log tablespace is not NULL.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_dat_tsp IS NOT NULL;
---- all sequences described in emaj_group_def have their index log tablespace attribute set to NULL
    RETURN QUERY
      SELECT 33, 1, grpdef_group, grpdef_schema, grpdef_tblseq, NULL::TEXT,
             format('in the group %s, for the sequence %s.%s, the index log tablespace is not NULL.',
                    quote_ident(grpdef_group), quote_ident(grpdef_schema), quote_ident(grpdef_tblseq))
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_schema = nspname AND grpdef_tblseq = relname AND relnamespace = pg_namespace.oid
          AND grpdef_group = ANY (v_groupNames) AND relkind = 'S' AND grpdef_log_idx_tsp IS NOT NULL;
--
    RETURN;
  END;
$_check_conf_groups$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(v_schema TEXT, v_tables TEXT[], v_group TEXT, v_properties JSONB, v_mark TEXT,
                                               v_multiTable BOOLEAN, v_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB v_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
-- The function is created as SECURITY DEFINER so that log schemas can be owned by superuser
  DECLARE
    v_function               TEXT;
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_priority               INT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_extraProperties        JSONB;
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_schemaPrefix           TEXT = 'emaj_';
    v_logSchema              TEXT;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_nbAssignedTbl          INT = 0;
  BEGIN
    v_function = CASE WHEN v_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END;
-- insert the begin entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- check supplied parameters
-- check the group name and if ok, get some properties of the group
    PERFORM emaj._check_group_names(v_groupNames := ARRAY[v_group], v_mayBeNull := FALSE, v_lockGroups := TRUE, v_checkList := '');
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_group;
-- check the supplied schema exists and is not an E-Maj schema
    PERFORM 1 FROM pg_catalog.pg_namespace
      WHERE nspname = v_schema;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" does not exist.', v_schema;
    END IF;
    PERFORM 1 FROM emaj.emaj_schema
      WHERE sch_name = v_schema;
    IF FOUND THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" is an E-Maj schema.', v_schema;
    END IF;
-- check tables
    IF NOT v_arrayFromRegex THEN
-- from the tables array supplied by the user, remove duplicates values, NULL and empty strings from the supplied table names array
      SELECT array_agg(DISTINCT table_name) INTO v_tables FROM unnest(v_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- check that application tables exist
      WITH tables AS (
        SELECT unnest(v_tables) AS table_name)
      SELECT string_agg(quote_ident(table_name), ', ') INTO v_list
        FROM (
          SELECT table_name FROM tables
          WHERE NOT EXISTS (
            SELECT 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid
                AND nspname = v_schema AND relname = table_name
                AND relkind IN ('r','p'))
        ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) do not exist.', quote_ident(v_schema), v_list;
      END IF;
    END IF;
-- check or discard partitioned application tables (only elementary partitions can be managed by E-Maj)
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schema AND relname = ANY(v_tables)
        AND relkind = 'p';
    IF v_list IS NOT NULL THEN
      IF NOT v_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are partitionned tables (only elementary partitions are supported'
                        ' by E-Maj).', quote_ident(v_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some partitionned tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
      END IF;
    END IF;
-- check or discard TEMP tables
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schema AND relname = ANY(v_tables)
        AND relkind = 'r' AND relpersistence = 't';
    IF v_list IS NOT NULL THEN
      IF NOT v_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are TEMP tables.', quote_ident(v_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some TEMP tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
      END IF;
    END IF;
-- check or discard UNLOGGED tables in rollbackable groups
    IF v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = v_schema AND relname = ANY(v_tables)
          AND relkind = 'r' AND relpersistence = 'u';
      IF v_list IS NOT NULL THEN
        IF NOT v_arrayFromRegex THEN
          RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are UNLOGGED tables.', quote_ident(v_schema), v_list;
        ELSE
          RAISE WARNING '_assign_tables: Some UNLOGGED tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
        END IF;
      END IF;
    END IF;
-- with PG11-, check or discard WITH OIDS tables in rollbackable groups
    IF emaj._pg_version_num() < 120000 AND v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
        FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = v_schema AND relname = ANY(v_tables)
          AND relkind = 'r' AND relhasoids;
      IF v_list IS NOT NULL THEN
        IF NOT v_arrayFromRegex THEN
          RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are declared WITH OIDS.', quote_ident(v_schema), v_list;
        ELSE
          RAISE WARNING '_assign_tables: Some WITH OIDS tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
        END IF;
      END IF;
    END IF;
-- check or discard tables whithout primary key in rollbackable groups
    IF v_groupIsRollbackable THEN
      SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
        FROM pg_catalog.pg_class t, pg_catalog.pg_namespace
        WHERE t.relnamespace = pg_namespace.oid
          AND nspname = v_schema AND t.relname = ANY(v_tables)
          AND relkind = 'r'
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                            WHERE c.relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = c.oid
                            AND contype = 'p' AND nspname = v_schema AND c.relname = t.relname);
      IF v_list IS NOT NULL THEN
        IF NOT v_arrayFromRegex THEN
          RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) have no PRIMARY KEY.', quote_ident(v_schema), v_list;
        ELSE
          RAISE WARNING '_assign_tables: Some tables without PRIMARY KEY (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
        END IF;
      END IF;
    END IF;
-- check or discard tables already assigned to a group
    SELECT string_agg(quote_ident(rel_tblseq), ', '), array_agg(rel_tblseq) INTO v_list, v_array
      FROM emaj.emaj_relation
      WHERE rel_schema = v_schema AND rel_tblseq = ANY(v_tables) AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      IF NOT v_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) already belong to a group.', quote_ident(v_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some tables already belonging to a group (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO v_tables
          FROM (SELECT unnest(v_tables) EXCEPT SELECT unnest(v_array)) AS t(remaining_table);
      END IF;
    END IF;
-- check the priority is numeric
    BEGIN
      v_priority = (v_properties->>'priority')::INT;
    EXCEPTION
      WHEN invalid_text_representation THEN
        RAISE EXCEPTION '_assign_tables: the "priority" property is not numeric.';
    END;
-- check that the tablespaces exist, if supplied
    v_logDatTsp = v_properties->>'log_data_tablespace';
    IF v_logDatTsp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = v_logDatTsp) THEN
      RAISE EXCEPTION '_assign_tables: the log data tablespace "%" does not exists.', v_logDatTsp;
    END IF;
    v_logIdxTsp = v_properties->>'log_index_tablespace';
    IF v_logIdxTsp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tablespace WHERE spcname = v_logIdxTsp) THEN
      RAISE EXCEPTION '_assign_tables: the log index tablespace "%" does not exists.', v_logIdxTsp;
    END IF;
-- check no properties are unknown
    v_extraProperties = v_properties - 'priority' - 'log_data_tablespace' - 'log_index_tablespace';
    IF v_extraProperties IS NOT NULL AND v_extraProperties <> '{}' THEN
      RAISE EXCEPTION '_assign_tables: properties "%" are unknown.', v_extraProperties;
    END IF;
-- check the supplied mark
    SELECT emaj._check_new_mark(array[v_group], v_mark) INTO v_markName;
-- OK,
    IF v_tables IS NULL OR v_tables = '{}' THEN
-- when no tables are finaly selected, just warn
      RAISE WARNING '_assign_tables: No table to process.';
    ELSE
-- get the time stamp of the operation
      SELECT emaj._set_time_stamp('A') INTO v_timeId;
-- for LOGGING groups, lock all tables to get a stable point
      IF v_groupIsLogging THEN
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(ARRAY[v_group], 'ROW EXCLUSIVE', FALSE);
-- and set the mark, using the same time identifier
        PERFORM emaj._set_mark_groups(ARRAY[v_group], v_markName, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- create new log schemas if needed
      v_logSchema = v_schemaPrefix || v_schema;
      IF NOT EXISTS (SELECT 0 FROM emaj.emaj_schema WHERE sch_name = v_logSchema) THEN
-- check that the schema doesn't already exist
        PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchema;
        IF FOUND THEN
          RAISE EXCEPTION '_assign_tables: The schema "%" should not exist. Drop it manually.',v_logSchema;
        END IF;
-- create the schema and give the appropriate rights
        EXECUTE format('CREATE SCHEMA %I',
                       v_logSchema);
        EXECUTE format('GRANT ALL ON SCHEMA %I TO emaj_adm',
                       v_logSchema);
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                       v_logSchema);
-- and record the schema creation into the emaj_schema and the emaj_hist tables
        INSERT INTO emaj.emaj_schema (sch_name) VALUES (v_logSchema);
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES (CASE WHEN v_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END, 'LOG_SCHEMA CREATED', quote_ident(v_logSchema));
      END IF;
-- disable event triggers that protect emaj components and keep in memory these triggers name
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- effectively create the log components for each table
      FOREACH v_oneTable IN ARRAY v_tables
      LOOP
-- create the table
        PERFORM emaj._add_tbl(v_schema, v_oneTable, v_group, v_priority, v_logDatTsp, v_logIdxTsp, v_groupIsLogging,
                              v_timeId, v_function);
-- insert an entry into the emaj_alter_plan table (so that future rollback may see the change)
        INSERT INTO emaj.emaj_alter_plan (altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_group_is_logging)
          VALUES (v_timeId, 'ADD_TBL', v_schema, v_oneTable, v_group, v_groupIsLogging);
        v_nbAssignedTbl = v_nbAssignedTbl + 1;
      END LOOP;
-- enable previously disabled event triggers
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- adjust the group characteristics
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (SELECT count(*) FROM emaj.emaj_relation
                                WHERE rel_group = group_name AND upper_inf(rel_time_range) AND rel_kind = 'r')
        WHERE group_name = v_group;
-- if the group is logging, check foreign keys with tables outside the groups (otherwise the check will be done at the group start time)
      IF v_groupIsLogging THEN
        PERFORM emaj._check_fk_groups(array[v_group]);
      END IF;
    END IF;
-- insert the end entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbAssignedTbl || ' tables assigned to the group ' || v_group);
    RETURN v_nbAssignedTbl;
  END;
$_assign_tables$;

CREATE OR REPLACE FUNCTION emaj._create_tbl(v_schema TEXT, v_tbl TEXT, v_groupName TEXT, v_priority INT, v_logDatTsp TEXT,
                                            v_logIdxTsp TEXT, v_timeId BIGINT, v_groupIsRollbackable BOOLEAN, v_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table.
-- Input: the application table to process, the group to add it into, the priority and tablespaces attributes, the time id of the
--        operation, 2 booleans indicating whether the group is rollbackable and whether the group is currently in logging state.
-- The objects created in the log schema:
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
    v_rlbkColList            TEXT;
    v_rlbkPkColList          TEXT;
    v_rlbkPkConditions       TEXT;
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
-- the checks on the table properties are performed by the calling functions
-- build the prefix of all emaj object to create
    IF length(v_tbl) <= 50 THEN
-- for not too long table name, the prefix is the table name itself
      v_emajNamesPrefix = v_tbl;
    ELSE
-- for long table names (over 50 char long), compute the suffix to add to the first 50 characters (#1, #2, ...), by looking at the
-- existing names
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
-- create the log table: it looks like the application table, with some additional technical columns
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('CREATE TABLE %s (LIKE %s) %s',
                    v_logTableName, v_fullTableName, v_dataTblSpace);
    EXECUTE format('ALTER TABLE %s'
                   ' ADD COLUMN emaj_verb      VARCHAR(3),'
                   ' ADD COLUMN emaj_tuple     VARCHAR(3),'
                   ' ADD COLUMN emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
                   ' ADD COLUMN emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
                   ' ADD COLUMN emaj_txid      BIGINT      DEFAULT txid_current(),'
                   ' ADD COLUMN emaj_user      VARCHAR(32) DEFAULT session_user',
                   v_logTableName);
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
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_alter_log_table_param);
    END IF;
-- create the index on the log table
    EXECUTE format('CREATE UNIQUE INDEX %s ON %s(emaj_gid, emaj_tuple)',
                    v_logIdxName, v_logTableName, v_idxTblSpace);
-- set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE format('ALTER TABLE ONLY %s CLUSTER ON %s',
                   v_logTableName, v_logIdxName);
-- remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables
    SELECT string_agg(action, ',') INTO v_stmt FROM (
      SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
        FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
          AND nspname = v_logSchema AND relname = v_baseLogTableName
          AND attnum > 0 AND attnum < v_attnum AND attisdropped = FALSE AND attnotnull) AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_stmt);
    END IF;
-- create the sequence associated to the log table
    EXECUTE format('CREATE SEQUENCE %s',
                   v_sequenceName);
-- create the log function and the log trigger
    PERFORM emaj._create_log_trigger_tbl(v_fullTableName, v_logTableName, v_sequenceName, v_logFnctName);
-- If the group is idle, deactivate the log trigger (it will be enabled at emaj_start_group time)
    IF NOT v_groupIsLogging THEN
      EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_log_trg',
                     v_fullTableName);
    END IF;
-- creation of the trigger that manage any TRUNCATE on the application table
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                   v_fullTableName);
    IF v_groupIsRollbackable THEN
-- For rollbackable groups, use the common _forbid_truncate_fnct() function that blocks the operation
      EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                     '  BEFORE TRUNCATE ON %s'
                     '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()',
                     v_fullTableName);
    ELSE
-- For audit_only groups, use the common _log_truncate_fnct() function that records the operation into the log table
      EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                     '  BEFORE TRUNCATE ON %s'
                     '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._log_truncate_fnct()',
                     v_fullTableName);
    END IF;
    IF NOT v_groupIsLogging THEN
      EXECUTE format('ALTER TABLE %s DISABLE TRIGGER emaj_trunc_trg',
                     v_fullTableName);
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                   v_logTableName);
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE %s TO emaj_adm',
                   v_logTableName);
    EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                   v_sequenceName);
    EXECUTE format('GRANT ALL PRIVILEGES ON SEQUENCE %s TO emaj_adm',
                   v_sequenceName);
-- Build some pieces of SQL statements that will be needed at table rollback and gen_sql times
--   left NULL if the table hos no pkey
    SELECT * FROM emaj._build_sql_tbl(v_fullTableName)
      INTO v_rlbkColList, v_rlbkPkColList, v_rlbkPkConditions, v_genColList,
           v_genValList, v_genSetList, v_genPkConditions, v_nbGenAlwaysIdentCol;
-- register the table into emaj_relation
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function, rel_emaj_verb_attnum, rel_has_always_ident_col,
                rel_sql_rlbk_columns, rel_sql_rlbk_pk_columns, rel_sql_rlbk_pk_conditions,
                rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions)
        VALUES (v_schema, v_tbl, int8range(v_timeId, NULL, '[)'), v_groupName, v_priority,
                v_logSchema, v_logDatTsp, v_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName, v_attnum, v_nbGenAlwaysIdentCol > 0,
                v_rlbkColList, v_rlbkPkColList, v_rlbkPkConditions,
                v_genColList, v_genValList, v_genSetList, v_genPkConditions);
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    SELECT string_agg(tgname, ', ' ORDER BY tgname) INTO v_triggerList FROM (
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'emaj\\_%\\_trg') AS t;
-- if yes, issue a warning
--   (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers (%). They will be automatically disabled during E-Maj rollback operations,'
                    ' unless they have been recorded into the list of triggers that may be kept enabled, with the'
                    ' emaj_ignore_app_trigger() function.', v_fullTableName, v_triggerList;
    END IF;
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._create_log_trigger_tbl(v_fullTableName TEXT, v_logTableName TEXT, v_sequenceName TEXT, v_logFnctName TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_trigger_tbl$
-- The function creates the log function and the associated log trigger for an application table.
-- It is called by several functions.
-- Inputs: the full name of the application table, the log table, the log sequence and the log function
-- The function is defined as SECURITY DEFINER so that emaj_adm role can manage the trigger on the application table.
  DECLARE
  BEGIN
-- drop the log trigger if it exists
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                   v_fullTableName);
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
    EXECUTE format('CREATE TRIGGER emaj_log_trg'
                   ' AFTER INSERT OR UPDATE OR DELETE ON %s'
                   '  FOR EACH ROW EXECUTE PROCEDURE %s()',
                   v_fullTableName, v_logFnctName);
    RETURN;
  END;
$_create_log_trigger_tbl$;

CREATE OR REPLACE FUNCTION emaj._build_sql_tbl(v_fullTableName TEXT, OUT v_rlbkColList TEXT, OUT v_rlbkPkColList TEXT,
                                               OUT v_rlbkPkConditions TEXT, OUT v_genColList TEXT, OUT v_genValList TEXT,
                                               OUT v_genSetList TEXT, OUT v_genPkConditions TEXT, OUT v_nbGenAlwaysIdentCol INT)
LANGUAGE plpgsql AS
$_build_sql_tbl$
-- This function creates all pieces of SQL that will be recorded into the emaj_relation table, for one application table.
-- They will later be used at rollback or SQL script generation time.
-- All SQL pieces are left empty if the table has no pkey, neither rollback nor sql script generation operations being possible
--   in this case
-- The Insert columns list remains empty if it is not needed to have a specific list (i.e. when the application table does not contain
--   any generated column)
-- Input: the full application table name
-- Output: 7 pieces of SQL, and the number of columns declared GENERATED ALWAYS AS IDENTITY
  DECLARE
    v_stmt                   TEXT;
    v_nbGenAlwaysExprCol     INTEGER;
    v_unquotedType           TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                             'int2','int4','int8','serial','bigserial',
                                             'real','double precision','float','float4','float8','oid'];
    r_col                    RECORD;
  BEGIN
--   build the pkey columns list and the "equality on the primary key" conditions for the rollback function
--     and for the UPDATE and DELETE statements of the sql generation function
--     (it takes column names in pg_attribute from the table's definition instead of index definition is mandatory
--     starting from pg9.0, joining tables with indkey instead of indexrelid)
    SELECT string_agg(quote_ident(attname), ','),
           string_agg('tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname), ' AND '),
           string_agg(
             CASE WHEN format_type = ANY(v_unquotedType) THEN
               quote_ident(replace(attname,'''','''''')) || ' = '' || o.' || quote_ident(attname) || ' || '''
                  ELSE
               quote_ident(replace(attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(attname) || ') || '''
             END, ' AND ')
      INTO v_rlbkPkColList, v_rlbkPkConditions, v_genPkConditions
      FROM (
        SELECT attname, regexp_replace(format_type(atttypid,atttypmod),E'\\(.*$','') AS format_type
          FROM pg_catalog.pg_attribute, pg_catalog.pg_index
          WHERE pg_attribute.attrelid = pg_index.indrelid
            AND attnum = ANY (indkey)
            AND indrelid = v_fullTableName::regclass AND indisprimary
            AND attnum > 0 AND attisdropped = FALSE
          ORDER BY attnum) AS t;
--
-- retrieve from pg_attribute simple columns list and indicators
-- if the table has no pkey, keep all the sql pieces to NULL (rollback or sql script generation operations being impossible)
    IF v_rlbkPkColList IS NOT NULL THEN
      v_stmt = 'SELECT string_agg(''tbl.'' || quote_ident(attname), '','') FILTER (WHERE attgenerated = ''''),'
--                             the columns list for rollback, excluding the GENERATED ALWAYS AS (expression) columns
               '       string_agg(quote_ident(replace(attname,'''''''','''''''''''')), '', '') FILTER (WHERE attgenerated = ''''),'
--                             the INSERT columns list for sql generation, excluding the GENERATED ALWAYS AS (expression) columns
               '       count(*) FILTER (WHERE attidentity = ''a''),'
--                             the number of GENERATED ALWAYS AS IDENTITY columns
               '       count(*) FILTER (WHERE attgenerated <> '''')'
--                             the number of GENERATED ALWAYS AS (expression) columns
               '  FROM ('
               '  SELECT attname, %s AS attidentity, %s AS attgenerated'
               '    FROM pg_catalog.pg_attribute'
               '    WHERE attrelid = %s::regclass'
               '      AND attnum > 0 AND NOT attisdropped'
               '  ORDER BY attnum) AS t';
      EXECUTE format(v_stmt,
                     CASE WHEN emaj._pg_version_num() >= 100000 THEN 'attidentity' ELSE '''''::TEXT' END,
                     CASE WHEN emaj._pg_version_num() >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                     quote_literal(v_fullTableName))
        INTO v_rlbkColList, v_genColList, v_nbGenAlwaysIdentCol, v_nbGenAlwaysExprCol;
      IF v_nbGenAlwaysExprCol = 0 THEN
-- if the table doesn't contain any generated columns, the is no need for the columns list in the INSERT clause
        v_genColList = '';
      END IF;
--
-- retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements
-- the logic is too complex to be build with aggregate functions. So loop on all columns.
      v_genValList = '';
      v_genSetList = '';
      FOR r_col IN EXECUTE format(
        ' SELECT attname, format_type(atttypid,atttypmod) AS format_type, %s AS attidentity, %s AS attgenerated'
        ' FROM pg_catalog.pg_attribute'
        ' WHERE attrelid = %s::regclass'
        '   AND attnum > 0 AND NOT attisdropped'
        ' ORDER BY attnum',
        CASE WHEN emaj._pg_version_num() >= 100000 THEN 'attidentity' ELSE '''''::TEXT' END,
        CASE WHEN emaj._pg_version_num() >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
        quote_literal(v_fullTableName))
      LOOP
-- test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric
-- data types)
        IF regexp_replace(r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            v_genValList = v_genValList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::TEXT,''NULL'') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            v_genSetList = v_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.'
                                        || quote_ident(r_col.attname) || ' ::TEXT,''NULL'') || '', ';
          END IF;
        ELSE
-- literal for this column must be quoted
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            v_genValList = v_genValList || ''' || quote_nullable(o.' || quote_ident(r_col.attname) || ') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            v_genSetList = v_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_nullable(n.'
                                        || quote_ident(r_col.attname) || ') || '', ';
          END IF;
        END IF;
      END LOOP;
-- suppress the final separators
      v_genValList = substring(v_genValList FROM 1 FOR char_length(v_genValList) - 2);
      v_genSetList = substring(v_genSetList FROM 1 FOR char_length(v_genSetList) - 2);
    END IF;
    RETURN;
  END;
$_build_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._remove_tbl(v_schema TEXT, v_table TEXT, v_group TEXT, v_groupIsLogging BOOLEAN,
                                            v_timeId BIGINT, v_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
    IF NOT v_groupIsLogging THEN
-- if the group is in idle state, drop the table immediately
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*, v_timeId) FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
    ELSE
-- if the group is in logging state, ...
-- ... get the current relation characteristics
      SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_function, rel_log_sequence
        INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logFunction, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
-- ... get the current log sequence characteristics
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO STRICT v_logSequenceLastValue
        FROM emaj.emaj_sequence
        WHERE sequ_schema = v_logSchema AND sequ_name = v_logSequence AND sequ_time_id = v_timeId;
-- ... compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names
      SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
        FROM
          (SELECT unnest(regexp_matches(rel_log_table,'_(\d+)$'))::INT AS suffix
             FROM emaj.emaj_relation
             WHERE rel_schema = v_schema AND rel_tblseq = v_table
          ) AS t;
-- ... rename the log table and its index (they may have been dropped)
      EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogTable, v_currentLogTable || v_namesSuffix);
      EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                     v_logSchema, v_currentLogIndex, v_currentLogIndex || v_namesSuffix);
-- ... drop the log and truncate triggers
--     (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
      v_fullTableName  = quote_ident(v_schema) || '.' || quote_ident(v_table);
      PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = v_schema AND relname = v_table AND relkind = 'r';
      IF FOUND THEN
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                       v_fullTableName);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                       v_fullTableName);
      END IF;
-- ... drop the log function and the log sequence
-- (but we keep the sequence related data in the emaj_sequence and the emaj_seq_hole tables)
      EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                     v_logSchema, v_logFunction);
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     v_logSchema, v_logSequence);
-- ... register the end of the relation time frame, the last value of the log sequence, the log table and index names change,
-- reflect the changes into the emaj_relation rows
--   - for all timeranges pointing to this log table and index
--     (do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
      UPDATE emaj.emaj_relation
        SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix,
            rel_log_function = NULL, rel_sql_rlbk_columns = NULL, rel_sql_rlbk_pk_columns = NULL, rel_sql_rlbk_pk_conditions = NULL,
            rel_log_seq_last_value = v_logSequenceLastValue
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND rel_log_table = v_currentLogTable;
--   - and close the last timerange
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range), v_timeId, '[)')
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'TABLE REMOVED', quote_ident(v_schema) || '.' || quote_ident(v_table),
              'From the ' || CASE WHEN v_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || v_group);
    RETURN;
  END;
$_remove_tbl$;

CREATE OR REPLACE FUNCTION emaj._move_tbl(v_schema TEXT, v_table TEXT, v_oldGroup TEXT, v_oldGroupIsLogging BOOLEAN, v_newGroup TEXT,
                                          v_newGroupIsLogging BOOLEAN, v_timeId BIGINT, v_function TEXT)
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
-- get the current relation characteristics
    SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_sequence,
           coalesce('TABLESPACE ' || quote_ident(rel_log_dat_tsp),''), coalesce('TABLESPACE ' || quote_ident(rel_log_idx_tsp),'')
      INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logSequence,
           v_dataTblSpace, v_idxTblSpace
      FROM emaj.emaj_relation
      WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
-- compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names
    SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
      FROM
        (SELECT unnest(regexp_matches(rel_log_table,'_(\d+)$'))::INT AS suffix
           FROM emaj.emaj_relation
           WHERE rel_schema = v_schema AND rel_tblseq = v_table
        ) AS t;
-- rename the log table and its index (they may have been dropped)
    EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                   v_logSchema, v_currentLogTable, v_currentLogTable || v_namesSuffix);
    EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                   v_logSchema, v_currentLogIndex, v_currentLogIndex || v_namesSuffix);
-- update emaj_relation to reflect the log table and index rename for all concerned rows
    UPDATE emaj.emaj_relation
      SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix
      WHERE rel_schema = v_schema AND rel_tblseq = v_table AND rel_log_table = v_currentLogTable;
-- create the new log table, by copying the just renamed table structure
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS) %s',
                    v_logSchema, v_currentLogTable, v_logSchema, v_currentLogTable || v_namesSuffix, v_dataTblSpace);
-- create the index on the new log table
    EXECUTE format('CREATE UNIQUE INDEX %I ON %I.%I(emaj_gid, emaj_tuple) %s',
                    v_currentLogIndex, v_logSchema, v_currentLogTable, v_idxTblSpace);
-- set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE format('ALTER TABLE ONLY %I.%I CLUSTER ON %I',
                   v_logSchema, v_currentLogTable, v_currentLogIndex);
-- grant appropriate rights to both emaj roles
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO emaj_viewer',
                   v_logSchema, v_currentLogTable);
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE %I.%I TO emaj_adm',
                   v_logSchema, v_currentLogTable);
-- register the end of the previous relation time frame and create a new relation time frame with the new group
    UPDATE emaj.emaj_relation
      SET rel_time_range = int8range(lower(rel_time_range),v_timeId,'[)')
      WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema,
                                    rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
                                    rel_emaj_verb_attnum, rel_has_always_ident_col,
                                    rel_sql_rlbk_columns, rel_sql_rlbk_pk_columns, rel_sql_rlbk_pk_conditions,
                                    rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
                                    rel_log_seq_last_value)
      SELECT rel_schema, rel_tblseq, int8range(v_timeId, NULL, '[)'), v_newGroup, rel_kind, rel_priority, rel_log_schema,
             v_currentLogTable, rel_log_dat_tsp, v_currentLogIndex, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
             rel_emaj_verb_attnum, rel_has_always_ident_col,
             rel_sql_rlbk_columns, rel_sql_rlbk_pk_columns, rel_sql_rlbk_pk_conditions,
             rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
             rel_log_seq_last_value
        FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper(rel_time_range) = v_timeId;
-- if the table is moved from an idle group to a group in logging state,
    IF NOT v_oldGroupIsLogging AND v_newGroupIsLogging THEN
-- ... get the log schema and sequence for the new relation
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_table AND upper_inf(rel_time_range);
-- ... record the new log sequence state in the emaj_sequence table for the current operation mark
      INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                  sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
        SELECT * FROM emaj._get_current_sequence_state(v_logSchema, v_logSequence, v_timeId);
    END IF;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'TABLE MOVED', quote_ident(v_schema) || '.' || quote_ident(v_table),
              'From the ' || CASE WHEN v_oldGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || v_oldGroup ||
              ' to the ' || CASE WHEN v_newGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || v_newGroup);
    RETURN;
  END;
$_move_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_tbl$
-- The function deletes a timerange for a table. This centralizes the deletion of all what has been created by _create_tbl() function.
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess, time id.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
  BEGIN
    v_fullTableName    = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- if the table is currently linked to a group, drop the log trigger, function and sequence
    IF upper_inf(r_rel.rel_time_range) THEN
-- check the table exists before dropping its triggers
      PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid
          AND nspname = r_rel.rel_schema AND relname = r_rel.rel_tblseq AND relkind = 'r';
      IF FOUND THEN
-- drop the log and truncate triggers on the application table
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %s',
                       v_fullTableName);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %s',
                       v_fullTableName);
      END IF;
-- drop the log function
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                       r_rel.rel_log_schema, r_rel.rel_log_function);
      END IF;
-- drop the sequence associated to the log table
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_sequence);
    END IF;
-- drop the log table if it is not referenced on other timeranges (for potentially other groups)
    IF NOT EXISTS(SELECT 1 FROM emaj.emaj_relation
                    WHERE rel_log_schema = r_rel.rel_log_schema AND rel_log_table = r_rel.rel_log_table
                      AND rel_time_range <> r_rel.rel_time_range) THEN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END IF;
-- process log sequence information if the sequence is not referenced in other timerange (for potentially other groups)
    IF NOT EXISTS(SELECT 1 FROM emaj.emaj_relation
                    WHERE rel_log_schema = r_rel.rel_log_schema AND rel_log_sequence = r_rel.rel_log_sequence
                      AND rel_time_range <> r_rel.rel_time_range) THEN
-- delete rows related to the log sequence from emaj_sequence table
-- (it may delete rows for other already processed time_ranges for the same table)
      DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_log_schema AND sequ_name = r_rel.rel_log_sequence;
-- delete rows related to the table from emaj_seq_hole table
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks)
      DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq;
    END IF;
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

CREATE OR REPLACE FUNCTION emaj._create_seq(v_schema TEXT, v_seq TEXT, v_groupName TEXT, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_create_seq$
-- The function records a sequence into a tables group
-- Required inputs: the application sequence to process, the group to add it into, the priority attribute, the time id of the operation.
  BEGIN
-- record the sequence in the emaj_relation table
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind)
      VALUES (v_schema, v_seq, int8range(v_timeId, NULL, '[)'), v_groupName, 'S');
    RETURN;
  END;
$_create_seq$;

CREATE OR REPLACE FUNCTION emaj._move_seq(v_schema TEXT, v_sequence TEXT, v_oldGroup TEXT, v_oldGroupIsLogging BOOLEAN, v_newGroup TEXT,
                                          v_newGroupIsLogging BOOLEAN, v_timeId BIGINT, v_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_seq$
-- The function change the group ownership of a sequence. It is called during an alter group or a dynamic assignment operation.
-- Required inputs: schema and sequence to move, old and new group names and their logging state,
--                  time stamp id of the operation, main calling function.
  BEGIN
-- register the end of the previous relation time frame and create a new relation time frame with the new group
    UPDATE emaj.emaj_relation
      SET rel_time_range = int8range(lower(rel_time_range),v_timeId,'[)')
      WHERE rel_schema = v_schema AND rel_tblseq = v_sequence AND upper_inf(rel_time_range);
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind)
      SELECT rel_schema, rel_tblseq, int8range(v_timeId, NULL, '[)'), v_newGroup, rel_kind
        FROM emaj.emaj_relation
        WHERE rel_schema = v_schema AND rel_tblseq = v_sequence AND upper(rel_time_range) = v_timeId;
-- insert an entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'SEQUENCE MOVED', quote_ident(v_schema) || '.' || quote_ident(v_sequence),
              'From the ' || CASE WHEN v_oldGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || v_oldGroup ||
              ' to the ' || CASE WHEN v_newGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || v_newGroup);
    RETURN;
  END;
$_move_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation, v_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence timerange.
-- Required inputs: row from emaj_relation corresponding to the appplication sequence to proccess.
  BEGIN
-- delete rows from emaj_sequence, but only when dealing with the last timerange of the sequence
    IF NOT EXISTS(SELECT 1 FROM emaj.emaj_relation
                    WHERE rel_schema = r_rel.rel_schema AND rel_tblseq = r_rel.rel_tblseq
                      AND rel_time_range <> r_rel.rel_time_range) THEN
      DELETE FROM emaj.emaj_sequence WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq;
    END IF;
-- keep a trace of the sequence group ownership history and finaly delete the sequence timerange from the emaj_relation table
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

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_minGlobalSeq BIGINT, v_maxGlobalSeq BIGINT, v_nbSession INT,
                                          v_isLoggedRlbk BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence.
-- The function is called by emaj._rlbk_session_exec().
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
    v_nbPk                   BIGINT;
  BEGIN
    v_fullTableName  = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName   = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName,
              'All log rows with emaj_gid > ' || v_minGlobalSeq || ' and <= ' || v_maxGlobalSeq);
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
    EXECUTE format('CREATE %s TABLE %s AS '
                   '  SELECT %s, min(emaj_gid) as emaj_gid FROM %s'
                   '    WHERE emaj_gid > %s AND emaj_gid <= %s'
                   '    GROUP BY %s',
                   v_tableType, v_tmpTable, r_rel.rel_sql_rlbk_pk_columns, v_logTableName,
                   v_minGlobalSeq, v_maxGlobalSeq, r_rel.rel_sql_rlbk_pk_columns);
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- delete all rows from the application table corresponding to each touched primary key
--   this deletes rows inserted or updated during the rolled back period
    EXECUTE format('DELETE FROM ONLY %s tbl USING %s keys WHERE %s',
                   v_fullTableName, v_tmpTable, r_rel.rel_sql_rlbk_pk_conditions);
-- for logged rollbacks, if the number of pkey to process is greater than 1.000, ANALYZE the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement
    IF v_isLoggedRlbk AND v_nbPk > 1000 THEN
      EXECUTE format('ANALYZE %s',
                     v_logTableName);
    END IF;
-- insert into the application table rows that were deleted or updated during the rolled back period
    EXECUTE format('INSERT INTO %s (%s) %s'
                   '  SELECT %s FROM %s tbl, %s keys '
                   '    WHERE %s AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
                   '      AND tbl.emaj_gid > %s AND tbl.emaj_gid <= %s',
                   v_fullTableName, replace(r_rel.rel_sql_rlbk_columns, 'tbl.',''),
                   CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
                   r_rel.rel_sql_rlbk_columns, v_logTableName, v_tmpTable,
                   r_rel.rel_sql_rlbk_pk_conditions,
                   v_minGlobalSeq, v_maxGlobalSeq);
-- drop the now useless temporary table
    EXECUTE format('DROP TABLE %s',
                   v_tmpTable);
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
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- delete obsolete log rows
    EXECUTE format('DELETE FROM %I.%I WHERE emaj_gid > %s',
                   r_rel.rel_log_schema, r_rel.rel_log_table, v_lastGlobalSeq);
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- record the sequence holes generated by the delete operation
-- this is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group() function
--   (and indirectly by emaj_estimate_rollback_group() and emaj_estimate_rollback_groups())
-- first delete, if exist, sequence holes that have disappeared with the rollback
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema AND sqhl_table = r_rel.rel_tblseq
        AND sqhl_begin_time_id >= v_beginTimeId AND sqhl_begin_time_id < v_endTimeId;
-- and then insert the new sequence hole
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE format('INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)'
                     ' VALUES (%L, %L, %s, %s, ('
                     '   SELECT CASE WHEN rel.is_called THEN rel.last_value + increment_by ELSE rel.last_value END'
                     '     FROM %I.%I rel, pg_sequences'
                     '     WHERE schemaname = %L AND sequencename = %L'
                     '   )-('
                     '   SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END'
                     '     FROM emaj.emaj_sequence'
                     '     WHERE sequ_schema = %L AND sequ_name = %L AND sequ_time_id = %s))',
                     r_rel.rel_schema, r_rel.rel_tblseq, v_beginTimeId, v_endTimeId, r_rel.rel_log_schema, r_rel.rel_log_sequence,
                     r_rel.rel_log_schema, r_rel.rel_log_sequence, r_rel.rel_log_schema, r_rel.rel_log_sequence, v_beginTimeId);
    ELSE
      EXECUTE format('INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)'
                     ' VALUES (%L, %L, %s, %s, ('
                     '   SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM %I.%I'
                     '   )-('
                     '   SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END'
                     '     FROM emaj.emaj_sequence'
                     '     WHERE sequ_schema = %L AND sequ_name = %L AND sequ_time_id = %s))',
                     r_rel.rel_schema, r_rel.rel_tblseq, v_beginTimeId, v_endTimeId, r_rel.rel_log_schema, r_rel.rel_log_sequence,
                     r_rel.rel_log_schema, r_rel.rel_log_sequence, v_beginTimeId);
    END IF;
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_ignore_app_trigger(v_action TEXT, v_schema TEXT, v_table TEXT, v_trigger TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_ignore_app_trigger$
-- This function records the list of application table triggers that must not be automatically disabled when launching a rollback
-- operation.
-- Input: the action to perform, either 'ADD' or 'REMOVE',
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
-- check that the trigger pattern effectively filter at least one trigger for the table
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
        AND tgname LIKE v_trigger AND NOT tgisinternal
        AND tgname IN ('emaj_trunc_trg', 'emaj_log_trg');
    IF v_trgList IS NOT NULL THEN
      RAISE WARNING 'emaj_ignore_app_trigger: the triggers "%" are E-Maj triggers and are not processed by the function.', v_trgList;
    END IF;
-- insert into the emaj_ignored_app_trigger table the not yet recorded triggers
    INSERT INTO emaj.emaj_ignored_app_trigger
      SELECT v_schema, v_table, tgname
        FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_tableOid
          AND tgname LIKE v_trigger AND NOT tgisinternal
          AND tgname NOT IN ('emaj_trunc_trg', 'emaj_log_trg')
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
-- This function rollbacks one application sequence to a given mark.
-- The function is called by emaj.emaj._rlbk_end().
-- Input: the emaj_group_def row related to the application sequence to process, time id of the mark to rollback to.
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
          RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".',
            v_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END;
-- Read the current sequence's characteristics
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    IF emaj._pg_version_num() >= 100000 THEN
      EXECUTE format('SELECT rel.last_value, start_value, increment_by, max_value, min_value, cache_size as cache_value, '
                     '       cycle as is_cycled, rel.is_called'
                     '  FROM %s rel, pg_catalog.pg_sequences '
                     '  WHERE schemaname = %L AND sequencename = %L',
                     v_fullSeqName, r_rel.rel_schema, r_rel.rel_tblseq)
              INTO STRICT curr_seq_rec;
    ELSE
      EXECUTE format('SELECT last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM %s',
                     v_fullSeqName)
              INTO STRICT curr_seq_rec;
    END IF;
-- Build the ALTER SEQUENCE statement, depending on the differences between the present values and the related
--   values at the requested mark time
    SELECT emaj._build_alter_seq(curr_seq_rec.last_value, curr_seq_rec.is_called, curr_seq_rec.increment_by,
                                 curr_seq_rec.start_value, curr_seq_rec.min_value, curr_seq_rec.max_value,
                                 curr_seq_rec.cache_value, curr_seq_rec.is_cycled, mark_seq_rec.sequ_last_val,
                                 mark_seq_rec.sequ_is_called, mark_seq_rec.sequ_increment, mark_seq_rec.sequ_start_val,
                                 mark_seq_rec.sequ_min_val, mark_seq_rec.sequ_max_val, mark_seq_rec.sequ_cache_val,
                                 mark_seq_rec.sequ_is_cycled) INTO v_stmt;
-- and execute the statement if at least one parameter has changed
    IF v_stmt <> '' THEN
      EXECUTE format('ALTER SEQUENCE %s %s',
                     v_fullSeqName, v_stmt);
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
-- Input: row from emaj_relation corresponding to the appplication table to proccess, the time stamp ids defining the time range to examine
--        (a end time stamp id set to NULL indicates the current situation)
-- Output: number of log rows between both marks for the table
  DECLARE
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- get the log table id at begin time id
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO STRICT v_beginLastValue
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_log_schema
        AND sequ_name = r_rel.rel_log_sequence
        AND sequ_time_id = v_beginTimeId;
    IF v_endTimeId IS NULL THEN
-- last time id is NULL, so examine the current state of the log table id
      IF emaj._pg_version_num() >= 100000 THEN
       EXECUTE format('SELECT CASE WHEN rel.is_called THEN rel.last_value ELSE rel.last_value - increment_by END'
                       '  FROM %I.%I rel, pg_sequences'
                       '  WHERE schemaname = %L  AND sequencename = %L ',
                       r_rel.rel_log_schema, r_rel.rel_log_sequence, r_rel.rel_log_schema, r_rel.rel_log_sequence)
          INTO v_endLastValue;
      ELSE
        EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM %I.%I',
                       r_rel.rel_log_schema, r_rel.rel_log_sequence)
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

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, v_firstEmajGid BIGINT, v_lastEmajGid BIGINT)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_tbl$
-- This function generates SQL commands representing all updates performed on a table between 2 marks
-- or beetween a mark and the current situation.
-- These commands are stored into a temporary table created by the _gen_sql_groups() calling function.
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
-- build schema specified table name and log table name
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- prepare sql skeletons for each statement type, using the pieces of sql recorded in the emaj_relation row at table assignment time
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''')
              || CASE WHEN r_rel.rel_sql_gen_ins_col <> '' THEN ' (' || r_rel.rel_sql_gen_ins_col || ')' ELSE '' END
              || CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END
              || ' VALUES (' || r_rel.rel_sql_gen_ins_val || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''')
              || ' SET ' || r_rel.rel_sql_gen_upd_set || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''')
              || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqTruncate = '''TRUNCATE ' || replace(v_fullTableName,'''','''''') || ';''';
-- build the restriction conditions on emaj_gid, depending on supplied marks range and the relation time range upper bound
    v_conditions = 'o.emaj_gid > ' || v_firstEmajGid;
--   get the EmajGid of the relation time range upper bound, if any
    IF NOT upper_inf(r_rel.rel_time_range) THEN
      SELECT time_last_emaj_gid INTO v_lastEmajGidRel FROM emaj.emaj_time_stamp WHERE time_id = upper(r_rel.rel_time_range);
    END IF;
--   if the relation time range upper bound is before the requested end mark, restrict the EmajGid upper limit
    IF v_lastEmajGidRel IS NOT NULL AND
       (v_lastEmajGid IS NULL OR (v_lastEmajGid IS NOT NULL AND v_lastEmajGidRel < v_lastEmajGid)) THEN
      v_lastEmajGid = v_lastEmajGidRel;
    END IF;
--   complete the restriction conditions
    IF v_lastEmajGid IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || v_lastEmajGid;
    END IF;
-- now scan the log table to process all statement types at once
    EXECUTE format('INSERT INTO emaj_temp_script '
                   'SELECT o.emaj_gid, 0, o.emaj_txid, CASE '
                   '    WHEN o.emaj_verb = ''INS'' THEN %s'
                   '    WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN %s'
                   '    WHEN o.emaj_verb = ''DEL'' THEN %s'
                   '    WHEN o.emaj_verb = ''TRU'' THEN %s'
                   '  END '
                   '  FROM %s o'
                   '       LEFT OUTER JOIN %s n ON n.emaj_gid = o.emaj_gid'
                   '                          AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'') '
                   ' WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
                   ' AND %s',
                   v_rqInsert, v_rqUpdate, v_rqDelete, v_rqTruncate, v_logTableName, v_logTableName, v_conditions);
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_seq(r_rel emaj.emaj_relation, v_firstMarkTimeId BIGINT, v_lastMarkTimeId BIGINT, v_nbSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_gen_sql_seq$
-- This function generates a SQL command to set the final characteristics of a sequence.
-- The command is stored into a temporary table created by the _gen_sql_groups() calling function.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time id at requested start and end marks,
--        the number of already processed sequences
-- Output: number of generated SQL statements (0 or 1)
  DECLARE
    v_fullSeqName            TEXT;
    v_refLastValue           BIGINT;
    v_refIsCalled            BOOLEAN;
    v_refIncrementBy         BIGINT;
    v_refStartValue          BIGINT;
    v_refMinValue            BIGINT;
    v_refMaxValue            BIGINT;
    v_refCacheValue          BIGINT;
    v_refIsCycled            BOOLEAN;
    v_stmt                   TEXT;
    v_trgLastValue           BIGINT;
    v_trgIsCalled            BOOLEAN;
    v_trgIncrementBy         BIGINT;
    v_trgStartValue          BIGINT;
    v_trgMinValue            BIGINT;
    v_trgMaxValue            BIGINT;
    v_trgCacheValue          BIGINT;
    v_trgIsCycled            BOOLEAN;
    v_endTimeId              BIGINT;
    v_rqSeq                  TEXT;
  BEGIN
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- get the sequence characteristics at start mark
    SELECT sequ_last_val, sequ_is_called, sequ_increment, sequ_start_val,
           sequ_min_val, sequ_max_val, sequ_cache_val, sequ_is_cycled
      INTO STRICT v_refLastValue, v_refIsCalled, v_refIncrementBy, v_refStartValue,
           v_refMinValue, v_refMaxValue, v_refCacheValue, v_refIsCycled
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = v_firstMarkTimeId;
-- get the sequence characteristics at end mark or the current state
    IF v_lastMarkTimeId IS NULL AND upper_inf(r_rel.rel_time_range) THEN
-- no supplied last mark and the sequence currently belongs to its group, so get current sequence characteritics
      IF emaj._pg_version_num() >= 100000 THEN
        v_stmt = 'SELECT rel.last_value, is_called, increment_by, start_value, min_value, max_value, cache_size, cycle '
              || 'FROM ' || v_fullSeqName  || ' rel, pg_catalog.pg_sequences '
              || ' WHERE schemaname = ' || quote_literal(r_rel.rel_schema) || ' AND sequencename = '
              || quote_literal(r_rel.rel_tblseq);
      ELSE
        v_stmt = 'SELECT last_value, is_called, increment_by, start_value, min_value, max_value, cache_value, is_cycled '
              || 'FROM ' || v_fullSeqName;
      END IF;
      EXECUTE v_stmt INTO v_trgLastValue, v_trgIsCalled, v_trgIncrementBy, v_trgStartValue,
                          v_trgMinValue, v_trgMaxValue, v_trgCacheValue, v_trgIsCycled;
    ELSE
-- a last mark is supplied, or the sequence does not belong to its groupe anymore, so get sequence characteristics from the emaj_sequence
-- table
      v_endTimeId = CASE WHEN upper_inf(r_rel.rel_time_range) OR v_lastMarkTimeId < upper(r_rel.rel_time_range)
                           THEN v_lastMarkTimeId
                         ELSE upper(r_rel.rel_time_range) END;
      SELECT sequ_last_val, sequ_is_called, sequ_increment, sequ_start_val,
             sequ_min_val, sequ_max_val, sequ_cache_val, sequ_is_cycled
        INTO STRICT v_trgLastValue, v_trgIsCalled, v_trgIncrementBy, v_trgStartValue,
             v_trgMinValue, v_trgMaxValue, v_trgCacheValue, v_trgIsCycled
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema AND sequ_name = r_rel.rel_tblseq
          AND sequ_time_id = v_endTimeId;
    END IF;
-- build the ALTER SEQUENCE clause
    SELECT emaj._build_alter_seq(v_refLastValue, v_refIsCalled, v_refIncrementBy, v_refStartValue,
                                 v_refMinValue, v_refMaxValue, v_refCacheValue, v_refIsCycled,
                                 v_trgLastValue, v_trgIsCalled, v_trgIncrementBy, v_trgStartValue,
                                 v_trgMinValue, v_trgMaxValue, v_trgCacheValue, v_trgIsCycled) INTO v_rqSeq;
-- insert into the temp table and return 1 if at least 1 characteristic needs to be changed
    IF v_rqSeq <> '' THEN
      v_rqSeq = 'ALTER SEQUENCE ' || v_fullSeqName || ' ' || v_rqSeq || ';';
      EXECUTE 'INSERT INTO emaj_temp_script '
              '  SELECT NULL, -1 * $1, txid_current(), $2'
        USING v_nbSeq + 1, v_rqSeq;
      RETURN 1;
    END IF;
-- otherwise return 0
    RETURN 0;
  END;
$_gen_sql_seq$;

CREATE OR REPLACE FUNCTION emaj._verify_groups(v_groups TEXT[], v_onErrorStop BOOLEAN)
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
    v_hint                   TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected
-- check the postgres version: E-Maj needs postgres 9.5+
    IF emaj._pg_version_num() < 90500 THEN
      RAISE EXCEPTION '_verify_groups: The current postgres version (%) is not compatible with this E-Maj version.'
                      ' It should be at least 9.5.', version();
    END IF;
-- OK, now look for groups unconsistency
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist
-- check all application relations referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq, r.rel_group,
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                    -- all relations currently belonging to the groups
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groups) AND upper_inf(rel_time_range)
            EXCEPT                                -- all relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range)
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (1): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log table for all tables referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
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
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function || '" is not found.'
               AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
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
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
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
      SELECT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
      WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
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
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)),
           cte_log_tables_columns AS (                -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
              AND relname = rel_log_table
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = FALSE AND attnum < rel_emaj_verb_attnum
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range))
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
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
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (6): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check all tables have their primary key if they belong to a rollbackable group
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key any more.' AS msg
        FROM emaj.emaj_relation, emaj.emaj_group
        WHERE rel_group = group_name
          AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND group_is_rollbackable
          AND NOT EXISTS
              (SELECT NULL FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
                 WHERE nspname = rel_schema AND relname = rel_tblseq
                   AND relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
                   AND contype = 'p')
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (7): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- for rollbackable groups, check no table has been altered as UNLOGGED or dropped and recreated as TEMP table after tables groups creation
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
        WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
          AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND group_name = rel_group AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (8): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- for rollbackable groups, with PG 11-, check no table has been altered as WITH OIDS after tables groups creation
    IF emaj._pg_version_num() < 120000 THEN
      FOR r_object IN
        SELECT rel_schema, rel_tblseq, rel_group,
               'In rollbackable group "' || rel_group || '", the table "' ||
               rel_schema || '"."' || rel_tblseq || '" is declared WITH OIDS.' AS msg
          FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_group
          WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
            AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
            AND group_name = rel_group AND group_is_rollbackable
            AND relhasoids
          ORDER BY 1,2,3
      LOOP
        IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %',r_object.msg,v_hint; END IF;
        RETURN NEXT r_object;
      END LOOP;
    END IF;
-- check the primary key structure of all tables belonging to rollbackable groups is unchanged
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In rollbackable group "' || rel_group || '", the primary key of the table "' ||
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
              AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND attnum = ANY (indkey)
              AND indisprimary
              AND attnum > 0 AND attisdropped = FALSE
            GROUP BY rel_schema, rel_tblseq, rel_group, rel_sql_rlbk_pk_columns
          ) AS t
          WHERE rel_sql_rlbk_pk_columns <> current_pk_columns
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (10): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check all log tables have the 6 required technical columns. It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In group "' || rel_group || '", the log table "' ||
             rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
             string_agg(attname,', ') || ').' AS msg
        FROM (
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
              FROM emaj.emaj_relation,
                  (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
              WHERE rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
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
                AND rel_group = ANY (v_groups) AND rel_kind = 'r' AND upper_inf(rel_time_range)
          ) AS t2
        GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_groups (11): % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_alter_group(v_groupName TEXT, v_mark TEXT DEFAULT 'ALTER_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_alter_group$
-- This function alters a tables group.
-- Input: group name
--        an optional mark name, used with groups in logging state
-- Output: number of tables and sequences belonging to the group after the operation
  DECLARE
    v_timeId                 BIGINT;
    v_nbRel                  INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('ALTER_GROUP', 'BEGIN', v_groupName);
-- alter the group
    SELECT * INTO v_timeId, v_nbRel
      FROM emaj._alter_groups(ARRAY[v_groupName], FALSE, v_mark, 'ALTER_GROUP'::TEXT, NULL);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'END', v_groupName, 'Timestamp Id : ' || v_timeId );
    RETURN v_nbRel;
  END;
$emaj_alter_group$;
COMMENT ON FUNCTION emaj.emaj_alter_group(TEXT, TEXT) IS
$$Alter an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_alter_groups(v_groupNames TEXT[], v_mark TEXT DEFAULT 'ALTER_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_alter_groups$
-- This function alters several tables groups.
-- Input: group names array
--        an optional mark name, used with groups in logging state
-- Output: number of tables and sequences belonging to the groups after the operation
  DECLARE
    v_timeId                 BIGINT;
    v_nbRel                  INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('ALTER_GROUPS', 'BEGIN', array_to_string(v_groupNames,','));
-- alter the group
    SELECT * INTO v_timeId, v_nbRel
      FROM emaj._alter_groups(v_groupNames, TRUE, v_mark, 'ALTER_GROUPS'::TEXT, NULL);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUPS', 'END', array_to_string(v_groupNames,','), 'Timestamp Id : ' || v_timeId );
    RETURN v_nbRel;
  END;
$emaj_alter_groups$;
COMMENT ON FUNCTION emaj.emaj_alter_groups(TEXT[], TEXT) IS
$$Alter several E-Maj groups.$$;

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
        SET group_last_alter_time_id = v_timeId, group_has_waiting_changes = FALSE,
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

CREATE OR REPLACE FUNCTION emaj._alter_exec(v_timeId BIGINT, v_callingFunction TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_alter_exec$
-- This function executes the alter groups operation that has been planned by the _alter_plan() function.
-- It looks at the emaj_alter_plan table and executes elementary step in proper order.
-- Input: timestamp id of the operation
--        name of the first level calling function
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
          PERFORM emaj._remove_tbl(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, r_plan.altr_group_is_logging,
                                   v_timeId, v_callingFunction);
--
        WHEN 'REMOVE_SEQ' THEN
-- remove a sequence from its group
          PERFORM emaj._remove_seq(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, r_plan.altr_group_is_logging,
                                   v_timeId, v_callingFunction);
--
        WHEN 'RESET_GROUP' THEN
-- reset a group
          PERFORM emaj._reset_groups(ARRAY[r_plan.altr_group]);
--
        WHEN 'REPAIR_TBL' THEN
          IF r_plan.altr_group_is_logging THEN
            RAISE EXCEPTION 'alter_exec: Cannot repair the table %.%. Its group % is in LOGGING state.',
              r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group;
          ELSE
-- remove the table from its group
            PERFORM emaj._drop_tbl(emaj.emaj_relation.*, v_timeId) FROM emaj.emaj_relation
              WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- get the is_rollbackable status of the related group
            SELECT group_is_rollbackable INTO v_isRollbackable
              FROM emaj.emaj_group WHERE group_name = r_plan.altr_group;
-- and recreate it
            PERFORM emaj._create_tbl(grpdef_schema, grpdef_tblseq, grpdef_group, grpdef_priority, grpdef_log_dat_tsp, grpdef_log_idx_tsp,
                                     v_timeId, v_isRollbackable, r_plan.altr_group_is_logging)
              FROM emaj.emaj_group_def
              WHERE grpdef_group = coalesce (r_plan.altr_new_group, r_plan.altr_group)
                AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
          END IF;
--
        WHEN 'REPAIR_SEQ' THEN
          RAISE EXCEPTION 'alter_exec: Internal error, trying to repair a sequence (%.%) is abnormal.',
            r_plan.altr_schema, r_plan.altr_tblseq;
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
          PERFORM emaj._change_log_data_tsp_tbl(r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_table,
                                                r_rel.rel_log_dat_tsp, v_logDatTsp, v_callingFunction);
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
          PERFORM emaj._change_log_index_tsp_tbl(r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_index,
                                                 r_rel.rel_log_idx_tsp, v_logIdxTsp, v_callingFunction);
--
        WHEN 'MOVE_TBL' THEN
-- move a table from one group to another group
          PERFORM emaj._move_tbl(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, r_plan.altr_group_is_logging,
                                r_plan.altr_new_group, r_plan.altr_new_group_is_logging, v_timeId, v_callingFunction);
--
        WHEN 'MOVE_SEQ' THEN
-- move a sequence from one group to another group
          PERFORM emaj._move_seq(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, r_plan.altr_group_is_logging,
                                r_plan.altr_new_group, r_plan.altr_new_group_is_logging, v_timeId, v_callingFunction);
--
        WHEN 'CHANGE_REL_PRIORITY' THEN
-- get the table description from emaj_relation
          SELECT * INTO r_rel FROM emaj.emaj_relation
            WHERE rel_schema = r_plan.altr_schema AND rel_tblseq = r_plan.altr_tblseq AND upper_inf(rel_time_range);
-- update the emaj_relation table to report the priority change
          PERFORM emaj._change_priority_tbl(r_plan.altr_schema, r_plan.altr_tblseq, r_rel.rel_priority, r_plan.altr_priority,
                                            v_callingFunction);
--
        WHEN 'ADD_TBL' THEN
-- add a table to a group
          PERFORM emaj._add_tbl(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, grpdef_priority, grpdef_log_dat_tsp,
                                grpdef_log_idx_tsp, r_plan.altr_group_is_logging, v_timeId, v_callingFunction)
            FROM emaj.emaj_group_def
            WHERE grpdef_group = r_plan.altr_group AND grpdef_schema = r_plan.altr_schema AND grpdef_tblseq = r_plan.altr_tblseq;
--
        WHEN 'ADD_SEQ' THEN
-- add a sequence to a group
          PERFORM emaj._add_seq(r_plan.altr_schema, r_plan.altr_tblseq, r_plan.altr_group, r_plan.altr_group_is_logging,
                                v_timeId, v_callingFunction);
--
      END CASE;
    END LOOP;
    RETURN;
  END;
$_alter_exec$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(v_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_groups_configuration$
-- This function returns a JSON formatted structure representing some or all configured tables groups
-- The function can be called by clients like emaj_web.
-- This is just a wrapper of the internal _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the tables groups content in JSON format
  BEGIN
    RETURN emaj._export_groups_conf(v_groups);
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT[]) IS
$$Generates a json structure describing configured tables groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(v_location TEXT, v_groups TEXT[] DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_groups_configuration$
-- This function stores some or all configured tables groups configuration into a file on the server.
-- The JSON structure is built by the _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the number of tables groups recorded in the file.
  DECLARE
    v_groupsJson             JSON;
  BEGIN
-- get the json structure
    SELECT emaj._export_groups_conf(v_groups) INTO v_groupsJson;
-- store the structure into the provided file name
    CREATE TEMP TABLE t (groups TEXT);
    INSERT INTO t
      SELECT line FROM regexp_split_to_table(v_groupsJson::TEXT, '\n') AS line;
    BEGIN
      EXECUTE format ('COPY t TO %s',
                      quote_literal(v_location));
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_export_groups_configuration: Unable to write to the % file.', v_location;
    END;
    DROP TABLE t;
-- return the number of recorded tables groups
    RETURN json_array_length(v_groupsJson->'tables_groups');
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT, TEXT[]) IS
$$Generates and stores in a file a json structure describing configured tables groups.$$;

CREATE OR REPLACE FUNCTION emaj._export_groups_conf(v_groups TEXT[] DEFAULT NULL)
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
    r_trigger                RECORD;
    r_sequence               RECORD;
  BEGIN
-- build the header of the JSON structure
    v_groupsText = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           (SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version') ||
                           ', at ' || current_timestamp || E'",\n';
-- check the group names array, if supplied. All the listed groups must exist.
    IF v_groups IS NOT NULL THEN
      SELECT string_agg(group_name, ', ') INTO v_unknownGroupsList FROM (
        SELECT * FROM unnest(v_groups) AS grp(group_name)
          WHERE NOT EXISTS (SELECT group_name FROM emaj.emaj_group WHERE emaj_group.group_name = grp.group_name)
        ) AS t;
      IF v_unknownGroupsList IS NOT NULL THEN
        RAISE EXCEPTION '_export_groups_conf: The tables groups % are unknown.', v_unknownGroupsList;
      END IF;
    END IF;
-- build the tables groups description
    v_groupsText = v_groupsText
                || E'  "tables_groups": [\n';
    FOR r_group IN
        SELECT group_name, group_is_rollbackable, group_comment, group_nb_table, group_nb_sequence
          FROM emaj.emaj_group
          WHERE (v_groups IS NULL OR group_name = ANY(v_groups))
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
-- build the tables list, if any
        v_groupsText = v_groupsText
                    || E'      "tables": [\n';
        FOR r_table IN
            SELECT rel_schema, rel_tblseq, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp,
                   (SELECT count(*) FROM emaj.emaj_ignored_app_trigger
                    WHERE trg_schema = rel_schema AND trg_table = rel_tblseq) AS nb_trigger
              FROM emaj.emaj_relation
              WHERE rel_kind = 'r' AND upper_inf(rel_time_range)
                AND rel_group = r_group.group_name
              ORDER BY rel_schema, rel_tblseq
        LOOP
          v_groupsText = v_groupsText
                      || E'        {\n'
                      ||  '          "schema": ' || to_json(r_table.rel_schema) || E',\n'
                      ||  '          "table": ' || to_json(r_table.rel_tblseq) || E',\n';
          IF r_table.rel_priority IS NOT NULL THEN
            v_groupsText = v_groupsText
                        || E'          "priority": '|| to_json(r_table.rel_priority) || E',\n';
          END IF;
          IF r_table.rel_log_dat_tsp IS NOT NULL THEN
            v_groupsText = v_groupsText
                        || E'          "log_data_tablespace": '|| to_json(r_table.rel_log_dat_tsp) || E',\n';
          END IF;
          IF r_table.rel_log_idx_tsp IS NOT NULL THEN
            v_groupsText = v_groupsText
                        || E'          "log_index_tablespace": '|| to_json(r_table.rel_log_idx_tsp) || E',\n';
          END IF;
          IF r_table.nb_trigger > 0 THEN
-- build the triggers to ignore for the table, if any
            v_groupsText = v_groupsText
                        || E'          "ignored_triggers": [\n';
            FOR r_trigger IN
                SELECT trg_name
                  FROM emaj.emaj_ignored_app_trigger
                  WHERE trg_schema = r_table.rel_schema AND trg_table = r_table.rel_tblseq
                  ORDER BY trg_name
            LOOP
              v_groupsText = v_groupsText
                          || E'            {\n'
                          ||  '              "trigger": ' || to_json(r_trigger.trg_name) || E',\n'
                          || E'            },\n';
            END LOOP;
            v_groupsText = v_groupsText
                        || E'          ],\n';
          END IF;
          v_groupsText = v_groupsText
                      || E'        },\n';
        END LOOP;
        v_groupsText = v_groupsText
                    || E'      ],\n';
      END IF;
      IF r_group.group_nb_sequence > 0 THEN
-- build the sequences list, if any
        v_groupsText = v_groupsText
                    || E'      "sequences": [\n';
        FOR r_sequence IN
            SELECT rel_schema, rel_tblseq
              FROM emaj.emaj_relation
              WHERE rel_kind = 'S' AND upper_inf(rel_time_range)
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
-- build the trailer and remove illicite commas at the end of arrays and attributes lists
    v_groupsText = v_groupsText
                || E'}\n';
    v_groupsText = regexp_replace(v_groupsText, E',(\n *(\]|}))', '\1', 'g');
-- test the JSON format by casting the text structure to json and report a warning in case of problem
--   (this should not fail, unless the function code is bogus)
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

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(v_json JSON, v_groups TEXT[] DEFAULT NULL,
                                                                 v_allowGroupsUpdate BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_groups_configuration$
-- This function import a supplied JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
-- Output: the number of created or altered tables groups
  DECLARE
    v_nbGroup                INT;
  BEGIN
-- just process the tables groups
    SELECT emaj._import_groups_conf(v_json, v_groups, v_allowGroupsUpdate) INTO v_nbGroup;
    RETURN v_nbGroup;
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(JSON,TEXT[],BOOLEAN) IS
$$Import a json structure describing tables groups to create or alter.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(v_location TEXT, v_groups TEXT[] DEFAULT NULL,
                                                                 v_allowGroupsUpdate BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_groups_configuration$
-- This function imports a file containing a JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - input file location
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
-- Output: the number of created or altered tables groups
  DECLARE
    v_groupsText             TEXT;
    v_json                   JSON;
    v_nbGroup                INT;
  BEGIN
-- read the input file and put its content into a temporary table
    CREATE TEMP TABLE t (groups TEXT);
    BEGIN
      EXECUTE format ('COPY t FROM %s',
                      quote_literal(v_location));
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_groups_configuration: Unable to read the % file.', v_location;
    END;
-- aggregate the lines into a single text variable
    SELECT string_agg(groups, E'\n') INTO v_groupsText FROM t;
    DROP TABLE t;
-- verify that the file content is a valid json structure
    BEGIN
      v_json = v_groupsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_groups_configuration: The file content is not a valid JSON content.';
    END;
-- proccess the tables groups
    SELECT emaj._import_groups_conf(v_json, v_groups, v_allowGroupsUpdate, v_location) INTO v_nbGroup;
    RETURN v_nbGroup;
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(TEXT,TEXT[],BOOLEAN) IS
$$Create or alter tables groups configuration from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf(v_json JSON, v_groups TEXT[], v_allowGroupsUpdate BOOLEAN,
                                                    v_location TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf$
-- This function processes a JSON formatted structure representing the tables groups to create or update.
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
-- For tables groups, "group_is_rollbackable" and "group_comment" attributes are optional.
-- For tables, "priority", "log_data_tablespace" and "log_index_tablespace" attributes are optional.
-- A tables group may have no "tables" or "sequences" arrays.
-- A table may have no "ignored_triggers" array.
-- The function replaces the content of the emaj_group_def table for the imported tables groups by the content of the JSON configuration.
-- Non existing groups are created empty.
-- The _alter_groups() function is used to process the assignement, the move, the removal or the attributes change for tables and
-- sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
-- Output: the number of created or altered tables groups
  DECLARE
    v_groupsJson             JSON;
    r_msg                    RECORD;
  BEGIN
-- performs various checks on the groups content described in the supplied JSON structure
    FOR r_msg IN
      SELECT chk_message FROM emaj._check_json_groups_conf(v_json)
        ORDER BY chk_msg_type, chk_group, chk_schema, chk_tblseq
    LOOP
      RAISE WARNING '_import_groups_conf (1): %', r_msg.chk_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the supplied JSON structure.';
    END IF;
-- extract the "tables_groups" json path
    v_groupsJson = v_json #> '{"tables_groups"}';
-- if not supplied by the caller, materialize the groups array, by aggregating all groups of the JSON structure
    IF v_groups IS NULL THEN
      SELECT array_agg("group") INTO v_groups
        FROM json_to_recordset(v_groupsJson) AS x("group" TEXT);
    END IF;
-- prepare the groups configuration import. This may report some other issues with the groups content
    FOR r_msg IN
      SELECT chk_message FROM emaj._import_groups_conf_prepare(v_json, v_groups, v_allowGroupsUpdate, v_location)
        ORDER BY chk_msg_type, chk_group, chk_schema, chk_tblseq
    LOOP
      RAISE WARNING '_import_groups_conf (2): %', r_msg.chk_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the JSON groups configuration.';
    END IF;
-- Ok
    RETURN emaj._import_groups_conf_exec(v_json, v_groups);
 END;
$_import_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(v_groupsJson JSON, v_groups TEXT[],
                                                    v_allowGroupsUpdate BOOLEAN, v_location TEXT)
RETURNS SETOF emaj._check_groups_conf_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepare the effective tables groups configuration import.
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
      SELECT 100, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT,
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
        SELECT 101, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT,
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
        SELECT 102, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT,
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
        WHERE ((chk_group = ANY (v_groups) AND chk_severity = 1)
            OR (chk_group = ANY (v_rollbackableGroups) AND chk_severity = 2))
        ORDER BY chk_msg_type, chk_group, chk_schema, chk_tblseq;
-- check that all listed triggers exist
    RETURN QUERY
      SELECT 110, 1, tmp_group, tmp_schema, tmp_table, tmp_trigger,
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
      SELECT 111, 1, tmp_group, tmp_schema, tmp_table, tmp_trigger,
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
-- This function complete a tables groups configuration import.
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
        INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_creation_time_id, group_has_waiting_changes,
                                     group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment)
          VALUES (r_group.groupJson ->> 'group', v_isRollbackable, v_timeId, FALSE,
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

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','),
              CASE WHEN v_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- check the group names
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := TRUE, v_checkList := 'IDLE')
      INTO v_groupNames;
    IF v_groupNames IS NOT NULL THEN
-- if there is at least 1 group to process, go on
-- check that no group is damaged
      PERFORM 0 FROM emaj._verify_groups(v_groupNames, TRUE);
-- check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(v_groupNames);
-- purge the emaj history, if needed
      PERFORM emaj._purge_hist();
-- if requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs
      IF v_resetLog THEN
        PERFORM emaj._reset_groups(v_groupNames);
--    drop the log schemas that would have been emptied by the _reset_groups() call
        SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
        PERFORM emaj._drop_log_schemas(CASE WHEN v_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, FALSE);
        PERFORM emaj._enable_event_triggers(v_eventTriggers);
      END IF;
-- check the supplied mark name (the check must be performed after the _reset_groups() call to allow to reuse an old mark name that is
-- being deleted
      IF v_mark IS NULL OR v_mark = '' THEN
        v_mark = 'START_%';
      END IF;
      SELECT emaj._check_new_mark(v_groupNames, v_mark) INTO v_markName;
-- OK, lock all tables to get a stable point
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
--   risk of deadlock.
      PERFORM emaj._lock_groups(v_groupNames,'SHARE ROW EXCLUSIVE',v_multiGroup);
-- enable all log triggers for the groups
-- for each relation currently belonging to the group,
      FOR r_tblsq IN
         SELECT rel_kind, quote_ident(rel_schema) || '.' || quote_ident(rel_tblseq) AS full_relation_name FROM emaj.emaj_relation
           WHERE upper_inf(rel_time_range) AND rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
          EXECUTE format('ALTER TABLE %s ENABLE TRIGGER emaj_log_trg, ENABLE TRIGGER emaj_trunc_trg',
                           r_tblsq.full_relation_name);
        END IF;
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

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- retrieve the timestamp and the emaj_gid value and the time stamp id of the target new first mark
    SELECT time_last_emaj_gid, mark_time_id INTO v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark, emaj.emaj_time_stamp
      WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_mark;
--
-- first process all obsolete time ranges for the group
--
-- drop obsolete old log tables
    FOR r_rel IN
          -- log tables for the group, whose end time stamp is older than the new first mark time stamp
          SELECT DISTINCT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r' AND rel_group = v_groupName AND upper(rel_time_range) <= v_markTimeId
        EXCEPT
          -- unless they are also used for more recent time range, or are also linked to other groups
          SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r'
              AND (upper(rel_time_range) > v_markTimeId OR upper_inf(rel_time_range) OR rel_group <> v_groupName)
          ORDER BY 1,2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- delete emaj_sequence rows corresponding to obsolete relation time range that will be deleted just later
-- (the related emaj_seq_hole rows will be deleted just later ; they are not directly linked to an emaj_relation row)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation r1
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence AND upper(rel_time_range) <= v_markTimeId
        AND (sequ_time_id < v_markTimeId                  -- all sequences prior the mark time
          OR (sequ_time_id = v_markTimeId                 -- and the sequence of the mark time
              AND NOT EXISTS (                            --   if it is not the lower bound of an adjacent time range
                SELECT 1 FROM emaj.emaj_relation r2
                  WHERE r2.rel_schema = r1.rel_log_schema AND r2.rel_tblseq = r1.rel_log_sequence
                    AND lower(r2.rel_time_range) = v_marktimeid)));
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
--
-- then process the current relation time range for the group
--
-- delete rows from all log tables
    FOR r_rel IN
        SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r'
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
          ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
-- delete log rows prior to the new first mark
      EXECUTE format('DELETE FROM %s WHERE emaj_gid <= $1',
                     r_rel.log_table_name)
        USING v_markGlobalSeq;
    END LOOP;
-- process emaj_seq_hole content
-- delete all existing holes, if any, before the mark
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks)
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id < v_markTimeId;
-- now the sequences related to the mark to delete can be suppressed
--   delete first application sequences related data for the group
--   the sequence state at time range bounds are kept (if the mark comes from a logging group alter operation)
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND rel_time_range @> sequ_time_id
        AND rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   delete then emaj sequences related data for the group
--   the sequence state at time range bounds are kept
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
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- purge the emaj history, if needed (even if no mark as been really dropped)
    PERFORM emaj._purge_hist();
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN,
                                           v_isAlterGroupAllowed BOOLEAN DEFAULT FALSE)
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
    v_isDblinkUsed           BOOLEAN;
    v_dbLinkSchema           TEXT;
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
                CASE WHEN v_isLoggedRlbk THEN 'Logged' ELSE 'Unlogged' END || ' rollback to mark ' || v_markName
                || ' [' || v_markTimestamp || ']'
               ) RETURNING hist_id INTO v_histId;
-- get the total number of tables for these groups
      SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
        FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- first try to open a dblink connection
      SELECT v_status, (v_status >= 0), CASE WHEN v_status >= 0 THEN v_schema ELSE NULL END
        INTO v_dbLinkCnxStatus, v_isDblinkUsed, v_dbLinkSchema
        FROM emaj._dblink_open_cnx('rlbk#1');
-- for parallel rollback (i.e. when nb sessions > 1), the dblink connection must be ok
      IF v_nbSession > 1 AND NOT v_isDblinkUsed THEN
        RAISE EXCEPTION '_rlbk_init: Cannot use several sessions without dblink connection capability. (Status of the dblink'
                        ' connection attempt = % - see E-Maj documentation)',
          v_dbLinkCnxStatus;
      END IF;
-- create the row representing the rollback event in the emaj_rlbk table and get the rollback id back
      v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, ' ||
               'rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
               'rlbk_dblink_schema, rlbk_is_dblink_used) ' ||
               'VALUES (' || quote_literal(v_groupNames) || ',' || quote_literal(v_markName) || ',' ||
               v_markTimeId || ',' || v_isLoggedRlbk || ',' || quote_nullable(v_isAlterGroupAllowed) || ',' ||
               v_nbSession || ',' || v_nbTblInGroups || ',' || v_nbSeqInGroups || ', ''PLANNING'',' || v_histId || ',' ||
               quote_nullable(v_dbLinkSchema) || ',' || v_isDblinkUsed || ') RETURNING rlbk_id';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_rlbkId;
-- call the rollback planning function to define all the elementary steps to perform,
-- compute their estimated duration and spread the elementary steps among sessions
      v_stmt = 'SELECT emaj._rlbk_planning(' || v_rlbkId || ')';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_effNbTable;
-- update the emaj_rlbk table to set the real number of tables to process and adjust the rollback status
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_table = ' || v_effNbTable ||
               ', rlbk_status = ''LOCKING'' ' || ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
    END IF;
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._reset_groups(v_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_reset_groups$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences images.
-- It is called by emaj_reset_group(), emaj_start_group() and emaj_alter_group() functions.
-- Input: group names array
-- Output: number of processed tables and sequences
-- There is no check of the groups state (this is done by callers).
-- The function is defined as SECURITY DEFINER so that an emaj_adm role can truncate log tables.
  DECLARE
    v_eventTriggers          TEXT[];
    r_rel                    RECORD;
  BEGIN
-- disable event triggers that protect emaj components and keep in memory these triggers name
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- delete all marks for the groups from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames);
-- delete emaj_sequence rows related to the tables of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation r1
      WHERE sequ_schema = rel_log_schema AND sequ_name = rel_log_sequence
        AND rel_group = ANY (v_groupNames) AND rel_kind = 'r'
        AND ((sequ_time_id <@ rel_time_range               -- all log sequences inside the relation time range
             AND (sequ_time_id <> lower(rel_time_range)    -- except the lower bound if
                  OR NOT EXISTS(                           --   it is the upper bound of another time range for another group
                     SELECT 1 FROM emaj.emaj_relation r2
                       WHERE r2.rel_log_schema = sequ_schema AND r2.rel_log_sequence = sequ_name
                         AND upper(r2.rel_time_range) = sequ_time_id
                         AND NOT (r2.rel_group = ANY (v_groupNames)) )))
         OR (sequ_time_id = upper(rel_time_range)          -- but including the upper bound if
                  AND NOT EXISTS (                         --   it is not the lower bound of another time range (for any group)
                     SELECT 1 FROM emaj.emaj_relation r3
                       WHERE r3.rel_log_schema = sequ_schema AND r3.rel_log_sequence = sequ_name
                         AND lower(r3.rel_time_range) = sequ_time_id))
            );
-- delete all sequence holes for the tables of the groups
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks)
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND rel_group = ANY (v_groupNames) AND rel_kind = 'r';
-- drop obsolete log tables (but keep those linked to other groups)
    FOR r_rel IN
          SELECT DISTINCT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' AND NOT upper_inf(rel_time_range)
        EXCEPT
          SELECT rel_log_schema, rel_log_table FROM emaj.emaj_relation
            WHERE rel_kind = 'r'
              AND (upper_inf(rel_time_range) OR NOT rel_group = ANY (v_groupNames))
          ORDER BY 1,2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- delete emaj_sequence rows related to the sequences of the groups
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq
        AND rel_group = ANY (v_groupNames) AND rel_kind = 'S'
        AND ((sequ_time_id <@ rel_time_range               -- all application sequences inside the relation time range
             AND (sequ_time_id <> lower(rel_time_range)    -- except the lower bound if
                  OR NOT EXISTS(                           --   it is the upper bound of another time range for another group
                     SELECT 1 FROM emaj.emaj_relation r2
                       WHERE r2.rel_schema = sequ_schema AND r2.rel_tblseq = sequ_name AND upper(r2.rel_time_range) = sequ_time_id
                         AND NOT (r2.rel_group = ANY (v_groupNames)) )))
         OR (sequ_time_id = upper(rel_time_range)          -- including the upper bound if
                  AND NOT EXISTS (                         --   it is not the lower bound of another time range for another group
                     SELECT 1 FROM emaj.emaj_relation r3
                       WHERE r3.rel_schema = sequ_schema AND r3.rel_tblseq = sequ_name AND lower(r3.rel_time_range) = sequ_time_id))
            );
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
      EXECUTE format('TRUNCATE %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- enable previously disabled event triggers
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
    RETURN sum(group_nb_table)+sum(group_nb_sequence) FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames);
  END;
$_reset_groups$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN, v_firstMark TEXT, v_lastMark TEXT,
                                                v_location TEXT, v_tblseqs TEXT[])
RETURNS BIGINT LANGUAGE plpgsql
SET DateStyle = 'ISO, YMD' AS
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
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - optional array of schema qualified table and sequence names to only process those tables and sequences
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_lastEmajGid            BIGINT;
    v_tblseqErr              TEXT;
    v_count                  INT;
    v_nbSQL                  BIGINT;
    v_nbSeq                  INT;
    v_cumNbSQL               BIGINT = 0;
    v_endComment             TEXT;
    v_dateStyle              TEXT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','),
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END ||
       CASE WHEN v_tblseqs IS NOT NULL THEN ' with tables/sequences filtering' ELSE '' END );
-- check the group name
    SELECT emaj._check_group_names(v_groupNames := v_groupNames, v_mayBeNull := v_multiGroup, v_lockGroups := FALSE, v_checkList := '')
      INTO v_groupNames;
-- if there is at least 1 group to process, go on
    IF v_groupNames IS NOT NULL THEN
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
-- check the array of tables and sequences to filter, if supplied.
-- each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups
      IF v_tblseqs IS NOT NULL THEN
        SELECT string_agg(t,', ' ORDER BY t), count(*)
          INTO v_tblseqErr, v_count FROM (
            SELECT t FROM unnest(v_tblseqs) AS t
              EXCEPT
            SELECT rel_schema || '.' || rel_tblseq FROM emaj.emaj_relation
              WHERE rel_time_range @> v_firstMarkTimeId AND rel_group = ANY (v_groupNames)  -- tables/sequences that belong to their group
                                                                                            -- at the start mark time
            ) AS t2;
        IF v_tblseqErr IS NOT NULL THEN
          RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) did not belong to any of the selected tables groups at % mark time.',
            v_count, v_tblseqErr, v_firstMark;
        END IF;
      END IF;
-- check that all tables had pk at start mark time
--   verifying the emaj_relation.rel_sql_gen_pk_conditions column
      SELECT string_agg(rel_schema || '.' || rel_tblseq, ', ' ORDER BY rel_schema, rel_tblseq), count(*)
        INTO v_tblseqErr, v_count FROM (
          SELECT * FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                               -- tables belonging to the groups
              AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))        -- filtered or not by the user
              AND rel_sql_gen_pk_conditions IS NULL                                               -- no pk at assignment time
          ) as t;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) had no pkey at % mark time.',
          v_count, v_tblseqErr, v_firstMark;
      END IF;
-- create a temporary table to hold the generated script
      DROP TABLE IF EXISTS emaj_temp_script CASCADE;
      CREATE TEMP TABLE emaj_temp_script (
        scr_emaj_gid           BIGINT,              -- the emaj_gid of the corresponding log row,
                                                    --   0 for initial technical statements,
                                                    --   NULL for final technical statements
        scr_subid              INT,                 -- used to distinguish several generated sql per log row
        scr_emaj_txid          BIGINT,              -- for future use, to insert commit statement at each txid change
        scr_sql                TEXT                 -- the generated sql text
      );
      GRANT SELECT ON emaj_temp_script TO PUBLIC;
-- test the supplied output file name by inserting a temporary line
      IF v_location IS NOT NULL THEN
        INSERT INTO emaj_temp_script SELECT 0, 1, 0, '-- SQL script generation in progress - started at ' || statement_timestamp();
        BEGIN
          PERFORM emaj._export_sql_script(v_location);
        EXCEPTION
          WHEN OTHERS THEN
            RAISE EXCEPTION '_gen_sql_groups: The file "%" cannot be used as script output file.', v_location;
        END;
        DELETE FROM emaj_temp_script;
      END IF;
-- end of checks
-- if there is no first mark for all groups, return quickly with a warning message
      IF v_firstMark IS NULL THEN
        RAISE WARNING '_gen_sql_groups: No mark exists for the group(s) "%".', array_to_string(v_groupNames,', ');
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
          VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
                  array_to_string(v_groupNames,','), 'No mark in the group(s) => no file has been generated');
        RETURN 0;
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
-- insert initial comments, define some session parameters:
--    - the standard_conforming_strings option to properly handle special characters,
--    - the DateStyle mode used at export time
-- and a transaction start
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
      SELECT setting INTO v_dateStyle FROM pg_settings WHERE name = 'DateStyle';
      INSERT INTO emaj_temp_script SELECT 0, 10, 0, 'SET standard_conforming_strings = OFF;';
      INSERT INTO emaj_temp_script SELECT 0, 11, 0, 'SET escape_string_warning = OFF;';
      INSERT INTO emaj_temp_script SELECT 0, 12, 0, 'SET datestyle = ' || quote_literal(v_dateStyle) || ';';
      INSERT INTO emaj_temp_script SELECT 0, 20, 0, 'BEGIN TRANSACTION;';
-- process tables
      FOR r_rel IN
          SELECT * FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                               -- tables belonging to the groups
              AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))        -- filtered or not by the user
              AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId,                            -- only tables having updates to process
                                    least(v_lastMarkTimeId, upper(rel_time_range))) > 0
            ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- for each application table referenced in the emaj_relation table, process the related log table, by calling the _gen_sql_tbl() function
        SELECT emaj._gen_sql_tbl(r_rel, v_firstEmajGid, v_lastEmajGid) INTO v_nbSQL;
        v_cumNbSQL = v_cumNbSQL + v_nbSQL;
      END LOOP;
-- process sequences
      v_nbSeq = 0;
      FOR r_rel IN
          SELECT * FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              AND rel_time_range @> v_firstMarkTimeId                                -- sequences belonging to the groups at the start mark
              AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs))         -- filtered or not by the user
            ORDER BY rel_schema DESC, rel_tblseq DESC
      LOOP
-- process each sequence and increment the sequence counter
        v_nbSeq = v_nbSeq + emaj._gen_sql_seq(r_rel, v_firstMarkTimeId, v_lastMarkTimeId, v_nbSeq);
      END LOOP;
-- add command to committhe transaction and reset the modified session parameters
      INSERT INTO emaj_temp_script SELECT NULL, 1, txid_current(), 'COMMIT;';
      INSERT INTO emaj_temp_script SELECT NULL, 10, txid_current(), 'RESET standard_conforming_strings;';
      INSERT INTO emaj_temp_script SELECT NULL, 11, txid_current(), 'RESET escape_string_warning;';
      INSERT INTO emaj_temp_script SELECT NULL, 11, txid_current(), 'RESET datestyle;';
-- if an output file is supplied, write the SQL script on the external file and drop the temporary table
      IF v_location IS NOT NULL THEN
        PERFORM emaj._export_sql_script(v_location);
        DROP TABLE IF EXISTS emaj_temp_script;
      ELSE
-- otherwise create a view to ease the generation script export
        CREATE TEMPORARY VIEW emaj_sql_script AS
          SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid;
        GRANT SELECT ON emaj_sql_script TO PUBLIC;
      END IF;
-- return the number of sql verbs generated into the output file
      v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    END IF;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
              array_to_string(v_groupNames,','), v_cumNbSQL || ' generated statements' ||
                CASE WHEN v_location IS NOT NULL THEN ' - script exported into ' || v_location ELSE ' - script not exported' END );
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

CREATE OR REPLACE FUNCTION emaj._export_param_conf()
RETURNS JSON LANGUAGE plpgsql AS
$_export_param_conf$
-- This function generates a JSON formatted structure representing the parameters registered in the emaj_param table.
-- All parameters are extracted, except the "emaj_version" key that is directly linked to the extension and thus is not updatable.
-- The E-Maj version is already displayed in the generated comment at the beginning of the structure.
-- Output: the parameters content in JSON format
  DECLARE
    v_params                 TEXT;
    v_paramsJson             JSON;
    r_param                  RECORD;
  BEGIN
-- build the header of the JSON structure
    v_params = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           (SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version') ||
                           ', at ' || current_timestamp || E'",\n';
-- build the parameters description
    v_params = v_params || E'  "parameters": [\n';
    FOR r_param IN
        SELECT param_key AS key,
               coalesce(to_json(param_value_text),
                        to_json(param_value_interval),
                        to_json(param_value_boolean),
                        to_json(param_value_numeric),
                        'null') as value
          FROM emaj.emaj_param
               JOIN (VALUES (1::INT, 'emaj_version'), (2,'dblink_user_password'),
                            (3, 'history_retention'), (4, 'alter_log_table'),
                            (5, 'avg_row_rollback_duration'), (5, 'avg_row_delete_log_duration'),
                            (7, 'avg_fkey_check_duration'), (8, 'fixed_step_rollback_duration'),
                            (9, 'fixed_table_rollback_duration'), (10, 'fixed_dblink_rollback_duration')
                    ) AS p(rank,key) ON (p.key = param_key)
          WHERE param_key <> 'emaj_version'
          ORDER BY rank
    LOOP
      v_params = v_params || E'    {\n'
                          ||  '      "key": ' || to_json(r_param.key) || E',\n'
                          ||  '      "value": ' || r_param.value || E'\n'
                          || E'    },\n';
    END LOOP;
    v_params = v_params || E'  ]\n';
-- build the trailer and remove illicite commas at the end of arrays and attributes lists
    v_params = v_params || E'}\n';
    v_params = regexp_replace(v_params, E',(\n *(\]|}))', '\1', 'g');
-- test the JSON format by casting the text structure to json and report a warning in case of problem
--   (this should not fail, unless the function code is bogus)
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

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration()
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_parameters_configuration$
-- This function returns a JSON formatted structure representing all the parameters registered in the emaj_param table.
-- The function can be called by clients like emaj_web.
-- This is just a wrapper of the internal _export_param_conf() function.
-- Output: the parameters content in JSON format
  BEGIN
    RETURN emaj._export_param_conf();
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration() IS
$$Generates a json structure describing the E-Maj parameters.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration(v_location TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_parameters_configuration$
-- This function stores the parameters configuration into a file on the server.
-- The JSON structure is built by the _export_param_conf() function.
-- Output: the number of parameters of the recorded JSON structure.
  DECLARE
    v_paramsJson             JSON;
  BEGIN
-- get the json structure
    SELECT emaj._export_param_conf() INTO v_paramsJson;
-- store the structure into the provided file name
    CREATE TEMP TABLE t (params TEXT);
    INSERT INTO t
      SELECT line FROM regexp_split_to_table(v_paramsJson::TEXT, '\n') AS line;
    BEGIN
      EXECUTE format ('COPY t TO %s',
                      quote_literal(v_location));
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_export_parameters_configuration: Unable to write to the % file.', v_location;
    END;
    DROP TABLE t;
-- return the number of recorded parameters
    RETURN json_array_length(v_paramsJson->'parameters');
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration(TEXT) IS
$$Generates and stores in a file a json structure describing the E-Maj parameters.$$;

CREATE OR REPLACE FUNCTION emaj._import_param_conf(v_paramsJson JSON, v_deleteCurrentConf BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_import_param_conf$
-- This function processes a JSON formatted structure representing the E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- If a parameter key is referenced several times, the last entry is the final value recorded into emaj_param.
-- The "emaj_version" parameter key is always left unchanged because it is a constant linked to the extension itself.
-- The expected JSON structure must contain an array like:
-- { "parameters": [
--      { "key": "...", "value": "..." },
--      { ... }
--    ] }
-- Input: - the parameter configuration structure in JSON format
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_parameters             JSON;
    v_nbParam                INT;
    v_key                    TEXT;
    v_value                  TEXT;
    r_param                  RECORD;
  BEGIN
-- look for the "parameters" json path
    v_parameters = v_paramsJson #> '{"parameters"}';
    IF v_parameters IS NULL THEN
      RAISE EXCEPTION '_import_param_conf: The "parameters" JSON sub-structure has not been found in the supplied structure';
    ELSE
      IF v_deleteCurrentConf THEN
-- if requested, delete the existing parameters, except the 'emaj_version'
--   (the trigger on emaj_param records the deletions into emaj_hist)
        DELETE FROM emaj.emaj_param WHERE param_key <> 'emaj_version';
      END IF;
-- process each parameter
      v_nbParam = 0;
      FOR r_param IN
          SELECT param FROM json_array_elements(v_parameters) AS t(param)
            WHERE param ->> 'key' <> 'emaj_version'
        LOOP
-- get each parameter from the list
          v_key = r_param.param ->> 'key';
          v_value = r_param.param ->> 'value';
-- check the key is valid.
          IF v_key NOT IN ('dblink_user_password', 'history_retention', 'alter_log_table',
                           'avg_row_rollback_duration', 'avg_row_delete_log_duration', 'avg_fkey_check_duration',
                           'fixed_step_rollback_duration', 'fixed_table_rollback_duration', 'fixed_dblink_rollback_duration') THEN
            RAISE EXCEPTION '_import_param_conf: "%" is not a known E-Maj parameter.', v_key;
          END IF;
          v_nbParam = v_nbParam + 1;
-- insert or update the parameter in the emaj_param table, selecting the right parameter value column type depending on the key
          IF v_key IN ('dblink_user_password', 'alter_log_table') THEN
            INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES (v_key, v_value)
              ON CONFLICT (param_key) DO
                UPDATE SET param_value_text = v_value WHERE EXCLUDED.param_key = v_key;
          ELSIF v_key IN ('history_retention', 'avg_row_rollback_duration', 'avg_row_delete_log_duration', 'avg_fkey_check_duration',
                         'fixed_step_rollback_duration', 'fixed_table_rollback_duration', 'fixed_dblink_rollback_duration') THEN
            INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES (v_key, v_value::INTERVAL)
              ON CONFLICT (param_key) DO
                UPDATE SET param_value_interval = v_value::INTERVAL WHERE EXCLUDED.param_key = v_key;
          END IF;
      END LOOP;
    END IF;
    RETURN v_nbParam;
  END;
$_import_param_conf$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(v_paramsJson JSON, v_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_parameters_configuration$
-- This function import a supplied JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- The function can be called by clients like emaj_web.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - the parameter configuration structure in JSON format
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_nbParam                INT;
  BEGIN
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN');
-- load the parameters
    SELECT emaj._import_param_conf(v_paramsJson, v_deleteCurrentConf) INTO v_nbParam;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(JSON,BOOLEAN) IS
$$Import a json structure describing E-Maj parameters to load.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(v_location TEXT, v_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_parameters_configuration$
-- This function imports a file containing a JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - input file location
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_paramsText             TEXT;
    v_paramsJson             JSON;
    v_nbParam                INT;
  BEGIN
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN', 'Input file: ' || quote_literal(v_location));
-- read the input file and put its content into a temporary table
    CREATE TEMP TABLE t (params TEXT);
    BEGIN
      EXECUTE format ('COPY t FROM %s',
                      quote_literal(v_location));
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_parameters_configuration: Unable to read the % file.', v_location;
    END;
-- aggregate the lines into a single text variable
    SELECT string_agg(params, E'\n') INTO v_paramsText FROM t;
    DROP TABLE t;
-- verify that the file content is a valid json structure
    BEGIN
      v_paramsJson = v_paramsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_parameters_configuration: The file content is not a valid JSON content.';
    END;
-- load the parameters
    SELECT emaj._import_param_conf(v_paramsJson, v_deleteCurrentConf) INTO v_nbParam;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(TEXT,BOOLEAN) IS
$$Import E-Maj parameters from a JSON formatted file.$$;

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
               '" is referenced by the the foreign key "' || conname ||
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

CREATE OR REPLACE FUNCTION emaj._verify_all_schemas()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_schemas$
-- The function verifies that all E-Maj schemas only contains E-Maj objects.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
  BEGIN
-- verify that the expected E-Maj schemas still exist
    RETURN QUERY
      SELECT DISTINCT 'Error: The E-Maj schema "' || sch_name || '" does not exist any more.' AS msg
        FROM emaj.emaj_schema
        WHERE NOT EXISTS (SELECT NULL FROM pg_catalog.pg_namespace WHERE nspname = sch_name)
        ORDER BY msg;
-- detect all objects that are not directly linked to a known table groups in all E-Maj schemas
-- scan pg_class, pg_proc, pg_type, pg_conversion, pg_operator, pg_opclass
    RETURN QUERY
      SELECT msg FROM (
-- look for unexpected tables
        SELECT nspname, 1, 'Error: In the schema "' || nspname ||
               '", the table "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND relnamespace = pg_namespace.oid AND relkind = 'r'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal tables
             AND NOT EXISTS                                                   -- exclude emaj log tables
                (SELECT NULL FROM emaj.emaj_relation WHERE rel_log_schema = nspname AND rel_log_table = relname)
        UNION ALL
-- look for unexpected sequences
        SELECT nspname, 2, 'Error: In the schema "' || nspname ||
               '", the sequence "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND relnamespace = pg_namespace.oid AND relkind = 'S'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal sequences
             AND NOT EXISTS                                                   -- exclude emaj log table sequences
                (SELECT NULL FROM emaj.emaj_relation WHERE rel_log_schema = nspname AND rel_log_sequence = relname)
        UNION ALL
-- look for unexpected functions
        SELECT nspname, 3, 'Error: In the schema "' || nspname ||
               '", the function "' || nspname || '"."' || proname  || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_proc, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND pronamespace = pg_namespace.oid
             AND (nspname <> v_emajSchema OR (proname NOT LIKE E'emaj\\_%' AND proname NOT LIKE E'\\_%'))
                                                                              -- exclude emaj internal functions
             AND NOT EXISTS (                                                 -- exclude emaj log functions
               SELECT NULL FROM emaj.emaj_relation WHERE rel_log_schema = nspname AND rel_log_function = proname)
        UNION ALL
-- look for unexpected composite types
        SELECT nspname, 4, 'Error: In the schema "' || nspname ||
               '", the type "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND relnamespace = pg_namespace.oid AND relkind = 'c'
             AND (nspname <> v_emajSchema OR (relname NOT LIKE E'emaj\\_%' AND relname NOT LIKE E'\\_%'))
                                                                              -- exclude emaj internal types
        UNION ALL
-- look for unexpected views
        SELECT nspname, 5, 'Error: In the schema "' || nspname ||
               '", the view "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND relnamespace = pg_namespace.oid  AND relkind = 'v'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal views
        UNION ALL
-- look for unexpected foreign tables
        SELECT nspname, 6, 'Error: In the schema "' || nspname ||
               '", the foreign table "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND relnamespace = pg_namespace.oid  AND relkind = 'f'
        UNION ALL
-- look for unexpected domains
        SELECT nspname, 7, 'Error: In the schema "' || nspname ||
               '", the domain "' || nspname || '"."' || typname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_type, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND typnamespace = pg_namespace.oid AND typisdefined and typtype = 'd'
        UNION ALL
-- look for unexpected conversions
        SELECT nspname, 8, 'Error: In the schema "' || nspname ||
               '", the conversion "' || nspname || '"."' || conname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_conversion, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND connamespace = pg_namespace.oid
        UNION ALL
-- look for unexpected operators
        SELECT nspname, 9, 'Error: In the schema "' || nspname ||
               '", the operator "' || nspname || '"."' || oprname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_operator, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND oprnamespace = pg_namespace.oid
        UNION ALL
-- look for unexpected operator classes
        SELECT nspname, 10, 'Error: In the schema "' || nspname ||
               '", the operator class "' || nspname || '"."' || opcname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_opclass, pg_catalog.pg_namespace, emaj.emaj_schema
           WHERE nspname = sch_name AND opcnamespace = pg_namespace.oid
        ORDER BY 1, 2, 3
      ) AS t;
    RETURN;
  END;
$_verify_all_schemas$;

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
-- check the value of the group_has_waiting_changes column of the emaj_group table, and reset it at the right value if needed
    PERFORM emaj._adjust_group_properties();
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
               LEFT OUTER JOIN emaj.emaj_group_def ON (rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
                                                       AND rel_group = grpdef_group)
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
              -- the tables that change their priority level
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
        SELECT DISTINCT rel_group AS group_name
          FROM tblseq_with_changes
      ),
      -- adjust the group_has_waiting_changes column, only when needed
      modified_group AS (
        UPDATE emaj.emaj_group SET group_has_waiting_changes = NOT group_has_waiting_changes
          WHERE (group_has_waiting_changes = FALSE
                 AND group_name IN (SELECT group_name FROM group_with_changes))
             OR (group_has_waiting_changes = TRUE
                 AND NOT EXISTS (SELECT 0 FROM group_with_changes WHERE group_with_changes.group_name = emaj_group.group_name))
          RETURNING group_name, group_has_waiting_changes
      ),
      -- insert a row in the history for each flag change
      hist_insert AS (
        INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
          SELECT 'ADJUST_GROUP_PROPERTIES', group_name, 'Set the group_has_waiting_changes column to ' || group_has_waiting_changes
            FROM modified_group
            ORDER BY group_name
      )
      SELECT count(*) INTO v_nbAdjustedGroups FROM modified_group;
    RETURN v_nbAdjustedGroups;
  END;
$_adjust_group_properties$;

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

GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, v_firstMarkTimeId BIGINT, v_lastMarkTimeId BIGINT) TO emaj_viewer;

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
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 3.2.0 completed');

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
