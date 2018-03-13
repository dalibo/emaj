-- emaj_upgrade_after_postgres_upgrade.sql
-- This psql script belongs to the E-Maj extension
--
-- The script has to be run after a major postgres version upgrade. It installs the new or modified E-Maj components needed by
-- the newly upgraded postgres version.
-- Currently, it only manages the components that are needed to setup the event trigger protection. These components are also created
-- by the standart installation procedure. But their effective creation depends on the postgres version used at installation time
-- because the event trigger feature doesn't exist in the oldest postgres versions supported by E-Maj. So the script is useful when
-- E-Maj was initially installed with a postgres version prior 9.5 and a postgres version upgrade leads to a more recent version.
--
-- It must be executed once connected as SUPERUSER.
--
--\set ECHO all
\set ON_ERROR_STOP on

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
-- the emaj version registered in emaj_param must be '<devel>'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '<devel>' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not <devel>.',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.1
    IF current_setting('server_version_num')::int < 90100 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with E-Maj <devel>. The PostgreSQL version should be at least 9.1.', current_setting('server_version');
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...
BEGIN;
-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

--
-- Create objects that depends on postgres version
--
DO
$do$
  BEGIN

-- beginning of 9.3+ specific code
    IF emaj._pg_version_num() >= 90300 THEN
-- the sql_drop event trigger is only possible with postgres 9.3+
-- if the emaj_protection_trg event trigger does not exist, create it
      PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname = 'emaj_protection_trg';
      IF FOUND THEN
        RAISE NOTICE 'E-Maj upgrade script: the "emaj_protection_trg" event trigger already exists.';
      ELSE
        RAISE NOTICE 'E-Maj upgrade script: create the "emaj_protection_trg" event trigger.';
--
CREATE OR REPLACE FUNCTION public._emaj_protection_event_trigger_fnct()
 RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_emaj_protection_event_trigger_fnct$
-- This function is called by the emaj_protection_trg event trigger
-- The function only blocks any attempt to drop the emaj schema or the emaj extension
-- It is located into the public schema to be able to detect the emaj schema removal attempt
-- It is also unlinked from the emaj extension to be able to detest the emaj extension removal attempt
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
        RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the schema "emaj". Please use the emaj_uninstall.sql script if you really want to remove all E-Maj components.';
      END IF;
      IF r_dropped.object_type = 'extension' AND r_dropped.object_name = 'emaj' THEN
-- detecting an attempt to drop the emaj extension
        RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the emaj extension. Please use the emaj_uninstall.sql script if you really want to remove all E-Maj components.';
      END IF;
    END LOOP;
  END;
$_emaj_protection_event_trigger_fnct$;
COMMENT ON FUNCTION public._emaj_protection_event_trigger_fnct() IS
$$E-Maj extension: support of the emaj_protection_trg event trigger.$$;

-- all commands involving an event trigger is submitted by an EXECUTE instruction
-- this is needed for compatibility with pre 9.3 postgres version that do not know event triggers
EXECUTE '
CREATE EVENT TRIGGER emaj_protection_trg
  ON sql_drop
  WHEN TAG IN (''DROP EXTENSION'',''DROP SCHEMA'')
  EXECUTE PROCEDURE public._emaj_protection_event_trigger_fnct();
';
EXECUTE '
COMMENT ON EVENT TRIGGER emaj_protection_trg IS
$$Blocks the removal of the emaj extension or schema.$$;
';

      END IF;
-- if the emaj_sql_drop_trg event trigger does not exist, create it
      PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname = 'emaj_sql_drop_trg';
      IF FOUND THEN
        RAISE NOTICE 'E-Maj upgrade script: the "emaj_sql_drop_trg" event trigger already exists.';
      ELSE
        RAISE NOTICE 'E-Maj upgrade script: create the "emaj_sql_drop_trg" event trigger.';
--

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
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the application schema "%". But it belongs to the active tables groups "%".', r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the schema being dropped is not an E-Maj schema containing log tables
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the schema "%". But dropping an E-Maj schema is not allowed.', r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'table' THEN
-- the object is a table
--   look at the emaj_relation table to verify that the table being dropped does not belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name
              AND group_name = rel_group AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the application table "%.%". But it belongs to the active tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the table being dropped is not a log table
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.schema_name AND rel_log_table = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the log table "%.%". But dropping an E-Maj log table is not allowed.',
                            r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'sequence' THEN
-- the object is a sequence
--   look at the emaj_relation table to verify that the sequence being dropped does not belong to any active (not stopped) group
          SELECT rel_group INTO v_groupName FROM emaj.emaj_relation, emaj.emaj_group
            WHERE rel_schema = r_dropped.schema_name AND rel_tblseq = r_dropped.object_name
              AND group_name = rel_group AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the application sequence "%.%". But it belongs to the active tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
--   look at the emaj_relation table to verify that the sequence being dropped is not a log sequence
          PERFORM 1 FROM emaj.emaj_relation
            WHERE rel_log_schema = r_dropped.schema_name AND rel_log_sequence = r_dropped.object_name;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the log sequence "%.%". But dropping an E-Maj sequence is not allowed.', r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'function' THEN
-- the object is a function
--   look at the emaj_relation table to verify that the function being dropped is not a log function
          PERFORM 1 FROM emaj.emaj_relation
            WHERE  r_dropped.object_identity = quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_function) || '()';
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the log function "%". But dropping an E-Maj log function is not allowed.', r_dropped.object_identity;
          END IF;
        WHEN r_dropped.object_type = 'trigger' THEN
-- the object is a trigger
--   if postgres version is 9.5+ (to see the 'original' column of the pg_event_trigger_dropped_objects() function),
--   look at the trigger name pattern to identify emaj trigger
--   and do not raise an exception if the triggers drop is derived from a drop of a table or a function
          IF emaj._pg_version_num() >= 90500 THEN
            IF r_dropped.original AND
               (r_dropped.object_identity LIKE 'emaj_log_trg%' OR r_dropped.object_identity LIKE 'emaj_trunc_trg%') THEN
              RAISE EXCEPTION 'E-Maj event trigger: attempting to drop the "%" E-Maj trigger. But dropping an E-Maj trigger is not allowed.', r_dropped.object_identity;
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

EXECUTE '
CREATE EVENT TRIGGER emaj_sql_drop_trg
  ON sql_drop
  WHEN TAG IN (''DROP FUNCTION'',''DROP SCHEMA'',''DROP SEQUENCE'',''DROP TABLE'',''DROP TRIGGER'')
  EXECUTE PROCEDURE emaj._event_trigger_sql_drop_fnct();
';
EXECUTE '
COMMENT ON EVENT TRIGGER emaj_sql_drop_trg IS
$$Controls the removal of E-Maj components.$$;
';

-- add both event trigger components to the emaj extension
ALTER EXTENSION emaj ADD FUNCTION emaj._event_trigger_sql_drop_fnct();
EXECUTE '
ALTER EXTENSION emaj ADD EVENT TRIGGER emaj_sql_drop_trg;
';

      END IF;
-- end of 9.3+ specific code
    END IF;

-- beginning of 9.5+ specific code
    IF emaj._pg_version_num() >= 90500 THEN
-- table_rewrite event trigger are only possible with postgres 9.5+
-- if the emaj_table_rewrite_trg event trigger does not exist, create it
      PERFORM 1 FROM pg_catalog.pg_event_trigger WHERE evtname = 'emaj_table_rewrite_trg';
      IF FOUND THEN
        RAISE NOTICE 'E-Maj upgrade script: the "emaj_table_rewrite_trg" event trigger already exists.';
      ELSE
        RAISE NOTICE 'E-Maj upgrade script: create the "emaj_table_rewrite_trg" event trigger.';
--

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
      RAISE EXCEPTION 'E-Maj event trigger: attempting to change the application table "%.%" structure. But the table belongs to the active tables group "%".',
                      v_tableSchema, v_tableName , v_groupName;
    END IF;
-- look at the emaj_relation table to verify that the table being rewritten is not a known log table
    SELECT rel_group INTO v_groupName FROM emaj.emaj_relation
      WHERE rel_log_schema = v_tableSchema AND rel_log_table = v_tableName;
    IF FOUND THEN
-- the table is an E-Maj log table, so raise an exception
      RAISE EXCEPTION 'E-Maj event trigger: attempting to change the log table "%.%" structure. But the table belongs to the tables group "%".',
                      v_tableSchema, v_tableName , v_groupName;
    END IF;
  END;
$_emaj_event_trigger_table_rewrite_fnct$;

COMMENT ON FUNCTION emaj._emaj_event_trigger_table_rewrite_fnct() IS
$$E-Maj extension: support of the emaj_table_rewrite_trg event trigger.$$;

EXECUTE '
CREATE EVENT TRIGGER emaj_table_rewrite_trg
  ON table_rewrite
  EXECUTE PROCEDURE emaj._emaj_event_trigger_table_rewrite_fnct();
';
EXECUTE '
COMMENT ON EVENT TRIGGER emaj_table_rewrite_trg IS
$$Controls some changes in E-Maj tables structure.$$;
';

-- add both event trigger components to the emaj extension
ALTER EXTENSION emaj ADD FUNCTION emaj._emaj_event_trigger_table_rewrite_fnct();
EXECUTE '
ALTER EXTENSION emaj ADD EVENT TRIGGER emaj_table_rewrite_trg;
';

      END IF;
-- end of 9.5+ specific code
    END IF;
  END;
$do$;
COMMIT;

