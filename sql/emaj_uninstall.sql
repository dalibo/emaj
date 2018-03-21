-- emaj_uninstall.sql
--
-- E-MAJ uninstall script : Version <devel>
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script uninstalls any E-Maj extension in version 2.0.0 and later.
-- It drops all components previously created by a "CREATE EXTENSION emaj;" statement and by the use of the extension.
--
-- The script must be executed by a role having SUPERUSER privileges.
--
-- After its execution, some operations may have to be done manually.

\set ON_ERROR_STOP ON
\set ECHO none
\echo '>>> Starting E-Maj uninstallation procedure...'

SET client_min_messages TO WARNING;

-- A single DO procedure performs the operation

DO LANGUAGE plpgsql 
$emaj_uninstall$
  DECLARE
    v_rolSuper              BOOLEAN;
    v_nonIdleGroupList      TEXT;
    v_nbObject              INTEGER;
    r_object                RECORD;
    v_roleToDrop            BOOLEAN;
    v_dbList                TEXT;
    v_granteeRoleList       TEXT;
    v_granteeClassList      TEXT;
    v_granteeFunctionList   TEXT;
    v_tspList               TEXT;
  BEGIN
--
-- Check the current role is superuser
    SELECT rolsuper INTO v_rolSuper FROM pg_roles WHERE rolname = current_user;
    IF NOT v_rolSuper THEN
      RAISE EXCEPTION 'emaj_uninstall: The role executing this script must be a superuser';
    END IF;
--
-- Check postgres version is at least 9.1
    IF current_setting('server_version_num')::int < 90100 THEN
      RAISE EXCEPTION 'emaj_uninstall: The postgres version is not compatible with this script (it should be at least 9.1)';
    END IF;
--
-- Check E-Maj is installed as an extension
    PERFORM 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_uninstall: E-Maj is not installed, or is not installed as an EXTENSION';
    END IF;
--
-- Check emaj schema is present
    PERFORM 1 FROM pg_namespace WHERE nspname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_uninstall: The schema ''emaj'' doesn''t exist';
    END IF;
--
-- Check that no E-Maj schema contain any non E-Maj object
    v_nbObject = 0;
    FOR r_object IN 
      SELECT msg FROM emaj._verify_all_schemas() msg 
        WHERE msg NOT LIKE 'The E-Maj schema % does not exist any more.'
      LOOP
-- a secondary schema contains objects that do not belong to E-Maj
      RAISE WARNING '%',r_object.msg;
      v_nbObject = v_nbObject + 1;
    END LOOP;
    IF v_nbObject > 0 THEN
      RAISE EXCEPTION 'There are % unexpected objects in E-Maj schemas. Drop them before reexecuting the uninstall function.', v_nbObject;
    END IF;
--
-- OK, all conditions are met
--
-- Disable event triggers that would block the DROP EXTENSION command
    PERFORM emaj.emaj_disable_protection_by_event_triggers();
--
-- If the emaj_demo_cleanup function exists (created by the emaj_demo.sql script), execute it
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_demo_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_demo_cleanup();
    END IF;
-- If the emaj_parallel_rollback_test_cleanup function exists (created by the emaj_prepare_parallel_rollback_test.sql script), execute it
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_parallel_rollback_test_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_parallel_rollback_test_cleanup();
    END IF;
--
-- Drop all created groups, bypassing potential errors, to remove all components not directly linked to the EXTENSION
    PERFORM emaj.emaj_force_drop_group(group_name) FROM emaj.emaj_group;
--
-- Drop the emaj extension
    DROP EXTENSION emaj;
--
-- Drop the primary schema.
    DROP SCHEMA IF EXISTS emaj CASCADE;
--
-- Drop the event trigger that is external to the extension, and its function
    DROP FUNCTION IF EXISTS public._emaj_protection_event_trigger_fnct() CASCADE;
--
-- Also revoke grants given on postgres functions to both emaj roles
    REVOKE ALL ON FUNCTION pg_size_pretty(bigint) FROM emaj_viewer;
    REVOKE ALL ON FUNCTION pg_database_size(name) FROM emaj_viewer;
    REVOKE ALL ON FUNCTION pg_size_pretty(bigint) FROM emaj_adm;
    REVOKE ALL ON FUNCTION pg_database_size(name) FROM emaj_adm;
-- revoke also the grant given to emaj_adm on the dblink_connect_u function at install time with E-Maj versions 1.n
    IF EXISTS(SELECT 1 FROM pg_catalog.pg_proc WHERE proname = 'dblink_connect_u') THEN
      REVOKE ALL ON FUNCTION dblink_connect_u(text,text) FROM emaj_adm;
    END IF;
--
-- Check if emaj roles can be dropped
    v_roleToDrop = true;
--
-- Are emaj_roles also used in other databases of the cluster ?
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_viewer' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_uninstall: emaj_viewer role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_adm' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_uninstall: emaj_adm role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to other roles ?
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_viewer';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_viewer role.', v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_adm';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_adm role.', v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_viewer role has some remaining grants on tables, views or sequences (%).', v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_adm=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_adm role has some remaining grants on tables, views or sequences (%).', v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to functions (other than just dropped emaj ones) ?
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeFunctionList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_viewer role has some remaining grants on functions (%).', v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_adm=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeClassList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_adm role has some remaining grants on functions (%).', v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
-- If emaj roles can be dropped, drop them
    IF v_roleToDrop THEN
-- revoke the remaining grants set on tablespaces
      SELECT string_agg(spcname, ', ') INTO v_tspList FROM pg_catalog.pg_tablespace
        WHERE array_to_string (spcacl,';') LIKE '%emaj_viewer=%' OR array_to_string (spcacl,';') LIKE '%emaj_adm=%';
      IF v_tspList IS NOT NULL THEN
        EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_viewer, emaj_adm';
      END IF;
-- and drop both emaj_viewer and emaj_adm roles
      DROP ROLE emaj_viewer, emaj_adm;
      RAISE WARNING 'emaj_uninstall: emaj_adm and emaj_viewer roles have been dropped.';
    ELSE
      RAISE WARNING 'emaj_uninstall: For these reasons, emaj roles are not dropped by this script.';
    END IF;
--
    RETURN;
  END;
$emaj_uninstall$;

SET client_min_messages TO default;
\echo '>>> E-maj successfully uninstalled from this database'

