--
-- E-MAJ uninstall script : V 0.12.0
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script drops all components previously created by emaj.sql script that installs E-maj contrib.
-- It is also usable when E-Maj has been installed with a CREATE EXTENSION statement (in PG 9.1+).
--
-- This script must be executed by a role having SUPERUSER privileges.
--
-- After its execution, some operations may have to be done manually.
-- In particular, the 'tspemaj' tablespace is not automaticaly dropped.

\set ON_ERROR_STOP ON
--\set QUIET ON
SET client_min_messages TO WARNING;
\echo 'E-maj objects deletion...'

BEGIN TRANSACTION;

-- Create a temporary function in public schema that performs the operation

CREATE or REPLACE FUNCTION public.emaj_uninstall() 
RETURNS VOID LANGUAGE plpgsql AS 
$emaj_uninstall$
-- This temporary function verifies if current role is superuser, and if all created group are in IDLE state.
  DECLARE
    v_flgSuper              BOOLEAN;
    r_group                 RECORD;
    v_nonIdleGroupList      TEXT;
    v_roleToDrop            BOOLEAN;
    r_database              RECORD;
    v_dbList                TEXT;
    r_role                  RECORD;
    v_granteeRoleList       TEXT;
    r_class                 RECORD;
    v_granteeClassList      TEXT;
  BEGIN
--
-- Is the current role superuser ?
    SELECT rolsuper INTO v_flgSuper FROM pg_roles WHERE rolname = current_user;
-- If no, stop
    IF v_flgSuper <> 'true' THEN
      RAISE EXCEPTION 'emaj_uninstall: The role executing this function must be a superuser';
    END IF;
--
-- Is emaj schema present ?
    PERFORM 1 FROM pg_namespace WHERE nspname = 'emaj';
    IF NOT FOUND THEN
-- If no, stop
      RAISE EXCEPTION 'emaj_uninstall: The schema ''emaj'' doesn''t exist';
    ELSE
-- If yes, check there are no remaining emaj group not in IDLE state.
-- This check is performed just to be sure that the script is not called at the bad moment.
      v_nonIdleGroupList = '';
      FOR r_group IN 
        SELECT group_name FROM emaj.emaj_group WHERE group_state <> 'IDLE'
      LOOP
        IF v_nonIdleGroupList = '' THEN
          v_nonIdleGroupList = r_group.group_name;
        ELSE
          v_nonIdleGroupList = v_nonIdleGroupList || ', ' || r_group.group_name;
        END IF;
      END LOOP;
      IF v_nonIdleGroupList <> '' THEN
        RAISE EXCEPTION 'emaj_uninstall: There are remaining active groups (not in IDLE state): %. Stop them before restarting the uninstall script',v_nonIdleGroupList;
      END IF;
    END IF;
-- OK, drop the schema. This will drop all tables groups. So no need to call emaj_drop_group() function.
    DROP SCHEMA IF EXISTS emaj CASCADE;
-- Also revoke grants given on postgres function to both emaj roles
    REVOKE ALL ON FUNCTION pg_size_pretty(bigint) FROM emaj_viewer;
    REVOKE ALL ON FUNCTION pg_database_size(name) FROM emaj_viewer;
    REVOKE ALL ON FUNCTION pg_size_pretty(bigint) FROM emaj_adm;
    REVOKE ALL ON FUNCTION pg_database_size(name) FROM emaj_adm;
--
-- Check if the schema 'myschema' used by test scripts exists.
    PERFORM 1 FROM pg_namespace WHERE nspname = 'myschema';
    IF FOUND THEN
      RAISE WARNING 'emaj_uninstall: A schema myschema exists on the database. It may have been created by an emaj test script. You can drop it if you wish using a "DROP SCHEMA myschema cascade;" command.';
    END IF;
--
-- Check if the role 'myuser' used by test scripts exists.
    PERFORM 1 FROM pg_roles WHERE rolname = 'myuser';
    IF FOUND THEN
      RAISE WARNING 'emaj_uninstall: A role myuser exists on the cluster. It may have been created by an emaj test script. You can drop it if you wish using a "DROP ROLE myuser;" command.';
    END IF;
--
-- Check if emaj roles can be dropped
    v_roleToDrop = true;
--
-- Are emaj_roles also used in other databases of the cluster ?
    v_dbList = '';
    FOR r_database IN
      SELECT DISTINCT datname FROM pg_shdepend shd, pg_database db, pg_roles r 
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_adm' AND datname <> current_database()
    LOOP
      IF v_dbList = '' THEN
        v_dbList = r_database.datname;
      ELSE
        v_dbList = v_dbList || ', ' || r_database.datname;
      END IF;
    END LOOP;
    IF v_dbList <> '' THEN
      RAISE WARNING 'emaj_uninstall: emaj_adm role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
    v_dbList = '';
    FOR r_database IN
      SELECT DISTINCT datname FROM pg_shdepend shd, pg_database db, pg_roles r 
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_viewer' AND datname <> current_database()
    LOOP
      IF v_dbList = '' THEN
        v_dbList = r_database.datname;
      ELSE
        v_dbList = v_dbList || ', ' || r_database.datname;
      END IF;
    END LOOP;
    IF v_dbList <> '' THEN
      RAISE WARNING 'emaj_uninstall: emaj_viewer role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to other roles ?
    v_granteeRoleList = '';
    FOR r_role IN 
   	  SELECT q.rolname FROM pg_auth_members m, pg_roles r, pg_roles q 
        WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_viewer'
    LOOP
      IF v_granteeRoleList = '' THEN
        v_granteeRoleList = r_role.rolname;
      ELSE
        v_granteeRoleList = v_granteeRoleList || ', ' || r_role.rolname;
      END IF;
    END LOOP;
    IF v_granteeRoleList <> '' THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_viewer role.', v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeRoleList = '';
    FOR r_role IN 
   	  SELECT q.rolname FROM pg_auth_members m, pg_roles r, pg_roles q 
        WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_adm'
    LOOP
      IF v_granteeRoleList = '' THEN
        v_granteeRoleList = r_role.rolname;
      ELSE
        v_granteeRoleList = v_granteeRoleList || ', ' || r_role.rolname;
      END IF;
    END LOOP;
    IF v_granteeRoleList <> '' THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_adm role.', v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
    v_granteeClassList = '';
    FOR r_class IN 
      SELECT nspname, relname FROM pg_namespace, pg_class 
        WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_viewer=%'
    LOOP
      IF v_granteeClassList = '' THEN
        v_granteeClassList = r_class.nspname || '.' || r_class.relname;
      ELSE
        v_granteeClassList = v_granteeClassList || ', ' || r_class.nspname || '.' || r_class.relname;
      END IF;
    END LOOP;
    IF v_granteeClassList <> '' THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_viewer role has some remaining grants on tables, views or sequences (%).', v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeClassList = '';
    FOR r_class IN 
      SELECT nspname, relname FROM pg_namespace, pg_class 
        WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_adm=%'
    LOOP
      IF v_granteeClassList = '' THEN
        v_granteeClassList = r_class.nspname || '.' || r_class.relname;
      ELSE
        v_granteeClassList = v_granteeClassList || ', ' || r_class.nspname || '.' || r_class.relname;
      END IF;
    END LOOP;
    IF v_granteeClassList <> '' THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_uninstall: emaj_adm role has some remaining grants on tables, views or sequences (%).', v_granteeClassList;
      v_roleToDrop = false;
    END IF;
-- If emaj roles can be dropped, drop them
    IF v_roleToDrop THEN
-- OK, drop both emaj_viewer and emaj_adm roles
      REVOKE ALL ON TABLESPACE tspemaj FROM emaj_viewer;
      DROP ROLE emaj_viewer;
      REVOKE ALL ON TABLESPACE tspemaj FROM emaj_adm;
      DROP ROLE emaj_adm;
      RAISE WARNING 'emaj_uninstall: emaj_adm and emaj_viewer roles have been dropped.';
    ELSE
      RAISE WARNING 'emaj_uninstall: For these reasons, emaj roles are not dropped by this procedure.';
    END IF;
--
-- Tablespace tspemaj is not dropped
    RAISE WARNING 'emaj_uninstall: The tablespace tspemaj is not dropped by this procedure. You can drop it if you wish using a "DROP TABLESPACE tspemaj" command.';
--
    RETURN;
  END;
$emaj_uninstall$;

SELECT public.emaj_uninstall();

DROP FUNCTION public.emaj_uninstall();

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-maj successfully uninstalled'

