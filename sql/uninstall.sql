--
-- E-MAJ uninstall script : V 0.10.0
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
    v_flgSuper           BOOLEAN;
    r_group              RECORD;
    v_nonIdleGroupList   TEXT    = '';
    r_role               RECORD;
    v_granteeRoleList    TEXT;
    r_class              RECORD;
    v_granteeClassList   TEXT;
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
      RAISE WARNING 'emaj_uninstall: The schema ''emaj'' doesn''t exist';
    ELSE
-- If yes, check there are no remaining emaj group not in IDLE state.
-- This check is performed just to be sure that the script is not called at the bad moment.
      FOR r_group IN 
        SELECT group_name FROM emaj.emaj_group WHERE group_state <> 'IDLE'
      LOOP
        IF v_nonIdleGroupList = '' THEN
          v_nonIdleGroupList = v_nonIdleGroupList || r_group.group_name;
        ELSE
          v_nonIdleGroupList = ', ' || v_nonIdleGroupList || r_group.group_name;
        END IF;
      END LOOP;
      IF v_nonIdleGroupList <> '' THEN
        RAISE EXCEPTION 'emaj_uninstall: There are remaining active groups (not in IDLE state): %. Stop them before restarting the uninstall script',v_nonIdleGroupList;
      END IF;
-- then drop the schema. This will drop all tables groups. So no need to call emaj_drop_group() function.
      DROP SCHEMA IF EXISTS emaj CASCADE;
    END IF;
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
-- Are emaj roles granted to other roles ?
-- And are emaj roles granted to relations (tables, views, sequences) other than emaj ones ?
-- If yes for one of these questions, roles are not automatically dropped.
-- First look at the emaj_viewer role and other granted roles
    v_granteeRoleList = '';
    FOR r_role IN 
   	  SELECT q.rolname FROM pg_auth_members m, pg_roles r, pg_roles q 
        WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_viewer'
    LOOP
      IF v_granteeRoleList = '' THEN
        v_granteeRoleList = v_granteeRoleList || r_role.rolname;
      ELSE
        v_granteeRoleList = v_granteeRoleList || ', ' || r_role.rolname;
      END IF;
    END LOOP;
    IF v_granteeRoleList <> '' THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_viewer role. REVOKE emaj_viewer from them and then DROP the emaj_viewer role',v_granteeRoleList;
    ELSE
-- Then check emaj_viewer has no application relations' grants
      v_granteeClassList = '';
      FOR r_class IN 
        SELECT nspname, relname FROM pg_namespace, pg_class 
          WHERE pg_namespace.oid = relnamespace
            AND substr(relname,1,5) <> 'emaj_' AND array_to_string (relacl,';') LIKE '%emaj_viewer=%'
      LOOP
        IF v_granteeClassList = '' THEN
          v_granteeClassList = v_granteeClassList || r_class.nspname || '.' || r_class.relname;
        ELSE
          v_granteeClassList = v_granteeClassList || ', ' || r_class.nspname || '.' || r_class.relname;
        END IF;
      END LOOP;
      IF v_granteeClassList <> '' THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_uninstall: emaj_viewer role has some remaining grants on tables, views or sequences (%). REVOKE these grants before dropping the emaj_viewer role',v_granteeClassList;
      ELSE
-- OK, drop the emaj_viewer role
        REVOKE ALL ON TABLESPACE tspemaj FROM emaj_viewer;
        DROP ROLE emaj_viewer;
      END IF;
    END IF;
-- Then look at the emaj_adm role and other granted roles
    v_granteeRoleList = '';
    FOR r_role IN 
   	  SELECT q.rolname FROM pg_auth_members m, pg_roles r, pg_roles q 
        WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_adm'
    LOOP
      IF v_granteeRoleList = '' THEN
        v_granteeRoleList = v_granteeRoleList || r_role.rolname;
      ELSE
        v_granteeRoleList = v_granteeRoleList || ', ' || r_role.rolname;
      END IF;
    END LOOP;
    IF v_granteeRoleList <> '' THEN
      RAISE WARNING 'emaj_uninstall: There are remaining roles (%) who have been granted emaj_adm role. REVOKE emaj_adm from them and then DROP the emaj_adm role',v_granteeRoleList;
    ELSE
-- Then check emaj_adm has no application relations' grants
      v_granteeClassList = '';
      FOR r_class IN 
        SELECT nspname, relname FROM pg_namespace, pg_class 
          WHERE pg_namespace.oid = relnamespace
            AND substr(relname,1,5) <> 'emaj_' AND array_to_string (relacl,';') LIKE '%emaj_adm=%'
      LOOP
        IF v_granteeClassList = '' THEN
          v_granteeClassList = v_granteeClassList || r_class.nspname || '.' || r_class.relname;
        ELSE
          v_granteeClassList = v_granteeClassList || ', ' || r_class.nspname || '.' || r_class.relname;
        END IF;
      END LOOP;
      IF v_granteeClassList <> '' THEN
        IF length(v_granteeClassList) > 200 THEN
          v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
        END IF;
        RAISE WARNING 'emaj_uninstall: emaj_adm role has some remaining grants on tables, views or sequences (%). REVOKE these grants before dropping the emaj_adm role',v_granteeClassList;
      ELSE
-- OK, drop the emaj_adm role
        REVOKE ALL ON TABLESPACE tspemaj FROM emaj_adm;
        DROP ROLE emaj_adm;
      END IF;
    END IF;
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

