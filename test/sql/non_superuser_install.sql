-- non_superuser_install.sql
--     single script to test emaj installed with psql by a non superuser role.
--     various use case are tested, giving more and more capabilities to the installer role
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement)
\set VERBOSITY terse

SET client_min_messages TO warning;

------------------------------------------------------------
-- Setup objects needed for the whole test scenario by the superuser.
------------------------------------------------------------

-- Drop emaj and test roles, if they exist and if nothing blocks it.
DROP ROLE IF EXISTS emaj_adm, emaj_viewer;
DROP ROLE IF EXISTS _regress_emaj_app, _regress_emaj_install, _regress_emaj_admin2;

-- _regress_emaj_app role: owns the application objects.
CREATE ROLE _regress_emaj_app LOGIN PASSWORD 'app';
GRANT ALL ON DATABASE regression TO _regress_emaj_app;

-- _regress_emaj_install owns the emaj environment.
CREATE ROLE _regress_emaj_install LOGIN PASSWORD 'install';
GRANT ALL ON DATABASE regression TO _regress_emaj_install;
GRANT CREATE ON SCHEMA public TO _regress_emaj_install;

-- _regress_emaj_admin2 is another administration role.
CREATE ROLE _regress_emaj_admin2 LOGIN PASSWORD 'admin2';

------------------------------------------------------------
-- Create application objects.
------------------------------------------------------------

-- Objects owned by the installer (_regress_emaj_install).

SET session_authorization TO _regress_emaj_install;

DROP SCHEMA IF EXISTS instSchema1 CASCADE;
CREATE SCHEMA instSchema1;

SET search_path TO instSchema1;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);

RESET search_path;

-- Objects owned by the another role (_regress_emaj_app).

SET session_authorization TO _regress_emaj_app;

DROP SCHEMA IF EXISTS appSchema1 CASCADE;
CREATE SCHEMA appSchema1;

SET search_path TO appSchema1;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);
CREATE TABLE myTbl2 (
  col21       SERIAL           not NULL,
  col22       TEXT             ,
  PRIMARY KEY (col21)
);

CREATE SEQUENCE mySeq1;

GRANT USAGE ON SCHEMA appschema1 TO _regress_emaj_install;
GRANT ALL ON ALL TABLES IN SCHEMA appSchema1 TO _regress_emaj_install;
GRANT ALL ON ALL SEQUENCES IN SCHEMA appSchema1 TO _regress_emaj_install;

RESET search_path;

------------------------------------------------------------
-- Step 1: the installer role has the minimum rights to use E-Maj.
------------------------------------------------------------

SET session_authorization TO _regress_emaj_install;

-- Install emaj.
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- Check the installation.
SELECT hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
SELECT verh_version FROM emaj.emaj_version_hist;
SELECT * FROM emaj.emaj_install_conf;
SELECT * FROM emaj.emaj_verify_all();

-- Build a table group.
SELECT emaj.emaj_create_group('instGroup1');
SELECT emaj.emaj_assign_table('instschema1', 'mytbl1', 'instGroup1');
SELECT emaj.emaj_assign_sequence('instschema1', 'mytbl1_col11_seq', 'instGroup1');

-- Start the group and perform data changes.
SELECT emaj.emaj_start_group('instGroup1', 'M1');
INSERT INTO instSchema1.mytbl1 (col12) VALUES ('Row 1');

-- Set a mark and perform data change.
SELECT emaj.emaj_set_mark_group('instGroup1', 'M2');
UPDATE instSchema1.mytbl1 SET col12 = 'Modified row 1' WHERE col11 = 1;
INSERT INTO instSchema1.mytbl1 (col12) VALUES ('Row 2');

-- Logged rollback to M2 (without dblink connection).
SELECT * FROM emaj.emaj_logged_rollback_group('instGroup1', 'M2');
SELECT hist_object, hist_wording FROM emaj.emaj_hist WHERE hist_function = 'DBLINK_OPEN_CNX' ORDER BY hist_id DESC LIMIT 1;

-- Test inoperative or forbidden function calls.
SELECT emaj.emaj_disable_protection_by_event_triggers();
SELECT emaj.emaj_enable_protection_by_event_triggers();

-- Missing pg_read_server_files grant.
SELECT emaj.emaj_import_groups_configuration('/tmp/emaj_test');
SELECT emaj.emaj_import_parameters_configuration('/tmp/emaj_test');

-- Missing pg_write_server_files grant.
SELECT emaj.emaj_export_groups_configuration('/tmp/emaj_test');
SELECT emaj.emaj_export_parameters_configuration('/tmp/emaj_test');
SELECT emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
SELECT emaj.emaj_dump_changes_group('instGroup1', 'M1', NULL, '', NULL, '/tmp');
SELECT emaj.emaj_snap_group('instGroup1', '/tmp', NULL);
SELECT emaj.emaj_gen_sql_group('instGroup1', 'M1', NULL, '/tmp');

-- Missing pg_execute_server_program grant.
RESET session_authorization;
GRANT pg_write_server_files TO _regress_emaj_install;
SET session_authorization TO _regress_emaj_install;

SELECT emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
SELECT emaj.emaj_dump_changes_group('instGroup1', 'M1', NULL, '', NULL, '/tmp');

RESET session_authorization;
REVOKE pg_write_server_files FROM _regress_emaj_install;

SET session_authorization TO _regress_emaj_install;

-- Use perl clients.

\! ${EMAJ_DIR}/client/emajStat.pl -d regression -U _regress_emaj_install -W install --regression-test --no-cls --interval 0.1 --iter 2

\! ${EMAJ_DIR}/client/emajRollbackMonitor.pl -d regression -U _regress_emaj_install -W install -i 0.1 -n 2 -l 2 -a 12 -v -r

\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_install -W install -g "instGroup1" -m M2 -s 2

------------------------------------------------------------
-- Step 2: install dblink and perform rollback (no reinstall).
------------------------------------------------------------

RESET session_authorization;

CREATE EXTENSION dblink;
GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO _regress_emaj_install;

SET session_authorization TO _regress_emaj_install;

-- Rollback to M2 with dblink connection.
SELECT emaj.emaj_set_param('dblink_user_password', 'user=_regress_emaj_install password=install');
SELECT * FROM emaj.emaj_rollback_group('instGroup1', 'M2');
SELECT hist_object, hist_wording FROM emaj.emaj_hist WHERE hist_function = 'DBLINK_OPEN_CNX' ORDER BY hist_id DESC LIMIT 1;

-- Parallel rollback.
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_install -W install -g "instGroup1" -m M2 -s 2

------------------------------------------------------------
-- Step 3: try to assign application objects owned by another role (no reinstall).
------------------------------------------------------------

-- Try to assign tables in group instGroup1.
SELECT emaj.emaj_assign_table('appschema1', 'mytbl1', 'instGroup1');
SELECT emaj.emaj_assign_tables('appschema1', ARRAY['mytbl1', 'mytbl2'], 'instGroup1');
SELECT emaj.emaj_assign_tables('appschema1', '.*', '', 'instGroup1');

-- Try to assign sequences in group instGroup1.
SELECT emaj.emaj_assign_sequence('appschema1', 'myseq1', 'instGroup1');
SELECT emaj.emaj_assign_sequences('appschema1', ARRAY['mytbl1_col11_seq', 'mytbl2_col21_seq'], 'instGroup1');
SELECT emaj.emaj_assign_sequences('appschema1', '.*', '', 'instGroup1');

-- Try to import groups configuration with tables and sequences having an owner who is not the installer role.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "instGroup1",
																	 "tables": [ { "schema": "appschema1", "table": "mytbl1" },
																				 { "schema": "appschema1", "table": "mytbl2" }],
																	 "sequences": [ { "schema": "appschema1", "sequence": "mytbl1_col11_seq" },
																			        { "schema": "appschema1", "sequence": "mytbl2_col21_seq" }] } ]}'::json,
											 NULL, TRUE);

-- Change the table/sequence ownership.
RESET session_authorization;
ALTER TABLE instschema1.mytbl1 owner TO _regress_emaj_app;

SET session_authorization TO _regress_emaj_install;
SELECT * FROM emaj.emaj_verify_all();

RESET session_authorization;
ALTER TABLE instschema1.mytbl1 owner TO _regress_emaj_install;

------------------------------------------------------------
-- Step 4: add grants to perform COPY FROM or TO (no reinstall).
------------------------------------------------------------
RESET session_authorization;

GRANT pg_read_server_files TO _regress_emaj_install;
GRANT pg_write_server_files TO _regress_emaj_install;
GRANT pg_execute_server_program TO _regress_emaj_install;

SET session_authorization TO _regress_emaj_install;

SELECT emaj.emaj_export_groups_configuration('/tmp/emaj_test');
SELECT emaj.emaj_import_groups_configuration('/tmp/emaj_test', NULL, TRUE);
SELECT emaj.emaj_export_parameters_configuration('/tmp/emaj_test');
SELECT emaj.emaj_import_parameters_configuration('/tmp/emaj_test');
SELECT emaj.emaj_gen_sql_dump_changes_group('instGroup1', 'M1', 'M2', '', NULL, '/tmp/emaj_test');
\! rm /tmp/emaj_test

------------------------------------------------------------
-- Step 5: let another role perform emaj administration tasks (no reinstall).
------------------------------------------------------------
RESET session_authorization;
GRANT _regress_emaj_install TO _regress_emaj_admin2;

RESET session_authorization;
SET session_authorization TO _regress_emaj_admin2;

SELECT * FROM emaj.emaj_verify_all();
SELECT emaj.emaj_stop_groups(emaj.emaj_get_logging_groups());

-- Drop the extension.
RESET session_authorization;
SET session_authorization TO _regress_emaj_install;
SELECT emaj.emaj_drop_extension();

------------------------------------------------------------
-- Step 6: create an emaj_adm role and reinstall the extension.
------------------------------------------------------------
RESET session_authorization;
CREATE ROLE emaj_adm;
GRANT emaj_adm TO _regress_emaj_install;
REVOKE ALL ON FUNCTION dblink_connect_u(TEXT, TEXT) FROM _regress_emaj_install;

SET session_authorization TO _regress_emaj_install;

-- Install emaj.
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- Check the installation.
SELECT hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
SELECT * FROM emaj.emaj_install_conf;
SELECT * FROM emaj.emaj_verify_all();

-- Drop the extension.
SELECT emaj.emaj_drop_extension();

------------------------------------------------------------
-- Step 7: add grants to _regress_emaj_install and reinstall the extension.
------------------------------------------------------------
RESET session_authorization;
GRANT emaj_adm TO _regress_emaj_install WITH ADMIN TRUE;
ALTER ROLE _regress_emaj_install CREATEROLE;

SET session_authorization TO _regress_emaj_install;

-- Install emaj.
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- Check the installation.
SELECT hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
SELECT * FROM emaj.emaj_install_conf;

-- Grant emaj_viewer to _regress_emaj_admin2 and try some emaj functions.
RESET session_authorization;
REVOKE _regress_emaj_install FROM _regress_emaj_admin2;
GRANT emaj_viewer TO _regress_emaj_admin2;

SET session_authorization TO _regress_emaj_admin2;
SELECT * FROM emaj.emaj_verify_all();

-- Grant emaj_adm to _regress_emaj_admin2 and try some emaj functions.
RESET session_authorization;
GRANT emaj_adm TO _regress_emaj_admin2;

RESET session_authorization;
SET session_authorization TO _regress_emaj_admin2;

SELECT emaj.emaj_cleanup_rollback_state();

-- Drop the extension.
RESET session_authorization;
SET session_authorization TO _regress_emaj_install;
\drgS
SELECT emaj.emaj_drop_extension();
\du

------------------------------------------------------------
-- Step 8: exit the scenario by leaving a test environment suitable for Emaj_web test.
------------------------------------------------------------
-- Revoke grants and capabilities.
RESET session_authorization;
REVOKE emaj_adm, emaj_viewer FROM _regress_emaj_install, _regress_emaj_admin2;

REVOKE pg_read_server_files FROM _regress_emaj_install;
REVOKE pg_write_server_files FROM _regress_emaj_install;
REVOKE pg_execute_server_program FROM _regress_emaj_install;

ALTER ROLE _regress_emaj_install NOCREATEROLE;

-- Drop emaj roles.
DROP ROLE emaj_adm, emaj_viewer;

-- Reinstall emaj.
SET session_authorization TO _regress_emaj_install;
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all
