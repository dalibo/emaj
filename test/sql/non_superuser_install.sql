-- non_superuser_install.sql
--     single script to test emaj installed with psql by a non superuser role.
--     various use case are tested, giving more and more capabilities to the installer role
--
SET client_min_messages TO WARNING;

------------------------------------------------------------
-- Setup objects needed for the whole test scenario by the superuser
------------------------------------------------------------

-- drop emaj and test roles, if they exist and if nothing blocks it
drop role if exists emaj_adm, emaj_viewer;
drop role if exists _regress_emaj_app, _regress_emaj_install, _regress_emaj_admin2;

-- _regress_emaj_app role: owns the application objects
create role _regress_emaj_app login password 'app';
grant all on database regression to _regress_emaj_app;

-- _regress_emaj_install owns the emaj environment
create role _regress_emaj_install login password 'install';
grant all on database regression to _regress_emaj_install;
grant create on schema public to _regress_emaj_install;

-- _regress_emaj_admin2 is another administration role
create role _regress_emaj_admin2 login password 'admin2';

-- needed extension
create extension dblink;
grant execute on function dblink_connect_u(text,text) to _regress_emaj_install;

------------------------------------------------------------
-- Create application objects
------------------------------------------------------------
set session_authorization to _regress_emaj_app;

-- Objects owned by the installer (_regress_emaj_install)

DROP SCHEMA IF EXISTS appSchema1 CASCADE;
CREATE SCHEMA appSchema1;

SET search_path=appSchema1;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);

CREATE SEQUENCE mySeq1;

GRANT USAGE ON SCHEMA appSchema1 TO _regress_emaj_install;
GRANT ALL ON ALL TABLES IN SCHEMA appSchema1 TO _regress_emaj_install;
GRANT ALL ON ALL SEQUENCES IN SCHEMA appSchema1 TO _regress_emaj_install;

reset search_path;

-- Objects owned by the another role (_regress_emaj_app)
set session_authorization to _regress_emaj_install;

DROP SCHEMA IF EXISTS instSchema1 CASCADE;
CREATE SCHEMA instSchema1;

SET search_path=instSchema1;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);

reset search_path;

------------------------------------------------------------
-- Step 1: the installer role has the minimum rights to use E-Maj
------------------------------------------------------------

set session_authorization to _regress_emaj_install;

-- install emaj
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- check the installation
select hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
select verh_version from emaj.emaj_version_hist;
select * from emaj.emaj_install_conf;
select * from emaj.emaj_verify_all();

-- build a tables group
select emaj.emaj_create_group('instGroup1');
select emaj.emaj_assign_table('instschema1', 'mytbl1', 'instGroup1');
select emaj.emaj_assign_sequence('instschema1', 'mytbl1_col11_seq', 'instGroup1');

-- start the group and perform data changes
select emaj.emaj_start_group('instGroup1', 'M1');
insert into instSchema1.mytbl1 (col12) values ('Row 1');

-- set a mark and perform data change
select emaj.emaj_set_mark_group('instGroup1', 'M2');
update instSchema1.mytbl1 set col12 = 'Modified row 1' where col11 = 1;
insert into instSchema1.mytbl1 (col12) values ('Row 2');

-- logged rollback to M2 without dblink connection
select * from emaj.emaj_logged_rollback_group('instGroup1','M2');
select hist_object, hist_wording from emaj.emaj_hist where hist_function = 'DBLINK_OPEN_CNX' order by hist_id desc limit 1;

-- rollback to M2 with dblink connection
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=_regress_emaj_install password=install');
select * from emaj.emaj_rollback_group('instGroup1','M2');
select hist_object, hist_wording from emaj.emaj_hist where hist_function = 'DBLINK_OPEN_CNX' order by hist_id desc limit 1;

-- test inoperative or forbidden function calls
select emaj.emaj_disable_protection_by_event_triggers();
select emaj.emaj_enable_protection_by_event_triggers();

-- missing pg_read_server_files grant
select emaj.emaj_import_groups_configuration('/tmp/emaj_test');
select emaj.emaj_import_parameters_configuration('/tmp/emaj_test');

-- missing pg_write_server_files grant
select emaj.emaj_export_groups_configuration('/tmp/emaj_test');
select emaj.emaj_export_parameters_configuration('/tmp/emaj_test');
select emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
select emaj.emaj_dump_changes_group('instGroup1', 'M1', NULL, '', NULL, '/tmp');
select emaj.emaj_snap_group('instGroup1', '/tmp', NULL);
select emaj.emaj_gen_sql_group('instGroup1', 'M1', NULL, '/tmp');

-- missing pg_execute_server_program grant
reset session_authorization;
grant pg_write_server_files to _regress_emaj_install;
set session_authorization to _regress_emaj_install;

select emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
select emaj.emaj_dump_changes_group('instGroup1', 'M1', NULL, '', NULL, '/tmp');

reset session_authorization;
revoke pg_write_server_files from _regress_emaj_install;

------------------------------------------------------------
-- Step 2: assign application objects owned by another role
------------------------------------------------------------

set session_authorization to _regress_emaj_install;

-- build another tables group
select emaj.emaj_create_group('appGroup1');
-- ######### emaj_assign_table needs TO BE FIXED ########
select emaj.emaj_assign_table('appschema1', 'mytbl1', 'appGroup1');
select emaj.emaj_assign_sequence('appschema1', 'myseq1', 'appGroup1');

-- start the group and perform data changes
select emaj.emaj_start_group('appGroup1', 'M1');
insert into appSchema1.mytbl1 (col12) values ('Row 1');

-- set a mark and perform data change
select emaj.emaj_set_mark_group('appGroup1', 'M2');
update appSchema1.mytbl1 set col12 = 'Modified row 1' where col11 = 1;
insert into appSchema1.mytbl1 (col12) values ('Row 2');

-- rollback to M2
select * from emaj.emaj_rollback_group('appGroup1','M2');

-- ok, but no dblink use (not enough rights)
select hist_object, hist_wording from emaj.emaj_hist where hist_function = 'DBLINK_OPEN_CNX' order by hist_id desc limit 1;

------------------------------------------------------------
-- Step 3: add grants to perform COPY FROM or TO
------------------------------------------------------------
reset session_authorization;

GRANT pg_read_server_files TO _regress_emaj_install;
GRANT pg_write_server_files TO _regress_emaj_install;
GRANT pg_execute_server_program TO _regress_emaj_install;

set session_authorization to _regress_emaj_install;

select emaj.emaj_export_groups_configuration('/tmp/emaj_test');
select emaj.emaj_import_groups_configuration('/tmp/emaj_test', NULL, TRUE);
select emaj.emaj_export_parameters_configuration('/tmp/emaj_test');
select emaj.emaj_import_parameters_configuration('/tmp/emaj_test');
select emaj.emaj_gen_sql_dump_changes_group('instGroup1', 'M1', 'M2', '', NULL, '/tmp/emaj_test');
\! rm /tmp/emaj_test

------------------------------------------------------------
-- Step 4: let another role perform emaj administration tasks
------------------------------------------------------------
reset session_authorization;
grant _regress_emaj_install to _regress_emaj_admin2;

reset session_authorization;
set session_authorization to _regress_emaj_admin2;

select * from emaj.emaj_verify_all();
select emaj.emaj_stop_groups(emaj.emaj_get_logging_groups());

-- drop the extension
reset session_authorization;
set session_authorization to _regress_emaj_install;
select emaj.emaj_drop_extension();

------------------------------------------------------------
-- Step 5: create an emaj_adm role and reinstall the extension
------------------------------------------------------------
reset session_authorization;
create role emaj_adm;
grant emaj_adm to _regress_emaj_install;
revoke all on function dblink_connect_u(text,text) from _regress_emaj_install;

set session_authorization to _regress_emaj_install;

-- install emaj
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- check the installation
select hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
select * from emaj.emaj_install_conf;
select * from emaj.emaj_verify_all();

-- drop the extension
select emaj.emaj_drop_extension();

------------------------------------------------------------
-- Step 6: add grants to _regress_emaj_install and reinstall the extension
------------------------------------------------------------
reset session_authorization;
GRANT emaj_adm TO _regress_emaj_install WITH ADMIN TRUE;
ALTER ROLE _regress_emaj_install CREATEROLE;

set session_authorization to _regress_emaj_install;

-- install emaj
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- check the installation
select hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
select * from emaj.emaj_install_conf;

-- grant emaj_viewer to _regress_emaj_admin2 and try some emaj functions
reset session_authorization;
revoke _regress_emaj_install from _regress_emaj_admin2;
grant emaj_viewer to _regress_emaj_admin2;

set session_authorization to _regress_emaj_admin2;
select * from emaj.emaj_verify_all();

-- grant emaj_adm to _regress_emaj_admin2 and try some emaj functions
reset session_authorization;
grant emaj_adm to _regress_emaj_admin2;

reset session_authorization;
set session_authorization to _regress_emaj_admin2;

select emaj.emaj_cleanup_rollback_state();

-- drop the extension
reset session_authorization;
set session_authorization to _regress_emaj_install;
\drgS
select emaj.emaj_drop_extension();
\du

------------------------------------------------------------
-- Step 7: exit the scenario by leaving a test environment suitable for Emaj_web test
------------------------------------------------------------
-- Revoke grants and capabilities
reset session_authorization;
revoke emaj_adm, emaj_viewer from _regress_emaj_install, _regress_emaj_admin2;

revoke pg_read_server_files from _regress_emaj_install;
revoke pg_write_server_files from _regress_emaj_install;
revoke pg_execute_server_program from _regress_emaj_install;

alter role _regress_emaj_install nocreaterole;

-- Drop emaj roles
drop role emaj_adm, emaj_viewer;

-- Reinstall emaj
set session_authorization to _regress_emaj_install;
\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all
