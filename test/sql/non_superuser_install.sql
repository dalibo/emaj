-- non_superuser_install.sql
--     single script to test emaj installed with psql by a non superuser role
--
SET client_min_messages TO WARNING;

------------------------------------------------------------
-- Create roles, give grants and create needed extensions
------------------------------------------------------------

-- _regress_emaj_install owns the emaj environment
create role _regress_emaj_install login password 'install' createrole;

-- anticipate the emaj_adm and emaj_viewer role creation
create role emaj_adm;
create role emaj_viewer;

-- and anticipate the grants to the installer
grant emaj_adm to _regress_emaj_install with admin true;
grant emaj_viewer to _regress_emaj_install;
\drgS

-- _regress_emaj_app owns the application objects
create role _regress_emaj_app login password 'app';

grant all on database regression to _regress_emaj_install, _regress_emaj_app;
grant create on schema public to _regress_emaj_install;

create extension dblink;

------------------------------------------------------------
-- Create the application objects and give grants on them to _regress_emaj_install
------------------------------------------------------------
set role _regress_emaj_app;

DROP SCHEMA IF EXISTS mySchema7 CASCADE;
CREATE SCHEMA mySchema7;
GRANT USAGE ON SCHEMA mySchema7 TO _regress_emaj_install;

SET search_path=mySchema7;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);

CREATE SEQUENCE mySeq1;

GRANT ALL ON ALL TABLES IN SCHEMA mySchema7 TO _regress_emaj_install;
GRANT ALL ON ALL SEQUENCES IN SCHEMA mySchema7 TO _regress_emaj_install;

reset search_path;

------------------------------------------------------------
-- install the extension using the psql script
------------------------------------------------------------
set role _regress_emaj_install;

\set ECHO errors
\i sql/emaj-devel.sql
\set ECHO all

-- check the installation
select hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
select verh_version from emaj.emaj_version_hist;
select * from emaj.emaj_install_conf;

------------------------------------------------------------
-- build a tables group
------------------------------------------------------------
select emaj.emaj_create_group('myGroup7');
----select emaj.emaj_assign_table('myschema7', 'mytbl1', 'myGroup7');
select emaj.emaj_assign_sequence('myschema7', 'myseq1', 'myGroup7');

------------------------------------------------------------
-- start the group and perform data changes
------------------------------------------------------------
select emaj.emaj_start_group('myGroup7', 'M1');

------------------------------------------------------------
-- test inoperative function calls
------------------------------------------------------------
select emaj.emaj_disable_protection_by_event_triggers();
select emaj.emaj_enable_protection_by_event_triggers();

------------------------------------------------------------
-- test forbidden function calls
------------------------------------------------------------
-- missing pg_read_server_files grant
select emaj.emaj_import_groups_configuration('/tmp/emaj_test');
select emaj.emaj_import_parameters_configuration('/tmp/emaj_test');

-- missing pg_write_server_files grant
select emaj.emaj_export_groups_configuration('/tmp/emaj_test');
select emaj.emaj_export_parameters_configuration('/tmp/emaj_test');
select emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
select emaj.emaj_dump_changes_group('myGroup7', 'M1', NULL, '', NULL, '/tmp');
select emaj.emaj_snap_group('myGroup7', '/tmp', NULL);
select emaj.emaj_gen_sql_group('myGroup7', 'M1', NULL, '/tmp');

-- missing pg_execute_server_program grant
reset role;
grant pg_write_server_files to _regress_emaj_install;
set role _regress_emaj_install;

select emaj.emaj_gen_sql_dump_changes_group(NULL, 'M1', NULL, '', NULL, '/tmp/emaj_test');
select emaj.emaj_dump_changes_group('myGroup7', 'M1', NULL, '', NULL, '/tmp');

reset role;
revoke pg_write_server_files from _regress_emaj_install;

------------------------------------------------------------
-- check the E-Maj environment
------------------------------------------------------------
select * from emaj.emaj_verify_all();

------------------------------------------------------------
-- drop the extension
------------------------------------------------------------
set role _regress_emaj_install;

select emaj.emaj_drop_extension();

-- Drop the extra extensions to get a stable re-install test
drop extension btree_gist;

reset role;
drop extension dblink;

-- Check that the emaj schema is not known anymore
\dn emaj

------------------------------------------------------------
-- Drop the application objects
------------------------------------------------------------
set role _regress_emaj_app;

DROP SCHEMA IF EXISTS mySchema7 CASCADE;

------------------------------------------------------------
-- Drop roles
------------------------------------------------------------
reset role;

revoke all on database regression from _regress_emaj_install, _regress_emaj_app;
revoke all on schema public from _regress_emaj_install;

drop role _regress_emaj_install, _regress_emaj_app;
drop role emaj_adm, emaj_viewer;

--------- for cases when there remains issues with the extension drop by the installer role ; to be removed once OK
----select emaj.emaj_drop_extension();
----drop role _regress_emaj_install, _regress_emaj_app;
