-- Verify.sql : test emaj_verify_all(), event_trigger management functions.
--              As well as recovery from user errors.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(7000);

-- Disable event triggers.
-- This is done to allow tests with missing or renamed or altered components.
SELECT emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- emaj_verify_all() test.
-----------------------------
-- Should be OK.
SELECT * FROM emaj.emaj_verify_all();

--
-- Dblink connection tests.
--
-- The "dblink not installed" and "lack of execute right on dblink_connect_u()" tests are located into the non_superuser_install.sql script.

-- Test a transaction isolation not READ COMMITTED.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Test the lack of dblink_user_password parameter.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', NULL);
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Test a bad dblink_user_password parameter content.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', 'bad_content');
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Test a dblink_user_password without emaj_adm rights.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', 'user=_regress_emaj_viewer password=viewer');
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Warnings on foreign keys.
BEGIN;
-- FK with one of both tables outside the table group.
  SELECT emaj.emaj_remove_tables('myschema2', '{"mytbl7", "mytbl8"}');
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

BEGIN;
-- FK with both tables in different table groups.
  SELECT emaj.emaj_move_table('myschema2', 'mytbl7', 'myGroup1');
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

BEGIN;
-- Inherited FK IMMEDIATE.
  ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1);
  ALTER TABLE myschema4.myTblR2 ADD FOREIGN KEY (col2, col3) REFERENCES myschema4.myTblP(col1, col2);
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

BEGIN;
-- Inherited FK with a ON DELETE|UPDATE clause.
  ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1)
    ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
  ALTER TABLE myschema4.myTblR2 add foreign key (col2, col3) REFERENCES myschema4.myTblP(col1, col2)
    ON DELETE RESTRICT DEFERRABLE INITIALLY IMMEDIATE;
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

--
-- Log schemas content errors tests.
--

-- Detection of unattended tables in E-Maj schemas.
BEGIN;
  CREATE TABLE emaj.dummy1_log (col1 int);
  CREATE TABLE emaj.dummy2 (col1 int);
  CREATE TABLE emaj_myschema1.emaj_dummy (col1 int);
  CREATE TABLE emaj.emaj_dummy (col1 int);               -- this one is NOT detected
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended functions in E-Maj schemas.
BEGIN;
  CREATE FUNCTION emaj.dummy1_log_fnct () RETURNS INT LANGUAGE SQL AS $$ SELECT 0 $$;
  CREATE FUNCTION emaj_myschema1.dummy2_rlbk_fnct () RETURNS INT LANGUAGE SQL AS $$ SELECT 0 $$;
  CREATE FUNCTION emaj_myschema1.dummy3_fnct () RETURNS INT LANGUAGE SQL AS $$ SELECT 0 $$;
  CREATE FUNCTION emaj._dummy4_fnct () RETURNS INT LANGUAGE SQL AS $$ SELECT 0 $$;      -- this one is NOT detected
  CREATE FUNCTION emaj.emaj_dummy5_fnct () RETURNS INT LANGUAGE SQL AS $$ SELECT 0 $$;  -- this one is NOT detected
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended sequences in E-Maj schemas.
BEGIN;
  CREATE TABLE emaj.dummy1_log (col1 serial);
  CREATE SEQUENCE emaj_myschema1.dummy2_seq;
  CREATE SEQUENCE emaj_myschema1.dummy3_log_seq;
  CREATE SEQUENCE emaj.emaj_dummy4_seq;                  -- this one is NOT detected
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended types in E-Maj schemas.
BEGIN;
  CREATE TYPE emaj.dummy1_type AS (col1 INT);
  CREATE TYPE emaj_myschema1.dummy2_type AS (col1 INT);
  CREATE TYPE emaj_myschema1.dummy3_type AS (col1 INT);
  CREATE TYPE emaj.emaj_dummy4_type AS (col1 INT);       -- this one is NOT detected
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended views in E-Maj schemas.
BEGIN;
  CREATE VIEW emaj.dummy1_view AS SELECT hist_id, hist_function, hist_event, hist_object FROM emaj.emaj_hist;
  CREATE VIEW emaj.dummy2_view AS SELECT hist_id, hist_function, hist_event, hist_object FROM emaj.emaj_hist;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended foreign tables in E-Maj schemas.
BEGIN;
  CREATE EXTENSION file_fdw;
  CREATE FOREIGN DATA WRAPPER file HANDLER file_fdw_handler;
  CREATE SERVER file_server FOREIGN DATA WRAPPER file;
  CREATE FOREIGN TABLE emaj.dummy1_ftbl (line TEXT) SERVER file_server OPTIONS(filename '/dev/NULL');
  CREATE FOREIGN TABLE emaj.dummy2_ftbl (line TEXT) SERVER file_server OPTIONS(filename '/dev/NULL');
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of unattended domains in E-Maj schemas.
BEGIN;
  CREATE DOMAIN emaj_myschema1.dummy1_domain AS INT CHECK (VALUE > 0);
  CREATE DOMAIN emaj_myschema1.dummy2_domain AS INT CHECK (VALUE > 0);
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;

--
-- Tests on groups errors.
--

-- Detection of a missing application schema.
BEGIN;
  DROP SCHEMA myschema1 CASCADE;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing application relation.
BEGIN;
  DROP TABLE myschema1.mytbl4;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of relation type change (a table is now a sequence!).
BEGIN;
  UPDATE emaj.emaj_relation SET rel_kind = 'S' WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' AND upper_inf(rel_time_range);
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing E-Maj log schema.
BEGIN;
  DROP SCHEMA emaj_myschema1 CASCADE;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing log trigger.
BEGIN;
  DROP TRIGGER emaj_log_trg ON myschema1.mytbl1;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing log function (and trigger).
BEGIN;
  DROP FUNCTION emaj_myschema1.mytbl1_log_fnct() CASCADE;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing truncate trigger.
BEGIN;
  DROP TRIGGER emaj_trunc_trg ON myschema1.mytbl1;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing log table.
BEGIN;
  DROP TABLE emaj_myschema1.mytbl1_log;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a missing log sequence.
BEGIN;
  DROP SEQUENCE emaj_myschema1.mytbl1_log_seq;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a change in the application table structure (new column).
BEGIN;
  ALTER table myschema1.mytbl1 ADD COLUMN newcol INT;
  ALTER table myschema1.mytbl1 ADD COLUMN othernewcol TEXT;
  ALTER table myschema1.mytbl2 ADD COLUMN newcol INT;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a change in the application table structure (column type change).
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP COLUMN col42;
  ALTER TABLE myschema1.mytbl4 ALTER COLUMN col45 TYPE varchar(15);
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of missing primary key on tables belonging to a rollbackable group.
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;                   -- table FROM a rollbackable group
  ALTER TABLE "phil's schema""3"."my""tbl4" DROP CONSTRAINT "my""tbl4_pkey" CASCADE;    -- table FROM an audit_only group
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of tables altered as UNLOGGED.
BEGIN;                                                                        -- needs 9.5+
  ALTER TABLE myschema1.mytbl4 SET unlogged;                                  -- table FROM a rollbackable group
  ALTER TABLE "phil's schema""3"."myTbl2\" SET unlogged;                      -- table FROM an audit_only group
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of modified primary key.
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;
  ALTER TABLE myschema1.mytbl4 ADD PRIMARY KEY (col41, col42);
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of STORED generated column whose expression has been dropped.
BEGIN;
  ALTER TABLE myschema1.mytbl2b ALTER COLUMN col22 DROP EXPRESSION;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a column transformed into generated column.
BEGIN;
  ALTER TABLE myschema1.mytbl2b DROP COLUMN col24, ADD COLUMN col24 BOOLEAN GENERATED ALWAYS AS (col21 > 3) STORED;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;
-- Detection of a corrupted log table (missing some technical columns).
BEGIN;
  ALTER TABLE emaj_myschema1.mytbl1_log DROP COLUMN emaj_verb, DROP column emaj_tuple;
  ALTER TABLE emaj_myschema1.mytbl4_log DROP COLUMN emaj_gid, DROP column emaj_user;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;

-- Almost all in 1.
BEGIN;
  CREATE TABLE emaj.dummy_log (col1 INT);
  CREATE FUNCTION emaj.dummy_log_fnct () returns INT LANGUAGE SQL AS $$ SELECT 0 $$;
  CREATE FUNCTION emaj.dummy_rlbk_fnct () returns INT LANGUAGE SQL AS $$ SELECT 0 $$;
  DROP TRIGGER emaj_log_trg on myschema1.mytbl1;
  DROP FUNCTION emaj_myschema1.mytbl1_log_fnct() CASCADE;
  DROP TABLE emaj_myschema1.mytbl1_log;
  ALTER TABLE myschema1.mytbl1 ADD COLUMN newcol INT;
  UPDATE emaj.emaj_relation SET rel_kind = 'S' WHERE rel_schema = 'myschema2' AND rel_tblseq = 'mytbl1' AND upper_inf(rel_time_range);
  ALTER TABLE myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;

--
-- Other tests.
--

-- Bad triggers in a "triggers to ignore at rollback time" array.
BEGIN;
-- Simulate a discrepancy between the emaj_relation table content and the existing triggers.
  UPDATE emaj.emaj_relation SET rel_ignored_triggers = '{"dummy1", "dummy2"}'
    WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' AND upper_inf(rel_time_range);
-- Check.
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
-- And fix.
  SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"ignored_triggers":null}'::JSONB);
  SELECT * FROM emaj.emaj_verify_all() t(msg) WHERE msg LIKE 'Error%';
ROLLBACK;

--------------------------------
-- User errors and recovery tests.
--------------------------------

-- Cases when an application table is altered.
BEGIN;
  ALTER TABLE myschema2.mytbl4 ADD COLUMN newcol INT;
  SAVEPOINT sp1;
-- Setting a mark or rollbacking fails.
    SELECT emaj.emaj_set_mark_group('myGroup2', 'dummyMark');
  ROLLBACK TO SAVEPOINT sp1;
    SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'EMAJ_LAST_MARK');
  ROLLBACK TO SAVEPOINT sp1;
-- But it is possible to stop, drop and recreate the group.
  SELECT emaj.emaj_stop_group('myGroup2');
  SAVEPOINT sp2;
    SELECT emaj.emaj_drop_group('myGroup2');
    SELECT emaj.emaj_create_group('myGroup2');
    SELECT emaj.emaj_assign_table('myschema2', 'mytbl4', 'myGroup2');
  ROLLBACK TO SAVEPOINT sp2;
-- Or stop and export/import the groups configuration.
  SELECT emaj.emaj_import_groups_configuration(emaj.emaj_export_groups_configuration(), ARRAY['myGroup2'], TRUE);
ROLLBACK;

-- Cases when an application table is dropped.
BEGIN;
  DROP TABLE myschema2.mytbl4;
-- Stopping group fails.
  SAVEPOINT sp1;
    SELECT emaj.emaj_stop_group('myGroup2');
  ROLLBACK TO SAVEPOINT sp1;
-- Just removing the table solves the issue.
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl4');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Cases when a log trigger on an application table is dropped.
BEGIN;
  DROP TRIGGER emaj_log_trg on myschema2.mytbl4;
-- Stopping group fails.
  SAVEPOINT sp1;
    SELECT emaj.emaj_stop_group('myGroup2');
  ROLLBACK TO SAVEPOINT sp1;
-- Reimporting the group configuration fails if the group is in logging state.
    SELECT emaj.emaj_import_groups_configuration(emaj.emaj_export_groups_configuration(), ARRAY['myGroup2'], TRUE);
  ROLLBACK TO SAVEPOINT sp1;
-- Removing and re-assigning the table solves the issue.
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl4');
  SELECT emaj.emaj_assign_table('myschema2', 'mytbl4', 'myGroup2');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Cases when a truncate trigger on an application table is dropped.
BEGIN;
  DROP TRIGGER emaj_trunc_trg ON myschema2.mytbl4;
-- Stopping group fails.
  SAVEPOINT sp1;
    SELECT emaj.emaj_stop_group('myGroup2');
  ROLLBACK TO SAVEPOINT sp1;
-- Removing and re-assigning the table solves the issue.
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl4');
  SELECT emaj.emaj_assign_table('myschema2', 'mytbl4', 'myGroup2');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Cases when a log sequence is dropped.
BEGIN;
  DROP SEQUENCE emaj_myschema2.mytbl4_log_seq;
-- Stopping group fails.
  SAVEPOINT sp1;
    SELECT emaj.emaj_stop_group('myGroup2');
  ROLLBACK TO SAVEPOINT sp1;
-- Removing the table fails also.
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl4');
  ROLLBACK TO SAVEPOINT sp1;
-- The only solution is to force the group's stop before removing/reassigning the table.
  SELECT emaj.emaj_force_stop_group('myGroup2');
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl4');
  SELECT emaj.emaj_assign_table('myschema2', 'mytbl4', 'myGroup2');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Cases when an application sequence is dropped.
BEGIN;
  DROP SEQUENCE myschema2.mySeq1;
-- Setting a mark or stopping the group fails.
-- Just removing the SEQUENCE solves the issue.
  SELECT emaj.emaj_remove_sequence('myschema2', 'myseq1');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-- Cases when an application schema is dropped.
SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA myschema2 CASCADE;
-- Stopping group fails.
  SAVEPOINT sp1;
    SELECT emaj.emaj_stop_group('myGroup2');
  ROLLBACK TO SAVEPOINT sp1;
-- The only solution is to force the group's stop and drop the group.
  SELECT emaj.emaj_force_stop_group('myGroup2');
  SELECT emaj.emaj_drop_group('myGroup2');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;
RESET client_min_messages;

-- Cases when non E-Maj related objects are stored in emaj log schemas.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  CREATE SEQUENCE emaj_myschema1.dummySeq;
  SAVEPOINT sp1;
-- Dropping group fails at log schema drop step.
    SELECT emaj.emaj_drop_group('myGroup1');
  ROLLBACK TO SAVEPOINT sp1;
-- Use emaj_verify_all() to understand the problem.
  SELECT * FROM emaj.emaj_verify_all();
-- Use emaj_force_drop_group to solve the problem.
  SELECT emaj.emaj_force_drop_group('myGroup1');
-- And everything is clean...
  SELECT * FROM emaj.emaj_verify_all();
ROLLBACK;

-----------------------------
-- Test event triggers.
-----------------------------
-- Disable twice event trigger (already disabled at the beginning of the createDrop.sql script).
SELECT emaj.emaj_disable_protection_by_event_triggers();

-- Enable twice.
SELECT emaj.emaj_enable_protection_by_event_triggers();
SELECT emaj.emaj_enable_protection_by_event_triggers();

--
-- Drop or alter various components.
--

-- Drop application components (the related table group is currently in logging state).
BEGIN;
  DROP TABLE myschema1.mytbl1 CASCADE;
ROLLBACK;
BEGIN;
  DROP SEQUENCE myschema2.mySeq1;
ROLLBACK;
SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA myschema1 CASCADE;
ROLLBACK;
RESET client_min_messages;

-- Drop primary keys.
-- Drop a primary key for a table belonging to a rollbackable table group (should be blocked).
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;
ROLLBACK;
-- Drop a primary key for a table belonging to an audit_only table group (should not fail).
BEGIN;
  ALTER TABLE "phil's schema""3"."phil's tbl1" DROP CONSTRAINT "phil's tbl1_pkey" CASCADE;
ROLLBACK;

-- Drop emaj components.
BEGIN;
  DROP TABLE emaj_myschema1."myTbl3_log";
ROLLBACK;
BEGIN;
  DROP SEQUENCE emaj_myschema1.mytbl1_log_seq;
ROLLBACK;
BEGIN;
  DROP FUNCTION emaj_myschema1."myTbl3_log_fnct"() CASCADE;
ROLLBACK;
BEGIN;
  DROP TRIGGER emaj_log_trg ON myschema1.mytbl1;
ROLLBACK;

SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA emaj CASCADE;
ROLLBACK;
BEGIN;
  DROP SCHEMA emaj_myschema1 CASCADE;
ROLLBACK;
RESET client_min_messages;

-- Dropping the extension in tested by the install sql script because it depends on the way the extension is created.

-- Change a table structure that leads to a table rewrite.
BEGIN;
  ALTER table myschema1.mytbl1 ALTER COLUMN col13 type VARCHAR(10);
ROLLBACK;
BEGIN;
  ALTER TABLE emaj_myschema1.mytbl1_log ALTER COLUMN col13 type VARCHAR(10);
ROLLBACK;

-- Rename a table and/or change its schema (not covered by event triggers).
BEGIN;
  ALTER TABLE myschema1.mytbl1 RENAME TO mytbl1_new_name;
  ALTER TABLE myschema1.mytbl1_new_name SET SCHEMA public;
  ALTER SCHEMA myschema1 RENAME TO renamed_myschema1;
ROLLBACK;
-- Change a table structure that doesn't lead to a table rewrite (not covered by event triggers).
BEGIN;
  ALTER TABLE myschema1.mytbl1 ADD COLUMN another_newcol BOOLEAN;
ROLLBACK;

-- Perform changes on application components with the related table group stopped (the event triggers should accept).
BEGIN;
  SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2']);
  ALTER TABLE myschema1.mytbl1 ALTER column col13 type varchar(10);
  DROP TABLE myschema1.mytbl1 CASCADE;
  DROP SEQUENCE myschema2.mySeq1;
ROLLBACK;

-- Drop the public._emaj_protection_event_trigger_fnct() technical function that is left outside the emaj extension.
DROP FUNCTION public._emaj_protection_event_trigger_fnct() CASCADE;

-- Missing event triggers.
BEGIN;
  DROP EVENT TRIGGER emaj_protection_trg;
  SELECT * FROM emaj.emaj_verify_all();
  SELECT emaj.emaj_enable_protection_by_event_triggers();
ROLLBACK;

-- A non emaj user should be able to create, alter and drop a table without being disturbed by E-Maj event triggers.
SET session_authorization TO _regress_emaj_anonym;

CREATE SCHEMA anonym_user_schema;
CREATE TABLE anonym_user_schema.anonym_user_table (col1 int);
ALTER TABLE anonym_user_schema.anonym_user_table ADD COLUMN col2 TEXT;
DROP TABLE anonym_user_schema.anonym_user_table;
DROP SCHEMA anonym_user_schema;

RESET session_authorization;

-----------------------------
-- Test end: check.
-----------------------------
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 7000 ORDER BY hist_id;
