-- verify.sql : test emaj_verify_all(), event_trigger management functions
--              as well as recovery from user errors
--

-- set sequence restart value
select public.handle_emaj_sequences(7000);

-- disable event triggers 
-- this is done to allow tests with missing or renamed or altered components
select emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- emaj_verify_all() test
-----------------------------
-- should be OK
select * from emaj.emaj_verify_all();

--
-- Unknown emaj_param parameter key
--
begin;
  insert into emaj.emaj_param (param_key) VALUES ('dummy_2'), ('dummy_1');
  select * from emaj.emaj_verify_all();
rollback;

--
-- dblink connection tests
--
-- The "dblink not installed" test is located into the install_psql.sql script

-- Test the lack of execute right on dblink_connect_u() (they are not yet granted to emaj_adm)
set role emaj_adm;
select * from emaj.emaj_verify_all();
reset role;

-- Test a transaction isolation not READ COMMITTED
begin transaction isolation level REPEATABLE READ;
  select * from emaj.emaj_verify_all();
rollback;

-- Test the lack of dblink_user_password parameter
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select * from emaj.emaj_verify_all();
rollback;

-- Test a bad dblink_user_password parameter content
begin;
  update emaj.emaj_param set param_value_text = 'bad_content' where param_key = 'dblink_user_password';
  select * from emaj.emaj_verify_all();
rollback;

-- Test a dblink_user_password without emaj_adm rights
begin;
  update emaj.emaj_param set param_value_text = 'user=_regress_emaj_viewer password=viewer' 
    where param_key = 'dblink_user_password';
  select * from emaj.emaj_verify_all();
rollback;

-- Warnings on foreign keys
begin;
-- FK with one of both tables outside the tables group
  select emaj.emaj_remove_tables('myschema2', '{"mytbl7","mytbl8"}');
  select * from emaj.emaj_verify_all();
rollback;

begin;
-- FK with both tables in different tables groups
  select emaj.emaj_move_table('myschema2', 'mytbl7', 'myGroup1');
  select * from emaj.emaj_verify_all();
rollback;

begin;
-- inherited FK IMMEDIATE
  alter table myschema4.myTblP drop constraint mytblp_col1_fkey, add foreign key (col1) references myschema4.mytblr1(col1);
  do $$ begin if emaj._pg_version_num() >= 120000 then
      alter table myschema4.myTblR2 add foreign key (col2, col3) references myschema4.myTblP(col1, col2);     -- Fails with PG 11-
    end if; end; $$;
  select * from emaj.emaj_verify_all();
rollback;

begin;
-- inherited FK with a ON DELETE|UPDATE clause
  alter table myschema4.myTblP drop constraint mytblp_col1_fkey, add foreign key (col1) references myschema4.mytblr1(col1)
    on update cascade deferrable initially deferred;
  do $$ begin if emaj._pg_version_num() >= 120000 then
    alter table myschema4.myTblR2 add foreign key (col2, col3) references myschema4.myTblP(col1, col2)     -- Fails with PG 11-
      on delete restrict deferrable initially immediate;
    end if; end; $$;
  select * from emaj.emaj_verify_all();
rollback;

--
-- log schemas content errors tests
--

-- detection of unattended tables in E-Maj schemas
begin;
  create table emaj.dummy1_log (col1 int);
  create table emaj.dummy2 (col1 int);
  create table emaj_myschema1.emaj_dummy (col1 int);
  create table emaj.emaj_dummy (col1 int);               -- this one is not detected
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended functions in E-Maj schemas
begin;
  create function emaj.dummy1_log_fnct () returns int language sql as $$ select 0 $$;
  create function emaj_myschema1.dummy2_rlbk_fnct () returns int language sql as $$ select 0 $$;
  create function emaj_myschema1.dummy3_fnct () returns int language sql as $$ select 0 $$;
  create function emaj._dummy4_fnct () returns int language sql as $$ select 0 $$;      -- this one is not detected
  create function emaj.emaj_dummy5_fnct () returns int language sql as $$ select 0 $$;  -- this one is not detected
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended sequences in E-Maj schemas
begin;
  create table emaj.dummy1_log (col1 serial);
  create sequence emaj_myschema1.dummy2_seq;
  create sequence emaj_myschema1.dummy3_log_seq;
  create sequence emaj.emaj_dummy4_seq;                  -- this one is not detected
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended types in E-Maj schemas
begin;
  create type emaj.dummy1_type as (col1 int);
  create type emaj_myschema1.dummy2_type as (col1 int);
  create type emaj_myschema1.dummy3_type as (col1 int);
  create type emaj.emaj_dummy4_type as (col1 int);       -- this one is not detected
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended views in E-Maj schemas
begin;
  create view emaj.dummy1_view as select hist_id, hist_function, hist_event, hist_object from emaj.emaj_hist;
  create view emaj.dummy2_view as select hist_id, hist_function, hist_event, hist_object from emaj.emaj_hist;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended foreign tables in E-Maj schemas
begin;
  create extension file_fdw;
  create foreign data wrapper file handler file_fdw_handler;
  create server file_server foreign data wrapper file;
  create foreign table emaj.dummy1_ftbl (ligne TEXT) server file_server options(filename '/dev/null');
  create foreign table emaj.dummy2_ftbl (ligne TEXT) server file_server options(filename '/dev/null');
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of unattended domains in E-Maj schemas
begin;
  create domain emaj_myschema1.dummy1_domain as int check (VALUE > 0);
  create domain emaj_myschema1.dummy2_domain as int check (VALUE > 0);
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;

--
-- tests on groups errors
--

-- detection of a missing application schema
begin;
  drop schema myschema1 cascade;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing application relation
begin;
  drop table myschema1.mytbl4;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of relation type change (a table is now a sequence!)
begin;
  update emaj.emaj_relation set rel_kind = 'S' where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing E-Maj log schema
begin;
  drop schema emaj_myschema1 cascade;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing log trigger
begin;
  drop trigger emaj_log_trg on myschema1.mytbl1;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing log function (and trigger)
begin;
  drop function emaj_myschema1.mytbl1_log_fnct() cascade;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing truncate trigger
begin;
  drop trigger emaj_trunc_trg on myschema1.mytbl1;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing log table
begin;
  drop table emaj_myschema1.mytbl1_log;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a missing log sequence
begin;
  drop sequence emaj_myschema1.mytbl1_log_seq;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a change in the application table structure (new column)
begin;
  alter table myschema1.mytbl1 add column newcol int;
  alter table myschema1.mytbl1 add column othernewcol text;
  alter table myschema1.mytbl2 add column newcol int;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a change in the application table structure (column type change)
begin;
  alter table myschema1.mytbl4 drop column col42;
  alter table myschema1.mytbl4 alter column col45 type varchar(15);
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of missing primary key on tables belonging to a rollbackable group
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;                   -- table from a rollbackable group
  alter table "phil's schema""3"."my""tbl4" drop constraint "my""tbl4_pkey" cascade;    -- table from an audit_only group
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of tables altered as UNLOGGED
begin;                                                                        -- needs 9.5+
  alter table myschema1.mytbl4 set unlogged;                                  -- table from a rollbackable group
  alter table "phil's schema""3"."myTbl2\" set unlogged;                      -- table from an audit_only group
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of modified primary key
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
  alter table myschema1.mytbl4 add primary key (col41, col42);
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of STORED generated column whose expression has been dropped
begin;
  alter table myschema1.mytbl2b alter column col22 drop expression;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a column transformed into generated column
begin;
  alter table myschema1.mytbl2b drop column col24, add column col24 BOOLEAN GENERATED ALWAYS AS (col21 > 3) STORED;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;
-- detection of a corrupted log table (missing some technical columns)
begin;
  alter table emaj_myschema1.mytbl1_log drop column emaj_verb, drop column emaj_tuple;
  alter table emaj_myschema1.mytbl4_log drop column emaj_gid, drop column emaj_user;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;

-- almost all in 1
begin;
  create table emaj.dummy_log (col1 int);
  create function emaj.dummy_log_fnct () returns int language sql as $$ select 0 $$;
  create function emaj.dummy_rlbk_fnct () returns int language sql as $$ select 0 $$;
  drop trigger emaj_log_trg on myschema1.mytbl1;
  drop function emaj_myschema1.mytbl1_log_fnct() cascade;
  drop table emaj_myschema1.mytbl1_log;
  alter table myschema1.mytbl1 add column newcol int;
  update emaj.emaj_relation set rel_kind = 'S' where rel_schema = 'myschema2' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;

--
-- other tests
--

-- bad triggers in a "triggers to ignore at rollback time" array
begin;
-- simulate a discrepancy between the emaj_relation table content and the existing triggers
  update emaj.emaj_relation set rel_ignored_triggers = '{"dummy1","dummy2"}'
    where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
-- check
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
-- and fix
  select emaj.emaj_modify_table('myschema1','mytbl1','{"ignored_triggers":null}'::jsonb);
  select * from emaj.emaj_verify_all() t(msg) where msg like 'Error%';
rollback;

--------------------------------
-- User errors and recovery tests 
--------------------------------

-- cases when an application table is altered
begin;
  alter table myschema2.mytbl4 add column newcol int;
  savepoint sp1;
-- setting a mark or rollbacking fails
    select emaj.emaj_set_mark_group('myGroup2','dummyMark');
  rollback to savepoint sp1;
    select * from emaj.emaj_rollback_group('myGroup2','EMAJ_LAST_MARK');
  rollback to savepoint sp1;
-- but it is possible to stop, drop and recreate the group
  select emaj.emaj_stop_group('myGroup2');
  savepoint sp2;
    select emaj.emaj_drop_group('myGroup2');
    select emaj.emaj_create_group('myGroup2');
    select emaj.emaj_assign_table('myschema2','mytbl4','myGroup2');
  rollback to savepoint sp2;
-- or stop and export/import the groups configuration
  select emaj.emaj_import_groups_configuration(emaj.emaj_export_groups_configuration(),array['myGroup2'],true);
rollback;

-- cases when an application table is dropped
begin;
  drop table myschema2.mytbl4;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- just removing the table solves the issue
  select emaj.emaj_remove_table('myschema2','mytbl4');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when a log trigger on an application table is dropped
begin;
  drop trigger emaj_log_trg on myschema2.mytbl4;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- reimporting the group configuration fails if the group is in logging state
    select emaj.emaj_import_groups_configuration(emaj.emaj_export_groups_configuration(),array['myGroup2'],true);
  rollback to savepoint sp1;
-- removing and re-assigning the table solves the issue 
  select emaj.emaj_remove_table('myschema2','mytbl4');
  select emaj.emaj_assign_table('myschema2','mytbl4','myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when a truncate trigger on an application table is dropped
begin;
  drop trigger emaj_trunc_trg on myschema2.mytbl4;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- removing and re-assigning the table solves the issue 
  select emaj.emaj_remove_table('myschema2','mytbl4');
  select emaj.emaj_assign_table('myschema2','mytbl4','myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when a log sequence is dropped
begin;
  drop sequence emaj_myschema2.mytbl4_log_seq;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- removing the table fails also
  select emaj.emaj_remove_table('myschema2','mytbl4');
  rollback to savepoint sp1;
-- the only solution is to force the group's stop before removing/reassigning the table
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_remove_table('myschema2','mytbl4');
  select emaj.emaj_assign_table('myschema2','mytbl4','myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when an application sequence is dropped
begin;
  drop sequence myschema2.mySeq1;
-- setting a mark or stopping the group fails
-- just removing the sequence solves the issue
  select emaj.emaj_remove_sequence('myschema2','myseq1');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when an application schema is dropped
SET client_min_messages TO WARNING;
begin;
  drop schema myschema2 cascade;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- the only solution is to force the group's stop and drop the group
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_drop_group('myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;
RESET client_min_messages;

-- cases when non E-Maj related objects are stored in emaj log schemas
begin;
  select emaj.emaj_stop_group('myGroup1');
  create sequence emaj_myschema1.dummySeq;
  savepoint sp1;
-- dropping group fails at log schema drop step
    select emaj.emaj_drop_group('myGroup1');
  rollback to savepoint sp1;
-- use emaj_verify_all() to understand the problem
  select * from emaj.emaj_verify_all();
-- use emaj_force_drop_group to solve the problem
  select emaj.emaj_force_drop_group('myGroup1');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-----------------------------
-- test event triggers
-----------------------------
-- disable twice event trigger (already disabled at the beginning of the createDrop.sql script)
select emaj.emaj_disable_protection_by_event_triggers();

-- enable twice
select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_enable_protection_by_event_triggers();

--
-- drop or alter various components
--

-- drop application components (the related tables group is currently in logging state)
begin;
  drop table myschema1.mytbl1 cascade;
rollback;
begin;
  drop sequence myschema2.mySeq1;
rollback;
SET client_min_messages TO WARNING;
begin;
  drop schema myschema1 cascade;
rollback;
RESET client_min_messages;

-- drop primary keys
-- drop a primary key for a table belonging to a rollbackable tables group (should be blocked)
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
rollback;
-- drop a primary key for a table belonging to an audit_only tables group (should not fail)
begin;
  alter table "phil's schema""3"."phil's tbl1" drop constraint "phil's tbl1_pkey" cascade;
rollback;

-- drop emaj components
begin;
  drop table emaj_myschema1."myTbl3_log";
rollback;
begin;
  drop sequence emaj_myschema1.mytbl1_log_seq;
rollback;
begin;
  drop function emaj_myschema1."myTbl3_log_fnct"() cascade;
rollback;
begin;
  drop trigger emaj_log_trg on myschema1.mytbl1;
rollback;

SET client_min_messages TO WARNING;
begin;
  drop schema emaj cascade;
rollback;
begin;
  drop schema emaj_myschema1 cascade;
rollback;
RESET client_min_messages;

-- dropping the extension in tested by the install sql script because it depends on the way the extension is created

-- change a table structure that leads to a table rewrite
begin;
  alter table myschema1.mytbl1 alter column col13 type varchar(10);
rollback;
begin;
  alter table emaj_myschema1.mytbl1_log alter column col13 type varchar(10);
rollback;

-- rename a table and/or change its schema (not covered by event triggers)
begin;
  alter table myschema1.mytbl1 rename to mytbl1_new_name;
  alter table myschema1.mytbl1_new_name set schema public;
  alter schema myschema1 rename to renamed_myschema1;
rollback;
-- change a table structure that doesn't lead to a table rewrite (not covered by event triggers)
begin;
  alter table myschema1.mytbl1 add column another_newcol boolean;
rollback;

-- perform changes on application components with the related tables group stopped (the event triggers should accept)
begin;
  select emaj.emaj_stop_groups(array['myGroup1','myGroup2']);
  alter table myschema1.mytbl1 alter column col13 type varchar(10);
  drop table myschema1.mytbl1 cascade;
  drop sequence myschema2.mySeq1;
rollback;

-- drop the public._emaj_protection_event_trigger_fnct() technical function that is left outside the emaj extension
drop function public._emaj_protection_event_trigger_fnct() CASCADE;

-- missing event triggers
begin;
  drop event trigger emaj_protection_trg;
  select * from emaj.emaj_verify_all();
  select emaj.emaj_enable_protection_by_event_triggers();
rollback;

-- a non emaj user should be able to create, alter and drop a table without being disturbed by E-Maj event triggers
set role _regress_emaj_anonym;

create schema anonym_user_schema;
create table anonym_user_schema.anonym_user_table (col1 int);
alter table anonym_user_schema.anonym_user_table add column col2 text;
drop table anonym_user_schema.anonym_user_table;
drop schema anonym_user_schema;

reset role;

-----------------------------
-- test end: check
-----------------------------
select hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist where hist_id >= 7000 order by hist_id) as t;
