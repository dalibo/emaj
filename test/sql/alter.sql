-- alter.sql : test emaj_alter_group() and emaj_alter_groups() functions
--
-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 6000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 6000;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 6000;

-----------------------------
-- stop, reset and drop groups
-----------------------------
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_reset_group('myGroup1');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup2');
select emaj.emaj_stop_group('phil''s group#3",','Simple stop mark');
select emaj.emaj_drop_group('phil''s group#3",');
select emaj.emaj_force_stop_group('myGroup4');
select emaj.emaj_drop_group('myGroup4');
select emaj.emaj_force_stop_group('emptyGroup');
select emaj.emaj_drop_group('emptyGroup');

-----------------------------
-- emaj_alter_group() tests on IDLE groups
-----------------------------
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup2');
select emaj.emaj_create_group('emptyGroup',true,true);
select emaj.emaj_create_group('myGroup4');

-- unknown group
select emaj.emaj_alter_group(NULL);
select emaj.emaj_alter_group('unknownGroup');
-- group in logging state (2 tables need to be repaired)
begin;
  select emaj.emaj_start_group('myGroup1','');
  select emaj.emaj_disable_protection_by_event_triggers();
  drop table emaj_myschema1.mytbl1_log;
  drop table emaj_myschema1.mytbl4_log;
  select emaj.emaj_enable_protection_by_event_triggers();
  select emaj.emaj_alter_group('myGroup1');
rollback;
-- alter a group with a table now already belonging to another group
begin;
  insert into emaj.emaj_group_def values ('myGroup1','myschema2','mytbl1');
  select emaj.emaj_alter_group('myGroup1');
rollback;
-- log tablespace cannot be changed for sequence
begin;
  update emaj.emaj_group_def set grpdef_log_dat_tsp = 'b' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_alter_group('myGroup1');
rollback;
begin;
  update emaj.emaj_group_def set grpdef_log_idx_tsp = 'b' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_alter_group('myGroup1');
rollback;
-- dropped application table
begin;
  select emaj.emaj_disable_protection_by_event_triggers();
  drop table myschema1.mytbl2b;
  select emaj.emaj_enable_protection_by_event_triggers();
  select emaj.emaj_alter_group('myGroup1');
rollback;

-- should be OK
-- nothing to change
select emaj.emaj_alter_group('emptyGroup');
select emaj.emaj_alter_group('myGroup1');
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
-- only 3 tables to remove (+ log schemas emajb)
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl4';
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_alter_group('myGroup1');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
-- only 1 sequence to remove
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
select emaj.emaj_alter_group('myGroup1');
select group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';
-- 3 tables to add
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL,'tsp log''2','tsp log''2');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10,'tsplog1');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl4',20,'tsplog1','tsp log''2');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_alter_group('myGroup1');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
-- only 1 sequence to add
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
select emaj.emaj_alter_group('myGroup1');
select group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';
-- only change the log data tablespace for 1 table
update emaj.emaj_group_def set grpdef_log_dat_tsp = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
select emaj.emaj_alter_group('myGroup1');
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log';
update emaj.emaj_group_def set grpdef_log_dat_tsp = 'tsp log''2' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
select emaj.emaj_alter_group('myGroup1');
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log';
-- change the log data tablespace for all tables of a group
update emaj.emaj_group_def set grpdef_log_dat_tsp = case when grpdef_log_dat_tsp is NULL then 'tsplog1' when grpdef_log_dat_tsp = 'tsplog1' then 'tsp log''2' else NULL end where grpdef_schema = 'myschema1' and grpdef_tblseq not like '%seq';
select emaj.emaj_alter_group('myGroup1');
update emaj.emaj_group_def set grpdef_log_dat_tsp = case when grpdef_log_dat_tsp = 'tsplog1' then NULL when grpdef_log_dat_tsp = 'tsp log''2' then 'tsplog1' else 'tsp log''2' end where grpdef_schema = 'myschema1' and grpdef_tblseq not like '%seq';
select emaj.emaj_alter_group('myGroup1');
-- only change the log index tablespace, using a session default tablespace
update emaj.emaj_group_def set grpdef_log_idx_tsp = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
set default_tablespace = tspemaj_renamed;
select emaj.emaj_alter_group('myGroup1');
reset default_tablespace;
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log_idx';
update emaj.emaj_group_def set grpdef_log_idx_tsp = 'tsp log''2' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
select emaj.emaj_alter_group('myGroup1');
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log_idx';
-- only change the priority
update emaj.emaj_group_def set grpdef_priority = 30 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
select rel_priority from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
update emaj.emaj_group_def set grpdef_priority = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
select rel_priority from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
update emaj.emaj_group_def set grpdef_priority = 20 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
select rel_priority from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);

-- change the table structure
alter table myschema1.mytbl1 add column newcol int;
select emaj.emaj_alter_group('myGroup1');
alter table myschema1.mytbl1 rename newcol to newcol2;
select emaj.emaj_alter_group('myGroup1');
alter table myschema1.mytbl1 alter column newcol2 type bigint;
select emaj.emaj_alter_group('myGroup1');
alter table myschema1.mytbl1 alter column newcol2 set default 0;
-- NB: changing default has no impact on emaj component 
select emaj.emaj_alter_group('myGroup1');
alter table myschema1.mytbl1 drop column newcol2;
select emaj.emaj_alter_group('myGroup1');

-- rename a table and/or change its schema
alter table myschema1.mytbl1 rename to mytbl1_new_name;
update emaj.emaj_group_def set grpdef_tblseq = 'mytbl1_new_name' 
  where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
alter table myschema1.mytbl1_new_name set schema public;
update emaj.emaj_group_def set grpdef_schema = 'public'
  where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1_new_name';
select emaj.emaj_alter_group('myGroup1');
alter table public.mytbl1_new_name rename to mytbl1;
alter table public.mytbl1 set schema myschema1;
update emaj.emaj_group_def set grpdef_schema = 'myschema1', grpdef_tblseq = 'mytbl1'
  where grpdef_schema = 'public' and grpdef_tblseq = 'mytbl1_new_name';
-- the next call gives a useless mark name parameter (the group is in idle state)
select emaj.emaj_alter_group('myGroup1','useless_mark_name');

-- missing emaj components
select emaj.emaj_disable_protection_by_event_triggers();
drop trigger emaj_log_trg on myschema1.mytbl1;
select emaj.emaj_alter_group('myGroup1');
drop function emaj_myschema1.mytbl1_log_fnct() cascade;
select emaj.emaj_alter_group('myGroup1');
drop table emaj_myschema1.mytbl1_log;
select emaj.emaj_alter_group('myGroup1');
select emaj.emaj_enable_protection_by_event_triggers();

-- multiple emaj_alter_group() on a logging group => fails
-- this test is commented because the generated error message differs from one run to another
--begin;
--  select emaj.emaj_start_group('myGroup4');
--  select emaj.emaj_alter_group('myGroup4');
--  select emaj.emaj_alter_group('myGroup4');
--rollback;

-----------------------------
-- emaj_alter_groups() tests on IDLE groups
-----------------------------

-- unknown groups
select emaj.emaj_alter_groups('{"myGroup1","unknownGroup"}');
-- no group at all
select emaj.emaj_alter_groups('{NULL,""}');
-- groups in logging state
begin;
  select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','');
  select emaj.emaj_disable_protection_by_event_triggers();
  drop table emaj_myschema1.mytbl1_log;
  drop table emaj_myschema2.mytbl1_log;
  select emaj.emaj_enable_protection_by_event_triggers();
  select emaj.emaj_alter_groups('{"myGroup2","myGroup1","myGroup4"}');
rollback;
-- alter groups with a table now already belonging to another group
begin;
  insert into emaj.emaj_group_def values ('myGroup1','myschema2','mytbl1');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
rollback;
-- a PRIMARY KEY is missing
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
rollback;

-- should be OK
-- 3 tables and 1 sequence to remove
select group_name, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1' or group_name = 'myGroup2' order by 1;
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl4';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
select group_name, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1' or group_name = 'myGroup2' order by 1;
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;

-- 3 tables and 1 sequence to add
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL,'tsp log''2','tsp log''2');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10,'tsplog1');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl4');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
select group_name, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1' or group_name = 'myGroup2' order by 1;
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;

-- only change the log data tablespace for 1 table, the log index tablespace for another table and the priority for a third one
update emaj.emaj_group_def set grpdef_log_dat_tsp = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = 'tsplog1' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
update emaj.emaj_group_def set grpdef_priority = 30 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
set default_tablespace = tspemaj_renamed;
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
reset default_tablespace;
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log';
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl6_log_idx';
select rel_priority from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
--
update emaj.emaj_group_def set grpdef_log_dat_tsp = 'tsp log''2' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = NULL where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
update emaj.emaj_group_def set grpdef_priority = 20 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl2b_log';
select spcname from pg_tablespace, pg_class where reltablespace = pg_tablespace.oid and relname = 'mytbl6_log_idx';
select rel_priority from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);

-- move 1 table and 1 sequence from a group to another
update emaj.emaj_group_def set grpdef_group = 'myGroup1' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_group = 'myGroup1' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3_col31_seq';
select rel_group, count(*) from emaj.emaj_relation where rel_group like 'myGroup%' and upper_inf(rel_time_range) group by 1 order by 1;
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name IN ('myGroup1','myGroup2') order by group_name;
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name IN ('myGroup1','myGroup2') order by group_name;
select rel_group, count(*) from emaj.emaj_relation where rel_group like 'myGroup%' and upper_inf(rel_time_range) group by 1 order by 1;
update emaj.emaj_group_def set grpdef_group = 'myGroup2' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_group = 'myGroup2' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3_col31_seq';
-- the next call gives a useless mark name parameter (the group is in idle state)
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','useless_mark_name_%');
select rel_group, count(*) from emaj.emaj_relation where rel_group like 'myGroup%' and upper_inf(rel_time_range) group by 1 order by 1;

-- empty idle groups
begin;
  delete from emaj.emaj_group_def where grpdef_group IN ('myGroup1','myGroup2');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
-- add one table or sequence to the empty groups
  insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
  insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
rollback;

-- checks are performed by the alterLogging.sql script
