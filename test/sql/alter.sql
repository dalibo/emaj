-- alter.sql : test emaj_alter_group(), emaj_alter_groups(),
--                  emaj_move_table(), emaj_move_tables(), emaj_move_sequence(), emaj_move_sequences()
--                  emaj_modify_table() and emaj_modify_tables() functions
--

-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 6000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 6000;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 6000;
alter sequence emaj.emaj_global_seq restart 60000;

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
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq');
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
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq');
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

-- move them back to their original group
update emaj.emaj_group_def set grpdef_group = 'myGroup2' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_group = 'myGroup2' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3_col31_seq';
-- the next call gives a useless mark name parameter (the group is in idle state)
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','useless_mark_name_%');
select rel_time_range, rel_group, rel_log_schema, rel_log_table, rel_log_index
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myTbl3' order by rel_time_range;
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

-----------------------------------
-- emaj_move_table
-----------------------------------

-- error cases
-- table not in a group
select emaj.emaj_move_table('dummySchema','mytbl1','myGroup2');
select emaj.emaj_move_table('myschema1','dummyTable','myGroup2');
-- bad new group
select emaj.emaj_move_table('myschema1','mytbl1','dummyGroup');
-- bad mark
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2','EMAJ_LAST_MARK');

-- move to the same group
select emaj.emaj_move_table('myschema1','mytbl1','myGroup1');

-- ok
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2');

-----------------------------------
-- emaj_move_tables with array
-----------------------------------
-- error cases
-- table not in a group
select emaj.emaj_move_tables('myschema1',array['dummyTable','mytbl1','mytbl2'],'myGroup1');
-- empty tables array
select emaj.emaj_move_tables('myschema1',array[]::text[],'myGroup1');
select emaj.emaj_move_tables('myschema1',null,'myGroup1');
select emaj.emaj_move_tables('myschema1',array[''],'myGroup1');

-- ok (with a duplicate table name)
select emaj.emaj_move_tables('myschema1',array['mytbl2','mytbl2b','mytbl2'],'myGroup2');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_kind = 'r' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_tables with filters
-----------------------------------
-- empty tables array
select emaj.emaj_move_tables('myschema1',null,null,'myGroup1');
select emaj.emaj_move_tables('myschema1','','','myGroup1');
select emaj.emaj_move_tables('myschema1','mytbl1','mytbl1','myGroup1');

-- ok and go back to myGroup1
select emaj.emaj_move_tables('myschema1','my(t|T)bl.*','mytbl2$','myGroup1');
select emaj.emaj_move_tables('myschema1','.*','','myGroup1');

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 6;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2') order by 1 desc ,2,3;

-----------------------------------
-- emaj_move_sequence
-----------------------------------

-- error cases
-- sequence not in a group
select emaj.emaj_move_sequence('dummySchema','myseq1','myGroup1');
select emaj.emaj_move_sequence('myschema2','dummySequence','myGroup1');
-- bad new group
select emaj.emaj_move_sequence('myschema2','myseq1','dummyGroup');
-- bad mark
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup1','EMAJ_LAST_MARK');

-- move to the same group
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup2');

-- ok
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup1');

-----------------------------------
-- emaj_move_sequences with array
-----------------------------------
-- error cases
-- sequence not in a group
select emaj.emaj_move_sequences('myschema2',array['dummySequence','myseq1'],'myGroup1');
-- empty tables array
select emaj.emaj_move_sequences('myschema2',array[]::text[],'myGroup1');
select emaj.emaj_move_sequences('myschema2',null,'myGroup1');
select emaj.emaj_move_sequences('myschema2',array[''],'myGroup1');

-- ok (with a duplicate sequence name)
select emaj.emaj_move_sequences('myschema2',array['myseq1','myseq1'],'myGroup4');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_kind = 'S' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_sequences with filters
-----------------------------------
-- empty sequences array
select emaj.emaj_move_sequences('myschema21',null,null,'myGroup2');
select emaj.emaj_move_sequences('myschema21','','','myGroup2');
select emaj.emaj_move_sequences('myschema21','myseq1','myseq1','myGroup2');

-- ok and go back to myGroup2
select emaj.emaj_move_sequences('myschema2','.*','','myGroup2');

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 3;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2','myGroup4') order by 1 desc ,2,3;

-----------------------------------
-- emaj_modify_table
-----------------------------------

-- error cases
-- table not in a group
select emaj.emaj_modify_table('dummySchema','mytbl1',null);
select emaj.emaj_modify_table('myschema1','dummyTable',null);

-- invalid priority
select emaj.emaj_modify_table('myschema1','mytbl1','{"priority":"not_numeric"}'::jsonb);
-- invalid tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":"dummytsp"}'::jsonb);
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_index_tablespace":"dummytsp"}'::jsonb);
-- unknown property
select emaj.emaj_modify_table('myschema1','mytbl1','{"unknown_property":null}'::jsonb);

-- bad mark
select emaj.emaj_modify_table('myschema1','mytbl1',null,'EMAJ_LAST_MARK');

-- ok
-- change a priority
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":1}');

-- change a log data tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":"tsp log''2"}');
-- change a log index tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_index_tablespace":"tsp log''2"}');

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-- revert changes
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":null}');
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":null, "log_index_tablespace":null}');

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-------------------------------------
---- emaj_modify_tables with array
-------------------------------------
-- error cases
-- table not in a group
select emaj.emaj_modify_tables('myschema2',array['dummyTable','mytbl1','mytbl2'],null);
-- empty tables array
select emaj.emaj_modify_tables('myschema2',array[]::text[],null);
select emaj.emaj_modify_tables('myschema2',null,null);
select emaj.emaj_modify_tables('myschema2',array[''],null);

---- ok (with a duplicate table name)
select emaj.emaj_modify_tables('myschema2',array['mytbl1','mytbl2','mytbl2'],'{"priority":10,"log_data_tablespace":"tsplog1"}');
select rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_tblseq in ('mytbl1','mytbl2') order by 1,2,3;

-----------------------------------
-- emaj_modify_tables with filters
-----------------------------------
-- empty tables array
select emaj.emaj_modify_tables('myschema2',null,null,null::jsonb);
select emaj.emaj_modify_tables('myschema2','','',null::jsonb);
select emaj.emaj_modify_tables('myschema2','mytbl1','mytbl1',null::jsonb);

-- ok and revert the previous changes
select emaj.emaj_modify_tables('myschema2','mytbl.*','','{"priority":null,"log_data_tablespace":null}'::jsonb);
select rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_tblseq like 'mytbl%' order by 1,2,3;

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 14;

-- checks are performed by the alterLogging.sql script
