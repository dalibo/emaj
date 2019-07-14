-- create_drop.sql : prepare groups content and test emaj_create_group(), emaj_comment_group(),
-- emaj_assign_table(), emaj_assign_tables(), emaj_remove_table(), emaj_remove_tables(),
-- emaj_ignore_app_trigger(), emaj_drop_group() and emaj_force_drop_group() functions
--
SET client_min_messages TO WARNING;
-----------------------------
-- prepare groups
-----------------------------
truncate emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2',NULL,'tsplog1','tsplog1');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL,'tsp log''2','tsp log''2');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10,'tsplog1');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl4',20,'tsplog1','tsp log''2');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b_col20_seq');

insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl1');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl2');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl4');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl5');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl6');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
-- The third group name contains space, comma # and '
-- (note myTbl4 from "phil's schema3" remains outside phil's group#3", group)
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','phil''s tbl1');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'myTbl2\\');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'myTbl2\\_col21_seq');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'phil''s seq\\1');

insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblm');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblc1');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblc2');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mypartp1');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mypartp2');

insert into emaj.emaj_group_def values ('myGroup5','myschema5','myunloggedtbl');
insert into emaj.emaj_group_def values ('myGroup5','myschema5','myoidstbl');

insert into emaj.emaj_group_def values ('myGroup6','myschema6','table_with_50_characters_long_name_____0_________0');
insert into emaj.emaj_group_def values ('myGroup6','myschema6','table_with_51_characters_long_name_____0_________0a');
insert into emaj.emaj_group_def values ('myGroup6','myschema6','table_with_55_characters_long_name_____0_________0abcde');
insert into emaj.emaj_group_def values ('myGroup6','myschema6','table_with_55_characters_long_name_____0_________0fghij');

insert into emaj.emaj_group_def values ('dummyGrp1','dummySchema','mytbl4');
insert into emaj.emaj_group_def values ('dummyGrp1','myschema1','dummyTable');
insert into emaj.emaj_group_def values ('dummyGrp2','emaj','emaj_param');
insert into emaj.emaj_group_def values ('dummyGrp2','emaj_myschema1','myTbl3_log');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema1','mytbl1');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema2','mytbl2');

-----------------------------
-- emaj_create_group() tests
-----------------------------

-- invalid group names
select emaj.emaj_create_group(NULL);
select emaj.emaj_create_group('',false);
-- group is unknown in emaj_group_def
select emaj.emaj_create_group('unknownGroup');
select emaj.emaj_create_group('unknownGroup',false);
-- an emtpy group to create is known in emaj_group_def
select emaj.emaj_create_group('myGroup1',true,true);
-- unknown schema or table in emaj_group_def
select emaj.emaj_create_group('dummyGrp1');
-- group with a partitionned table (in PG 10+) (abort for lack of PRIMARY KEY with prior PG versions)
begin;
  insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblp');
  select emaj.emaj_create_group('myGroup4');
rollback;
-- group with a temp table
begin;
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  insert into emaj.emaj_group_def 
    select 'myGroup5',nspname,'mytemptbl' from pg_class, pg_namespace
      where relnamespace = pg_namespace.oid and relname = 'mytemptbl';
  select emaj.emaj_create_group('myGroup5');
rollback;
-- group with an unlogged table and a WITH OIDS table
begin;
  select emaj.emaj_create_group('myGroup5',true);
rollback;
-- table without pkey for a rollbackable group
select emaj.emaj_create_group('phil''s group#3",',true);
-- sequence with tablespaces defined in the emaj_group_def table
begin;
  update emaj.emaj_group_def set grpdef_log_dat_tsp = 'something', grpdef_log_idx_tsp = 'something' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- table with invalid tablespaces
begin;
  update emaj.emaj_group_def set grpdef_log_dat_tsp = 'dummyTablespace', grpdef_log_idx_tsp = 'dummyTablespace'
    where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- already existing log schema
begin;
  create schema emaj_myschema1;
  select emaj.emaj_create_group('myGroup1');
rollback;

-- bad alter_log_table parameter
begin;
  insert into emaj.emaj_param (param_key, param_value_text) values ('alter_log_table','dummmy content');
  select emaj.emaj_create_group('myGroup1');
rollback;

-- should be OK
select emaj.emaj_create_group('myGroup1');

-- explicitely create an empty group (here audit_only)
select emaj.emaj_create_group('emptyGroup',false,true);

-- should be OK, but with a warning for linked table not protected by E-Maj
alter table myschema2.myTbl6 add foreign key (col61) references myschema2.myTbl7 (col71) deferrable initially immediate;
alter table myschema2.myTbl8 add foreign key (col81) references myschema2.myTbl6 (col61) deferrable;
select emaj.emaj_create_group('myGroup2',true);

-- should be OK, but with a warning for linked table belonging to another group
begin;
  update emaj.emaj_group_def set grpdef_group = 'dummyGrp3' 
    where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
  select emaj.emaj_create_group('phil''s group#3",',false);
rollback;

-- should be OK, but with a warning for linked table not belonging to any group
begin;
  delete from emaj.emaj_group_def
    where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
  select emaj.emaj_create_group('phil''s group#3",',false);
rollback;

-- should be OK
select emaj.emaj_create_group('phil''s group#3",',false);
select emaj.emaj_create_group('myGroup4');
select emaj.emaj_create_group('myGroup5',false);
select emaj.emaj_create_group('myGroup6');

-- create a group with a table from an E-Maj log schema
select emaj.emaj_create_group('dummyGrp2',false);

-- create a group with a table already belonging to another group
select emaj.emaj_create_group('dummyGrp3');

-- already created
select emaj.emaj_create_group('myGroup2');

-- impact of created groups
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;

select group_name, group_is_rollbackable, group_creation_time_id,
       group_last_alter_time_id, group_has_waiting_changes, group_is_logging, 
       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  from emaj.emaj_group order by group_name;
select * from emaj.emaj_relation order by rel_group, rel_priority, rel_schema, rel_tblseq, rel_time_range;
select schemaname, tablename, tableowner, tablespace from pg_tables where schemaname like 'emaj\_%' order by schemaname, tablename;

-----------------------------
-- emaj_comment_group() tests
-----------------------------

-- unknown group
select emaj.emaj_comment_group(NULL,NULL);
select emaj.emaj_comment_group('unknownGroup',NULL);

-- should be OK
select emaj.emaj_comment_group('myGroup1','a first comment for group #1');
select emaj.emaj_comment_group('myGroup1','a better comment for group #1');
select emaj.emaj_comment_group('emptyGroup','an empty group');

select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_comment_group('myGroup1',NULL);
select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';

-----------------------------------
-- emaj_assign_table
-----------------------------------
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_create_group('myGroup1b', true, true);  -- rollbackable and empty

-- error cases
-- bad group name
select emaj.emaj_assign_table('myschema1','mytbl1','dummyGroup');
-- bad schema
select emaj.emaj_assign_table('dummySchema','mytbl1','myGroup1b');
select emaj.emaj_assign_table('emaj','mytbl1','myGroup1b');
-- bad table
select emaj.emaj_assign_table('myschema1','dummyTable','myGroup1b');
-- partitionned table (abort for lack of PRIMARY KEY with prior PG versions)
select emaj.emaj_assign_table('myschema4','mytblp','myGroup1b');
-- temp table
begin;
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  select emaj.emaj_assign_table(nspname,'mytemptbl','myGroup1b') from pg_class, pg_namespace
    where relnamespace = pg_namespace.oid and relname = 'mytemptbl';
rollback;
-- unlogged table
select emaj.emaj_assign_table('myschema5','myunloggedtbl','myGroup1b');
-- table WITH OIDS
select emaj.emaj_assign_table('myschema5','myoidstbl','myGroup1b');
-- table without PKEY
select emaj.emaj_assign_table('phil''s schema3','myTbl2\','myGroup1b');

-- invalid priority
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b','{"priority":"not_numeric"}'::jsonb);

-- invalid tablespace
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b','{"log_data_tablespace":"dummytsp"}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b','{"log_index_tablespace":"dummytsp"}'::jsonb);

-- unknown property
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b','{"unknown_property":null}'::jsonb);

-- bad mark
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b',null,'EMAJ_LAST_MARK');

-- erroneously existing log schema
begin;
  create schema emaj_myschema1;
  select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b');
rollback;

-- ok
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b');
-- already assigned table
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1b');

-----------------------------------
-- emaj_assign_tables
-----------------------------------
-- error cases
-- bad group name
select emaj.emaj_assign_tables('dummySchema',array['dummyTable'],'dummyGroup');
-- bad tables
select emaj.emaj_assign_tables('myschema1',array['dummytbl1','dummytbl2'],'myGroup1b');
-- empty tables array
select emaj.emaj_assign_tables('myschema1',array[]::text[],'myGroup1b');
select emaj.emaj_assign_tables('myschema1',null,'myGroup1b');
select emaj.emaj_assign_tables('myschema1',array[''],'myGroup1b');

-- ok (with a duplicate table name)
select emaj.emaj_assign_tables('myschema1',array['mytbl2','mytbl2b','mytbl2'],'myGroup1b');

select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1b';

-----------------------------------
-- emaj_remove_table
-----------------------------------

-- error cases
-- table not in a group
select emaj.emaj_remove_table('dummySchema','mytbl1');
select emaj.emaj_remove_table('myschema1','dummyTable');
-- empty tables array
select emaj.emaj_remove_tables('myschema1',array[]::text[]);
select emaj.emaj_remove_tables('myschema1',null);
select emaj.emaj_remove_tables('myschema1',array['']);
-- bad mark
select emaj.emaj_remove_table('myschema1','mytbl1','EMAJ_LAST_MARK');

-- ok
select emaj.emaj_remove_table('myschema1','mytbl1');

-----------------------------------
-- emaj_remove_tables
-----------------------------------
-- error cases
-- table not in a group
select emaj.emaj_remove_tables('myschema1',array['dummyTable','mytbl1','mytbl2']);

-- ok (with a duplicate table name)
select emaj.emaj_remove_tables('myschema1',array['mytbl2','mytbl2b','mytbl2']);

select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1b';

select emaj.emaj_drop_group('myGroup1b');
select emaj.emaj_create_group('myGroup1');

-----------------------------------
-- emaj_ignore_app_trigger
-----------------------------------

-- unknown action
select emaj.emaj_ignore_app_trigger('dummy','dummy','dummy','dummy');

-- unknown schema
select emaj.emaj_ignore_app_trigger('ADD','dummy','mytbl2','dummy');

-- unknown table, and empty or NULL triggers array
select emaj.emaj_ignore_app_trigger('ADD','myschema1','dummy','dummy');

-- unknown triggers
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','dummy');

-- emaj triggers
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','emaj_trunc_trg');
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','emaj_%_trg');

-- add one trigger
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','mytbl2trg1');
select * from emaj.emaj_ignored_app_trigger order by trg_schema, trg_table, trg_name;

-- add the same
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','mytbl2trg1');
select * from emaj.emaj_ignored_app_trigger order by trg_schema, trg_table, trg_name;

-- add all triggers for a table
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','%');
select * from emaj.emaj_ignored_app_trigger order by trg_schema, trg_table, trg_name;

-- remove one trigger
select emaj.emaj_ignore_app_trigger('REMOVE','myschema1','mytbl2','mytbl2trg1');
select * from emaj.emaj_ignored_app_trigger order by trg_schema, trg_table, trg_name;

-- remove several triggers
select emaj.emaj_ignore_app_trigger('REMOVE','myschema1','mytbl2','%');
select * from emaj.emaj_ignored_app_trigger order by trg_schema, trg_table, trg_name;

-----------------------------
-- emaj_drop_group() tests
-----------------------------

-- unknown group
select emaj.emaj_drop_group(NULL);
select emaj.emaj_drop_group('unknownGroup');
-- group in logging state
select emaj.emaj_start_group('myGroup1','');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_stop_group('myGroup1');
-- log schema with an object blocking the schema drop
begin;
  create table emaj_myschema1.dummy_log (col1 int);
  select emaj.emaj_drop_group('myGroup1');
rollback;
-- should be OK
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_drop_group('myGroup2');
select emaj.emaj_drop_group('emptyGroup');
select emaj.emaj_drop_group('myGroup5');
select emaj.emaj_drop_group('myGroup6');

-- already dropped
select emaj.emaj_drop_group('myGroup2');

-----------------------------
-- emaj_force_drop_group() tests
-----------------------------

-- unknown group
select emaj.emaj_force_drop_group(NULL);
select emaj.emaj_force_drop_group('unknownGroup');
-- already dropped
select emaj.emaj_force_drop_group('myGroup2');
-- should be OK
select emaj.emaj_create_group('myGroup1',false);
select emaj.emaj_start_group('myGroup1','');
select emaj.emaj_force_drop_group('myGroup1');

select emaj.emaj_create_group('myGroup2',true);
select emaj.emaj_force_drop_group('myGroup2');

-----------------------------
-- test end: check and force sequences id
-----------------------------
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select sch_name from emaj.emaj_schema order by 1;
select * from emaj.emaj_rel_hist order by 1,2,3;
select hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist order by hist_id;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;
