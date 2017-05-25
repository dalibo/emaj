-- create_drop.sql : prepare groups content and test emaj_create_group(), emaj_comment_group() 
-- emaj_drop_group() and emaj_force_drop_group() functions
--
SET client_min_messages TO WARNING;
-----------------------------
-- prepare groups
-----------------------------
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2',NULL,NULL,NULL,'tsplog1','tsplog1');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL,'b',NULL,'tsp log''2','tsp log''2');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10,'C',NULL,'tsplog1');
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl4',20,NULL,NULL,'tsplog1','tsp log''2');

insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl1');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl2');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3',NULL,'C');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl4',NULL,NULL,'myschema2_mytbl4');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl5',NULL,NULL,'otherPrefix4mytbl5');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl6');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
-- The third group name contains space, comma # and '
-- (note myTbl4 from "phil's schema3" remains outside phil's group#3", group)
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','phil''s tbl1',NULL,' #''3');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'myTbl2\\');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'myTbl2\\_col21_seq');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'phil''s seq\\1');

insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblm');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblc1');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblc2');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mypartp1');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mypartp2');

insert into emaj.emaj_group_def values ('dummyGrp1','dummySchema','mytbl4');
insert into emaj.emaj_group_def values ('dummyGrp1','myschema1','dummyTable');
insert into emaj.emaj_group_def values ('dummyGrp2','emaj','emaj_param');
insert into emaj.emaj_group_def values ('dummyGrp2','emajC','myschema1_myTbl3_log');
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
-- group with an unlogged table
begin;
  insert into emaj.emaj_group_def values ('myGroup5','myschema5','myunloggedtbl');
  select emaj.emaj_create_group('myGroup5');
rollback;
-- group with a WITH OIDS table
begin;
  insert into emaj.emaj_group_def values ('myGroup5','myschema5','myoidstbl');
  select emaj.emaj_create_group('myGroup5');
rollback;
-- table without pkey for a rollbackable group
select emaj.emaj_create_group('phil''s group#3",',true);
-- sequence with a log schema suffix defined in the emaj_group_def table
begin;
  update emaj.emaj_group_def set grpdef_log_schema_suffix = 'something' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- sequence with an emaj names prefix defined in the emaj_group_def table
begin;
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'something' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- sequence with tablespaces defined in the emaj_group_def table
begin;
  update emaj.emaj_group_def set grpdef_log_dat_tsp = 'something', grpdef_log_idx_tsp = 'something' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- table with invalid tablespace
begin;
  update emaj.emaj_group_def set grpdef_log_dat_tsp = 'dummyTablespace' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- already existing secondary schema
begin;
  create schema emajb;
  select emaj.emaj_create_group('myGroup1');
rollback;
-- conflict on emaj names prefix inside the group to create
begin;
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2';
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'myschema1_mytbl4' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';
  select emaj.emaj_create_group('myGroup1');
rollback;
-- conflict on emaj names prefix with already create groups
begin;
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  select emaj.emaj_create_group('myGroup1');
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup2' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl1';
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'myschema1_mytbl2' where grpdef_group = 'myGroup2' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl2';
  select emaj.emaj_create_group('myGroup2');
rollback;
-- mix a lot of errors
begin;
  update emaj.emaj_group_def set grpdef_log_schema_suffix = 'something', grpdef_emaj_names_prefix = 'something', grpdef_log_dat_tsp = 'something' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  update emaj.emaj_group_def set grpdef_emaj_names_prefix = 'samePrefix' where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2';
  alter table myschema1.mytbl1 set with oids;
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

-- create a group with a table from an E-Maj secondary schema
select emaj.emaj_create_group('dummyGrp2',false);

-- create a group with a table already belonging to another group
select emaj.emaj_create_group('dummyGrp3');

-- already created
select emaj.emaj_create_group('myGroup2');

-- impact of created groups
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select group_name, group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_is_rollbackable, 
       group_creation_time_id, group_last_alter_time_id, group_comment
 from emaj.emaj_group order by group_name;
select * from emaj.emaj_relation order by rel_group, rel_priority, rel_schema, rel_tblseq;
select * from pg_tables where schemaname like 'emaj%' order by tablename;

-----------------------------
-- emaj_comment_group() tests
-----------------------------

-- unknown group
select emaj.emaj_comment_group(NULL,NULL);
select emaj.emaj_comment_group('unkownGroup',NULL);

-- should be OK
select emaj.emaj_comment_group('myGroup1','a first comment for group #1');
select emaj.emaj_comment_group('myGroup1','a better comment for group #1');
select emaj.emaj_comment_group('emptyGroup','an empty group');

select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_comment_group('myGroup1',NULL);
select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';

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
-- secondary schema with an object blocking the schema drop
begin;
  create table emajb.dummy_log (col1 int);
  select emaj.emaj_drop_group('myGroup1');
rollback;
-- should be OK
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_drop_group('myGroup2');
select emaj.emaj_drop_group('emptyGroup');

-- already dropped
select emaj.emaj_drop_group('myGroup2');

-----------------------------
-- emaj_force_drop_group() tests
-----------------------------

-- unknown group
select emaj.emaj_force_drop_group(NULL);
select emaj.emaj_force_drop_group('unkownGroup');
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
select hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist order by hist_id;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;

