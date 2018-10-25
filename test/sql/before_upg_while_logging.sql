-- before_upg_while_logging.sql : complex scenario executed by an emaj_adm role
-- The E-Maj version is changed while groups are in logging state
-- This script is the part of operations performed before the upgrade
--
SET datestyle TO ymd;
-----------------------------
-- grant emaj_adm role 
-----------------------------
grant emaj_adm to emaj_regression_tests_adm_user;

set role emaj_regression_tests_adm_user;

-----------------------------
-- prepare groups
-----------------------------
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2',NULL);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl4',20);
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl1');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl2');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl4');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
-- The third group name contains space, comma # and '
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','phil''s tbl1');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'myTbl2\\');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3',E'phil''s seq\\1');
insert into emaj.emaj_group_def values ('dummyGrp1','dummySchema','mytbl4');
insert into emaj.emaj_group_def values ('dummyGrp2','myschema1','dummyTable');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema1','mytbl1');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema2','mytbl2');

-----------------------------
-- set the default_tablespace parameter to tspemaj to log tables and indexes into this tablespace
-----------------------------
SET default_tablespace TO tspemaj;

-----------------------------
-- create and start groups
-----------------------------
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup2',true);
select emaj.emaj_create_group('phil''s group#3",',false);

select emaj.emaj_start_group('myGroup1','M1');
select emaj.emaj_start_group('myGroup2','M1');
select emaj.emaj_start_group('phil''s group#3",','M1');

-----------------------------
-- Step 1 : for myGroup1, update tables, set 2 marks, perform 2 unlogged rollbacks and protect the group and its last mark
-----------------------------
-- 
set search_path=myschema1;
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
insert into myTbl2 values (1,'ABC','2010-12-31');
delete from myTbl1 where col11 > 10;
insert into myTbl2 values (2,'DEF',NULL);
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
--
select emaj.emaj_set_mark_group('myGroup1','M2');
--
insert into myTbl4 values (1,'FK...',1,1,'ABC');
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2;
insert into myTbl4 values (3,'FK...',1,10,'ABC');
delete from myTbl1 where col11 = 10;
update myTbl1 set col12='DEF' where col11 <= 2;
--
select emaj.emaj_set_mark_group('myGroup1','M3');
select emaj.emaj_comment_mark_group('myGroup1','M3','Third mark set');
--
delete from myTbl1 where col11 > 3;
select emaj.emaj_rollback_group('myGroup1','M3');
insert into myTbl2 values (3,'GHI',NULL);
update myTbl4 set col43 = 3 where col41 = 2;
select emaj.emaj_rollback_group('myGroup1','M3');
--
select emaj.emaj_protect_mark_group('myGroup1','M3');
select emaj.emaj_protect_group('myGroup1');

-----------------------------
-- Step 2 : for myGroup2, start, update tables and set 2 marks 
-----------------------------
set search_path=myschema2;
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
insert into myTbl2 values (1,'ABC','2010-01-01');
delete from myTbl1 where col11 > 10;
select nextval('myschema2.myseq1');
insert into myTbl2 values (2,'DEF',NULL);
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
--
select emaj.emaj_set_mark_group('myGroup2','M2');
--
set search_path=myschema2;
select nextval('myschema2.myseq1');
select nextval('myschema2.myseq1');
select nextval('myschema2.myseq1');
--
reset role;
alter sequence mySeq1 NO MAXVALUE NO CYCLE;
set role emaj_regression_tests_adm_user;
--
insert into myTbl4 values (1,'FK...',1,1,'ABC');
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2;
--
select emaj.emaj_set_mark_group('myGroup2','M3');
-----------------------------
-- Step 3 : for myGroup2, double logged rollback
-----------------------------
select emaj.emaj_logged_rollback_group('myGroup2','M2');
select emaj.emaj_logged_rollback_group('myGroup2','M3');

-----------------------------
-- Step 4 : for both myGroup1 and myGroup2, set a common mark
-----------------------------
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Common');

-----------------------------
-- Step 5 : alter group myGroup1 by changing a log schema and removing a table
-----------------------------
update emaj.emaj_group_def set grpdef_log_schema_suffix = 'b' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';

reset role;
select emaj.emaj_alter_group('myGroup1');
set role emaj_regression_tests_adm_user;

-----------------------------
-- Checking steps 1 to 5
-----------------------------
-- emaj tables
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;

-- to be uncommented in the next version
--select group_name, group_is_rollbackable, group_creation_time_id,
--       group_last_alter_time_id, group_has_waiting_changes, group_is_logging, 
--       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
--  from emaj.emaj_group order by group_name;
select group_name, group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_is_rollbackable, 
       group_creation_time_id, group_last_alter_time_id, group_comment
  from emaj.emaj_group order by group_name;

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, 
       mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark 
  from emaj.emaj_mark order by mark_id;

select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;

select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size
  from emaj.emaj_seq_hole order by 1,2,3;

select * from emaj.emaj_alter_plan order by 1,2,3,4,5;

-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.mySchema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema1_myTbl3_log_1" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
--
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;

-------------------------------
-- Specific tests for this upgrade
-------------------------------
update emaj.emaj_group_def set grpdef_priority = 21 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
