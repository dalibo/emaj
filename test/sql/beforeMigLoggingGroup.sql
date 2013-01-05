-- beforeMigLoggingGroup.sql : complex scenario executed by an emaj_adm role
-- The E-Maj version is changed while groups are in logging state
-- This script is the part of operations performed before the migration
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
-- create and start groups
-----------------------------
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup2',true);
select emaj.emaj_create_group('phil''s group#3",',false);

select emaj.emaj_start_group('myGroup1','M1');
select emaj.emaj_start_group('myGroup2','M1');
select emaj.emaj_start_group('phil''s group#3",','M1');

-----------------------------
-- Step 1 : for myGroup1, update tables and set 2 marks
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
set search_path=myschema1;
insert into myTbl4 values (1,'FK...',1,1,'ABC');
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2;
insert into myTbl4 values (3,'FK...',1,10,'ABC');
delete from myTbl1 where col11 = 10;
update myTbl1 set col12='DEF' where col11 <= 2;
--
select emaj.emaj_set_mark_group('myGroup1','M3');
select emaj.emaj_comment_mark_group('myGroup1','M3','Third mark set');

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
-- Step 3 : for myGroup2, double logged rollback then delete first mark 
-----------------------------
select emaj.emaj_logged_rollback_group('myGroup2','M2');
select emaj.emaj_logged_rollback_group('myGroup2','M3');

-----------------------------
-- Checking steps 1 to 3
-----------------------------
-- emaj tables
select group_name, group_state, group_nb_table, group_nb_sequence, group_is_rollbackable, group_comment 
from emaj.emaj_group order by group_nb_table;

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_is_deleted, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id from emaj.emaj_mark order by mark_id;

