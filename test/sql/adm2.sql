-- adm2.sql : complex scenario executed by an emaj_adm role. 
--            Follows adm1.sql, and includes more specific test cases
--
set role emaj_regression_tests_adm_user;
-----------------------------
-- Step 8 : use of multi-group functions, start_group(s) without log reset and use deleted marks
-----------------------------
-- stop both groups
select emaj.emaj_stop_groups(array['myGroup1','myGroup2']);

-- start both groups
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- set a mark for both groups
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');

-- logged rollback to Multi-1
select * from emaj.emaj_logged_rollback_groups(array['myGroup1','myGroup2'],'Multi-1',false) order by 1,2;

-- rollback to Multi-2
select * from emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-2',false) order by 1,2;

-- rollback and stop to Multi-1
select * from emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-1',false) order by 1,2;
select emaj.emaj_stop_groups(array['myGroup1','myGroup2'],'Stop after rollback');

-- try to start both groups, but with an old deleted mark name
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- really start both groups
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1b', false);

-- try to rollback several groups to a deleted common mark
select * from emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- set again a mark for both groups
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');

-- delete the mark for only 1 group and get detailed statistics for the other group
select emaj.emaj_delete_mark_group('myGroup1','Multi-2');

-- use this mark for the other group before delete it
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Multi-2',NULL);
select * from emaj.emaj_rollback_group('myGroup2','Multi-2',false) order by 1,2;
select emaj.emaj_delete_mark_group('myGroup2','Multi-2');

-- get statistics using deleted marks
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','M1','M2');

-- delete intermediate deleted marks
select emaj.emaj_delete_mark_group('myGroup1','Multi-1');
select emaj.emaj_delete_mark_group('myGroup2','Multi-1');
-- ... and reuse mark names for parallel rollback test
select emaj.emaj_rename_mark_group('myGroup1','Multi-1b','Multi-1');
select emaj.emaj_rename_mark_group('myGroup2','Multi-1b','Multi-1');

-- rename a deleted mark
select emaj.emaj_rename_mark_group('myGroup2','M2','Deleted M2');

-- use emaj_get_previous_mark_group and delete an initial deleted mark
select emaj.emaj_delete_before_mark_group('myGroup2',
      (select emaj.emaj_get_previous_mark_group('myGroup2',
             (select time_clock_timestamp from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup2' and mark_group = 'myGroup2' and mark_name = 'M3')+'0.000001 SECOND'::interval)));

-- comment a deleted mark
select emaj.emaj_comment_mark_group('myGroup2','M3','This mark is deleted');

-- try to get a rollback duration estimate on a deleted mark
select emaj.emaj_estimate_rollback_group('myGroup2','M3',TRUE);

-- try to rollback on a deleted mark
select * from emaj.emaj_rollback_group('myGroup2','M3',false) order by 1,2;

-----------------------------
-- Checking step 8
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl6_log order by emaj_gid, emaj_tuple desc;

-----------------------------
-- Step 9 : test the emaj_alter_group() and emaj_alter_groups() functions with the audit-only phil's group#3, group and a mix of rollbackable and audit-only groups
-----------------------------
-- create the audit-only group empty and then populate it with tables and sequences
select emaj.emaj_create_group('phil''s group#3",',false,true);
select emaj.emaj_assign_tables('phil''s schema3', '.*', 'mytbl4', 'phil''s group#3",');
select emaj.emaj_assign_sequences('phil''s schema3', '.*', '', 'phil''s group#3",');

-- disable event triggers for this step to allow an application table structure change
select emaj.emaj_disable_protection_by_event_triggers();

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(11);

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_priority = 1, grpdef_log_idx_tsp = 'tsplog1'
  where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
select emaj.emaj_alter_group('phil''s group#3",');
select emaj.emaj_start_group('phil''s group#3",','M1_after_alter_group');
select emaj.emaj_stop_group('phil''s group#3",');

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(10);

set role emaj_regression_tests_adm_user;

select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_create_group('myGroup4');

update emaj.emaj_group_def set grpdef_priority = NULL, grpdef_log_idx_tsp = NULL
  where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
update emaj.emaj_group_def set grpdef_priority = 999
  where grpdef_schema = 'myschema4' and grpdef_tblseq = 'mytblm';

select emaj.emaj_alter_groups('{"phil''s group#3\",","myGroup4"}');

select emaj.emaj_drop_group('phil''s group#3",');
select emaj.emaj_drop_group('myGroup4');
-----------------------------
-- Checking step 9
-----------------------------
select * from emaj.emaj_alter_plan where altr_time_id > 10000 order by 1,2,3,4,5;

-----------------------------
-- Step 10 : for phil''s group#3", recreate the group as rollbackable, update tables, 
--           rename a mark, then delete 2 marks then delete all before a mark 
-----------------------------
-- prepare phil's group#3, group
--
reset role;
alter table "phil's schema3"."myTbl2\" add primary key (col21);
set role emaj_regression_tests_adm_user;
select emaj.emaj_create_group('phil''s group#3",',true);
select emaj.emaj_start_group('phil''s group#3",','M1_rollbackable');
--
set search_path=public,"phil's schema3";
--
insert into "phil's tbl1" select i, 'AB''C', E'\\014'::bytea from generate_series (1,31) as i;
update "phil's tbl1" set "phil\s col13" = E'\\034'::bytea where "phil's col11" <= 3;
update "phil's tbl1" set "phil\s col13" = E'\\034'''::bytea where "phil's col11" between 18 and 22;
insert into myTbl4 (col41) values (1);
insert into myTbl4 (col41) values (2);
insert into "myTbl2\" values (1,'ABC','2010-12-31');
delete from "phil's tbl1" where "phil's col11" > 20;
insert into "myTbl2\" values (2,'DEF',NULL);
select nextval(E'"phil''s schema3"."phil''s seq\\1"');
--
select emaj.emaj_set_mark_group('phil''s group#3",','M2_rollbackable');
select emaj.emaj_set_mark_group('phil''s group#3",','M2_again!');
--
delete from "phil's tbl1" where "phil's col11" = 10;
update "phil's tbl1" set "phil's col12" = 'DEF' where "phil's col11" <= 2;

select nextval(E'"phil''s schema3"."phil''s seq\\1"');
--
select emaj.emaj_set_mark_groups(array['phil''s group#3",'],'phil''s mark #1');
select emaj.emaj_comment_mark_group('phil''s group#3",','phil''s mark #1','Third mark set');
--
select emaj.emaj_rename_mark_group('phil''s group#3",','phil''s mark #1','phil''s mark #3');
-- 
select emaj.emaj_delete_mark_group('phil''s group#3",','M2_again!');
--
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('phil''s group#3",','','');
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('phil''s group#3",','phil''s mark #3','');
--
select * from emaj.emaj_logged_rollback_group('phil''s group#3",','phil''s mark #3',false) order by 1,2;
select * from emaj.emaj_rollback_group('phil''s group#3",','phil''s mark #3',false) order by 1,2;

-----------------------------
-- Checking step 10
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;
-- user tables
select * from "phil's schema3"."phil's tbl1" order by "phil's col11","phil's col12";
select * from "phil's schema3"."myTbl2\" order by col21;
-- log tables
select "phil's col11", "phil's col12", "phil\s col13", emaj_verb, emaj_tuple, emaj_gid from "emaj_phil's schema3"."phil's tbl1_log" order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from "emaj_phil's schema3"."myTbl2\_log" order by emaj_gid, emaj_tuple desc;

-----------------------------
-- Step 11 : for myGroup1, in a transaction, update tables and rollback the transaction, 
--           then rollback to previous mark 
-----------------------------
set search_path=public,myschema1;
--
begin transaction;
  delete from mytbl1;
rollback;
--
select * from emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK',false) order by 1,2;

-----------------------------
-- Checking step 11
-----------------------------
-- emaj tables
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

-----------------------------
-- Step 12 : tests snaps and script generation functions
-----------------------------
-- first add some updates for tables with unusual types (arrays, geometric)
set search_path=public,myschema2;
insert into myTbl5 values (10,'{"abc","def","ghi"}','{1,2,3}',NULL,'{}');
insert into myTbl5 values (20,array['abc','def','ghi'],array[3,4,5],array['2000/02/01'::date,'2000/02/28'::date],'{"id":1000, "c1":"abc"}');
update myTbl5 set col54 = '{"2010/11/28","2010/12/03"}', col55 = '{"id":1001, "c2":"def"}' where col54 is null;
insert into myTbl6 select i+10, point(i,1.3), '((0,0),(2,2))', circle(point(5,5),i),'((-2,-2),(3,0),(1,4))','10.20.30.40/27' from generate_series (1,8) as i;
update myTbl6 set col64 = '<(5,6),3.5>', col65 = null where col61 <= 13;

-- also add rows with unusual text content
insert into mytbl2 values (10, E'row 1 \r... and row 2 with a '' (quote) character',null);
insert into mytbl2 values (11, E'row 1 with a true \\ character\r... and row 2 with two \\n true and a '' (quote) characters',null);
-- and manupulate NULL characters
insert into mytbl1 values (200, E'Start\tEnd ', E'A\\000B'::BYTEA);
update mytbl1 set col12 = E' Start\tEnd' where col11 = 200;
delete from mytbl1 where col13 = E'A\\000B'::BYTEA;

-- reset directory for snaps
\! rm -Rf /tmp/emaj_test/snaps
\! mkdir -p /tmp/emaj_test/snaps
-- ... and snap the all groups
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','CSV HEADER');
select emaj.emaj_snap_group('myGroup2','/tmp/emaj_test/snaps','CSV HEADER');
select emaj.emaj_snap_group('phil''s group#3",','/tmp/emaj_test/snaps','CSV HEADER');

\! ls /tmp/emaj_test/snaps

-- reset directory for emaj_gen_sql_group tests
\! rm -Rf /tmp/emaj_test/sql_scripts
\! mkdir /tmp/emaj_test/sql_scripts

-- generate sql script for each active group (and check the result with detailed log statistics + number of sequences)
select emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myGroup1.sql');
select coalesce(sum(stat_rows),0) + 2 as check from emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myGroup2.sql', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1','myschema2.myTbl3_col31_seq']);
select sum(stat_rows) + 2 as check from emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('phil''s group#3",', 'M1_rollbackable', NULL, '/tmp/emaj_test/sql_scripts/Group3.sql');
select sum(stat_rows) + 2 as check from emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'M1_rollbackable', NULL);

-- process \\ in script files
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i_s -s 's/\\\\/\\/g'
-- comment transaction commands for the need of the current test
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
-- mask timestamp in initial comment
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'

\! ls /tmp/emaj_test/sql_scripts

-- reset directory for second set of snaps
\! rm -Rf /tmp/emaj_test/snaps2
\! mkdir /tmp/emaj_test/snaps2
-- in a single transaction and as superuser:
--   rollback groups, replay updates with generated scripts, snap groups again and cancel the transaction
reset role;
begin;
  select * from emaj.emaj_rollback_group('myGroup1','Multi-1',false) order by 1,2;
  select * from emaj.emaj_rollback_group('myGroup2','Multi-1',false) order by 1,2;
  select * from emaj.emaj_rollback_group('phil''s group#3",','M1_rollbackable',false) order by 1,2;

\i /tmp/emaj_test/sql_scripts/myGroup1.sql
\i /tmp/emaj_test/sql_scripts/myGroup2.sql
\i /tmp/emaj_test/sql_scripts/Group3.sql

  select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps2','CSV HEADER');
  select emaj.emaj_snap_group('myGroup2','/tmp/emaj_test/snaps2','CSV HEADER');
  select emaj.emaj_snap_group('phil''s group#3",','/tmp/emaj_test/snaps2','CSV HEADER');
rollback;

-- mask timestamp in _INFO files
\! sed -i_s -s 's/at .*/at [ts]/' /tmp/emaj_test/snaps/_INFO /tmp/emaj_test/snaps2/_INFO
-- and compare both snaps sets
-- sequences are detected as different because of :
-- - the effect of RESTART on is_called and next_val attributes
-- - internal log_cnt value being reset
\! diff --exclude _INFO_s /tmp/emaj_test/snaps /tmp/emaj_test/snaps2

set role emaj_regression_tests_adm_user;

-----------------------------
-- Step 13 : test use of a table with a very long name (63 characters long)
-----------------------------
select emaj.emaj_stop_group('phil''s group#3",');

-- rename the "phil's tbl1" table and alter its group
reset role;
alter table "phil's schema3"."phil's tbl1" rename to table_with_very_looooooooooooooooooooooooooooooooooooooong_name;

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_tblseq = 'table_with_very_looooooooooooooooooooooooooooooooooooooong_name'
  where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'phil''s tbl1';
select emaj.emaj_alter_group('phil''s group#3",');

-- use the table and its group
select emaj.emaj_start_group('phil''s group#3",','M1_after_alter_group');

update "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name set "phil's col12" = 'GHI' where "phil's col11" between 6 and 9;
select emaj.emaj_set_mark_group('phil''s group#3",','M2');
delete from "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name where "phil's col11" > 18;

select * from emaj.emaj_rollback_group('phil''s group#3",','M1_after_alter_group',false) order by 1,2;
select emaj.emaj_stop_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

--
reset role;
alter table "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name rename to "phil's tbl1";
update emaj.emaj_group_def set grpdef_tblseq = 'phil''s tbl1'
  where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'table_with_very_looooooooooooooooooooooooooooooooooooooong_name';

-----------------------------
-- Step 14 : test use of groups or marks protection
-----------------------------
set role emaj_regression_tests_adm_user;
-- try to rollback a protected group
select emaj.emaj_protect_group('myGroup2');
select * from emaj.emaj_rollback_group('myGroup2','M3',false) order by 1,2;
select emaj.emaj_unprotect_group('myGroup2');

-- try to rollback over a protected mark
select emaj.emaj_set_mark_group('myGroup1','Mark_to_protect');
select emaj.emaj_protect_mark_group('myGroup1','Mark_to_protect');
select * from emaj.emaj_rollback_group('myGroup1','Multi-1',false) order by 1,2;
select emaj.emaj_unprotect_mark_group('myGroup1','Mark_to_protect');
select emaj.emaj_delete_mark_group('myGroup1','Mark_to_protect');

-----------------------------
-- Step 15 : test complex use of rollbacks consolidations
-----------------------------
set search_path=public,myschema1;

-- 2 consolidations of 2 logged rollbacks
-- Multi-1 MC1 MC2 RMC1S RMC1D      MC3 MC4 MC5 RMC3S RMC3D
--         ^       lrg(MC1)         ^           lrg(MC3)
--         xxxxxxxxxxxxxxx conso(RMC1D)
-- 	       	                        xxxxxxxxxxxxxxxxxxx conso(RMC3D)

select emaj.emaj_set_mark_group('myGroup1','MC1');
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (2000,2012) as i;
insert into myTbl2 values (2000,'TC1',NULL);
delete from myTbl1 where col11 > 2010;

select emaj.emaj_set_mark_group('myGroup1','MC2');
update myTbl2 set col22 = 'TC2' WHERE col22 ='TC1';

select * from emaj.emaj_logged_rollback_group('myGroup1','MC1',false) order by 1,2;
insert into myTbl2 values (2000,'TC3',NULL);

select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','RLBK_MC1_DONE');

select emaj.emaj_set_mark_group('myGroup1','MC3');
insert into "myTbl3" (col33) select generate_series(2000,2039,4)/100;
insert into myTbl4 values (2000,'FK...',1,10,'ABC');
update myTbl4 set col43 = NULL where col41 = 2000;

select emaj.emaj_set_mark_group('myGroup1','MC4');
select emaj.emaj_set_mark_group('myGroup1','MC5');

select * from emaj.emaj_logged_rollback_group('myGroup1','MC3',false) order by 1,2;
select cons_group, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), 
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_target_rlbk_mark_time_id, cons_rows, cons_marks, cons_marks
  from emaj.emaj_get_consolidable_rollbacks();
  
-- consolidate both logged rollback at once
select cons_target_rlbk_mark_name, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), 
       emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark_name)
  from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup1';

select cons_group, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), 
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_target_rlbk_mark_time_id, cons_rows, cons_marks
  from emaj.emaj_get_consolidable_rollbacks();

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup1','Multi-1',NULL);

-- 2 consolidations of 2 nested logged rollbacks
-- MC6 MC7 MC8  RMC8S RMC8D M9 RMC6S RMC6D
--         ^    lrg(MC8)
--         xxxxxxxxxxxx conso(RMC8D)
-- ^                           lrb(MC6)
-- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx conso(RMC6D)

select emaj.emaj_set_mark_group('myGroup1','MC6');
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (2000,2012) as i;
insert into myTbl2 values (3000,'TC6',NULL);
delete from myTbl1 where col11 > 2010;

select emaj.emaj_set_mark_group('myGroup1','MC7');
update myTbl2 set col22 = 'TC7' WHERE col22 ='TC6';

select emaj.emaj_set_mark_group('myGroup1','MC8');
insert into myTbl2 values (3001,'TC8',NULL);

select * from emaj.emaj_logged_rollback_group('myGroup1','MC8',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','EMAJ_LAST_MARK');

select emaj.emaj_set_mark_group('myGroup1','MC9');
insert into "myTbl3" (col33) select generate_series(2000,2039,4)/100;
insert into myTbl4 values (2000,'FK...',1,10,'ABC');
update myTbl4 set col43 = NULL where col41 = 2000;

select * from emaj.emaj_logged_rollback_group('myGroup1','MC6',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','EMAJ_LAST_MARK');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup1','Multi-1',NULL);

-- consolidation of 2 logged rollbacks referencing the same mark
-- MC10  RMC10S RMC10D M11 RMC10S RMC10D
-- ^     lrg(MC10)
-- ^                       lrb(MC10)
-- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx conso(RMC10D)

select emaj.emaj_set_mark_group('myGroup1','MC10');
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (3000,3010) as i;
delete from myTbl1 where col11 > 3005;

select * from emaj.emaj_logged_rollback_group('myGroup1','MC10',false) order by 1,2;
update myTbl2 set col22 = 'TC7' WHERE col22 ='TC6';

select emaj.emaj_set_mark_group('myGroup1','MC11');
insert into myTbl4 values (3000,'FK...',1,10,'ABC');
update myTbl4 set col43 = NULL where col41 = 3000;

select * from emaj.emaj_logged_rollback_group('myGroup1','MC10',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','EMAJ_LAST_MARK');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup1','Multi-1',NULL);

-- consolidation of 1 from 2 overlapping logged rollbacks
-- MC15 MC16 RMC15S RMC15D MC17 RMC16S RMC16D
-- ^         lrg(MC15)
--      ^                       lrg(MC16)
-- xxxxxxxxxxxxxxxxx conso(RMC15D)

select emaj.emaj_set_mark_group('myGroup1','MC15');
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (4000,4012) as i;
insert into myTbl2 values (4000,'TC15',NULL);
delete from myTbl1 where col11 > 4010;

select emaj.emaj_set_mark_group('myGroup1','MC16');
update myTbl2 set col22 = 'TC16' WHERE col22 ='TC15';

select * from emaj.emaj_logged_rollback_group('myGroup1','MC15',false) order by 1,2;
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','RLBK_MC15_DONE');

select emaj.emaj_set_mark_group('myGroup1','MC17');
insert into myTbl2 values (4001,'TC15',NULL);

select * from emaj.emaj_logged_rollback_group('myGroup1','MC16',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','RLBK_MC15_DONE');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup1','Multi-1',NULL);
select * from myTbl1 order by col11;
select * from myTbl2 order by col21;

select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence where sequ_schema = 'emaj_myschema1' order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole where sqhl_schema = 'myschema1' order by 1,2,3;

select * from emaj.emaj_rollback_group('myGroup1','Multi-1',false) order by 1,2;

-----------------------------
-- Step 16 : test transactions with several emaj operations
-----------------------------
select emaj.emaj_create_group('myGroup4');

-- several similar operations in a single transaction, using different mark names
begin;
  select emaj.emaj_start_group('myGroup4','M1');
  select emaj.emaj_set_mark_group('myGroup4','M2');
  select emaj.emaj_set_mark_group('myGroup4','M3');
  select emaj.emaj_alter_group('myGroup4','M4');
  select emaj.emaj_alter_group('myGroup4','M5');
  select emaj.emaj_stop_group('myGroup4','M6');
  select emaj.emaj_start_group('myGroup4','M7',false);
  select emaj.emaj_stop_group('myGroup4','M8');
commit;
select * from emaj.emaj_mark where mark_group = 'myGroup4' order by mark_time_id, mark_group;

-----------------------------
-- Step 17 : test partition attach and detach
-----------------------------
-- Needs postgres 10+

set role emaj_regression_tests_adm_user;
select emaj.emaj_start_group('myGroup4','Start');
insert into mySchema4.myTblP values (-1,'Stored in partition 1'), (1,'Stored in partition 2');
select emaj.emaj_set_mark_group('myGroup4','M1');
update mySchema4.myTblP set col1 = 2 where col1 = 1;

-- create a new partition and add it into the group ; in passing also add the sequence linked to the serial column of the mother table
reset role;
CREATE TABLE mySchema4.myPartP3 PARTITION OF mySchema4.myTblP (PRIMARY KEY (col1)) FOR VALUES FROM (10) TO (19);
-- create the table with PG 9.6- so that next scripts do not abort
CREATE TABLE IF NOT EXISTS mySchema4.myPartP3 (PRIMARY KEY (col1)) INHERITS (mySchema4.myTblP);
grant all on mySchema4.myPartP3 to emaj_regression_tests_adm_user;

set role emaj_regression_tests_adm_user;
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mypartp3');
insert into emaj.emaj_group_def values ('myGroup4','myschema4','mytblp_col3_seq');
select emaj.emaj_alter_group('myGroup4','Add partition 3');
insert into mySchema4.myTblP values (11,'Stored in partition 3');

-- remove an obsolete partition ; in passing also remove the sequence linke to the serial column of the mother table
delete from emaj.emaj_group_def where grpdef_schema = 'myschema4' and grpdef_tblseq = 'mypartp1';
delete from emaj.emaj_group_def where grpdef_schema = 'myschema4' and grpdef_tblseq = 'mytblp_col3_seq';
select emaj.emaj_alter_group('myGroup4','Remove partition 1');

reset role;
drop table mySchema4.myPartP1;
set role emaj_regression_tests_adm_user;

-- verify that emaj_adm has the proper grants to delete old marks leading to an old log table drop
begin transaction;
  select emaj.emaj_delete_before_mark_group('myGroup4','EMAJ_LAST_MARK');
rollback;

-- look at statistics and log content
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup4',NULL, NULL) order by 1,2,3,4;
select * from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq like 'mypar%' order by rel_tblseq, rel_time_range;
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP3_log order by emaj_gid;
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP1_log_1 order by emaj_gid;

-- rollback to a mark set before the first changes
select * from emaj.emaj_rollback_group('myGroup4','Start');
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup4','Start',true);

-- testing a row update leading to a partition change (needs pg 11)
insert into mySchema4.myTblP values (1,'Initialy stored in partition 2'), (11,'Stored in partition 3');
select emaj.emaj_set_mark_group('myGroup4','Before update');
update mySchema4.myTblP set col1 = 12 where col1=1;
select emaj.emaj_logged_rollback_group('myGroup4','Before update');
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP2_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP3_log;

select emaj.emaj_stop_group('myGroup4');
select emaj.emaj_drop_group('myGroup4');

-----------------------------
-- Step 18 : test defect with application table or sequence
--           also test some changes on the unlogged and the with oids tables
-----------------------------
update emaj.emaj_group_def set grpdef_group = 'phil''s group#3",' where grpdef_schema = 'myschema5';
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
select emaj.emaj_create_group('phil''s group#3",',false);
select emaj.emaj_start_group('phil''s group#3",','start');

-----------------------------
-- test changes on the unlogged and the with oids tables
-----------------------------
insert into myschema5.myUnloggedTbl values (10),(11),(12);
update myschema5.myUnloggedTbl set col1 = 13 where col1 = 12;
delete from myschema5.myUnloggedTbl where col1 = 10;
insert into myschema5.myOidsTbl values (20),(21),(22);
update myschema5.myOidsTbl set col1 = 23 where col1 = 22;
delete from myschema5.myOidsTbl where col1 = 21;

select col1, emaj_verb, emaj_tuple, emaj_gid, emaj_user from emaj_myschema5.myUnloggedTbl_log order by emaj_gid;
select col1, emaj_verb, emaj_tuple, emaj_gid, emaj_user from emaj_myschema5.myOidsTbl_log order by emaj_gid;


-- disable event triggers for this step and change an application table structure
select emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- test remove_and_add operations to repair an application table
-----------------------------
reset role;
alter table "phil's schema3".mytbl4 alter column col45 type char(11);
set role emaj_regression_tests_adm_user;

select * from emaj.emaj_verify_all();

delete from emaj.emaj_group_def where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'mytbl4';
select emaj.emaj_alter_group('phil''s group#3",','remove_the_damaged_table');

insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
select emaj.emaj_alter_group('phil''s group#3",','re_add_the_table');

select * from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'mytbl4' order by rel_time_range;

-----------------------------
-- test a remove operation to fix the case of a deleted log table, log sequence or log function
-----------------------------
reset role;
drop table "emaj_phil's schema3".mytbl4_log;
set role emaj_regression_tests_adm_user;
select * from emaj.emaj_verify_all();

delete from emaj.emaj_group_def where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'mytbl4';
select emaj.emaj_alter_group('phil''s group#3",','remove_the_damaged_table_2');

insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
select emaj.emaj_alter_group('phil''s group#3",','re_add_the_table_2');

select * from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'mytbl4' order by rel_time_range;

-----------------------------
-- test a remove operation to fix the case of a deleted application table or sequence
-----------------------------
-- in fact just rename the table and the sequence
reset role;
alter table "phil's schema3".mytbl4 rename to mytbl4_sav;
alter sequence "phil's schema3"."phil's seq\1" rename to "phil's seq\1_sav";

set role emaj_regression_tests_adm_user;

-- try to set a mark or alter the group
begin;
  select emaj.emaj_set_mark_group('phil''s group#3",','should fails');
rollback;
begin;
  select emaj.emaj_alter_group('phil''s group#3",','should also fails');
rollback;

delete from emaj.emaj_group_def where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'mytbl4';
delete from emaj.emaj_group_def where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'phil''s seq\1';
select emaj.emaj_alter_group('phil''s group#3",','remove_the_dropped_table_and_seq');

-- check that the log table does not exist anymore
select count(*) from emaj."phil's schema3_mytbl4_log";

-- revert the changes
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','phil''s seq\1');
reset role;
alter table "phil's schema3".mytbl4_sav rename to mytbl4;
alter table "phil's schema3".mytbl4 alter column col45 type char(10);
alter sequence "phil's schema3"."phil's seq\1_sav" rename to "phil's seq\1";
select emaj.emaj_alter_group('phil''s group#3",','revert_last_changes');

-- ree-nable the event triggers and drop the group
set role emaj_regression_tests_adm_user;
select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_stop_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

-----------------------------
-- Step 19 : test use of dynamic tables group management (assign, move, remove, change)
-----------------------------

-- create, start and populate groups
-- grp_tmp is empty, grp_tmp_3 and grp_tmp_4 contains tables and sequences frop respectively phil''s schema3 and myschema4
-- grp_tmp_4 is started before being populated
select emaj.emaj_create_group('grp_tmp',true,true);
select emaj.emaj_create_group('grp_tmp_3',true,true);
select emaj.emaj_create_group('grp_tmp_4',true,true);
select emaj.emaj_start_groups('{"grp_tmp","grp_tmp_4"}','Start');
begin;
  select emaj.emaj_assign_tables('phil''s schema3','.*','','grp_tmp_3');
  select emaj.emaj_assign_sequences('phil''s schema3','.*','','grp_tmp_3');
  select emaj.emaj_assign_tables('myschema4','.*','','grp_tmp_4');
  select emaj.emaj_assign_sequences('myschema4','.*','','grp_tmp_4');
  select emaj.emaj_ignore_app_trigger('ADD','myschema4','mytblm','mytblm_insert_trigger');
commit;
select emaj.emaj_start_group('grp_tmp_3','Start');
select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1');

-- perform some changes and set marks
insert into "phil's schema3".mytbl4 (col41)
  select i from generate_series(3,8) i;
delete from "phil's schema3"."myTbl2\";
insert into "phil's schema3"."myTbl2\" (col22,col23)
  select 'After Mk1','12-31-2020' from generate_series(1,3);

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk2');

insert into "phil's schema3"."myTbl2\" (col22,col23)
  select 'After Mk2','12-31-2030' from generate_series(1,3);
select nextval(E'"phil''s schema3"."phil''s seq\\1"');
insert into myschema4.mytblm
  select '2006-06-30'::date + ('1 year'::interval) * i, i, 'After Mk2'
    from generate_series(0,9) i;

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3');

delete from myschema4.mytblm
  where col1 = '2006-06-30';
update myschema4.mytblm set col3 = 'After Mk2 and updated after Mk3'
  where col1 > '2013-01-01';

-- rollback to the previous mark (old syntax)
select emaj.emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3');

-- change some priority and log tablespaces
select emaj.emaj_modify_tables('phil''s schema3','.*tbl1','','{"priority":-1,"log_data_tablespace":"tsp log''2"}'::jsonb,'Modify 1 table');

-- move all tables and sequences into grp_tmp and set a common mark
select emaj.emaj_move_tables('phil''s schema3','.*','','grp_tmp','Move_tbl_3_to_tmp');
select emaj.emaj_move_sequences('phil''s schema3','.*','','grp_tmp','Move_seq_3_to_tmp');
select emaj.emaj_move_tables('myschema4','.*','','grp_tmp','Move_tbl_4_to_tmp');
select emaj.emaj_move_sequences('myschema4','.*','','grp_tmp','Move_seq_4_to_tmp');

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk4');

-- perform some other changes and set marks
update "phil's schema3".mytbl4 set col42 = 'Updated after Mk4'
  where col41 > 5;
delete from "phil's schema3"."myTbl2\"
  where col21 = 4;
delete from "phil's schema3".mytbl4
  where col41 = 4;

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk5');

insert into myschema4.mytblm
  select '2017-06-30'::date + ('1 year'::interval) * i, 5, 'After Mk5'
    from generate_series(0,3) i;
select nextval(E'"phil''s schema3"."phil''s seq\\1"');

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk6');

update myschema4.mytblm set col3 = 'After Mk5 and updated after Mk6'
  where col1 > '2017-01-01';

-- remove the table mytblm and the sequence phil's seq\1
select emaj.emaj_remove_table('myschema4','mytblc1','Remove_mytblc1');
select emaj.emaj_remove_sequence('phil''s schema3','phil''s seq\1','Remove_myseq1');

-- logged rollback to Mk5
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk5', true);
select emaj.emaj_rename_mark_group(group_name,'EMAJ_LAST_MARK','End_logged_rollback')
  from (values ('grp_tmp_3'),('grp_tmp_4'),('grp_tmp')) as t(group_name);

-- perform some other changes and set marks
delete from myschema4.mytblm
  where col1 = '2018-06-30';

select emaj.emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk7');

delete from mySchema4.mytblm;

-- consolidate the logged rollback
select * from emaj.emaj_get_consolidable_rollbacks() order by 1,2;
select emaj.emaj_consolidate_rollback_group('grp_tmp','End_logged_rollback');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null)
  order by stat_first_mark_datetime, stat_schema, stat_table;
select mark_time_id, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_group,
       mark_is_deleted, mark_log_rows_before_next
  from emaj.emaj_mark where mark_group in ('grp_tmp_3','grp_tmp_4','grp_tmp')
  order by 1,2,3;

-- generate sql script
\! rm -Rf /tmp/emaj_test/sql_scripts
\! mkdir /tmp/emaj_test/sql_scripts
select emaj.emaj_gen_sql_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null,'/tmp/emaj_test/sql_scripts/allGroups.sql');
--  \! grep -iP '(insert|update|delete|alter)' /tmp/emaj_test/sql_scripts/allGroups.sql
\! rm -Rf /tmp/emaj_test/sql_scripts

-- revert the priority and log tablespaces changes
select emaj.emaj_modify_tables('phil''s schema3','.*tbl1','','{"priority":null,"log_data_tablespace":null}'::jsonb,'revert changes for 1 table');

-- rollback to a mark set before the tables and sequences move
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3', false);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3',true);

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null)
  order by stat_first_mark_datetime, stat_schema, stat_table;

-- delete all marks before Mk3
select emaj.emaj_delete_before_mark_group(group_name,'Mk3')
  from (values ('grp_tmp_3'),('grp_tmp_4'),('grp_tmp')) as t(group_name);

-- test a remove_table following log sequence deletion
select emaj.emaj_disable_protection_by_event_triggers();
reset role;
drop sequence "emaj_phil's schema3".mytbl4_log_seq;
set role emaj_regression_tests_adm_user;
select emaj.emaj_enable_protection_by_event_triggers();
select * from emaj.emaj_verify_all();
--     a removal while the group is LOGGING fails
select emaj.emaj_remove_table('phil''s schema3','mytbl4');
select emaj.emaj_force_stop_group('grp_tmp');
select emaj.emaj_remove_table('phil''s schema3','mytbl4');
select emaj.emaj_assign_table('phil''s schema3','mytbl4','grp_tmp');
select * from emaj.emaj_verify_all();

----  select * from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq = 'mytblc1' order by rel_time_range;
----  select emaj.emaj_reset_group('grp_tmp');
----  select * from emaj.emaj_verify_all();
----  select * from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq = 'mytblc1' order by rel_time_range;
----select emaj.emaj_start_group('grp_tmp','Group restart');

-- reset groups at their initial state
select emaj.emaj_stop_group('grp_tmp_3');
select emaj.emaj_drop_group('grp_tmp_3');
select emaj.emaj_set_mark_group('grp_tmp_4','Last_mark');
select emaj.emaj_force_drop_group('grp_tmp_4');
select emaj.emaj_stop_group('grp_tmp');
select emaj.emaj_drop_group('grp_tmp');

select * from emaj.emaj_rel_hist order by 1,2,3;
select * from emaj.emaj_relation where rel_schema in ('phil''s schema3','myschema4');

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
-- first set all rollback events state
select emaj.emaj_cleanup_rollback_state();

-- check rollback related tables
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_nb_table,
       rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
       case when rlbk_end_datetime is null then 'null' else '[ts]' end as "end_datetime",
       regexp_replace(array_to_string(rlbk_messages,'#'),E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rlbk where rlbk_id >= 10000 order by rlbk_id;
select rlbs_rlbk_id, rlbs_session, 
       case when rlbs_end_datetime is null then 'null' else '[ts]' end as "end_datetime"
  from emaj.emaj_rlbk_session where rlbs_rlbk_id >= 10000  order by rlbs_rlbk_id, rlbs_session;
select rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_object_def, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  from emaj.emaj_rlbk_plan where rlbp_rlbk_id >= 10000  order by rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object;
select rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id, rlbt_quantity
  from emaj.emaj_rlbk_stat where rlbt_rlbk_id >= 10000 order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 10000 order by time_id;
select hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist order by hist_id;
--
reset role;

-- the groups are left in their current state for the parallel rollback test.
select count(*) from mySchema1.myTbl4;
select count(*) from mySchema1.myTbl1;
select count(*) from mySchema1.myTbl2; 
select count(*) from mySchema1."myTbl3";
select count(*) from mySchema1.myTbl2b;
select count(*) from mySchema2.myTbl4;
select count(*) from mySchema2.myTbl1;
select count(*) from mySchema2.myTbl2; 
select count(*) from mySchema2."myTbl3";
select count(*) from mySchema2.myTbl5;
select count(*) from mySchema2.myTbl6;
