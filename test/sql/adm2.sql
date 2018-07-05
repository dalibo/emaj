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
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_id;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.mySchema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj."otherPrefix4mytbl5_log" order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl6_log order by emaj_gid, emaj_tuple desc;

-----------------------------
-- Step 9 : test the emaj_alter_group() and emaj_alter_groups() functions with the audit-only phil's group#3, group and a mix of rollbackable and audit-only groups
-----------------------------
select emaj.emaj_create_group('phil''s group#3",',false);

-- disable event triggers for this step to allow an application table structure change
select emaj.emaj_disable_protection_by_event_triggers();

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(11);

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_priority = 1, grpdef_log_schema_suffix = '3', grpdef_emaj_names_prefix = 'phil_mytbl2'
  where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
select emaj.emaj_alter_group('phil''s group#3",');
select emaj.emaj_start_group('phil''s group#3",','M1_after_alter_group');
select emaj.emaj_stop_group('phil''s group#3",');

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(10);

set role emaj_regression_tests_adm_user;

select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_create_group('myGroup4');

update emaj.emaj_group_def set grpdef_priority = NULL, grpdef_log_schema_suffix = NULL, grpdef_emaj_names_prefix = NULL
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
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_id;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey;
-- user tables
select * from "phil's schema3"."phil's tbl1" order by "phil's col11","phil's col12";
select * from "phil's schema3"."myTbl2\" order by col21;
-- log tables
select "phil's col11", "phil's col12", "phil\s col13", emaj_verb, emaj_tuple, emaj_gid from "emaj #'3"."phil's schema3_phil's tbl1_log" order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj."phil's schema3_myTbl2\_log" order by emaj_gid, emaj_tuple desc;

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
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey;

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

-- reset directory for snaps
\! rm -Rf /tmp/emaj_test/snaps
\! mkdir /tmp/emaj_test/snaps
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
select coalesce(sum(stat_rows),0) + 1 as check from emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myGroup2.sql', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1','myschema2.myTbl3_col31_seq']);
select sum(stat_rows) + 2 as check from emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('phil''s group#3",', 'M1_rollbackable', NULL, '/tmp/emaj_test/sql_scripts/Group3.sql');
select sum(stat_rows) + 1 as check from emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'M1_rollbackable', NULL);

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
-- Step 12 : test use of a table with a very long name (63 characters long)
-----------------------------
select emaj.emaj_stop_group('phil''s group#3",');

-- rename the "phil's tbl1" table and alter its group
reset role;
alter table "phil's schema3"."phil's tbl1" rename to table_with_very_looooooooooooooooooooooooooooooooooooooong_name;

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_tblseq = 'table_with_very_looooooooooooooooooooooooooooooooooooooong_name', grpdef_emaj_names_prefix = 'short' where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'phil''s tbl1';
select emaj.emaj_alter_group('phil''s group#3",');

-- use the table and its group
select emaj.emaj_start_group('phil''s group#3",','M1_after_alter_group');

update "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name set "phil's col12" = 'GHI' where "phil's col11" between 6 and 9;
select emaj.emaj_set_mark_group('phil''s group#3",','M2');
delete from "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name where "phil's col11" > 18;

select * from emaj.emaj_rollback_group('phil''s group#3",','M1_after_alter_group',false) order by 1,2;
select emaj.emaj_stop_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

-----------------------------
-- Step 13 : test use of groups or marks protection
-----------------------------
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
-- Step 14 : test complex use of rollbacks consolidations
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

select cons_group, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), 
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_id, cons_target_rlbk_mark_id, cons_rows, cons_marks, cons_marks
  from emaj.emaj_get_consolidable_rollbacks();

-- consolidate both logged rollback at once
select cons_target_rlbk_mark_name, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), 
       emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark_name)
  from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup1';

select cons_group, regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), 
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_id, cons_target_rlbk_mark_id, cons_rows, cons_marks
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

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_id;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence where sequ_name like 'myschema1%' order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole where sqhl_schema = 'myschema1' order by 1,2,3;

select * from emaj.emaj_rollback_group('myGroup1','Multi-1',false) order by 1,2;

-----------------------------
-- Step 15 : test transactions with several emaj operations
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
select * from emaj.emaj_mark where mark_group = 'myGroup4' order by mark_id;

-----------------------------
-- Step 16 : test partition attach and detach
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

-- look at statistics and log content
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup4',NULL, NULL) order by 1,2,3,4;
select * from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq like 'mypar%' order by rel_tblseq, rel_time_range;
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mypartP3_log order by emaj_gid;
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mypartP1_log_1 order by emaj_gid;

-- rollback to a mark set before the first changes
select * from emaj.emaj_rollback_group('myGroup4','Start');
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup4','Start',true);

-- testing a row update leading to a partition change (needs pg 11)
insert into mySchema4.myTblP values (1,'Initialy stored in partition 2'), (11,'Stored in partition 3');
select emaj.emaj_set_mark_group('myGroup4','Before update');
update mySchema4.myTblP set col1 = 12 where col1=1;
select emaj.emaj_logged_rollback_group('myGroup4','Before update');
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mypartP2_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mypartP3_log;

select emaj.emaj_stop_group('myGroup4');
select emaj.emaj_drop_group('myGroup4');

-----------------------------
-- Step 17 : test defect with application table or sequence
-----------------------------
insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
select emaj.emaj_create_group('phil''s group#3",',false);
select emaj.emaj_start_group('phil''s group#3",','start');

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

select * from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'mytbl4';

-----------------------------
-- test a remove operation to fix the case of a deleted log table, log sequence or log function
-----------------------------
reset role;
drop table emaj."phil's schema3_mytbl4_log";
set role emaj_regression_tests_adm_user;
select * from emaj.emaj_verify_all();

delete from emaj.emaj_group_def where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = 'mytbl4';
select emaj.emaj_alter_group('phil''s group#3",','remove_the_damaged_table_2');

insert into emaj.emaj_group_def values ('phil''s group#3",','phil''s schema3','mytbl4');
select emaj.emaj_alter_group('phil''s group#3",','re_add_the_table_2');

select * from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'mytbl4';

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

-- ree-nable the event triggers and drop the group
set role emaj_regression_tests_adm_user;
select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_stop_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
-- first set all rollback events state
select emaj.emaj_cleanup_rollback_state();

-- check rollback related tables
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, 
       rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id, rlbk_is_dblink_used,
       case when rlbk_end_datetime is null then 'null' else '[ts]' end as "end_datetime",
       regexp_replace(array_to_string(rlbk_messages,'#'),E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rlbk where rlbk_id >= 10000 order by rlbk_id;
select rlbs_rlbk_id, rlbs_session, 
       case when rlbs_end_datetime is null then 'null' else '[ts]' end as "end_datetime"
  from emaj.emaj_rlbk_session where rlbs_rlbk_id >= 10000  order by rlbs_rlbk_id, rlbs_session;
select rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_fkey_def, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  from emaj.emaj_rlbk_plan where rlbp_rlbk_id >= 10000  order by rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey;
select rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey, rlbt_rlbk_id, rlbt_quantity
  from emaj.emaj_rlbk_stat where rlbt_rlbk_id >= 10000 order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey;

select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 10000 order by time_id;
select hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist order by hist_id;
--
reset role;
alter table "phil's schema3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name rename to "phil's tbl1";

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
