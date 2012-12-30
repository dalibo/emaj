-- adm2.sql : complex scenario executed by an emaj_adm role. 
--            Follows adm1.sql, and includes more specific test cases
--
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
select emaj.emaj_logged_rollback_groups(array['myGroup1','myGroup2'],'Multi-1');

-- rollback to Multi-2
select emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-2');

-- rollback and stop to Multi-1
select emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-1');
select emaj.emaj_stop_groups(array['myGroup1','myGroup2'],'Stop after rollback');

-- try to start both groups, but with an old deleted mark name
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- really start both groups
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1b', false);

-- set again a mark for both groups
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');

-- delete the mark for only 1 group and get detailed statistics for the other group
select emaj.emaj_delete_mark_group('myGroup1','Multi-2');

-- use this mark for the other group before delete it
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Multi-2',NULL);
select emaj.emaj_rollback_group('myGroup2','Multi-2');
select emaj.emaj_delete_mark_group('myGroup2','Multi-2');

-- get statistics using deleted marks
select * from emaj.emaj_detailed_log_stat_group('myGroup2','M1','M2');

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
             (select mark_datetime from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'M3')+'0.000001 SECOND'::interval)));

-- comment a deleted mark
select emaj.emaj_comment_mark_group('myGroup2','M3','This mark is deleted');

-- try to get rollback duration on a deleted mark
select emaj.emaj_estimate_rollback_duration('myGroup2','M3');

-- try to rollback on a deleted mark
select emaj.emaj_rollback_group('myGroup2','M3');

-----------------------------
-- Checking step 8
-----------------------------
-- emaj tables
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
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
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl6_log order by emaj_gid, emaj_tuple desc;

-----------------------------
-- Step 9 : test the emaj_alter_group function with non rollbackable phil's group#3, group
-----------------------------
set role emaj_regression_tests_adm_user;
select emaj.emaj_create_group('phil''s group#3",',false);

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(11);

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_priority = 1 where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
select emaj.emaj_alter_group('phil''s group#3",');
--select rel_priority from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'phil''s tbl1';
select emaj.emaj_start_group('phil''s group#3",','M1_after_alter_group');
select emaj.emaj_stop_group('phil''s group#3",');

reset role;
alter table "phil's schema3"."phil's tbl1" alter column "phil's col12" type char(10);

set role emaj_regression_tests_adm_user;
update emaj.emaj_group_def set grpdef_priority = NULL where grpdef_schema = 'phil''s schema3' and grpdef_tblseq = E'myTbl2\\';
select emaj.emaj_alter_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

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
set search_path="phil's schema3";
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
select * from emaj.emaj_log_stat_group('phil''s group#3",','','');
select * from emaj.emaj_detailed_log_stat_group('phil''s group#3",','phil''s mark #3','');
--
select emaj.emaj_logged_rollback_group('phil''s group#3",','phil''s mark #3');
select emaj.emaj_rollback_group('phil''s group#3",','phil''s mark #3');

-----------------------------
-- Checking step 10
-----------------------------
-- emaj tables
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select * from emaj.emaj_fk order by fk_groups, fk_session, fk_name;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_nb_rows from emaj.emaj_rlbk_stat
  order by rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_operation;
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
set search_path=myschema1;
--
begin transaction;
  delete from mytbl1;
rollback;
--
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');

-----------------------------
-- Checking step 11
-----------------------------
-- emaj tables
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_nb_rows from emaj.emaj_rlbk_stat
  order by rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_operation;

-----------------------------
-- Step 12 : tests snaps and script generation functions
-----------------------------
-- first add some updates for tables with unusual types (arrays, geometric)
set search_path=myschema2;
insert into myTbl5 values (10,'{"abc","def","ghi"}','{1,2,3}',NULL);
insert into myTbl5 values (20,array['abc','def','ghi'],array[3,4,5],array['2000/02/01'::date,'2000/02/28'::date]);
update myTbl5 set col54 = '{"2010/11/28","2010/12/03"}' where col54 is null;
insert into myTbl6 select i+10, point(i,1.3), '((0,0),(2,2))', circle(point(5,5),i),'((-2,-2),(3,0),(1,4))','10.20.30.40/27' from generate_series (1,8) as i;
update myTbl6 set col64 = '<(5,6),3.5>', col65 = null where col61 <= 13;

-- reset directory for snaps
\! rm -R /tmp/emaj_test/snaps
\! mkdir /tmp/emaj_test/snaps
-- ... and snap the all groups
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','CSV HEADER');
select emaj.emaj_snap_group('myGroup2','/tmp/emaj_test/snaps','CSV HEADER');
select emaj.emaj_snap_group('phil''s group#3",','/tmp/emaj_test/snaps','CSV HEADER');

\! ls /tmp/emaj_test/snaps

-- reset directory for emaj_gen_sql_group tests
\! rm -R /tmp/emaj_test/sql_scripts
\! mkdir /tmp/emaj_test/sql_scripts

--select * from emaj.emaj_mark order by mark_group, mark_id;
-- generate sql script for each active group (and check the result with detailed log statistics + number of sequences)
select emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myGroup1.sql');
select coalesce(sum(stat_rows),0) + 1 as check from emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myGroup2.sql');
select sum(stat_rows) + 2 as check from emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('phil''s group#3",', 'M1_rollbackable', NULL, '/tmp/emaj_test/sql_scripts/Group3.sql');
select sum(stat_rows) + 1 as check from emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'M1_rollbackable', NULL);

-- process \\ in script files
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i_s -s 's/\\\\/\\/g'
-- comment transaction commands for the need of the current test
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
-- mask timestamp in initial comment
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .* by/at [ts] by/'

\! ls /tmp/emaj_test/sql_scripts

-- reset directory for second set of snaps
\! rm -R /tmp/emaj_test/snaps2
\! mkdir /tmp/emaj_test/snaps2
-- in a single transaction and as superuser:
--   rollback groups, replay updates with generated scripts, snap groups again and cancel the transaction
reset role;
begin;
  select emaj.emaj_rollback_group('myGroup1','Multi-1');
  select emaj.emaj_rollback_group('myGroup2','Multi-1');
  select emaj.emaj_rollback_group('phil''s group#3",','M1_rollbackable');

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
-- test end: check, reset history and force sequences id
-----------------------------
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from emaj.emaj_hist order by hist_id;
--
reset role;
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 10000;
alter sequence emaj.emaj_mark_mark_id_seq restart 1000;
alter sequence emaj.emaj_sequence_sequ_id_seq restart 1000;
alter sequence emaj.emaj_seq_hole_sqhl_id_seq restart 1000;

-- the groups are left in their current state for the parallel rollback test.
-- perform some updates to prepare the parallel rollback test
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
delete from mySchema1.myTbl4;
delete from mySchema1.myTbl1;
delete from mySchema1.myTbl2; 
delete from mySchema1."myTbl3";
delete from mySchema1.myTbl2b;
delete from mySchema2.myTbl4;
delete from mySchema2.myTbl1;
delete from mySchema2.myTbl2; 
delete from mySchema2."myTbl3";
delete from mySchema2.myTbl5;
delete from mySchema2.myTbl6 where col61 <> 0;
alter sequence mySchema2.mySeq1 restart 9999;

