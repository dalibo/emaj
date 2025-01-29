-- adm2.sql : complex scenario executed by an emaj_adm role. 
--            Follows adm1.sql, and includes more specific test cases
--

-- set sequence restart value
select public.handle_emaj_sequences(14000);

-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/adm2'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

set role emaj_regression_tests_adm_user1;

-- before going on, save and reload parameters
select emaj.emaj_import_parameters_configuration(emaj.emaj_export_parameters_configuration());

-----------------------------
-- Step 8 : use of multi-group functions, start_group(s) without log reset and use old marks (i.e. set before the latest group start)
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

-- try to start both groups, but with an old mark name
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- really start both groups
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Multi-1b', false);

-- try to rollback several groups to an old common mark
select * from emaj.emaj_rollback_groups(array['myGroup1','myGroup2'],'Multi-1', false);

-- set again a mark for both groups
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');

-- delete the mark for only 1 group and get detailed statistics for the other group
select emaj.emaj_delete_mark_group('myGroup1','Multi-2');

-- use this mark for the other group before delete it
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Multi-2',NULL);
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2', 'Multi-2', NULL)
  order by stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
select * from emaj.emaj_rollback_group('myGroup2','Multi-2',false) order by 1,2;
select emaj.emaj_delete_mark_group('myGroup2','Multi-2');

-- get statistics using old marks
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','M1','M2');
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2', 'M1', 'M2')
  order by stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;

-- delete intermediate old marks
select emaj.emaj_delete_mark_group('myGroup1','Multi-1');
select emaj.emaj_delete_mark_group('myGroup2','Multi-1');
-- ... and reuse mark names for parallel rollback test
select emaj.emaj_rename_mark_group('myGroup1','Multi-1b','Multi-1');
select emaj.emaj_rename_mark_group('myGroup2','Multi-1b','Multi-1');

-- rename an old mark
select emaj.emaj_rename_mark_group('myGroup2','M2','Deleted M2');

-- use emaj_get_previous_mark_group and delete an initial old mark
select emaj.emaj_delete_before_mark_group('myGroup2',
      (select emaj.emaj_get_previous_mark_group('myGroup2',
             (select time_clock_timestamp from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup2' and mark_group = 'myGroup2' and mark_name = 'M3')+'0.000001 SECOND'::interval)));

-- comment an old mark
select emaj.emaj_comment_mark_group('myGroup2','M3','This mark is old');

-- try to get a rollback duration estimate on an old mark
select emaj.emaj_estimate_rollback_group('myGroup2','M3',TRUE);

-- try to rollback on an old mark
select * from emaj.emaj_rollback_group('myGroup2','M3',false) order by 1,2;

-----------------------------
-- Checking step 8
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14000 order by hist_id;

-- log tables
reset role;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, col55, col56, col57, col58, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl6_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(14200);

-----------------------------
-- Step 9 : for phil''s group#3", recreate the group as rollbackable, update tables, 
--          rename a mark, then delete 2 marks then delete all before a mark 
-----------------------------
-- prepare phil's group#3, group
--
reset role;
alter table "phil's schema""3"."myTbl2\" add primary key (col21);
set role emaj_regression_tests_adm_user2;

select emaj.emaj_create_group('phil''s group#3",');
select emaj.emaj_assign_tables('phil''s schema"3','.*','my"tbl4','phil''s group#3",');
select emaj.emaj_assign_sequences('phil''s schema"3','.*',null,'phil''s group#3",');

select emaj.emaj_start_group('phil''s group#3",','M1_rollbackable');
--
reset role;
set search_path=public,"phil's schema""3";
--
insert into "phil's tbl1" select i, 'AB''C', E'\\014'::bytea from generate_series (1,31) as i;
update "phil's tbl1" set "phil\s""col13" = E'\\034'::bytea where "phil's col11" <= 3;
update "phil's tbl1" set "phil\s""col13" = E'\\034'''::bytea where "phil's col11" between 18 and 22;
insert into "my""tbl4" (col41) values (1);
insert into "my""tbl4" (col41) values (2);
insert into "myTbl2\" values (1,'ABC','2010-12-31');
delete from "phil's tbl1" where "phil's col11" > 20;
insert into "myTbl2\" values (2,'DEF',NULL);
select nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
--

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('phil''s group#3",','M2_rollbackable');
select emaj.emaj_set_mark_group('phil''s group#3",','M2_again!');
--
reset role;
delete from "phil's tbl1" where "phil's col11" = 10;
update "phil's tbl1" set "phil's col12" = 'DEF' where "phil's col11" <= 2;

select nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
--
set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_groups(array['phil''s group#3",'],'phil''s mark #1','Third mark set');
--
select emaj.emaj_rename_mark_group('phil''s group#3",','phil''s mark #1','phil''s mark #3');
-- 
select emaj.emaj_delete_mark_group('phil''s group#3",','M2_again!');
--
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('phil''s group#3",','M1_rollbackable','');
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('phil''s group#3",','phil''s mark #3','');
--
select * from emaj.emaj_logged_rollback_group('phil''s group#3",','phil''s mark #3',false) order by 1,2;
select * from emaj.emaj_rollback_group('phil''s group#3",','phil''s mark #3',false) order by 1,2;

-----------------------------
-- Checking step 9
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14200 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14200 order by hist_id;

-- user tables
reset role;
select * from "phil's schema""3"."phil's tbl1" order by "phil's col11","phil's col12";
select * from "phil's schema""3"."myTbl2\" order by col21;
-- log tables
set role emaj_regression_tests_adm_user2;
select "phil's col11", "phil's col12", "phil\s""col13", emaj_verb, emaj_tuple, emaj_gid from "emaj_phil's schema""3"."phil's tbl1_log" order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from "emaj_phil's schema""3"."myTbl2\_log" order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(14300);

-----------------------------
-- Step 10 : for myGroup1, in a transaction, update tables and rollback the transaction, 
--           then rollback to previous mark 
-----------------------------
reset role;
set search_path=public,myschema1;
--
begin transaction;
  delete from mytbl1;
rollback;
--

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK',false) order by 1,2;

-----------------------------
-- Checking step 10
-----------------------------
-- emaj tables
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14300 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14300 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(14400);

-----------------------------
-- Step 11 : tests snaps and script generation functions
-----------------------------
-- first perform changes in a table with generated columns
reset role;
set search_path=public,myschema1;
insert into mytbl2b (col21) values (10),(11);
update mytbl2b set col21 = 12 where col21 = 11;
delete from mytbl2b where col21 >= 10;

-- add some updates for tables with unusual types (arrays, geometric)
set search_path=public,myschema2;
insert into myTbl5 values (10,'{"abc","def","ghi"}','{1,2,3}',NULL,'{}',NULL,'{"id":1000}','[2020-01-01, 2021-01-01)',NULL);
insert into myTbl5 values (20,array['abc','def','ghi'],array[3,4,5],array['2000/02/01'::date,'2000/02/28'::date],'{"id":1001, "c1":"abc"}',NULL,'{"id":1001}',NULL,XMLPARSE (CONTENT '<foo>bar</foo>'));
update myTbl5 set col54 = '{"2010/11/28","2010/12/03"}', col55 = '{"id":1001, "c2":"def"}', col57 = '{"id":1001, "c3":"ghi"}' where col54 is null;
insert into myTbl6 select i+10, point(i,1.3), '((0,0),(2,2))', circle(point(5,5),i),'((-2,-2),(3,0),(1,4))','10.20.30.40/27','EXECUTING',(i+10,point(i,1.3))::mycomposite from generate_series (1,8) as i;
update myTbl6 set col64 = '<(5,6),3.5>', col65 = null, col67 = 'COMPLETED' where col61 <= 13;

-- also add rows with unusual text content
insert into mytbl2 values (10, E'row 1 \r... and row 2 with a '' (quote) character',null);
insert into mytbl2 values (11, E'row 1 with a true \\ character\r... and row 2 with two \\n true and a '' (quote) characters',null);
-- and manupulate NULL characters
insert into mytbl1 values (200, E'Start\tEnd ', E'A\\000B'::BYTEA);
update mytbl1 set col12 = E' Start\tEnd' where col11 = 200;
delete from mytbl1 where col13 = E'A\\000B'::BYTEA;

-- also apply some changes in sequence characteristics
alter sequence myschema2.myseq1 minvalue 1 maxvalue 100 increment 10 start 21 restart 11 cache 2 cycle;

-- create the directory for the first snaps set
\! mkdir -p $EMAJTESTTMPDIR/snaps1
-- ... and snap all groups
set role emaj_regression_tests_adm_user1;
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR' || '/snaps1','CSV HEADER');
select emaj.emaj_snap_group('myGroup2',:'EMAJTESTTMPDIR' || '/snaps1','CSV HEADER');
select emaj.emaj_snap_group('phil''s group#3",',:'EMAJTESTTMPDIR' || '/snaps1','CSV HEADER');

\! ls $EMAJTESTTMPDIR/snaps1

-- generate a sql script for each active group (and check the result with detailed log statistics + number of sequences)
select emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myGroup1.sql');
select coalesce(sum(stat_rows),0) + 1 /* sequence change */ as check from emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myGroup2.sql', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1','myschema2.myTbl3_col31_seq']);
select sum(stat_rows) + 1 /* sequence change */ as check from emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-1', NULL);
select emaj.emaj_gen_sql_group('phil''s group#3",', 'M1_rollbackable', NULL, :'EMAJTESTTMPDIR' || '/Group3.sql');
select sum(stat_rows) + 1 /* sequence change*/ as check from emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'M1_rollbackable', NULL);

-- generate another sql script for myGroup1 but with a manual export and check both scripts are the same
select emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, NULL);
\setenv FILE1 :EMAJTESTTMPDIR'/myGroup1_2.sql'
\copy (select * from emaj_sql_script) to program 'cat >$FILE1'
-- mask timestamp in initial comment and compare
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'
\! diff $EMAJTESTTMPDIR/myGroup1.sql $EMAJTESTTMPDIR/myGroup1_2.sql

-- process \\ characters in script files
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i_s -s 's/\\\\/\\/g'
-- comment transaction commands for the need of the current test
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
-- mask timestamp in initial comment
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'

\! ls $EMAJTESTTMPDIR

-- create the directory for the second snaps set
\! mkdir $EMAJTESTTMPDIR/snaps2
-- in a single transaction and as superuser:
--   rollback groups, replay updates with generated scripts, snap groups again and cancel the transaction
begin;
  select * from emaj.emaj_rollback_group('myGroup1','Multi-1',false) order by 1,2;
  select * from emaj.emaj_rollback_group('myGroup2','Multi-1',false) order by 1,2;
  select * from emaj.emaj_rollback_group('phil''s group#3",','M1_rollbackable',false) order by 1,2;

  \! cat $EMAJTESTTMPDIR/myGroup1.sql
  reset role;
  \set FILE1 :EMAJTESTTMPDIR '/myGroup1.sql'
  \i :FILE1
  \set FILE2 :EMAJTESTTMPDIR '/myGroup2.sql'
  \i :FILE2
  \set FILE3 :EMAJTESTTMPDIR '/Group3.sql'
  \i :FILE3

  set role emaj_regression_tests_adm_user1;
  select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR' || '/snaps2','CSV HEADER');
  select emaj.emaj_snap_group('myGroup2',:'EMAJTESTTMPDIR' || '/snaps2','CSV HEADER');
  select emaj.emaj_snap_group('phil''s group#3",',:'EMAJTESTTMPDIR' || '/snaps2','CSV HEADER');
rollback;

-- mask timestamp in _INFO files
\! sed -i_s -s 's/at .*/at [ts]/' $EMAJTESTTMPDIR/snaps1/_INFO $EMAJTESTTMPDIR/snaps2/_INFO
-- and compare both snaps sets
-- sequences are detected as different because of :
-- - the effect of RESTART on is_called and next_val attributes
-- - internal log_cnt value being reset
\! diff --exclude _INFO_s $EMAJTESTTMPDIR/snaps1 $EMAJTESTTMPDIR/snaps2

-- reset the sequence myschema2.myseq1 to its previous characteristics
reset role;
alter sequence myschema2.myseq1 restart 1004 start 1000 increment 1 maxvalue 9223372036854775807 minvalue 1000 cache 1 no cycle;

set role emaj_regression_tests_adm_user2;

-----------------------------
-- Checking step 11
-----------------------------
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14400 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14400 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(14500);

-----------------------------
-- Step 12 : test use of a table with a very long name (63 characters long)
-----------------------------
select emaj.emaj_stop_group('phil''s group#3",');

-- remove the "phil's tbl1" table, rename it and reassign it to its group
select emaj.emaj_remove_table('phil''s schema"3','phil''s tbl1');

reset role;
alter table "phil's schema""3"."phil's tbl1" rename to table_with_very_looooooooooooooooooooooooooooooooooooooong_name;

set role emaj_regression_tests_adm_user1;
select emaj.emaj_assign_table('phil''s schema"3', 'table_with_very_looooooooooooooooooooooooooooooooooooooong_name', 'phil''s group#3",');

-- use the table and its group
select emaj.emaj_start_group('phil''s group#3",','M1_after_table_rename');

reset role;
update "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name set "phil's col12" = 'GHI' where "phil's col11" between 6 and 9;

set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('phil''s group#3",','M2');

reset role;
delete from "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name where "phil's col11" > 18;

set role emaj_regression_tests_adm_user1;
select * from emaj.emaj_rollback_group('phil''s group#3",','M1_after_table_rename',false) order by 1,2;
select emaj.emaj_stop_group('phil''s group#3",');
select emaj.emaj_drop_group('phil''s group#3",');

--
reset role;
alter table "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name rename to "phil's tbl1";

-----------------------------
-- Checking step 12
-----------------------------
set role emaj_regression_tests_adm_user1;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14500 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14500 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(14600);

-----------------------------
-- Step 13 : test use of groups or marks protection
-----------------------------
set role emaj_regression_tests_adm_user2;
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
-- Checking step 13
-----------------------------
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14600 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14600 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(14700);

-----------------------------
-- Step 14 : test complex use of rollbacks consolidations, with an application trigger kept enabled
-----------------------------
set search_path=public,myschema1;
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers":["mytbl2trg2"]}');

-- 2 consolidations of 2 logged rollbacks
-- Multi-1 MC1 MC2 RMC1S RMC1D      MC3 MC4 MC5 RMC3S RMC3D
--         ^       lrg(MC1)         ^           lrg(MC3)
--         xxxxxxxxxxxxxxx conso(RMC1D)
-- 	       	                        xxxxxxxxxxxxxxxxxxx conso(RMC3D)

select emaj.emaj_set_mark_group('myGroup1','MC1');

reset role;
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (2000,2012) as i;
insert into myTbl2 values (2000,'TC1',NULL);
delete from myTbl1 where col11 > 2010;

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','MC2');

reset role;
update myTbl2 set col22 = 'TC2' WHERE col22 ='TC1';

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup1','MC1',false) order by 1,2;

reset role;
insert into myTbl2 values (2000,'TC3',NULL);

set role emaj_regression_tests_adm_user2;
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','RLBK_MC1_DONE');
select emaj.emaj_set_mark_group('myGroup1','MC3');

reset role;
insert into "myTbl3" (col33) select generate_series(2000,2039,4)/100;
insert into myTbl4 values (2000,'FK...',1,10,'ABC');
update myTbl4 set col44 = NULL where col41 = 2000;

set role emaj_regression_tests_adm_user2;
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

reset role;
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (2000,2012) as i;
insert into myTbl2 values (3000,'TC6',NULL);
delete from myTbl1 where col11 > 2010;

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','MC7');

reset role;
update myTbl2 set col22 = 'TC7' WHERE col22 ='TC6';

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','MC8');

reset role;
insert into myTbl2 values (3001,'TC8',NULL);

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup1','MC8',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','EMAJ_LAST_MARK');

select emaj.emaj_set_mark_group('myGroup1','MC9');

reset role;
insert into "myTbl3" (col33) select generate_series(2000,2039,4)/100;
insert into myTbl4 values (2000,'FK...',1,10,'ABC');
update myTbl4 set col44 = NULL where col41 = 2000;

set role emaj_regression_tests_adm_user2;
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

reset role;
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (3000,3010) as i;
delete from myTbl1 where col11 > 3005;

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup1','MC10',false) order by 1,2;

reset role;
update myTbl2 set col22 = 'TC7' WHERE col22 ='TC6';

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','MC11');

reset role;
insert into myTbl4 values (3000,'FK...',1,10,'ABC');
update myTbl4 set col44 = NULL where col41 = 3000;

set role emaj_regression_tests_adm_user2;
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

reset role;
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (4000,4012) as i;
insert into myTbl2 values (4000,'TC15',NULL);
delete from myTbl1 where col11 > 4010;

set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','MC16');

reset role;
update myTbl2 set col22 = 'TC16' WHERE col22 ='TC15';

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup1','MC15',false) order by 1,2;
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','RLBK_MC15_DONE');

select emaj.emaj_set_mark_group('myGroup1','MC17');

reset role;
insert into myTbl2 values (4001,'TC15',NULL);

set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup1','MC16',false) order by 1,2;
select emaj.emaj_consolidate_rollback_group('myGroup1','RLBK_MC15_DONE');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup1','Multi-1',NULL);

reset role;
select * from myTbl1 order by col11;
select * from myTbl2 order by col21;

-----------------------------
-- Checking step 14
-----------------------------
set role emaj_regression_tests_adm_user2;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence where sequ_schema = 'emaj_myschema1' order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table where tbl_schema = 'myschema1' order by tbl_time_id, tbl_schema, tbl_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole where sqhl_schema = 'myschema1' order by 1,2,3;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 14700 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 14700 order by hist_id;


select * from emaj.emaj_rollback_group('myGroup1','Multi-1',true) order by 1,2;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
reset role;
