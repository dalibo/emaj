-- after_upg_while_logging.sql : complex scenario executed by an emaj_adm role
-- The E-Maj version is changed while groups are in logging state
-- This script is the part of operations performed after the upgrade
--
-----------------------------
-- Step 1 : check the E-Maj installation
-----------------------------
select * from emaj.emaj_verify_all();

select * from emaj.emaj_log_session order by 1,2;

select time_event, count(*) from emaj.emaj_time_stamp where time_event in ('S','X') group by 1 order by 1;

select group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging, 
       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  from emaj.emaj_group order by group_name;

select * from emaj.emaj_group_hist order by grph_group, grph_time_range;

select * from emaj.emaj_relation_change order by 1,2,3,4;

-----------------------------
-- Step 2 : for both groups, rollback to the common mark just set before the upgrade, after having unprotected the first group
-----------------------------
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Common',false) order by 1,2;
select emaj.emaj_unprotect_group('myGroup1');
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Common',true) order by 1,2;

-----------------------------
-- Step 3 : for myGroup1, update tables, then unprotect, logged_rollback, rename the end rollback mark and consolidate the rollback
-----------------------------
set search_path=myschema1;
--
update "myTbl3" set col33 = col33 / 2;
--
select * from emaj.emaj_rollback_group('myGroup1','M2',false) order by 1,2;
select emaj.emaj_unprotect_mark_group('myGroup1','M3');
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup1','M2',true) order by 1,2;
--
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','End_rollback_to_M2');
select emaj.emaj_consolidate_rollback_group('myGroup1','End_rollback_to_M2');

-----------------------------
-- Step 4 : for myGroup1, update tables and sequences again then set 3 marks and get stats
-----------------------------
insert into myTbl1 select i, 'DEF', E'\\000'::bytea from generate_series (100,110) as i;
insert into myTbl2 values (3,'GHI','2010-01-02');
delete from myTbl1 where col11 = 1;
select nextval('myschema1."myTbl3_col31_seq"');
--
select emaj.emaj_set_mark_group('myGroup1','M4');
--
update "myTbl3" set col33 = col33 / 2;
--
select emaj.emaj_set_mark_group('myGroup1','M5');
--
update myTbl1 set col11 = 99 where col11 = 1;
--
select emaj.emaj_set_mark_group('myGroup1','M6');
--
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup1', 'M2', 'M6') order by 1,2,3,4;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup1', 'M2', 'M6')
  order by stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;

-----------------------------
-- Step 5 : for myGroup2, logged rollback again then unlogged rollback 
-----------------------------
select * from emaj.emaj_logged_rollback_group('myGroup2','M2',false) order by 1,2;
--
select * from emaj.emaj_rollback_group('myGroup2','M3',false) order by 1,2;

-----------------------------
-- Step 6 : for myGroup1, update tables, rollback, other updates, then logged rollback
-----------------------------
set search_path=myschema1;
--
insert into myTbl1 values (1, 'Step 6', E'\\000'::bytea);
insert into myTbl4 values (11,'FK...',1,1,'Step 6');
insert into myTbl4 values (12,'FK...',1,1,'Step 6');
truncate myTbl2 cascade;
--
select * from emaj.emaj_rollback_group('myGroup1','M5',false) order by 1,2;
--
insert into myTbl1 values (1, 'Step 6', E'\\001'::bytea);
copy myTbl4 from stdin;
11		1	1	Step 6
12		1	1	Step 6
\.
--
select * from emaj.emaj_logged_rollback_group('myGroup1','M4',false) order by 1,2;

-----------------------------
-- Step 7 : for myGroup1, generate a sql script on the whole time frame
-----------------------------
select emaj.emaj_gen_sql_group('myGroup1','M1',NULL,'/dev/null',array['myschema1.mytbl1','myschema1.mytbl2','myschema1.mytbl4']);
select emaj.emaj_gen_sql_group('myGroup1','M1',NULL,'/dev/null',array['myschema1.myTbl3']);

-----------------------------
-- Step 8 : for myGroup1, update tables, rename a mark, then delete 2 marks then delete all before a mark 
-----------------------------
set search_path=myschema1;
--
delete from "myTbl3" where col31 between 14 and 18;
--
select emaj.emaj_rename_mark_group('myGroup1',mark_name,'Before logged rollback to M4') from emaj.emaj_mark where mark_name like 'RLBK_M4_%_START';
-- 
select emaj.emaj_delete_mark_group('myGroup1',mark_name) from emaj.emaj_mark where mark_name like 'RLBK_M4_%_DONE';
select emaj.emaj_delete_mark_group('myGroup1','M1');
--
select emaj.emaj_delete_before_mark_group('myGroup1','M4');

-----------------------------
-- Step 9 : for myGroup6, perform a table change and verify that no log trigger has been called
--          and finaly drop the group
-----------------------------
insert into mySchema6.table_with_51_characters_long_name_____0_________0a values (1),(2);
select count(*) from emaj_myschema6."table_with_51_characters_long_name_____0_________0#1_log";

select emaj.emaj_start_group('myGroup6', 'Start G6');
insert into mySchema6.table_with_51_characters_long_name_____0_________0a values (3),(4);
delete from mySchema6.table_with_51_characters_long_name_____0_________0a;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup6','Start G6',NULL) order by 1,2,3,4;
select * from emaj.emaj_rollback_group('myGroup6', 'Start G6');
select emaj.emaj_stop_group('myGroup6');
select emaj.emaj_drop_group('myGroup6');

-----------------------------
-- test end: check and reset history
-----------------------------
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d(\\d?)','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist 
  order by hist_id;
--
reset role;
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 30000;

-- the groups are left in their current state for the parallel rollback test.
-- perform some updates to prepare the parallel rollback test
-- set a mark for both groups
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-1');

select count(*) from mySchema1.myTbl4;
select count(*) from mySchema1.myTbl1;
select count(*) from mySchema1.myTbl2; 
select count(*) from mySchema1."myTbl3";
select count(*) from mySchema1.myTbl2b;
select count(*) from mySchema2.myTbl4;
select count(*) from mySchema2.myTbl1;
select count(*) from mySchema2.myTbl2; 
select count(*) from mySchema2."myTbl3";
delete from mySchema1.myTbl4;
delete from mySchema1.myTbl1;
delete from mySchema1.myTbl2; 
delete from mySchema1."myTbl3";
delete from mySchema1.myTbl2b;
delete from mySchema2.myTbl4;
delete from mySchema2.myTbl1;
delete from mySchema2.myTbl2; 
delete from mySchema2."myTbl3";
alter sequence mySchema2.mySeq1 restart 9999;
