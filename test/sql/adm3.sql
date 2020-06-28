-- adm3.sql : complex scenario executed by an emaj_adm role. 
--            Follows adm1.sql and adm2.sql
--

set role emaj_regression_tests_adm_user;
set search_path=public,myschema1;

-- before going on, save parameters on a file and reload them
select emaj.emaj_export_parameters_configuration('/tmp/param_config.json');
select emaj.emaj_import_parameters_configuration('/tmp/param_config.json', true);

-- also save the groups configuration on a file
select emaj.emaj_export_groups_configuration('/tmp/groups_config.json');

\! rm /tmp/param_config.json /tmp/groups_config.json

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
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table,
       rel_emaj_verb_attnum, rel_has_always_ident_col, rel_log_seq_last_value
  from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq like 'mypar%' order by rel_tblseq, rel_time_range;
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

select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  from emaj.emaj_relation where rel_schema = 'phil''s schema3' and rel_tblseq = 'mytbl4' order by rel_time_range;

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

-- export the initial groups configuration
select emaj.emaj_export_groups_configuration('/tmp/step19_groups_config.json', array['grp_tmp','grp_tmp_3','grp_tmp_4']);

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
select * from emaj.emaj_verify_all();
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
\! rm -Rf /tmp/emaj_test/sql_scripts/*

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
-- note that the warning about the mytblp_col3_seq sequence is normal
select * from emaj.emaj_verify_all();
--     a removal while the group is LOGGING fails
select emaj.emaj_remove_table('phil''s schema3','mytbl4');
select emaj.emaj_force_stop_group('grp_tmp');
select emaj.emaj_remove_table('phil''s schema3','mytbl4');
select emaj.emaj_assign_table('phil''s schema3','mytbl4','grp_tmp');
select * from emaj.emaj_verify_all();
select emaj.emaj_start_group('grp_tmp','Group restart');

-- import the groups configuration
select emaj.emaj_import_groups_configuration('/tmp/step19_groups_config.json', null, true);

-- reset groups at their initial state
select emaj.emaj_stop_group('grp_tmp_3');
select emaj.emaj_drop_group('grp_tmp_3');
select emaj.emaj_set_mark_group('grp_tmp_4','Last_mark');
select emaj.emaj_force_drop_group('grp_tmp_4');
select emaj.emaj_stop_group('grp_tmp');
select emaj.emaj_drop_group('grp_tmp');

select * from emaj.emaj_rel_hist order by 1,2,3;
select count(*) from emaj.emaj_relation where rel_schema in ('phil''s schema3','myschema4');

\! rm /tmp/step19_groups_config.json

-----------------------------
-- Step 20 : test TRUNCATE (log, statistics, rollback, sql generation and replay)
-----------------------------

SET client_min_messages TO WARNING;
select emaj.emaj_create_group('truncateTestGroup',true,true);
select emaj.emaj_assign_tables('phil''s schema3','.*','','truncateTestGroup');
select emaj.emaj_assign_tables('myschema4','.*','','truncateTestGroup');
RESET client_min_messages;
select emaj.emaj_start_group('truncateTestGroup','M1');

truncate "phil's schema3"."phil's tbl1" cascade;
truncate myschema4.myTblC2;
truncate myschema4.myPartP3;

select count(*) from "phil's schema3"."phil's tbl1";
select count(*) from "emaj_phil's schema3"."phil's tbl1_log";
select is_called, last_value from "emaj_phil's schema3"."phil's tbl1_log_seq";

select emaj.emaj_set_mark_group('truncateTestGroup','M2');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('truncateTestGroup','M1',null) where stat_rows > 0 order by 1,2,3,4;

select * from emaj.emaj_logged_rollback_group('truncateTestGroup','M1', false);

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('truncateTestGroup','M1',null) where stat_rows > 0 order by 1,2,3,4;

select emaj.emaj_gen_sql_group('truncateTestGroup','M1','M2','/tmp/emaj_test/sql_scripts/step20_gensql.sql');
-- TODO: replace the previous statement by the next one, once the issue with the emaj_logged_rollback_group() and emaj_gen_sql_groups() functions will be fixed
--select emaj.emaj_gen_sql_group('truncateTestGroup','M1',null,'/tmp/emaj_test/sql_scripts/step20_gensql.sql');

select * from emaj.emaj_rollback_group('truncateTestGroup','M1', false);

\! sed -i -s 's/at .*$/at [ts]$/' /tmp/emaj_test/sql_scripts/step20_gensql.sql
\! sed -i -s 's/\\\\/\\/g' /tmp/emaj_test/sql_scripts/step20_gensql.sql
\! sed -i -s 's/^COMMIT/ROLLBACK/' /tmp/emaj_test/sql_scripts/step20_gensql.sql

\i /tmp/emaj_test/sql_scripts/step20_gensql.sql

\! rm /tmp/emaj_test/sql_scripts/step20_gensql.sql

select emaj.emaj_stop_group('truncateTestGroup');
select emaj.emaj_drop_group('truncateTestGroup');

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
select hist_function, hist_event, hist_object,
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
