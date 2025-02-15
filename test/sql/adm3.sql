-- adm3.sql : complex scenario executed by an emaj_adm role. 
--            Follows adm1.sql and adm2.sql
--

-- set sequence restart value
select public.handle_emaj_sequences(17000);

-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/adm3'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

set role emaj_regression_tests_adm_user1;
set search_path = public,myschema1,emaj;

-----------------------------
-- Step 15 : export / import configurations
-----------------------------
-- save parameters on a file and reload them
select emaj_export_parameters_configuration(:'EMAJTESTTMPDIR' || '/param_config.json');
select emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/param_config.json', true);

-- also save the groups configuration on a file
select emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json');

-----------------------------
-- Checking step 15
-----------------------------
-- emaj tables
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 17000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 17000 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(17100);

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 16 : test transactions with several emaj operations
-----------------------------
select emaj_create_group('myGroup4');
select emaj_assign_tables('myschema4','.*',null,'myGroup4');

-- several similar operations in a single transaction, using different mark names
begin;
  select emaj_start_group('myGroup4','M1');
  select emaj_set_mark_group('myGroup4','M2');
  select emaj_set_mark_group('myGroup4','M3');
  select emaj_stop_group('myGroup4','M4');
  select emaj_start_group('myGroup4','M5',false);
  select emaj_stop_group('myGroup4','M6');
commit;

-----------------------------
-- Checking step 16
-----------------------------
-- emaj tables
select * from emaj.emaj_mark where mark_group = 'myGroup4' order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 17100 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 17100 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(17200);

-----------------------------
-- Step 17 : test partition attach and detach
-----------------------------
-- Needs postgres 10+

set role emaj_regression_tests_adm_user2;
select emaj_start_group('myGroup4','Start');

reset role;
insert into mySchema4.myTblR1 values (-2), (-1), (0), (1), (2), (11);
insert into mySchema4.myTblP values (-2, 'A', 'Stored in partition 1A'), (-1, 'P', 'Stored in partition 1B'),
									(0, 'Q', 'Stored in partition 2'), (1, 'X', 'Stored in partition 2'),
									(2, 'Y', 'Also stored in partition 2');
----insert into mySchema4.myTblP values (-1,'Stored in partition 1'), (1,'Stored in partition 2');

select emaj_set_mark_group('myGroup4','M1');
----update mySchema4.myTblP set col1 = 2 where col1 = 1;
update myschema4.myTblP set col1 = -2, col3 = 'Moved to partition 1B' where col1 = 2;

-- create a new partition and add it into the group ; in passing also add the sequence linked to the serial column of the mother table
CREATE TABLE mySchema4.myPartP3 PARTITION OF mySchema4.myTblP FOR VALUES FROM (10) TO (19);
-- create the table with PG 9.6- so that next scripts do not abort
CREATE TABLE IF NOT EXISTS mySchema4.myPartP3 () INHERITS (mySchema4.myTblP);
-- add a PK (will fail with PG12+ because of the global PK)
ALTER TABLE mySchema4.myPartP3 ADD PRIMARY KEY (col1);
grant all on mySchema4.myPartP3 to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;

set role emaj_regression_tests_adm_user1;
select emaj_assign_table('myschema4','mypartp3','myGroup4',null,'Add partition 3');
select emaj_assign_sequence('myschema4','mytblp_col4_seq','myGroup4','Add partition 3_seq');

reset role;
insert into mySchema4.myTblP values (11, 'A', 'Stored in partition 3');

-- remove obsolete partitions ; in passing also remove the sequence linked to the serial column of the mother table
set role emaj_regression_tests_adm_user1;
select emaj_remove_tables('myschema4', ARRAY['mypartp1a', 'mypartp1b'], 'Remove partition 1a & 1b');
select emaj_remove_sequence('myschema4','mytblp_col4_seq','Remove partition 1_seq');

reset role;
drop table mySchema4.myPartP1a, mySchema4.myPartP1b cascade;
set role emaj_regression_tests_adm_user2;

-- verify that emaj_adm has the proper grants to delete old marks leading to an old log table drop
begin transaction;
  select emaj_delete_before_mark_group('myGroup4','EMAJ_LAST_MARK');
rollback;

-- look at statistics and log content
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj_log_stat_group('myGroup4','Start', NULL) order by 1,2,3,4;
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table,
       rel_emaj_verb_attnum, rel_has_always_ident_col, rel_log_seq_last_value
  from emaj.emaj_relation where rel_schema = 'myschema4' and rel_tblseq like 'mypar%' order by rel_tblseq, rel_time_range;

reset role;
select col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP3_log order by emaj_gid;
select col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartP1a_log_1 order by emaj_gid;

-- rollback to a mark set before the first changes
set role emaj_regression_tests_adm_user2;
select * from emaj_rollback_group('myGroup4','Start');
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj_rollback_group('myGroup4','Start',true);

select emaj_stop_group('myGroup4');
select emaj_drop_group('myGroup4');

-----------------------------
-- Checking step 17
-----------------------------
-- emaj tables
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 17200 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 17200 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(17400);

-----------------------------
-- Step 18 : test defect with application table or sequence
--           also test some changes on the unlogged and the with oids tables
-----------------------------
select emaj_create_group('phil''s group#3",',false);
select emaj_assign_tables('phil''s schema"3','.*',null,'phil''s group#3",');
select emaj_assign_sequences('phil''s schema"3','.*',null,'phil''s group#3",');
select emaj_assign_tables('myschema5','.*',null,'phil''s group#3",');

select emaj_start_group('phil''s group#3",','start');

-----------------------------
-- test changes on the unlogged and the with oids tables
-----------------------------
reset role;
insert into myschema5.myUnloggedTbl values (10),(11),(12);
update myschema5.myUnloggedTbl set col1 = 13 where col1 = 12;
delete from myschema5.myUnloggedTbl where col1 = 10;
insert into myschema5.myOidsTbl values (20),(21),(22);
update myschema5.myOidsTbl set col1 = 23 where col1 = 22;
delete from myschema5.myOidsTbl where col1 = 21;

set role emaj_regression_tests_adm_user2;
select col1, emaj_verb, emaj_tuple, emaj_gid, emaj_user from emaj_myschema5.myUnloggedTbl_log order by emaj_gid;
select col1, emaj_verb, emaj_tuple, emaj_gid, emaj_user from emaj_myschema5.myOidsTbl_log order by emaj_gid;


-- disable event triggers for this step and change an application table structure
select emaj_disable_protection_by_event_triggers();

-----------------------------
-- test remove_and_add operations to repair an application table
-----------------------------
reset role;
alter table "phil's schema""3"."my""tbl4" alter column col45 type char(11);

set role emaj_regression_tests_adm_user1;
select * from emaj_verify_all();
select emaj_remove_table('phil''s schema"3','my"tbl4','remove_the_damaged_table');
select emaj_assign_table('phil''s schema"3', 'my"tbl4', 'phil''s group#3",', null, 're_add_the_table');

select * from emaj.emaj_relation where rel_schema = 'phil''s schema"3' and rel_tblseq = 'my"tbl4' order by rel_time_range;

-----------------------------
-- test a remove operation to fix the case of a dropped log table, log sequence or log function
-----------------------------
reset role;
drop table "emaj_phil's schema""3"."my""tbl4_log";

set role emaj_regression_tests_adm_user1;
select * from emaj_verify_all();
select emaj_remove_table('phil''s schema"3','my"tbl4','remove_the_damaged_table_2');
select emaj_assign_table('phil''s schema"3', 'my"tbl4', 'phil''s group#3",', null, 're_add_the_table_2');

select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  from emaj.emaj_relation where rel_schema = 'phil''s schema"3' and rel_tblseq = 'my"tbl4' order by rel_time_range;

-----------------------------
-- test a remove operation to fix the case of a dropped application table or sequence
-----------------------------
-- in fact just rename the table and the sequence
reset role;
alter table "phil's schema""3"."my""tbl4" rename to mytbl4_sav;
alter sequence "phil's schema""3"."phil's""seq\1" rename to "phil's""seq\1_sav";

set role emaj_regression_tests_adm_user2;

-- try to set a mark
begin;
  select emaj_set_mark_group('phil''s group#3",','should fails');
rollback;

select emaj_remove_table('phil''s schema"3','my"tbl4','remove_the_dropped_table');
select emaj_remove_sequence('phil''s schema"3','phil''s"seq\1','remove_the_dropped_seq');

-- check that the log table does not exist anymore
select count(*) from "emaj_phil's schema3"."my""tbl4_log";

-- revert the changes
reset role;
alter table "phil's schema""3".mytbl4_sav rename to "my""tbl4";
alter table "phil's schema""3"."my""tbl4" alter column col45 type char(10);
alter sequence "phil's schema""3"."phil's""seq\1_sav" rename to "phil's""seq\1";

set role emaj_regression_tests_adm_user1;
select emaj_assign_table('phil''s schema"3','my"tbl4','phil''s group#3",',null,'revert_last_changes_tbl');
select emaj_assign_sequence('phil''s schema"3','phil''s"seq\1','phil''s group#3",','revert_last_changes_seq');

-- ree-nable the event triggers and drop the group
select emaj_enable_protection_by_event_triggers();
select emaj_stop_group('phil''s group#3",');
select emaj_drop_group('phil''s group#3",');

-----------------------------
-- Checking step 18
-----------------------------
-- emaj tables
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 17400 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 17400 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(17600);

-----------------------------
-- Step 19 : test use of dynamic tables group management (assign, move, remove, change)
-----------------------------

-- create, start and populate groups
-- grp_tmp is empty, grp_tmp_3 and grp_tmp_4 contains tables and sequences frop respectively phil''s schema3 and myschema4
-- grp_tmp_4 is started before being populated
select emaj_create_group('grp_tmp');
select emaj_create_group('grp_tmp_3');
select emaj_create_group('grp_tmp_4');
select emaj_start_groups('{"grp_tmp","grp_tmp_4"}','Start');
begin;
  select emaj_assign_tables('phil''s schema"3','.*','','grp_tmp_3');
  select emaj_assign_sequences('phil''s schema"3','.*','','grp_tmp_3');
  select emaj_assign_tables('myschema4','.*','','grp_tmp_4');
  select emaj_assign_sequences('myschema4','.*','','grp_tmp_4');
  select emaj_modify_table('myschema4','mytblm','{"ignored_triggers": "mytblm_insert_trigger"}'::jsonb);
commit;
select emaj_start_group('grp_tmp_3','Start');
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1');

-- export the initial groups configuration
select emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json', array['grp_tmp','grp_tmp_3','grp_tmp_4']);

-- perform some changes and set marks
reset role;
insert into "phil's schema""3"."my""tbl4" (col41)
  select i from generate_series(3,8) i;
delete from "phil's schema""3"."myTbl2\";
insert into "phil's schema""3"."myTbl2\" (col22,col23)
  select 'After Mk1','12-31-2020' from generate_series(1,3);

set role emaj_regression_tests_adm_user1;
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk2');

reset role;
insert into "phil's schema""3"."myTbl2\" (col22,col23)
  select 'After Mk2','12-31-2030' from generate_series(1,3);
select nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
insert into myschema4.mytblm
  select '2006-06-30'::date + ('1 year'::interval) * i, i, 'After Mk2'
    from generate_series(0,9) i;

set role emaj_regression_tests_adm_user1;
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3');

reset role;
delete from myschema4.mytblm
  where col1 = '2006-06-30';
update myschema4.mytblm set col3 = 'After Mk2 and updated after Mk3'
  where col1 > '2013-01-01';

-- rollback to the previous mark
set role emaj_regression_tests_adm_user1;
select * from emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3');

-- change some priority and log tablespaces
select emaj_modify_tables('phil''s schema"3','.*tbl1','','{"priority":-1,"log_data_tablespace":"tsp log''2"}'::jsonb,'Modify 1 table');

-- move all tables and sequences into grp_tmp and set a common mark
select emaj_move_tables('phil''s schema"3','.*','','grp_tmp','Move_tbl_3_to_tmp');
select * from emaj_verify_all();
select emaj_move_sequences('phil''s schema"3','.*','','grp_tmp','Move_seq_3_to_tmp');
select emaj_move_tables('myschema4','.*','','grp_tmp','Move_tbl_4_to_tmp');
select emaj_move_sequences('myschema4','.*','','grp_tmp','Move_seq_4_to_tmp');

select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk4');

-- perform some other changes and set marks
reset role;
update "phil's schema""3"."my""tbl4" set col42 = 'Updated after Mk4'
  where col41 > 5;
delete from "phil's schema""3"."myTbl2\"
  where col21 = 4;
delete from "phil's schema""3"."my""tbl4"
  where col41 = 4;

set role emaj_regression_tests_adm_user1;
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk5');

reset role;
insert into myschema4.mytblm
  select '2017-06-30'::date + ('1 year'::interval) * i, 5, 'After Mk5'
    from generate_series(0,3) i;
select nextval(E'"phil''s schema""3"."phil''s""seq\\1"');

set role emaj_regression_tests_adm_user1;
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk6');

reset role;
update myschema4.mytblm set col3 = 'After Mk5 and updated after Mk6'
  where col1 > '2017-01-01';

-- remove the table mytblm and the sequence phil's seq\1
set role emaj_regression_tests_adm_user1;
select emaj_remove_table('myschema4','mytblc1','Remove_mytblc1');
select emaj_remove_sequence('phil''s schema"3','phil''s"seq\1','Remove_myseq1');

-- logged rollback to Mk5
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj_logged_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk5', true);
select emaj_rename_mark_group(group_name,'EMAJ_LAST_MARK','End_logged_rollback')
  from (values ('grp_tmp_3'),('grp_tmp_4'),('grp_tmp')) as t(group_name);

-- perform some other changes and set marks
reset role;
delete from myschema4.mytblm
  where col1 = '2018-06-30';

set role emaj_regression_tests_adm_user1;
select emaj_set_mark_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk7');

reset role;
delete from mySchema4.mytblm;

-- consolidate the logged rollback
set role emaj_regression_tests_adm_user1;
select * from emaj_get_consolidable_rollbacks() order by 1,2;
select emaj_consolidate_rollback_group('grp_tmp','End_logged_rollback');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj_log_stat_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null)
  order by stat_first_mark_datetime, stat_schema, stat_table;
select mark_time_id, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_group,
       mark_log_rows_before_next
  from emaj.emaj_mark where mark_group in ('grp_tmp_3','grp_tmp_4','grp_tmp')
  order by 1,2,3;

-- generate sql script
select emaj_gen_sql_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null,:'EMAJTESTTMPDIR' || '/allGroups.sql');
--  \! grep -iP '(insert|update|delete|alter)' $EMAJTESTTMPDIR/allGroups.sql

-- revert the priority and log tablespaces changes
select emaj_modify_tables('phil''s schema"3','.*tbl1','','{"priority":null,"log_data_tablespace":null}'::jsonb,'revert changes for 1 table');

-- rollback to a mark set before the tables and sequences move
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3', false);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj_rollback_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk3',true);

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj_log_stat_groups('{"grp_tmp_3","grp_tmp_4","grp_tmp"}','Mk1',null)
  order by stat_first_mark_datetime, stat_schema, stat_table;

-- delete all marks before Mk3
select emaj_delete_before_mark_group(group_name,'Mk3')
  from (values ('grp_tmp_3'),('grp_tmp_4'),('grp_tmp')) as t(group_name);

-- test a remove_table following log sequence deletion
reset role;
select emaj_disable_protection_by_event_triggers();
drop sequence "emaj_phil's schema""3"."my""tbl4_log_seq";
select emaj_enable_protection_by_event_triggers();

set role emaj_regression_tests_adm_user2;
-- note that the warning about the mytblp_col3_seq sequence is normal
select * from emaj_verify_all();
--     a removal while the group is LOGGING fails
select emaj_remove_table('phil''s schema"3','my"tbl4');
select emaj_force_stop_group('grp_tmp');
select emaj_remove_table('phil''s schema"3','my"tbl4');
select emaj_assign_table('phil''s schema"3','my"tbl4','grp_tmp');
select * from emaj_verify_all();
select emaj_start_group('grp_tmp','Group restart');

-- import the groups configuration
select emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json', null, true, 'REVERT_CHANGES');

-- reset groups at their initial state
select emaj_stop_group('grp_tmp_3');
select emaj_drop_group('grp_tmp_3');
select emaj_set_mark_group('grp_tmp_4','Last_mark');
select emaj_force_drop_group('grp_tmp_4');
select emaj_stop_group('grp_tmp');
select emaj_drop_group('grp_tmp');

select * from emaj.emaj_rel_hist order by 1,2,3;
select count(*) from emaj.emaj_relation where rel_schema in ('phil''s schema"3','myschema4');

-----------------------------
-- Checking step 19
-----------------------------
-- emaj tables
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 17600 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 17600 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(18000);

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 20 : test emaj_gen_sql_dump_changes_group() and emaj_dump_changes_group()
-----------------------------

select emaj_gen_sql_dump_changes_group('myGroup1','M4','M5','Consolidation=FULL',NULL,:'EMAJTESTTMPDIR' || '/dump_changes.sql');
\! wc -l $EMAJTESTTMPDIR/dump_changes.sql

select emaj_dump_changes_group('myGroup1','M4','M5',NULL,'{"myschema1.myTbl3"}',:'EMAJTESTTMPDIR');
\! wc -l $EMAJTESTTMPDIR/myschema1_myTbl3.changes $EMAJTESTTMPDIR/_INFO

-- Build the table/sequence names array using the emaj_log_stat_group() function result
SELECT emaj_gen_sql_dump_changes_group('myGroup1','M4','M5',
                                            'EMAJ_COLUMNS=(emaj_tuple, emaj_gid, emaj_verb), CONSOLIDATION=FULL, ORDER_BY=TIME',
                                            (SELECT array_agg(distinct stat_schema || '.' || stat_table) FROM emaj_log_stat_group('myGroup1','M4','M5'))
                                           );
SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number;

-- Create a table and a view from a consolidated vision of a log table
DO $$
DECLARE
  v_stmt TEXT;
BEGIN
  PERFORM emaj_gen_sql_dump_changes_group('myGroup1','M4','M5', 'CONSOLIDATION=FULL, EMAJ_COLUMNS=MIN', ARRAY['myschema1.myTbl3']);
  SELECT sql_text INTO v_stmt FROM emaj_temp_sql WHERE sql_schema = 'myschema1' and sql_tblseq = 'myTbl3' AND sql_line_number = 1;
  EXECUTE 'CREATE TABLE public.myTbl3_cons_log_table_M4_M5 AS ' || v_stmt;
  EXECUTE 'CREATE VIEW public.myTbl3_cons_log_view_M4_M5 AS ' || v_stmt;
END;
$$;
SELECT emaj_tuple, col33 FROM public.myTbl3_cons_log_table_M4_M5 WHERE col31 = 13 ORDER BY emaj_tuple DESC;
SELECT emaj_tuple, col33 FROM public.myTbl3_cons_log_view_M4_M5 WHERE col31 = 13;   -- already sorted

DROP TABLE IF EXISTS public.myTbl3_cons_log_table_M4_M5;
DROP VIEW IF EXISTS public.myTbl3_cons_log_view_M4_M5;

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 21 : test TRUNCATE (log, statistics, rollback, sql generation and replay)
-----------------------------

SET client_min_messages TO WARNING;
select emaj_create_group('truncateTestGroup');
select emaj_assign_tables('phil''s schema"3','.*','','truncateTestGroup');
select emaj_assign_tables('myschema4','.*','','truncateTestGroup');
RESET client_min_messages;
select emaj_start_group('truncateTestGroup','M1');

reset role;
truncate "phil's schema""3"."phil's tbl1" cascade;
truncate myschema4.myTblC2;
truncate myschema4.myPartP3;

select count(*) from "phil's schema""3"."phil's tbl1";

set role emaj_regression_tests_adm_user2;
select count(*) from "emaj_phil's schema""3"."phil's tbl1_log";
select is_called, last_value from "emaj_phil's schema""3"."phil's tbl1_log_seq";

select emaj_set_mark_group('truncateTestGroup','M2');

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj_detailed_log_stat_group('truncateTestGroup','M1',null);

SELECT emaj_dump_changes_group('truncateTestGroup', 'M1', 'M2', 'CONSOLIDATION=NONE, EMAJ_COLUMNS=MIN', '{"myschema4.mypartp3"}', :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema4_mypartp3.changes
SELECT emaj_dump_changes_group('truncateTestGroup', 'M1', 'M2', 'CONSOLIDATION=PARTIAL', '{"myschema4.mypartp3"}', :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema4_mypartp3.changes
\! rm $EMAJTESTTMPDIR/*

select * from emaj_logged_rollback_group('truncateTestGroup','M1', false);

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj_detailed_log_stat_group('truncateTestGroup','M1',null);

select emaj_gen_sql_group('truncateTestGroup','M1','M2',:'EMAJTESTTMPDIR' || '/gensql.sql');
-- TODO: replace the previous statement by the next one, once the issue with the emaj_logged_rollback_group() and emaj_gen_sql_groups() functions will be fixed
--select emaj_gen_sql_group('truncateTestGroup','M1',null,:'EMAJTESTTMPDIR' || '/gensql.sql');

select * from emaj_rollback_group('truncateTestGroup','M1', false);

\! sed -i -s 's/at .*$/at [ts]$/' $EMAJTESTTMPDIR/gensql.sql
\! sed -i -s 's/\\\\/\\/g' $EMAJTESTTMPDIR/gensql.sql
\! sed -i -s 's/^COMMIT/ROLLBACK/' $EMAJTESTTMPDIR/gensql.sql

\set FILE1 :EMAJTESTTMPDIR '/gensql.sql'
reset role;
\i :FILE1

\! rm $EMAJTESTTMPDIR/*

set role emaj_regression_tests_adm_user2;
select emaj_stop_group('truncateTestGroup');
select emaj_drop_group('truncateTestGroup');

-- first set all rollback events state
select emaj_cleanup_rollback_state();

-----------------------------
-- Checking step 21
-----------------------------
-- emaj tables
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_nb_table,
       rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
       case when rlbk_end_datetime is null then 'null' else '[ts]' end as "end_datetime",
       regexp_replace(array_to_string(rlbk_messages,'#'),E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rlbk where rlbk_id >= 10000 order by rlbk_id;
select rlbs_rlbk_id, rlbs_session, 
       case when rlbs_end_datetime is null then 'null' else '[ts]' end as "end_datetime"
  from emaj.emaj_rlbk_session where rlbs_rlbk_id >= 10000  order by rlbs_rlbk_id, rlbs_session;
select rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_object_def, rlbp_app_trg_type, rlbp_is_repl_role_replica, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  from emaj.emaj_rlbk_plan where rlbp_rlbk_id >= 10000  order by rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object;
select rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id, rlbt_quantity
  from emaj.emaj_rlbk_stat where rlbt_rlbk_id >= 10000 order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 18000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 18000 order by hist_id;

select * from emaj.emaj_log_session where lower(lses_time_range) >= 12000 order by lses_group, lses_time_range;
select * from emaj.emaj_group_hist order by grph_group, grph_time_range;

-----------------------------
-- check grants on other functions to emaj_adm role
-----------------------------
select emaj_get_previous_mark_group('dummyGroup', 'EMAJ_LAST_MARK');
select emaj_estimate_rollback_groups(array['dummyGroup'], 'dummyMark', FALSE);
select * from emaj_rollback_activity();
select substr(pg_size_pretty(pg_database_size(current_database())),1,0);
select emaj_forget_group('dummyGroup');

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

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
