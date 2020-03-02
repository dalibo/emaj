-- misc.sql : test miscellaneous functions
--

-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 5000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 5000;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 5000;
alter sequence emaj.emaj_global_seq restart 50000;

-----------------------------
-- emaj_reset_group() test
-----------------------------

-- group is unknown in emaj_group_def
select emaj.emaj_reset_group(NULL);
select emaj.emaj_reset_group('unknownGroup');

-- group not in logging state
select emaj.emaj_reset_group('myGroup1');

-- stop group
select emaj.emaj_stop_group('myGroup1');

-- log tables are not yet empty
select count(*) from emaj_myschema1.mytbl1_log;
select count(*) from emaj_myschema1.mytbl2_log;
select count(*) from emaj_myschema1.mytbl2b_log;
select count(*) from emaj_myschema1."myTbl3_log";
select count(*) from emaj_myschema1.mytbl4_log;

-- should be OK
select emaj.emaj_reset_group('myGroup1');
begin;
  select emaj.emaj_stop_group('emptyGroup');
  select emaj.emaj_reset_group('emptyGroup');
rollback;

select count(*) from emaj_myschema1.mytbl1_log;
select count(*) from emaj_myschema1.mytbl2_log;
select count(*) from emaj_myschema1.mytbl2b_log;
select count(*) from emaj_myschema1."myTbl3_log";
select count(*) from emaj_myschema1.mytbl4_log;

-- test the "no initial mark" error message for the emaj_gen_sql_group()
--   this test has been moved here because, the emaj_reset_group() function cannot be used into a transaction
select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, NULL);

-- start myGroup1
select emaj.emaj_start_group('myGroup1','Mark21');
-----------------------------
-- log updates on myschema2 between 3 mono-group and multi-groups marks 
-----------------------------
set search_path=public,myschema2;
-- set a multi-groups mark
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-1');
-- inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger)
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,10100) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 500;
delete from myTbl1 where col11 > 10000;
insert into myTbl2 select i, 'DEF', current_date from generate_series (1,900) as i;
-- set marks
select emaj.emaj_set_mark_group('myGroup2','Mark22');
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');
-- inserts/updates/deletes in myTbl3 and myTbl4
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl4 select i,'FK...',i,1,'ABC' from generate_series (1,100) as i;
-- set marks
select emaj.emaj_set_mark_group('myGroup2','Mark23');
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-3');

-----------------------------
-- emaj_log_stat_group(), emaj_log_stat_groups(), emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups() test
-----------------------------
-- group is unknown in emaj_group_def
select * from emaj.emaj_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_log_stat_groups(array['unknownGroup'],NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_detailed_log_stat_groups(array['unknownGroup'],NULL,NULL);

-- invalid marks
select * from emaj.emaj_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_log_stat_group('myGroup2',NULL,'dummyEndMark');
select * from emaj.emaj_detailed_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'dummyEndMark');

select * from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],NULL,NULL);
select * from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','dummyEndMark');
select * from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],NULL,NULL);
select * from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','dummyEndMark');

-- start mark > end mark
-- original test (uncomment for unit test)
--  select * from emaj.emaj_log_stat_group('myGroup2','Mark23','Mark22');
--  select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark23','Mark22');

-- just check the error is trapped, because the error message contains timestamps
create function test_log(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) returns void language plpgsql as 
$$
begin
  begin
    perform count(*) from emaj.emaj_log_stat_group(v_groupName,v_firstMark,v_lastMark);
    return;
  exception when raise_exception then
    raise notice 'Error trapped on emaj_log_stat_group() call';
  end;
  begin
    perform count(*) from emaj.emaj_detailed_log_stat_group(v_groupName,v_firstMark,v_lastMark);
    return;
  exception when raise_exception then
    raise notice 'Error trapped on emaj_detailed_log_stat_group() call';
  end;
  return;
end;
$$;
select test_log('myGroup2','Mark23','Mark22');
select test_log('myGroup2','EMAJ_LAST_MARK','Mark22');
drop function test_log(text,text,text);

-- should be ok
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2',NULL,NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','','')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2',NULL,'Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','','')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_table;

-- empty group
select * from emaj.emaj_log_stat_group('emptyGroup',NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group('emptyGroup',NULL,NULL);

-- groups without any mark
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_reset_group('myGroup1');
  select * from emaj.emaj_log_stat_groups(array['myGroup1'],NULL,NULL);
  select * from emaj.emaj_detailed_log_stat_groups(array['myGroup1'],NULL,NULL);
rollback;

-----------------------------
-- emaj_estimate_rollback_group() and emaj_estimate_rollback_groups() tests
-----------------------------

-- group is unknown in emaj_group_def
select emaj.emaj_estimate_rollback_group(NULL,NULL,FALSE);
select emaj.emaj_estimate_rollback_group('unknownGroup',NULL,TRUE);
select emaj.emaj_estimate_rollback_groups('{"myGroup2","unknownGroup"}',NULL,TRUE);

-- invalid marks
select emaj.emaj_estimate_rollback_group('myGroup2','dummyMark',TRUE);
select emaj.emaj_estimate_rollback_groups(array['myGroup1','myGroup2'],'Mark21',TRUE);

-- group not in logging state
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_estimate_rollback_group('myGroup1','Mark11',FALSE);
rollback;

-- estimate a rollback of an empty group
select emaj.emaj_estimate_rollback_group('emptyGroup','EGM4',TRUE);

-- insert 1 timing parameters (=> so use 3 default values)
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_step_rollback_duration','2.5 millisecond'::interval);

-- analyze tables to get proper reltuples statistics
vacuum analyze myschema2.mytbl4;
select reltuples from pg_class, pg_namespace where relnamespace=pg_namespace.oid and relname = 'mytbl4' and nspname = 'myschema2';

-- estimate with empty rollback statistics and default parameters
delete from emaj.emaj_rlbk_stat;

-- estimates with empty rollback statistics but 1 temporarily modified parameter ; no table to rollback
-- check in passing that the simulation is not blocked by protections set on groups or marks
begin;
  select emaj.emaj_protect_group('myGroup2');
  select emaj.emaj_protect_mark_group('myGroup2','EMAJ_LAST_MARK');
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','1.4 millisecond'::interval);
  select emaj.emaj_estimate_rollback_group('myGroup2','EMAJ_LAST_MARK',FALSE);
-- should return 0.011200 sec
  select emaj.emaj_unprotect_mark_group('myGroup2','EMAJ_LAST_MARK');
  select emaj.emaj_unprotect_group('myGroup2');
rollback;

select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 1.425100 sec

-- estimates with empty rollback statistics but temporarily modified parameters
begin;
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','150 microsecond'::interval);
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','12 microsecond'::interval);
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_fkey_check_duration','27 microsecond'::interval);
  UPDATE emaj.emaj_param SET param_value_interval = '7 millisecond'::interval WHERE param_key = 'fixed_step_rollback_duration';
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_dblink_rollback_duration','2.5 millisecond'::interval);
  select emaj.emaj_estimate_rollback_groups('{"myGroup2"}','Mark21',TRUE);
-- should return 1.860700 sec
rollback;

-- estimate with added rollback statistics about fkey drops, recreations and checks
--   drop the foreign key on emaj_rlbk_stat to easily temporarily insert dummy rows
alter table emaj.emaj_rlbk_stat drop constraint emaj_rlbk_stat_rlbt_rlbk_id_fkey;
insert into emaj.emaj_rlbk_stat values
  ('DROP_FK','','','',1,1,'0.003 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('ADD_FK','myschema2','mytbl4','mytbl4_col44_fkey',1,300,'0.036 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('SET_FK_IMM','myschema2','mytbl4','mytbl4_col43_fkey',1,2000,'0.030 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DROP_FK','','','',2,1,'0.0042 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('ADD_FK','myschema2','mytbl4','mytbl4_col44_fkey',2,200,'0.020 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('SET_FK_IMM','myschema2','mytbl4','mytbl4_col43_fkey',2,1200,'0.015 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 1.440962 sec

-- estimate with added statistics about tables rollbacks
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl1','',1,5350,'1.000 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',1,100,'0.004 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',2,200,'0.010 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',3,20000,'1.610 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','myTbl3','',1,99,'0.004 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','myTbl3','',2,101,'0.008 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl4','',1,50000,'3.600 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 2.309566 sec

-- estimate with added statistics about log deletes and CTRLxDBLINK pseudo steps
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl1','',1,5350,'0.250 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',1,150,'0.001 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',2,200,'0.003 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',3,20000,'1.610 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','myTbl3','',1,99,'0.001 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','myTbl3','',2,151,'0.002 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl4','',1,50000,'0.900 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL+DBLINK','','','',1,10,'0.005 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL-DBLINK','','','',2,10,'0.035 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL-DBLINK','','','',3,10,'0.025 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 2.675653 sec

-- estimate with 2 groups and a SET_FK_DEF step
vacuum analyze myschema1.mytbl4;
select reltuples from pg_class, pg_namespace where relnamespace=pg_namespace.oid and relname = 'mytbl4' and nspname = 'myschema1';
begin;
-- temporarily insert new rows into myTbl4 of myschema1
  insert into myschema1.myTbl4 select i,'FK...',2,1,'ABC' from generate_series (10,20) as i;
  select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Multi-1',FALSE);
-- should return 2.729023 sec
rollback;

-- delete all manualy inserted rollback statistics, cleanup the statistics table and recreate its foreign key
delete from emaj.emaj_rlbk_stat;
vacuum emaj.emaj_rlbk_stat;
alter table emaj.emaj_rlbk_stat add FOREIGN KEY (rlbt_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);

-----------------------------
-- emaj_snap_group() test
-----------------------------
-- set/reset directory for snaps
\! mkdir -p /tmp/emaj_test/snaps
\! rm -R /tmp/emaj_test/snaps
\! mkdir /tmp/emaj_test/snaps

-- group is unknown in emaj_group_def
select emaj.emaj_snap_group(NULL,NULL,NULL);
select emaj.emaj_snap_group('unknownGroup',NULL,NULL);

-- invalid directory
select emaj.emaj_snap_group('myGroup1',NULL,NULL);
select emaj.emaj_snap_group('myGroup1','unknown_directory',NULL);
select emaj.emaj_snap_group('myGroup1','/unknown_directory',NULL);

-- invalid COPY TO options
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','dummy_option');

-- SQL injection attempt
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','; CREATE ROLE fake LOGIN PASSWORD '''' SUPERUSER');

-- should be OK (even when executed twice, files being overwriten)
select emaj.emaj_snap_group('emptyGroup','/tmp/emaj_test/snaps','');
\! ls /tmp/emaj_test/snaps
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','');
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','CSV HEADER DELIMITER '';'' ');
\! ls /tmp/emaj_test/snaps

-----------------------------
-- emaj_snap_log_group() test
-----------------------------
-- set/reset directory for log snaps
\! mkdir -p /tmp/emaj_test/log_snaps
\! rm -R /tmp/emaj_test/log_snaps
\! mkdir /tmp/emaj_test/log_snaps

-- group is unknown in emaj_group_def
select emaj.emaj_snap_log_group(NULL,NULL,NULL,NULL,NULL);
select emaj.emaj_snap_log_group('unknownGroup',NULL,NULL,NULL,NULL);

-- invalid directory
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK',NULL,NULL);
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','unknown_directory',NULL);
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/unknown_directory',NULL);

-- invalid start mark
select emaj.emaj_snap_log_group('myGroup2','unknownMark','EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps',NULL);

-- invalid end mark
select emaj.emaj_snap_log_group('myGroup2','','unknownMark','/tmp/emaj_test/log_snaps',NULL);

-- start mark > end mark
-- just check the error is trapped, because the error message contents timestamps
create function test_snap_log(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) returns void language plpgsql as 
$$
begin
  begin
    perform emaj.emaj_snap_log_group(v_groupName,v_firstMark,v_lastMark,'/tmp/emaj_test/log_snaps',NULL);
    return;
  exception when raise_exception then
    raise notice 'Error trapped on emaj_snap_log_group() call';
  end;
  return;
end;
$$;
select test_snap_log('myGroup2','Mark23','Mark21');
select test_snap_log('myGroup2','EMAJ_LAST_MARK','Mark22');
drop function test_snap_log(text,text,text);

-- invalid COPY TO options
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps', 'dummy_option');

-- SQL injection attempt
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps','; CREATE ROLE fake LOGIN PASSWORD '''' SUPERUSER');

-- should be ok
select emaj.emaj_snap_log_group('emptyGroup','EGM3','EGM4','/tmp/emaj_test/log_snaps',NULL);
\! ls /tmp/emaj_test/log_snaps
\! cat /tmp/emaj_test/log_snaps/emptyGroup_sequences_at_EGM3
\! rm /tmp/emaj_test/log_snaps/*

select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps',NULL);
select emaj.emaj_snap_log_group('myGroup2','','','/tmp/emaj_test/log_snaps','CSV');
select emaj.emaj_snap_log_group('myGroup2','Mark21',NULL,'/tmp/emaj_test/log_snaps','CSV HEADER');
select emaj.emaj_snap_log_group('myGroup2','Mark21','Mark21','/tmp/emaj_test/log_snaps','CSV');
select emaj.emaj_snap_log_group('myGroup2','Mark21','Mark23','/tmp/emaj_test/log_snaps',NULL);

-- mark name with special characters
select emaj.emaj_set_mark_group('myGroup2',E'/<*crazy mark$>\\');
select emaj.emaj_snap_log_group('myGroup2','Mark21',E'/<*crazy mark$>\\','/tmp/emaj_test/log_snaps',NULL);

\! ls /tmp/emaj_test/log_snaps |sed s/[0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9]/\[timestamp_mask\]/g
select emaj.emaj_delete_mark_group('myGroup2',E'/<*crazy mark$>\\');

-----------------------------
-- emaj_get_current_log_table() test
-----------------------------
-- not found
select * from emaj.emaj_get_current_log_table('myschema1', 'dummy_table');
-- found
select * from emaj.emaj_get_current_log_table('myschema1', 'mytbl1');
select 'select count(*) from ' || quote_ident(log_schema) || '.' || quote_ident(log_table)
  from emaj.emaj_get_current_log_table('myschema1','mytbl1');

-----------------------------
-- emaj_gen_sql_group() and emaj_gen_sql_groups() test
-----------------------------
-- set/reset directory for snaps
\! mkdir -p /tmp/emaj_test/sql_scripts
\! rm -R /tmp/emaj_test/sql_scripts
\! mkdir /tmp/emaj_test/sql_scripts

-- group is unknown in emaj_group_def
select emaj.emaj_gen_sql_group(NULL, NULL, NULL, NULL);
select emaj.emaj_gen_sql_group('unknownGroup', NULL, NULL, NULL, NULL);

select emaj.emaj_gen_sql_groups(NULL, NULL, NULL, NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","unknownGroup"}', NULL, NULL, NULL);

-- invalid start mark
select emaj.emaj_gen_sql_group('myGroup2', 'unknownMark', NULL, NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'Mark11', NULL, NULL, NULL);

-- invalid end mark
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'unknownMark', NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'Multi-1', 'Mark11', NULL);

-- end mark is prior start mark
-- (mark timestamps are temporarily changed so that regression test can return a stable error message)
begin;
  update emaj.emaj_time_stamp set time_clock_timestamp = '2000-01-01 12:00:00+00' 
    from emaj.emaj_mark
    where time_id = mark_time_id and mark_group = 'myGroup2' and mark_name = 'Mark21';
  update emaj.emaj_time_stamp set time_clock_timestamp = '2000-01-01 13:00:00+00'
    from emaj.emaj_mark
    where time_id = mark_time_id and mark_group = 'myGroup2' and mark_name = 'Mark22';
  select emaj.emaj_gen_sql_group('myGroup2', 'Mark22', 'Mark21', NULL);
rollback;
begin;
  update emaj.emaj_time_stamp set time_clock_timestamp = '2000-01-01 12:00:00+00'
    from emaj.emaj_mark
    where time_id = mark_time_id and mark_name = 'Multi-2';
  update emaj.emaj_time_stamp set time_clock_timestamp = '2000-01-01 13:00:00+00'
    from emaj.emaj_mark
    where time_id = mark_time_id and mark_name = 'Multi-3';
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-3', 'Multi-2', NULL);
rollback;

-- start mark with the same name but that doesn't correspond to the same point in time
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Mark21', 'Multi-2', NULL);
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], NULL, 'Multi-2', NULL, NULL);

-- start mark with the same point in time but not with the same name
begin;
  select emaj.emaj_stop_groups(array['myGroup1','myGroup2']);
  select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Common_mark_name');
  select emaj.emaj_rename_mark_group('myGroup1', 'Common_mark_name', 'Renamed');
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], NULL, NULL, NULL);
rollback;

-- end mark with the same name but that doesn't correspond to the same point in time
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', 'Mark21', NULL);

-- empty table/sequence names array
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array['']);

-- unknown table/sequence names in the tables filter
select emaj.emaj_gen_sql_group('myGroup2', NULL, NULL, '/tmp/emaj_test/sql_scripts/myFile', array['foo']);
select emaj.emaj_gen_sql_group('myGroup2', NULL, NULL, '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema1.mytbl1','myschema2.myTbl3_col31_seq','phil''s schema3.phil''s tbl1']);
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema1.mytbl1','foo','myschema2.myTbl3_col31_seq','phil''s schema3.phil''s tbl1']);

-- the tables group contains a table without pkey
select emaj.emaj_gen_sql_group('phil''s group#3",', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/Group3');

-- invalid location path name
select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/unknownDirectory/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, '/tmp/unknownDirectory/myFile');

-- should be ok
-- generated files content is checked later in adm2.sql scenario
-- getting counters from detailed log statistics + the number of sequences included in the group allows a comparison
--   with the result of the emaj_gen_sql_group() function
select emaj.emaj_gen_sql_group('emptyGroup', NULL, NULL, '/tmp/emaj_test/sql_scripts/myFile');
select emaj.emaj_gen_sql_group('myGroup2', NULL, NULL, '/tmp/emaj_test/sql_scripts/myFile');
select sum(stat_rows)+2 as check from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,NULL);
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, '/tmp/emaj_test/sql_scripts/myFile');
select sum(stat_rows)+2 as check from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21',NULL);
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'Mark22', '/tmp/emaj_test/sql_scripts/myFile');
select sum(stat_rows)+2 as check from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'Mark22');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, '/tmp/emaj_test/sql_scripts/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-2', 'Multi-3', '/tmp/emaj_test/sql_scripts/myFile');
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile');
select sum(stat_rows)+2 as check from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'EMAJ_LAST_MARK');

-- should be ok with no output file supplied
select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, NULL);
\copy (select * from emaj_sql_script) to '/dev/null'
drop table emaj_temp_script cascade;

-- should be ok, with tables and sequences filtering
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'EMAJ_LAST_MARK');
-- all tables and sequences
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1','myschema2.myTbl3_col31_seq']);
-- minus 1 sequence
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1']);
-- minus 1 table
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1']);
-- only 1 sequence
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema2.myTbl3_col31_seq']);
-- only 1 table (with a strange name and belonging to a group having another table without pkey)
select emaj.emaj_gen_sql_group('phil''s group#3",', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'phil''s schema3.phil''s tbl1']);
-- several groups and 1 table of each, with redondancy in the tables array
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', 'Multi-3', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema1.mytbl4','myschema2.mytbl4','myschema1.mytbl4','myschema2.mytbl4']);
\! grep 'only for' /tmp/emaj_test/sql_scripts/myFile

-- generate a sql script after a table structure change but on a time frame prior the change
begin;
  select emaj.emaj_set_mark_group('myGroup2','Test sql generation');
  insert into mySchema2.myTbl2 values(1000,'Text','01/01/2000');
  update mySchema2.myTbl2 set col22 = 'New text' where col21 = 1000;
  delete from mySchema2.myTbl2 where col21 = 1000;
  select emaj.emaj_remove_table('myschema2', 'mytbl2','Before ALTER mytbl2');
  alter table mySchema2.myTbl2 rename column col21 to id;
  select emaj.emaj_gen_sql_group('myGroup2', 'Test sql generation', 'Before ALTER mytbl2','/tmp/emaj_test/sql_scripts/altered_tbl.sql');
rollback;
-- comment transaction commands and mask the timestamp in the initial comment for the need of the current test
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
\! find /tmp/emaj_test/sql_scripts -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'
-- and execute the generated script
begin;
  \i /tmp/emaj_test/sql_scripts/altered_tbl.sql
rollback;

-----------------------------
-- test a table reclustering (it will use the pkey index as clustered index) and a vacuum full
-----------------------------
cluster emaj_myschema1.mytbl1_log;
vacuum full emaj_myschema1.mytbl1_log;

-----------------------------
-- try forbiden actions on emaj_param
-----------------------------
truncate emaj.emaj_param;
delete from emaj.emaj_param where param_key = 'emaj_version';

-----------------------------
-- test parameters export and import functions
-----------------------------
-- direct export
--   ok
select json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

-- export in file
--   error
select emaj.emaj_export_parameters_configuration('/tmp/dummy/location/file');
--   ok
select emaj.emaj_export_parameters_configuration('/tmp/orig_param_config');
\! wc -l /tmp/orig_param_config

-- direct import
--   error
--     no "parameters" array
select emaj.emaj_import_parameters_configuration('{ "dummy_json": null }'::json);
--     unknown attributes
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "emaj_version", "unknown_attribute_1": null, "unknown_attribute_2": null} ] }'::json);
--     missing or null "key" attributes
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "value": "no_key"} ] }'::json);
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": null} ] }'::json);

--   ok
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": "1 day"} ] }'::json);
--     "null" "value" attribute
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": null} ] }'::json);
--     missing "value" attribute
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention"} ] }'::json);
select json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

-- import from file
--   error
select emaj.emaj_import_parameters_configuration('/tmp/dummy/location/file');
\! echo 'not a json content' >/tmp/bad_param_config
select emaj.emaj_import_parameters_configuration('/tmp/bad_param_config');
\! echo '{ "dummy_json": null }' >/tmp/bad_param_config
select emaj.emaj_import_parameters_configuration('/tmp/bad_param_config');
\! echo '{ "parameters": [ { "key": "bad_key", "value": null} ] }' >/tmp/bad_param_config
select emaj.emaj_import_parameters_configuration('/tmp/bad_param_config');

--   ok
select emaj.emaj_import_parameters_configuration('/tmp/orig_param_config', true);
select json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

select emaj.emaj_import_parameters_configuration('/tmp/orig_param_config', false);

\! rm /tmp/orig_param_config /tmp/bad_param_config

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist where hist_id >= 5000 order by hist_id) as t;
