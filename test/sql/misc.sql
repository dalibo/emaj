-- misc.sql : test miscellaneous functions
--

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
select count(*) from emaj.myschema1_mytbl1_log;
select count(*) from emaj.myschema1_mytbl2_log;
select count(*) from emajb.myschema1_mytbl2b_log;
select count(*) from "emajC"."myschema1_myTbl3_log";
select count(*) from emaj.myschema1_mytbl4_log;

-- should be OK
select emaj.emaj_reset_group('myGroup1');

select count(*) from emaj.myschema1_mytbl1_log;
select count(*) from emaj.myschema1_mytbl2_log;
select count(*) from emajb.myschema1_mytbl2b_log;
select count(*) from "emajC"."myschema1_myTbl3_log";
select count(*) from emaj.myschema1_mytbl4_log;

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
-- emaj_log_stat_group() and emaj_detailled_log_stat_group() test
-----------------------------
-- group is unknown in emaj_group_def
select * from emaj.emaj_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_log_stat_group('unknownGroup',NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group('unknownGroup',NULL,NULL);

-- invalid marks
select * from emaj.emaj_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_log_stat_group('myGroup2',NULL,'dummyEndMark');
select * from emaj.emaj_detailed_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'dummyEndMark');

-- start mark > end mark
-- just check the error is trapped, because the error message contents timestamps
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
select * from emaj.emaj_log_stat_group('myGroup2',NULL,NULL)
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','','')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2',NULL,'Mark22')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,NULL)
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','','')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'Mark22')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select * from emaj.emaj_detailed_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;

-- groups without any mark
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_reset_group('myGroup1');
  select * from emaj.emaj_log_stat_group('myGroup1',NULL,NULL);
  select * from emaj.emaj_detailed_log_stat_group('myGroup1',NULL,NULL);
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

-- insert 1 timing parameters (=> so use 3 default values)
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_step_rollback_duration','2.5 millisecond'::interval);

-- analyze tables to get proper reltuples statistics
vacuum analyze myschema2.mytbl4;
select reltuples from pg_class, pg_namespace where relnamespace=pg_namespace.oid and relname = 'mytbl4' and nspname = 'myschema2';

-- estimate with empty rollback statistics and default parameters
delete from emaj.emaj_rlbk_stat;

-- estimates with empty rollback statistics but 1 temporarily modified parameter ; no table to rollback
begin;
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','1.4 millisecond'::interval);
  select emaj.emaj_estimate_rollback_group('myGroup2','EMAJ_LAST_MARK',FALSE);
-- should return 0.011200 sec
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
insert into emaj.emaj_rlbk_stat values
  ('DROP_FK','','','',1,'2000/01/01 00:01:00',1,'0.003 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('ADD_FK','myschema2','mytbl4','mytbl4_col44_fkey',1,'2000/01/01 00:01:00',300,'0.036 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('SET_FK_IMM','myschema2','mytbl4','mytbl4_col43_fkey',1,'2000/01/01 00:01:00',2000,'0.030 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DROP_FK','','','',2,'2000/01/01 00:02:00',1,'0.0042 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('ADD_FK','myschema2','mytbl4','mytbl4_col44_fkey',2,'2000/01/01 00:02:00',200,'0.020 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('SET_FK_IMM','myschema2','mytbl4','mytbl4_col43_fkey',2,'2000/01/01 00:02:00',1200,'0.015 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 1.440962 sec

-- estimate with added statistics about tables rollbacks
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl1','',1,'2000/01/01 00:01:00',5350,'1.000 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',1,'2000/01/01 00:01:00',100,'0.004 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',2,'2000/01/01 00:02:00',200,'0.010 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl2','',3,'2000/01/01 00:03:00',20000,'1.610 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','myTbl3','',1,'2000/01/01 00:01:00',99,'0.004 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','myTbl3','',2,'2000/01/01 00:02:00',101,'0.008 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('RLBK_TABLE','myschema2','mytbl4','',1,'2000/01/01 00:01:00',50000,'3.600 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 2.309566 sec

-- estimate with added statistics about log deletes and CTRLxDBLINK pseudo steps
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl1','',1,'2000/01/01 00:01:00',5350,'0.250 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',1,'2000/01/01 00:01:00',150,'0.001 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',2,'2000/01/01 00:02:00',200,'0.003 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl2','',3,'2000/01/01 00:03:00',20000,'1.610 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','myTbl3','',1,'2000/01/01 00:01:00',99,'0.001 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','myTbl3','',2,'2000/01/01 00:02:00',151,'0.002 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('DELETE_LOG','myschema2','mytbl4','',1,'2000/01/01 00:01:00',50000,'0.900 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL+DBLINK','','','',1,'2000/01/01 00:01:00',10,'0.005 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL-DBLINK','','','',2,'2000/01/01 00:02:00',10,'0.035 SECONDS'::interval);
insert into emaj.emaj_rlbk_stat values
  ('CTRL-DBLINK','','','',3,'2000/01/01 00:03:00',10,'0.025 SECONDS'::interval);
select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 2.675653 sec

-- estimate with 2 groups and a SET_FK_DEF step
vacuum analyze myschema1.mytbl4;
select reltuples from pg_class, pg_namespace where relnamespace=pg_namespace.oid and relname = 'mytbl4' and nspname = 'myschema1';
begin;
-- temporarily insert new rows into myTbl4 of myschema1
  insert into myschema1.myTbl4 select i,'FK...',2,1,'ABC' from generate_series (10,20) as i;
  select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Multi-1',FALSE);
-- should return 2.728023 sec
rollback;

-- delete all manualy inserted rollback statistics and cleanup the statistics table
delete from emaj.emaj_rlbk_stat;
vacuum emaj.emaj_rlbk_stat;

-----------------------------
-- emaj_snap_group() and  emaj_snap_log_group() test
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
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','');
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/snaps','CSV HEADER DELIMITER '';'' ');
\! ls /tmp/emaj_test/snaps

-- set/reset directory for log snaps
\! mkdir -p /tmp/emaj_test/log_snaps
\! rm -R /tmp/emaj_test/log_snaps
\! mkdir /tmp/emaj_test/log_snaps

-- group is unknown in emaj_group_def
select emaj.emaj_snap_log_group(NULL,NULL,NULL,NULL,NULL);
select emaj.emaj_snap_log_group('unknownGroup',NULL,NULL,NULL,NULL);

-- invalid start mark
select emaj.emaj_snap_log_group('myGroup2','unknownMark','EMAJ_LAST_MARK',NULL,NULL);

-- invalid end mark
select emaj.emaj_snap_log_group('myGroup2','','unknownMark',NULL,NULL);

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

-- invalid directory
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK',NULL,NULL);
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','unknown_directory',NULL);
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/unknown_directory',NULL);

-- invalid COPY TO options
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps', 'dummy_option');

-- SQL injection attempt
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps','; CREATE ROLE fake LOGIN PASSWORD '''' SUPERUSER');

-- should be ok
select emaj.emaj_snap_log_group('myGroup2',NULL,'EMAJ_LAST_MARK','/tmp/emaj_test/log_snaps',NULL);
select emaj.emaj_snap_log_group('myGroup2','','','/tmp/emaj_test/log_snaps','CSV');
select emaj.emaj_snap_log_group('myGroup2','Mark21',NULL,'/tmp/emaj_test/log_snaps','CSV HEADER');
select emaj.emaj_snap_log_group('myGroup2','Mark21','Mark21','/tmp/emaj_test/log_snaps','CSV');
select emaj.emaj_snap_log_group('myGroup2','Mark21','Mark23','/tmp/emaj_test/log_snaps',NULL);
\! ls /tmp/emaj_test/log_snaps |sed s/[0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9]/\[timestamp_mask\]/g

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

-- the tables group contains a table without pkey
select emaj.emaj_gen_sql_group('phil''s group#3",', NULL, NULL, '/tmp/emaj_test/sql_scripts/Group3');
select emaj.emaj_gen_sql_groups(array['myGroup1','phil''s group#3",'], NULL, NULL, '/tmp/emaj_test/sql_scripts/Group3');

-- invalid start mark
select emaj.emaj_gen_sql_group('myGroup2', 'unknownMark', NULL, NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'Mark11', NULL, NULL, NULL);

-- invalid end mark
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'unknownMark', NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', NULL, 'Mark11', NULL);

-- end mark is prior start mark
-- (mark timestamps are temporarily changed so that regression test can return a stable error message)
begin;
  update emaj.emaj_mark set mark_datetime = '2000-01-01 12:00:00+00' 
    where mark_group = 'myGroup2' and mark_name = 'Mark21';
  update emaj.emaj_mark set mark_datetime = '2000-01-01 13:00:00+00'
    where mark_group = 'myGroup2' and mark_name = 'Mark22';
  select emaj.emaj_gen_sql_group('myGroup2', 'Mark22', 'Mark21', NULL);
rollback;
begin;
  update emaj.emaj_mark set mark_datetime = '2000-01-01 12:00:00+00' where mark_name = 'Multi-2';
  update emaj.emaj_mark set mark_datetime = '2000-01-01 13:00:00+00' where mark_name = 'Multi-3';
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-3', 'Multi-2', NULL);
rollback;

-- start mark with the same name but that doesn't correspond to the same point in time
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Mark21', 'Multi-2', NULL);
  select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], NULL, 'Multi-2', NULL, NULL);

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

-- invalid location path name
select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, NULL);
select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/unknownDirectory/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, '/tmp/unknownDirectory/myFile');

-- should be ok (generated files content is checked later in adm2.sql scenario)
-- (getting counters from detailed log statistics + the number of sequences included in the group allows a comparison with the result of emaj_gen_sql_group function)
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

-- should be ok, with tables and sequences filtering
select * from emaj.emaj_detailed_log_stat_group('myGroup2',NULL,'EMAJ_LAST_MARK');
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
-- only 1 table
select emaj.emaj_gen_sql_group('myGroup2', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema2.mytbl4']);
-- several groups and 1 table of each, with redondancy in the tables array
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', 'Multi-3', '/tmp/emaj_test/sql_scripts/myFile', array[
     'myschema1.mytbl4','myschema2.mytbl4','myschema1.mytbl4','myschema2.mytbl4']);
\! grep 'only for' /tmp/emaj_test/sql_scripts/myFile
-----------------------------
-- emaj_verify_all() test
-----------------------------
-- should be OK
select * from emaj.emaj_verify_all();
-- detection of unattended tables in E-Maj schemas
begin;
  create table emaj.dummy1_log (col1 int);
  create table emaj.dummy2 (col1 int);
  create table emajb.emaj_dummy (col1 int);
  create table emaj.emaj_dummy (col1 int);               -- this one is not detected
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended functions in E-Maj schemas
begin;
  create function emaj.dummy1_log_fnct () returns int language sql as $$ select 0 $$;
  create function "emajC".dummy2_rlbk_fnct () returns int language sql as $$ select 0 $$;
  create function "emajC".dummy3_fnct () returns int language sql as $$ select 0 $$;
  create function emaj._dummy4_fnct () returns int language sql as $$ select 0 $$;      -- this one is not detected
  create function emaj.emaj_dummy5_fnct () returns int language sql as $$ select 0 $$;  -- this one is not detected
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended sequences in E-Maj schemas
begin;
  create table emaj.dummy1_log (col1 serial);
  create sequence emajb.dummy2_seq;
  create sequence emajb.dummy3_log_seq;
  create sequence emaj.emaj_dummy4_seq;                  -- this one is not detected
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended types in E-Maj schemas
begin;
  create type emaj.dummy1_type as (col1 int);
  create type emajb.dummy2_type as (col1 int);
  create type emajb.dummy3_type as (col1 int);
  create type emaj.emaj_dummy4_type as (col1 int);       -- this one is not detected
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended views in E-Maj schemas
begin;
  create view emaj.dummy1_view as select hist_id, hist_function, hist_event, hist_object from emaj.emaj_hist;
  create view emaj.dummy2_view as select hist_id, hist_function, hist_event, hist_object from emaj.emaj_hist;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended foreign tables in E-Maj schemas
-- (this only gives pertinent results with postgres 9.1+ version)
begin;
  create extension file_fdw;
  create foreign data wrapper file handler file_fdw_handler;
  create server file_server foreign data wrapper file;
  create foreign table emaj.dummy1_ftbl (ligne TEXT) server file_server options(filename '/tmp/emaj_test/log_snaps/_INFO');
  create foreign table emaj.dummy2_ftbl (ligne TEXT) server file_server options(filename '/tmp/emaj_test/log_snaps/_INFO');
  select * from emaj.emaj_verify_all();
rollback;
-- detection of unattended domains in E-Maj schemas
begin;
  create domain "emajC".dummy1_domain as int check (VALUE > 0);
  create domain "emajC".dummy2_domain as int check (VALUE > 0);
  select * from emaj.emaj_verify_all();
rollback;

-- tests on groups errors

-- detection of too old group
begin;
  update emaj.emaj_group set group_pg_version = '8.0.0' where group_name = 'myGroup1';
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing application schema
begin;
  drop schema myschema1 cascade;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing application relation
begin;
  drop table myschema1.mytbl4;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of relation type change (a table is now a sequence!)
begin;
  update emaj.emaj_relation set rel_kind = 'S' where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1';
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing E-Maj secondary schema
begin;
  drop schema emajb cascade;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing log trigger
begin;
  drop trigger myschema1_mytbl1_emaj_log_trg on myschema1.mytbl1;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing log function (and trigger)
begin;
  drop function emaj.myschema1_mytbl1_log_fnct() cascade;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing truncate trigger
begin;
  drop trigger myschema1_mytbl1_emaj_trunc_trg on myschema1.mytbl1;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a missing log table
begin;
  drop table emaj.myschema1_mytbl1_log;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a change in the application table structure (new column)
begin;
  alter table myschema1.mytbl1 add column newcol int;
  alter table myschema1.mytbl1 add column othernewcol text;
  alter table myschema1.mytbl2 add column newcol int;
  select * from emaj.emaj_verify_all();
rollback;
-- detection of a change in the application table structure (column type change)
begin;
  alter table myschema1.mytbl4 drop column col42;
  alter table myschema1.mytbl4 alter column col45 type varchar(15);
  select * from emaj.emaj_verify_all();
rollback;

-- all in 1
begin;
  create table emaj.dummy_log (col1 int);
  create function emaj.dummy_log_fnct () returns int language sql as $$ select 0 $$;
  create function emaj.dummy_rlbk_fnct () returns int language sql as $$ select 0 $$;
  update emaj.emaj_group set group_pg_version = '8.0.0' where group_name = 'myGroup1';
  drop trigger myschema1_mytbl1_emaj_log_trg on myschema1.mytbl1;
  drop function emaj.myschema1_mytbl1_log_fnct() cascade;
  drop table emaj.myschema1_mytbl1_log;
  alter table myschema1.mytbl1 add column newcol int;
  update emaj.emaj_relation set rel_kind = 'S' where rel_schema = 'myschema2' and rel_tblseq = 'mytbl1';
  select * from emaj.emaj_verify_all();
rollback;

--------------------------------
-- User errors and recovery tests 
--------------------------------
SET client_min_messages TO WARNING;

-- cases when an application table is altered
begin;
  alter table myschema2.mytbl4 add column newcol int;
-- setting a mark or rollbacking fails
  savepoint sp1;
    select emaj.emaj_set_mark_group('myGroup2','dummyMark');
  rollback to savepoint sp1;
    select emaj.emaj_rollback_group('myGroup2','EMAJ_LAST_MARK');
  rollback to savepoint sp1;
-- but it is possible to stop, drop and recreate the group
  select emaj.emaj_stop_group('myGroup2');
  savepoint sp2;
    select emaj.emaj_drop_group('myGroup2');
    select emaj.emaj_create_group('myGroup2');
  rollback to savepoint sp2;
-- or stop and alter the group
  select emaj.emaj_alter_group('myGroup2');
rollback;

-- cases when an application table is dropped
begin;
  drop table myschema2.mytbl4;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- the only solution is to change the emaj_group_def table, force the group's stop and recreate or alter the group
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl4';
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_drop_group('myGroup2');
  select emaj.emaj_create_group('myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when a log trigger on an application table is dropped
begin;
  drop trigger myschema2_mytbl4_emaj_log_trg on myschema2.mytbl4;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- the only solution is to change the emaj_group_def table, force the group's stop and recreate or alter the group
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl4';
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_alter_group('myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when an application sequence is dropped
begin;
  drop sequence myschema2.mySeq1;
-- setting a mark or stopping the group fails
-- the only solution is to change the emaj_group_def table, force the group's stop and recreate or alter the group
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myseq1';
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_alter_group('myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when an application schema is dropped
begin;
  drop schema myschema2 cascade;
-- stopping group fails
  savepoint sp1;
    select emaj.emaj_stop_group('myGroup2');
  rollback to savepoint sp1;
-- the only solution is to force the group's stop and drop the group
  select emaj.emaj_force_stop_group('myGroup2');
  select emaj.emaj_drop_group('myGroup2');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-- cases when non E-Maj related objects are stored in emaj secondary schemas
begin;
  create sequence emajb.dummySeq;
-- dropping group fails at secondary schema drop step
  select emaj.emaj_stop_group('myGroup1');
  savepoint sp1;
    select emaj.emaj_drop_group('myGroup1');
  rollback to savepoint sp1;
-- use emaj_verify_all() to understand the problem
  select * from emaj.emaj_verify_all();
-- use emaj_force_drop_group to solve the problem
  select emaj.emaj_force_drop_group('myGroup1');
-- and everything is clean...
  select * from emaj.emaj_verify_all();
rollback;

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 6000;
alter sequence emaj.emaj_mark_mark_id_seq restart 600;
alter sequence emaj.emaj_sequence_sequ_id_seq restart 6000;
alter sequence emaj.emaj_seq_hole_sqhl_id_seq restart 600;
alter sequence emaj.emaj_global_seq restart 100000;

