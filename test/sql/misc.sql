-- misc.sql : test miscellaneous functions
--   emaj_reset_group(), 
--   emaj_log_stat_group(), emaj_log_stat_groups(), emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups(),
--   emaj_sequence_stat_group() and emaj_sequence_stat_groups()
--   emaj_estimate_rollback_group() and emaj_estimate_rollback_groups(),
--   emaj_snap_group(),
--   emaj_get_current_log_table(),
--   emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group(),
--   emaj_gen_sql_group() and emaj_gen_sql_groups(),
--   table reclustering,
--   emaj_import_parameters_configuration() and emaj_export_parameters_configuration(),
--   emaj_purge_histories().
--

-- set sequence restart value
select public.handle_emaj_sequences(5000);

-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/misc'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR
   
-----------------------------
-- emaj_reset_group() test
-----------------------------

-- group is unknown
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

-- start myGroup1
select emaj.emaj_start_group('myGroup1','Mark21');
-----------------------------
-- log updates on myschema2 between 3 mono-group and multi-groups marks
-----------------------------
set search_path=public,myschema2;

-- set a multi-groups mark
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-1');

-- inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger) and increment myseq1
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,10100) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 500;
delete from myTbl1 where col11 > 10000;
insert into myTbl2 select i, 'DEF', current_date from generate_series (1,900) as i;
select nextval('myschema2.myseq1');
select nextval('myschema2.myseq1');

-- set marks
select emaj.emaj_set_mark_group('myGroup2','Mark22');
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-2');

-- inserts/updates/deletes in myTbl3 and myTbl4and increment and alter myseq1
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl4 select i,'FK...',i,1,'ABC' from generate_series (1,100) as i;
select nextval('myschema2.myseq1');
alter sequence myschema2.myseq1 MAXVALUE 10000;

-- set marks
select emaj.emaj_set_mark_group('myGroup2','Mark23');
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'Multi-3');

-- reset the sequence alter
alter sequence myschema2.myseq1 MAXVALUE 2000;

-----------------------------
-- emaj_log_stat_group(), emaj_log_stat_groups(), emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups() test
-----------------------------
-- group is unknown
select * from emaj.emaj_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_log_stat_groups(array['unknownGroup'],NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group(NULL,NULL,NULL);
select * from emaj.emaj_detailed_log_stat_groups(array['unknownGroup'],NULL,NULL);

-- invalid marks
select * from emaj.emaj_log_stat_group('myGroup2',NULL,NULL);
select * from emaj.emaj_log_stat_group('myGroup2','',NULL);
select * from emaj.emaj_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_log_stat_group('myGroup2','Mark22','dummyEndMark');
select * from emaj.emaj_detailed_log_stat_group('myGroup2','dummyStartMark',NULL);
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','dummyEndMark');

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
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
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
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
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
select * from emaj.emaj_log_stat_group('emptyGroup','SM2',NULL);
select * from emaj.emaj_detailed_log_stat_group('emptyGroup','SM2',NULL);

-----------------------------
-- emaj_sequence_stat_group(), emaj_sequence_stat_groups() test
-----------------------------

-- group is unknown
SELECT * from emaj.emaj_sequence_stat_group('dummy', NULL, NULL);

-- start mark is null or unknown
SELECT * from emaj.emaj_sequence_stat_group('myGroup1', NULL, NULL);
SELECT * from emaj.emaj_sequence_stat_groups(ARRAY['myGroup1'], 'dummy', NULL);

-- end mark is unknown
SELECT * from emaj.emaj_sequence_stat_group('myGroup1', 'EMAJ_LAST_MARK', 'dummy');

-- end mark is prior start mark (not tested as this is the same piece of code as for emaj_log_stat_group()

-- empty group
select * from emaj.emaj_sequence_stat_group('emptyGroup','SM2',NULL);

-- should be ok
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_sequence;

select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_groups(array['myGroup1','myGroup2'],'Multi-1',NULL)
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_last_mark, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_sequence;

-- check for emaj_reset_group() and emaj_log_stat_groups(), emaj_detailed_log_stat_group() and emaj_sequence_stat_group() functions family
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5000 order by hist_id) as t;

-- set sequence restart value
select public.handle_emaj_sequences(5200);

-----------------------------
-- emaj_estimate_rollback_group() and emaj_estimate_rollback_groups() tests
--
-- When emaj is not created as an extension - i.e. is created with the psql script - the estimate durations are different.
-- This is normal as the FK are not processed the same way.
-----------------------------

-- group is unknown
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
-- should return 0.0205 sec
  select emaj.emaj_unprotect_mark_group('myGroup2','EMAJ_LAST_MARK');
  select emaj.emaj_unprotect_group('myGroup2');
rollback;

select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 1.4086 sec (or 1.4291 sec is emaj is not an extension)

-- estimates with empty rollback statistics but temporarily modified parameters
begin;
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','150 microsecond'::interval);
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','12 microsecond'::interval);
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_fkey_check_duration','27 microsecond'::interval);
  UPDATE emaj.emaj_param SET param_value_interval = '7 millisecond'::interval WHERE param_key = 'fixed_step_rollback_duration';
  INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_dblink_rollback_duration','2.5 millisecond'::interval);
  select emaj.emaj_estimate_rollback_groups('{"myGroup2"}','Mark21',TRUE);
-- should return 1.814 sec (or 1.8285 sec is emaj is not an extension)
rollback;

-- estimate with added rollback statistics about fkey drops, recreations and checks, and rollback sequences
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
insert into emaj.emaj_rlbk_stat values
  ('RLBK_SEQUENCES','','','',1,2,'0.003 SECONDS'::interval);

select emaj.emaj_estimate_rollback_group('myGroup2','Mark21',FALSE);
-- should return 1.4071 sec (or 1.435306 sec is emaj is not an extension)

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
-- should return 2.275704 sec (or 2.30391 sec is emaj is not an extension)

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
-- should return 2.643791 sec (or 2.668997 sec is emaj is not an extension)

-- estimate with 2 groups and a SET_FK_DEF step
vacuum analyze myschema1.mytbl4;
select reltuples from pg_class, pg_namespace where relnamespace=pg_namespace.oid and relname = 'mytbl4' and nspname = 'myschema1';
begin;
-- temporarily insert new rows into myTbl4 of myschema1
  insert into myschema1.myTbl4 select i,'FK...',2,1,'ABC' from generate_series (10,20) as i;
  select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Multi-1',FALSE);
-- should return 2.675001 sec (or 2.730913 sec is emaj is not an extension)
rollback;

-- delete all manualy inserted rollback statistics, cleanup the statistics table and recreate its foreign key
delete from emaj.emaj_rlbk_stat;
vacuum emaj.emaj_rlbk_stat;
alter table emaj.emaj_rlbk_stat add FOREIGN KEY (rlbt_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);

-- check for emaj_estimate_rollback_group()
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5200 order by hist_id) as t;

-- set sequence restart value
select public.handle_emaj_sequences(5300);

-----------------------------
-- emaj_snap_group() tests
-----------------------------

-- group is unknown
select emaj.emaj_snap_group(NULL,NULL,NULL);
select emaj.emaj_snap_group('unknownGroup',NULL,NULL);

-- invalid directory
select emaj.emaj_snap_group('myGroup1',NULL,NULL);
select emaj.emaj_snap_group('myGroup1','unknown_directory',NULL);
select emaj.emaj_snap_group('myGroup1','/unknown_directory',NULL);

-- invalid COPY TO options
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR','dummy_option');

-- SQL injection attempt
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR','; CREATE ROLE fake LOGIN PASSWORD '''' SUPERUSER');

-- should be OK (even when executed twice, files being overwriten)
select emaj.emaj_snap_group('emptyGroup',:'EMAJTESTTMPDIR','');
\! ls $EMAJTESTTMPDIR
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR','');
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR','CSV HEADER DELIMITER '';'' ');
\! ls $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- emaj_get_current_log_table() tests
-----------------------------
-- not found
select * from emaj.emaj_get_current_log_table('myschema1', 'dummy_table');
-- found
select * from emaj.emaj_get_current_log_table('myschema1', 'mytbl1');
select 'select count(*) from ' || quote_ident(log_schema) || '.' || quote_ident(log_table)
  from emaj.emaj_get_current_log_table('myschema1','mytbl1');

-- check for emaj_snap_group()
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5300 order by hist_id) as t;

-- set sequence restart value
select public.handle_emaj_sequences(5400);

-----------------------------
-- emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group() tests
-----------------------------

--
-- Test errors with input parameters
--

-- group is unknown
select emaj.emaj_gen_sql_dump_changes_group('dummy', NULL, NULL, NULL, NULL);

-- start mark is null or unknown
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', NULL, NULL, NULL, NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'dummy', NULL, NULL, NULL);

-- end mark is null or unknown
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', NULL, NULL, NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'dummy', NULL, NULL);

-- end mark is prior start mark
-- just check the error is trapped, because the error message contents timestamps
do language plpgsql
$$
begin
  begin
    perform emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark23', 'Mark21', NULL, NULL);
    return;
  exception when raise_exception then
    raise notice 'Error trapped on emaj_gen_sql_dump_changes_group() call';
  end;
end;
$$;

-- invalid options
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER = dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'COLS_ORDER = PK', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=()', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION = dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION = FULL', NULL);
select emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION = partial', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = ()', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = (emaj_tuple , dummy)', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'NO_EMPTY_FILES', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'Tables_Only, Sequences_Only', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY = dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'ORDER_BY = pk', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SQL_FORMAT = dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), SQL_FORMAT=PRETTY', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=dummy', NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=(format csv)', NULL);
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=dummy', NULL, :'EMAJTESTTMPDIR');
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=()', NULL, :'EMAJTESTTMPDIR');
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=dummy', NULL, :'EMAJTESTTMPDIR');
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=(dummy)', NULL, :'EMAJTESTTMPDIR');
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=(;)', NULL, :'EMAJTESTTMPDIR');

-- invalid relations array
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY['dummy']);

-- invalid output location
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, NULL);
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, 'dummy');
select emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, '/dummy');
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, NULL);
select emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, 'dummy');

--
-- various options influencing the generated SQL
--

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', '', NULL);
SELECT * FROM emaj_temp_sql;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=NONE,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=PARTIAL,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=FULL,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=ALL,CONSOLIDATION=FULL,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=MIN,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=(emaj_tuple, emaj_gid, emaj_txid),SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY=PK,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY = TIME,
																			 Consolidation = partial,
																			 Sql_Format = Pretty', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1','myseq1') and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER=LOG_TABLE,CONSOLIDATION=FULL,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq = 'mytbl1' and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER=PK,SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq = 'mytbl1' and sql_line_number >= 1 order by sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SEQUENCES_ONLY', NULL);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 group by sql_rel_kind ORDER BY sql_rel_kind;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'TABLES_ONLY', NULL);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 group by sql_rel_kind ORDER BY sql_rel_kind;

-- test the tables/sequences array filter
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY[]::TEXT[]);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY[NULL, '']::TEXT[]);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY['myschema2.mytbl1', 'myschema2.myseq1']);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 group by 1;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SEQUENCES_ONLY', ARRAY['myschema2.mytbl1']);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'TABLES_ONLY', ARRAY['myschema2.myseq1']);

-- test output as a sql/psql script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SQL_FORMAT=PRETTY', ARRAY['myschema2.mytbl1', 'myschema2.myseq1'],
											:'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || ')', ARRAY['myschema2.mytbl1', 'myschema2.myseq1'],
											:'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23',
											'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), PSQL_COPY_OPTIONS=(format csv, delimiter '';'', header)',
											ARRAY['myschema2.mytbl1', 'myschema2.myseq1'], :'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
\! rm $EMAJTESTTMPDIR/*

-- Perform logged changes

select emaj.emaj_set_mark_group('myGroup1','Dump_changes_tests_M1');

-- Build the base content of the test table
UPDATE myschema1.myTbl4 SET col42 = 'Initial row' WHERE col41 = 1 and col43 = 1;
INSERT INTO myschema1.myTbl4
  SELECT i, 'Initial row', 1, 1, 'ABC' FROM generate_series(2,9) i;

select emaj.emaj_set_mark_group('myGroup1','Dump_changes_tests_M2');

-- Record changes

INSERT INTO myschema1.myTbl4 VALUES (10, 'Inserted row', 1, 1, 'ABC');

DELETE FROM myschema1.myTbl4 WHERE col41 = 1 and col43 = 1;

UPDATE myschema1.myTbl4 SET col43 = 2, col42 = 'PK col changed' WHERE col41 = 2 and col43 = 1;

UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed' WHERE col41 = 3 and col43 = 1;

UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed' WHERE col41 = 4 and col43 = 1;
UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed twice' WHERE col41 = 4 and col43 = 1;

UPDATE myschema1.myTbl4 SET col44 = 1 WHERE col41 = 5 and col43 = 1;  -- actually no change

select emaj.emaj_set_mark_group('myGroup1','Dump_changes_tests_M3');

INSERT INTO myschema1.myTbl4 VALUES (11, 'Inserted and deleted row', 2, 1, 'ABC');
DELETE FROM myschema1.myTbl4 WHERE col41 = 11 and col43 = 2;

DELETE FROM myschema1.myTbl4 WHERE col41 = 6 and col43 = 1;
INSERT INTO myschema1.myTbl4 VALUES (6, 'Deleted and inserted row', 1, 1, 'ABC');

DELETE FROM myschema1.myTbl4 WHERE col41 = 7 and col43 = 1;
INSERT INTO myschema1.myTbl4 VALUES (7, 'Initial row', 1, 1, 'ABC');    -- totaly unchanged row

INSERT INTO myschema1.myTbl4 VALUES (12, 'Inserted, updated and deleted row', 2, 1, 'ABC');
UPDATE myschema1.myTbl4 SET col44 = 2 WHERE col41 = 12 and col43 = 2;
DELETE FROM myschema1.myTbl4 WHERE col41 = 12 and col43 = 2;

SELECT nextval('myschema1."myTbl3_col31_seq"');

select * from emaj.emaj_set_mark_group('myGroup1','Dump_changes_tests_M4');

-- Directly dump changes
SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M3',
									'NO_EMPTY_FILES, EMAJ_COLUMNS=MIN',
									NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

CREATE EXTENSION adminpack;
SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M3', 'Dump_changes_tests_M4',
									'NO_EMPTY_FILES, COPY_OPTIONS=(format csv, header), EMAJ_COLUMNS=(emaj_verb,emaj_tuple,emaj_gid)',
									NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! grep -v '  started at ' $EMAJTESTTMPDIR/_INFO
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*
DROP EXTENSION adminpack;

SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
									'CONSOLIDATION=FULL, COPY_OPTIONS=(format csv, delimiter '';'', force_quote *, header)',
                                    NULL, :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
									'CONSOLIDATION=PARTIAL',
                                    NULL, :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

-- Dump changes for tables with unusual data types and consolidation.

SELECT emaj.emaj_dump_changes_group('myGroup2', 'Multi-2', 'Multi-3',
									'CONSOLIDATION=PARTIAL',
                                    '{"myschema2.mytbl5", "myschema2.mytbl6"}', :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup2', 'Multi-2', 'Multi-3',
									'CONSOLIDATION=FULL',
                                    '{"myschema2.mytbl5", "myschema2.mytbl6"}', :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

-- Generate SQL statements with the PSQL_COPY_DIR option and execute the generated script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
											'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), CONSOLIDATION=FULL',
											NULL, :'EMAJTESTTMPDIR' || '/sql_script');
\i :EMAJTESTTMPDIR/sql_script
\! ls -1sS $EMAJTESTTMPDIR
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

select * from emaj.emaj_rollback_group('myGroup1','Dump_changes_tests_M1');
select emaj.emaj_cleanup_rollback_state();
select emaj.emaj_delete_mark_group('myGroup1','EMAJ_LAST_MARK');

-- Test using quotes in schema, table or group names
SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=NONE', NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! grep -v '  started at ' $EMAJTESTTMPDIR/_INFO
\! cat $EMAJTESTTMPDIR/"phil's_schema3_myTbl2__col21_seq.changes"

SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=PARTIAL', '{"phil''s schema3.phil''s tbl1"}', :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=FULL', '{"phil''s schema3.phil''s tbl1"}', :'EMAJTESTTMPDIR');
\! rm $EMAJTESTTMPDIR/*

-- Checks for emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group()
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5400 order by hist_id) as t;

-- set sequence restart value
select public.handle_emaj_sequences(5500);

-----------------------------
-- emaj_gen_sql_group() and emaj_gen_sql_groups() tests
-----------------------------

-- group is unknown
select emaj.emaj_gen_sql_group(NULL, NULL, NULL, NULL);
select emaj.emaj_gen_sql_group('unknownGroup', NULL, NULL, NULL, NULL);

select emaj.emaj_gen_sql_groups(NULL, NULL, NULL, NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","unknownGroup"}', NULL, NULL, NULL);

-- invalid start mark
select emaj.emaj_gen_sql_group('myGroup2', 'unknownMark', NULL, NULL);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'Mark11', NULL, NULL, NULL);

-- invalid end mark
select emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', 'unknownMark', NULL);
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

-- end mark with the same name but that doesn't correspond to the same point in time
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', 'Mark21', NULL);

-- empty table/sequence names array
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array['']);

-- unknown table/sequence names in the tables filter
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile', array['foo']);
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema1.mytbl1','myschema2.myTbl3_col31_seq','phil''s schema3.phil''s tbl1']);
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema1.mytbl1','foo','myschema2.myTbl3_col31_seq','phil''s schema3.phil''s tbl1']);

-- the tables group contains a table without pkey
select emaj.emaj_gen_sql_group('phil''s group#3",', 'Mark4', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/Group3');

-- invalid location path name
select emaj.emaj_gen_sql_group('myGroup1', 'Mark21', NULL, '/tmp/unknownDirectory/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, '/tmp/unknownDirectory/myFile');

-- should be ok
-- generated files content is checked later in adm2.sql scenario
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile');
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'Mark22', :'EMAJTESTTMPDIR' || '/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myFile');
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-2', 'Multi-3', :'EMAJTESTTMPDIR' || '/myFile');
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile');

-- should be ok with no output file supplied
select emaj.emaj_gen_sql_group('myGroup1', 'Mark21', NULL, NULL);
\copy (select * from emaj_sql_script) to '/dev/null'
drop table emaj_temp_script cascade;

-- should be ok, with tables and sequences filtering
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK');
-- all tables and sequences
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1','myschema2.myTbl3_col31_seq']);
-- minus 1 sequence
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3','myschema2.mytbl4',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1']);
-- minus 1 table
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema2.mytbl1','myschema2.mytbl2','myschema2.myTbl3',
     'myschema2.mytbl5','myschema2.mytbl6','myschema2.myseq1']);
-- only 1 sequence
select emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema2.myTbl3_col31_seq']);
-- only 1 table (with a strange name and belonging to a group having another table without pkey)
select emaj.emaj_gen_sql_group('phil''s group#3",', 'Mark4', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', array[
     'phil''s schema3.phil''s tbl1']);
-- several groups and 1 table of each, with redondancy in the tables array
select emaj.emaj_gen_sql_groups(array['myGroup1','myGroup2'], 'Multi-1', 'Multi-3', :'EMAJTESTTMPDIR' || '/myFile', array[
     'myschema1.mytbl4','myschema2.mytbl4','myschema1.mytbl4','myschema2.mytbl4']);
\! grep 'only for' $EMAJTESTTMPDIR/myFile

-- generate a sql script after a table structure change but on a time frame prior the change
begin;
  select emaj.emaj_set_mark_group('myGroup2','Test sql generation');
  insert into mySchema2.myTbl2 values(1000,'Text','01/01/2000');
  update mySchema2.myTbl2 set col22 = 'New text' where col21 = 1000;
  delete from mySchema2.myTbl2 where col21 = 1000;
  select emaj.emaj_remove_table('myschema2', 'mytbl2','Before ALTER mytbl2');
  alter table mySchema2.myTbl2 rename column col21 to id;
  select emaj.emaj_gen_sql_group('myGroup2', 'Test sql generation', 'Before ALTER mytbl2', :'EMAJTESTTMPDIR' || '/altered_tbl.sql');
rollback;
-- comment transaction commands and mask the timestamp in the initial comment for the need of the current test
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'
-- and execute the generated script
\set SQLSCRIPT :EMAJTESTTMPDIR '/altered_tbl.sql'
begin;
  \i :SQLSCRIPT
rollback;

\! rm $EMAJTESTTMPDIR/*

-- check for emaj_gen_sql_group()
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5500 order by hist_id) as t;

-- set sequence restart value
select public.handle_emaj_sequences(5600);

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
-- emaj_export_parameters_configuration() and emaj_import_parameters_configuration() tests
-----------------------------
-- direct export
--   ok
select json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

-- export in file
--   error
select emaj.emaj_export_parameters_configuration('/tmp/dummy/location/file');
--   ok
select emaj.emaj_export_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config');
\! wc -l $EMAJTESTTMPDIR/orig_param_config

-- direct import
--   error
--     no "parameters" array
select emaj.emaj_import_parameters_configuration('{ "dummy_json": null }'::json);
--     unknown attributes
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "emaj_version", "unknown_attribute_1": null, "unknown_attribute_2": null} ] }'::json);
--     missing or null "key" attributes
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "value": "no_key"} ] }'::json);
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": null} ] }'::json);
--     invalid key
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "unknown_param" } ] }'::json);
--     duplicate key
select emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention" }, { "key": "history_retention" } ] }'::json);

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
\! echo 'not a json content' >$EMAJTESTTMPDIR/bad_param_config
select emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');
\! echo '{ "dummy_json": null }' >$EMAJTESTTMPDIR/bad_param_config
select emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');
\! echo '{ "parameters": [ { "key": "bad_key", "value": null} ] }' >$EMAJTESTTMPDIR/bad_param_config
select emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');

--   ok
select emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config', true);
select json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

select emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config', false);

-----------------------------
-- emaj_purge_histories() tests
-----------------------------
select emaj.emaj_purge_histories(NULL);
select emaj.emaj_purge_histories('0 SECOND');

-- check for emaj_import_parameters_configuration() and emaj_purge_histories()
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5600 order by hist_id) as t;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
