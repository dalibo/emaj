-- stat.sql : test statistics functions
--   emaj_log_stat_group(), emaj_log_stat_groups()
--   emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups(),
--   emaj_sequence_stat_group() and emaj_sequence_stat_groups()
--   _get_sequences_last_value()
--

-- set sequence restart value
select public.handle_emaj_sequences(5000);

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
select * from emaj.emaj_log_stat_group(null,null,null);
select * from emaj.emaj_log_stat_groups(array['unknownGroup'],null,null);
select * from emaj.emaj_detailed_log_stat_group(null,null,null);
select * from emaj.emaj_detailed_log_stat_groups(array['unknownGroup'],null,null);

-- invalid marks
select * from emaj.emaj_log_stat_group('myGroup2',null,null);
select * from emaj.emaj_log_stat_group('myGroup2','',null);
select * from emaj.emaj_log_stat_group('myGroup2','dummyStartMark',null);
select * from emaj.emaj_log_stat_group('myGroup2','Mark22','dummyEndMark');
select * from emaj.emaj_detailed_log_stat_group('myGroup2','dummyStartMark',null);
select * from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','dummyEndMark');

select * from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],null,null);
select * from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','dummyEndMark');
select * from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],null,null);
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
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark21',null)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1',null)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21',null)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_table;

select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1',null)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_table;

-- empty group
select * from emaj.emaj_log_stat_group('emptyGroup','SM2',null);
select * from emaj.emaj_detailed_log_stat_group('emptyGroup','SM2',null);

-- warning on marks range too wide to be contained by a single log session
select emaj.emaj_stop_group('myGroup4', 'myGroup4_stop');
select emaj.emaj_start_group('myGroup4', 'myGroup4_restart', false);
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup4','myGroup4_start','myGroup4_restart')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  from emaj.emaj_log_stat_group('myGroup4','myGroup4_start','')
  order by stat_group, stat_schema, stat_table;

-----------------------------
-- emaj_sequence_stat_group(), emaj_sequence_stat_groups() test
-----------------------------

-- group is unknown
select * from emaj.emaj_sequence_stat_group('dummy', null, null);

-- start mark is null or unknown
select * from emaj.emaj_sequence_stat_group('myGroup1', null, null);
select * from emaj.emaj_sequence_stat_groups(ARRAY['myGroup1'], 'dummy', null);

-- end mark is unknown
select * from emaj.emaj_sequence_stat_group('myGroup1', 'EMAJ_LAST_MARK', 'dummy');

-- end mark is prior start mark (not tested as this is the same piece of code as for emaj_log_stat_group()

-- empty group
select * from emaj.emaj_sequence_stat_group('emptyGroup','SM2',null);

-- should be ok
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark21',null)
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_group('myGroup2','EMAJ_LAST_MARK','')
  order by stat_group, stat_schema, stat_sequence;

select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_groups(array['myGroup1','myGroup2'],'Multi-1',null)
  order by stat_group, stat_schema, stat_sequence;
select stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
  from emaj.emaj_sequence_stat_groups(array['myGroup1','myGroup2'],'Multi-1','Multi-3')
  order by stat_group, stat_schema, stat_sequence;

----------------------------------------------------------
-- emaj_log_stat_table() and emaj_log_stat_sequence() test
----------------------------------------------------------
select emaj.emaj_set_mark_group('myGroup2', 'before_log_stat_tblseq');

-- Test errors with input parameters
-- schema is unknown
select * from emaj.emaj_log_stat_table('dummy', 'mytbl1');
select * from emaj.emaj_log_stat_sequence('dummy', 'mytbl1');

-- table/sequence is unknown
select * from emaj.emaj_log_stat_table('myschema2', null);
select * from emaj.emaj_log_stat_table('myschema2', 'dummy', 'myGroup1', 'M1');
select * from emaj.emaj_log_stat_sequence('myschema2', null);
select * from emaj.emaj_log_stat_sequence('myschema2', 'dummy', 'myGroup1', 'M1');

-- bad timestamp or marks interval
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', '2030/01/01'::TIMESTAMPTZ, '2020/01/01'::TIMESTAMPTZ);
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', '2030/01/01'::TIMESTAMPTZ, '2020/01/01'::TIMESTAMPTZ);
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23', 'myGroup2', 'Mark22');
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23', 'myGroup2', 'Mark22');

-- bad group or mark names
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'dummy_group', 'M1', 'myGroup2', 'M3');
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'dummy_mark', 'myGroup2', 'M3');
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', null);
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'M1', 'dummy_group', 'M3');
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'M1', 'myGroup2', 'dummy_mark');

select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'dummy_group', 'M1', 'myGroup2', 'M3');
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'dummy_mark', 'myGroup2', 'M3');
select * from emaj.emaj_log_stat_sequence('myschema2', 'mytbl1', 'myGroup2', null);
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'M1', 'dummy_group', 'M3');
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'M1', 'myGroup2', 'dummy_mark');

-- Should be OK

-- no bounds
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1');

-- limit the timeframe with marks range
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23');

select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', null, null, 'myGroup2', 'Mark22');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', null, null, 'myGroup2', 'Mark22');

select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark23');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark23');

-- marks range from another group
select emaj.emaj_set_mark_group('emptyGroup', 'log_stat_1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'emptyGroup', 'log_stat_1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'emptyGroup', 'log_stat_1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'emptyGroup', 'log_stat_1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'emptyGroup', 'log_stat_1');

-- Same mark as lower and upper bounds
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark22');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark22');

-- No data on the requested time or marks interval
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', '2020/01/01'::TIMESTAMPTZ, '2020/01/02'::TIMESTAMPTZ);
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', '2020/01/01'::TIMESTAMPTZ, '2020/01/02'::TIMESTAMPTZ);
select * from emaj.emaj_log_stat_table('myschema2', 'mytbl1', null, null, 'emptyGroup', 'SM2');    -- very old upper bound mark
select * from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', null, null, 'emptyGroup', 'SM2');

-- limit the timeframe with timestamps
begin transaction;
  select emaj.emaj_set_mark_group('myGroup2', 'tmp_mark');
  insert into myTbl1 select i, 'TMP', 'TMP' from generate_series (100000,100005) AS i;
  select nextval('myschema2.myseq1');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
      from emaj.emaj_log_stat_table('myschema2', 'mytbl1', transaction_timestamp(), null);
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
      from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', transaction_timestamp(), null);

-- Same timestamp as lower and upper bounds
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
      from emaj.emaj_log_stat_table('myschema2', 'mytbl1', transaction_timestamp(), transaction_timestamp());
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
      from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', transaction_timestamp(), transaction_timestamp());
commit;

-- perform rollbacks
--   a logged rollback ...
select * from emaj.emaj_logged_rollback_group('myGroup2', 'Multi-1');

select emaj.emaj_rename_mark_group('myGroup2', emaj.emaj_get_previous_mark_group('myGroup2', 'EMAJ_LAST_MARK'), 'RLBK_START');
select emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'RLBK_DONE');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1');

--    ... to consolidate
begin transaction;
  select * from emaj.emaj_consolidate_rollback_group('myGroup2', 'RLBK_DONE');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
      from emaj.emaj_log_stat_table('myschema2', 'mytbl1');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
      from emaj.emaj_log_stat_sequence('myschema2', 'myseq1');
rollback;

--   an unlogged rollback
select * from emaj.emaj_rollback_group('myGroup2', 'RLBK_DONE');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23');

-- delete an intermediate mark (the mark and table/sequence state are deleted)
select emaj.emaj_delete_mark_group('myGroup2', 'tmp_mark');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
    from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'before_log_stat_tblseq', 'myGroup2', 'RLBK_START');
select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'before_log_stat_tblseq', 'myGroup2', 'RLBK_START');

-- perform group stop and restart
begin transaction;
  select emaj.emaj_stop_group('myGroup2', 'stop');
  select emaj.emaj_start_group('myGroup2', 'restart', false);
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
      from emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Multi-3');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
      from emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Multi-3');

-- delete oldest marks and marks at log session boundaries
  select emaj.emaj_set_mark_group('myGroup2', '1 after start');
  select emaj.emaj_delete_before_mark_group('myGroup2', 'before_log_stat_tblseq');
  select emaj.emaj_delete_mark_group('myGroup2', 'stop');
  select emaj.emaj_delete_mark_group('myGroup2', 'restart');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_changes, stat_rollbacks
      from emaj.emaj_log_stat_table('myschema2', 'mytbl1');
  select stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
      from emaj.emaj_log_stat_sequence('myschema2', 'myseq1');
rollback;

select * from emaj.emaj_rollback_group('myGroup2', 'before_log_stat_tblseq');
select * from emaj.emaj_delete_mark_group('myGroup2', 'before_log_stat_tblseq');

-----------------------------
-- _get_sequences_last_value() test
-----------------------------
-- get all groups, tables and sequences
select * from emaj._get_sequences_last_value(null, null, null, null, null, null)
  where p_key <> 'current_epoch' order by 1;
-- filter on groups
select p_key from emaj._get_sequences_last_value('Group', null, null, null, null, null) order by 1;
select p_key from emaj._get_sequences_last_value(null, 'Group', null, null, null, null) order by 1;
-- filter on tables and sequences
select p_key from emaj._get_sequences_last_value(null, null, 'tbl2|tbl4', '2b', 'col', 'myschema2') order by 1;


-- check for statistics functions family
select hist_id, hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user
  from (select * from emaj.emaj_hist where hist_id >= 5000 order by hist_id) as t;
