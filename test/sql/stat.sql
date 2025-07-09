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
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark21',NULL)
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark21','EMAJ_LAST_MARK')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark22')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','Mark22','Mark23')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','EMAJ_LAST_MARK','')
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

-- warning on marks range too wide to be contained by a single log session
select emaj.emaj_stop_group('myGroup4', 'myGroup4_stop');
select emaj.emaj_start_group('myGroup4', 'myGroup4_restart', false);
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup4','myGroup4_start','myGroup4_restart')
  order by stat_group, stat_schema, stat_table;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup4','myGroup4_start','')
  order by stat_group, stat_schema, stat_table;

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

