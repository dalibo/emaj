-- alter.sql : tests groups structure changes with groups in IDLE state
-- test emaj_remove_table(), emaj_remove_tables(), emaj_remove_sequence(), emaj_remove_sequences(),
--      emaj_move_table(), emaj_move_tables(), emaj_move_sequence(), emaj_move_sequences(),
--      emaj_modify_table() and emaj_modify_tables() functions
--

-- define the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/alter'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

-- set sequence restart value
select public.handle_emaj_sequences(8000);

-----------------------------
-- stop, reset and drop and recreate groups
-----------------------------
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_reset_group('myGroup1');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup2');
select emaj.emaj_stop_group('phil''s group#3",','Simple stop mark');
select emaj.emaj_drop_group('phil''s group#3",');
select emaj.emaj_force_stop_group('myGroup4');
select emaj.emaj_drop_group('myGroup4');
select emaj.emaj_force_stop_group('emptyGroup');
select emaj.emaj_drop_group('emptyGroup');
select emaj.emaj_create_group('myGroup1');

-- rebuild all original groups
SET client_min_messages TO WARNING;
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', array['myGroup1','myGroup2','emptyGroup','myGroup4','myGroup5'], true);
-- add a table without PK to the audit_only group
select emaj.emaj_assign_table('phil''s schema"3', 'myTbl2\', 'myGroup5');

RESET client_min_messages;

-----------------------------------
-- emaj_remove_table
-----------------------------------

-- error cases
-- missing table
select emaj.emaj_remove_table('dummySchema','mytbl1');
select emaj.emaj_remove_table('myschema1','dummyTable');
-- table not in a group
select emaj.emaj_remove_table('myschema6','table_with_50_characters_long_name_____0_________0');
-- bad relation kind
select emaj.emaj_remove_table('myschema1','mytbl2b_col20_seq');
-- bad mark
select emaj.emaj_remove_table('myschema1','mytbl1','EMAJ_LAST_MARK');

-- ok
select emaj.emaj_remove_table('myschema1','mytbl1');

-----------------------------------
-- emaj_remove_tables with array
-----------------------------------
-- error cases
-- empty tables array
select emaj.emaj_remove_tables('myschema1',array[]::text[]);
select emaj.emaj_remove_tables('myschema1',null);
select emaj.emaj_remove_tables('myschema1',array['']);
-- missing table
select emaj.emaj_remove_tables('myschema1',array['dummyTable','mytbl1','mytbl2']);

-- ok (with a duplicate table name)
select emaj.emaj_remove_tables('myschema1',array['mytbl2','mytbl2b','mytbl2']);

-----------------------------------
-- emaj_remove_tables with filters
-----------------------------------
-- bad schema
select emaj.emaj_remove_tables('dummySchema',null,null);

-- empty tables array
select emaj.emaj_remove_tables('myschema1',null,null);
select emaj.emaj_remove_tables('myschema1','','');
select emaj.emaj_remove_tables('myschema1','mytbl1','mytbl1');

-- ok
select emaj.emaj_remove_tables('myschema1','my(t|T)bl\d$','mytbl2');

select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';

-----------------------------------
-- emaj_remove_sequence
-----------------------------------

-- error cases
-- missing sequence
select emaj.emaj_remove_sequence('dummySchema','myseq1');
select emaj.emaj_remove_sequence('myschema2','dummySequence');
-- sequence not in a group
select emaj.emaj_remove_sequence('myschema2','myseq2');
-- bad relation kind
select emaj.emaj_remove_sequence('myschema2','mytbl1');
-- bad mark
select emaj.emaj_remove_sequence('myschema2','myseq1','EMAJ_LAST_MARK');

-- ok
select emaj.emaj_remove_sequence('myschema2','myseq1');

-----------------------------------
-- emaj_remove_sequences with array
-----------------------------------
-- error cases
-- empty sequences array
select emaj.emaj_remove_sequences('myschema2',array[]::text[]);
select emaj.emaj_remove_sequences('myschema2',null);
select emaj.emaj_remove_sequences('myschema2',array['']);
-- missing sequence
select emaj.emaj_remove_sequences('myschema2',array['dummyTable','myseq2']);

-- ok (with a duplicate sequence name)
select emaj.emaj_assign_sequence('myschema2','myseq2','myGroup2');
select emaj.emaj_remove_sequences('myschema2',array['myseq2','myseq2']);

-----------------------------------
-- emaj_remove_sequences with filters
-----------------------------------
-- bad schema
select emaj.emaj_remove_sequences('dummySchema',null,null);

-- empty tables array
select emaj.emaj_remove_sequences('myschema2',null,null);
select emaj.emaj_remove_sequences('myschema2','','');
select emaj.emaj_remove_sequences('myschema2','myseq1','myseq1');

-- ok
select emaj.emaj_remove_sequences('myschema2','.*','');

select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup2';

-- rebuild all original groups
SET client_min_messages TO WARNING;
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', array['myGroup1','myGroup2','emptyGroup','myGroup4'], true);
RESET client_min_messages;

-- check for emaj_remove_table() and emaj_remove_sequence() functions family
select * from emaj.emaj_relation_change where rlchg_time_id >= 8000 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 8000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 8000 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(8200);

-----------------------------------
-- emaj_move_table
-----------------------------------

-- error cases
-- missing table
select emaj.emaj_move_table('dummySchema','mytbl1','myGroup2');
select emaj.emaj_move_table('myschema1','dummyTable','myGroup2');
-- table not in a group
select emaj.emaj_move_table('myschema6','table_with_50_characters_long_name_____0_________0','myGroup1');
-- bad relation kind
select emaj.emaj_move_table('myschema1','mytbl2b_col20_seq','myGroup2');

-- bad new group
select emaj.emaj_move_table('myschema1','mytbl1','dummyGroup');
-- bad mark
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2','EMAJ_LAST_MARK');

-- bad table type for target rollbackable group
select emaj.emaj_move_table('myschema5', 'myunloggedtbl', 'myGroup1');
select emaj.emaj_move_table('phil''s schema"3', 'myTbl2\', 'myGroup1');

-- move to the same group
select emaj.emaj_move_table('myschema1','mytbl1','myGroup1');

-- ok
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2');

-----------------------------------
-- emaj_move_tables with array
-----------------------------------
-- error cases
-- missing table
select emaj.emaj_move_tables('myschema1',array['dummyTable','mytbl1','mytbl2'],'myGroup1');
-- empty tables array
select emaj.emaj_move_tables('myschema1',array[]::text[],'myGroup1');
select emaj.emaj_move_tables('myschema1',null,'myGroup1');
select emaj.emaj_move_tables('myschema1',array[''],'myGroup1');

-- bad table type for target rollbackable group
select emaj.emaj_move_tables('myschema5', array['myunloggedtbl'], 'myGroup1');

-- move to the same group
select emaj.emaj_move_tables('myschema1',array['mytbl2','mytbl2b'],'myGroup1');

-- ok (with a duplicate table name)
select emaj.emaj_move_tables('myschema1',array['mytbl2','mytbl2b','mytbl2'],'myGroup2');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_kind = 'r' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_tables with filters
-----------------------------------
-- bad schema
select emaj.emaj_move_tables('dummySchema',null,null,'myGroup2');

-- empty tables array
select emaj.emaj_move_tables('myschema1',null,null,'myGroup1');
select emaj.emaj_move_tables('myschema1','','','myGroup1');
select emaj.emaj_move_tables('myschema1','mytbl1','mytbl1','myGroup1');

-- ok and go back to myGroup1
select emaj.emaj_move_tables('myschema1','my(t|T)bl.*','mytbl2$','myGroup1');
select emaj.emaj_move_tables('myschema1','.*','','myGroup1');

-- bad table type for target rollbackable group
select emaj.emaj_move_tables('myschema5', '.*', 'oids', 'myGroup1');     -- to exclude 'myoidstbl'
select emaj.emaj_move_tables('phil''s schema"3', 'bl2', '', 'myGroup1');  -- to select 'mytbl2\'

select * from emaj.emaj_relation_change where rlchg_time_id > 8000 order by 1 desc,2,3,4 limit 6;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2') order by 1 desc ,2,3;

-----------------------------------
-- emaj_move_sequence
-----------------------------------

-- error cases
-- missing sequence
select emaj.emaj_move_sequence('dummySchema','myseq1','myGroup1');
select emaj.emaj_move_sequence('myschema2','dummySequence','myGroup1');
-- sequence not in a group
select emaj.emaj_move_sequence('myschema2','myseq2','myGroup1');
-- bad relation kind
select emaj.emaj_move_sequence('myschema2','mytbl1','myGroup1');
-- bad new group
select emaj.emaj_move_sequence('myschema2','myseq1','dummyGroup');
-- bad mark
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup1','EMAJ_LAST_MARK');

-- move to the same group
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup2');

-- ok
select emaj.emaj_move_sequence('myschema2','myseq1','myGroup1');

-----------------------------------
-- emaj_move_sequences with array
-----------------------------------
-- error cases
-- missing sequence
select emaj.emaj_move_sequences('myschema2',array['dummySequence','myseq1'],'myGroup1');
-- empty tables array
select emaj.emaj_move_sequences('myschema2',array[]::text[],'myGroup1');
select emaj.emaj_move_sequences('myschema2',null,'myGroup1');
select emaj.emaj_move_sequences('myschema2',array[''],'myGroup1');

-- move to the same group
select emaj.emaj_move_sequences('myschema2',array['myseq1','myTbl3_col31_seq'],'myGroup2');
select emaj.emaj_move_sequences('myschema2',array['myseq1','myTbl3_col31_seq'],'myGroup2');

-- ok (with a duplicate sequence name)
select emaj.emaj_move_sequences('myschema2',array['myseq1','myseq1'],'myGroup4');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_kind = 'S' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_sequences with filters
-----------------------------------
-- bad schema
select emaj.emaj_move_sequences('dummySchema',null,null,'myGroup2');

-- empty sequences array
select emaj.emaj_move_sequences('myschema2',null,null,'myGroup2');
select emaj.emaj_move_sequences('myschema2','','','myGroup2');
select emaj.emaj_move_sequences('myschema2','myseq1','myseq1','myGroup2');

-- ok and go back to myGroup2
select emaj.emaj_move_sequences('myschema2','.*','','myGroup2');

select * from emaj.emaj_relation_change where rlchg_time_id > 8000 order by 1 desc,2,3,4 limit 3;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2','myGroup4') order by 1 desc ,2,3;

-- check for emaj_move_table() and emaj_move_sequence() functions family
select * from emaj.emaj_relation_change where rlchg_time_id >= 8200 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 8200 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 8200 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(8400);

-----------------------------------
-- emaj_modify_table
-----------------------------------

-- error cases
-- missing table
select emaj.emaj_modify_table('dummySchema','mytbl1',null);
select emaj.emaj_modify_table('myschema1','dummyTable',null);
-- table not in a group
select emaj.emaj_modify_table('myschema6','table_with_50_characters_long_name_____0_________0',null);
-- bad relation kind
select emaj.emaj_modify_table('myschema1','mytbl2b_col20_seq',null);

-- invalid priority
select emaj.emaj_modify_table('myschema1','mytbl1','{"priority":"not_numeric"}'::jsonb);
-- invalid tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":"dummytsp"}'::jsonb);
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_index_tablespace":"dummytsp"}'::jsonb);
-- unknown property
select emaj.emaj_modify_table('myschema1','mytbl1','{"unknown_property":null}'::jsonb);

-- bad mark
select emaj.emaj_modify_table('myschema1','mytbl1',null,'EMAJ_LAST_MARK');

-- ok
-- no property
select emaj.emaj_modify_table('myschema1','mytbl2',NULL);
select emaj.emaj_modify_table('myschema1','mytbl2','{ }');

-- change a priority
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":1}');

-- change a log data tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":"tsp log''2"}');
-- change a log index tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_index_tablespace":"tsp log''2"}');

-- change a triggers list, by adding 1 trigger
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers":["mytbl2trg1"]}');
select rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers from emaj.emaj_relation where rel_ignored_triggers is not null order by 1,2,3;
-- add the same
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers":["mytbl2trg1"]}');
-- add all triggers for a table
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers_profiles":[".*"]}');
select rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers from emaj.emaj_relation where rel_ignored_triggers is not null order by 1,2,3;

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-- revert changes
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":null}');
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":null, "log_index_tablespace":null}');
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers":null}');

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-------------------------------------
---- emaj_modify_tables with array
-------------------------------------
-- error cases
-- missing table
select emaj.emaj_modify_tables('myschema2',array['dummyTable','mytbl1','mytbl2'],null);
-- empty tables array
select emaj.emaj_modify_tables('myschema2',array[]::text[],null);
select emaj.emaj_modify_tables('myschema2',null,null);
select emaj.emaj_modify_tables('myschema2',array[''],null);

---- ok (with a duplicate table name)
select emaj.emaj_modify_tables('myschema2',array['mytbl1','mytbl2','mytbl2'],'{"priority":10,"log_data_tablespace":"tsplog1"}');
select rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_tblseq in ('mytbl1','mytbl2') order by 1,2,3;

-----------------------------------
-- emaj_modify_tables with filters
-----------------------------------
-- bad schema
select emaj.emaj_modify_tables('dummySchema',null,null,null::jsonb);

-- empty tables array
select emaj.emaj_modify_tables('myschema2',null,null,null::jsonb);
select emaj.emaj_modify_tables('myschema2','','',null::jsonb);
select emaj.emaj_modify_tables('myschema2','mytbl1','mytbl1',null::jsonb);

-- ok and revert the previous changes
select emaj.emaj_modify_tables('myschema2','mytbl.*','','{"priority":null,"log_data_tablespace":null}'::jsonb);
select rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_tblseq like 'mytbl%' order by 1,2,3;

-- check for emaj_modify_table() functions family
select * from emaj.emaj_relation_change where rlchg_time_id >= 8400 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 8400 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 8400 order by hist_id;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
