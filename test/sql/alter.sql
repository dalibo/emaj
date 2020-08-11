-- alter.sql : test emaj_move_table(), emaj_move_tables(), emaj_move_sequence(), emaj_move_sequences()
--                  emaj_modify_table() and emaj_modify_tables() functions
--

-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 6000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 6000;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 6000;
alter sequence emaj.emaj_global_seq restart 60000;

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
select emaj.emaj_create_group('myGroup2');
select emaj.emaj_create_group('emptyGroup',true,true);
select emaj.emaj_create_group('myGroup4');

-----------------------------------
-- emaj_move_table
-----------------------------------

-- error cases
-- table not in a group
select emaj.emaj_move_table('dummySchema','mytbl1','myGroup2');
select emaj.emaj_move_table('myschema1','dummyTable','myGroup2');
-- bad new group
select emaj.emaj_move_table('myschema1','mytbl1','dummyGroup');
-- bad mark
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2','EMAJ_LAST_MARK');

-- move to the same group
select emaj.emaj_move_table('myschema1','mytbl1','myGroup1');

-- ok
select emaj.emaj_move_table('myschema1','mytbl1','myGroup2');

-----------------------------------
-- emaj_move_tables with array
-----------------------------------
-- error cases
-- table not in a group
select emaj.emaj_move_tables('myschema1',array['dummyTable','mytbl1','mytbl2'],'myGroup1');
-- empty tables array
select emaj.emaj_move_tables('myschema1',array[]::text[],'myGroup1');
select emaj.emaj_move_tables('myschema1',null,'myGroup1');
select emaj.emaj_move_tables('myschema1',array[''],'myGroup1');

-- ok (with a duplicate table name)
select emaj.emaj_move_tables('myschema1',array['mytbl2','mytbl2b','mytbl2'],'myGroup2');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_kind = 'r' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_tables with filters
-----------------------------------
-- empty tables array
select emaj.emaj_move_tables('myschema1',null,null,'myGroup1');
select emaj.emaj_move_tables('myschema1','','','myGroup1');
select emaj.emaj_move_tables('myschema1','mytbl1','mytbl1','myGroup1');

-- ok and go back to myGroup1
select emaj.emaj_move_tables('myschema1','my(t|T)bl.*','mytbl2$','myGroup1');
select emaj.emaj_move_tables('myschema1','.*','','myGroup1');

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 6;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2') order by 1 desc ,2,3;

-----------------------------------
-- emaj_move_sequence
-----------------------------------

-- error cases
-- sequence not in a group
select emaj.emaj_move_sequence('dummySchema','myseq1','myGroup1');
select emaj.emaj_move_sequence('myschema2','dummySequence','myGroup1');
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
-- sequence not in a group
select emaj.emaj_move_sequences('myschema2',array['dummySequence','myseq1'],'myGroup1');
-- empty tables array
select emaj.emaj_move_sequences('myschema2',array[]::text[],'myGroup1');
select emaj.emaj_move_sequences('myschema2',null,'myGroup1');
select emaj.emaj_move_sequences('myschema2',array[''],'myGroup1');

-- ok (with a duplicate sequence name)
select emaj.emaj_move_sequences('myschema2',array['myseq1','myseq1'],'myGroup4');

select rel_schema, rel_tblseq, rel_group from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_kind = 'S' and upper_inf(rel_time_range)
  order by 1,2,3;

-----------------------------------
-- emaj_move_sequences with filters
-----------------------------------
-- empty sequences array
select emaj.emaj_move_sequences('myschema21',null,null,'myGroup2');
select emaj.emaj_move_sequences('myschema21','','','myGroup2');
select emaj.emaj_move_sequences('myschema21','myseq1','myseq1','myGroup2');

-- ok and go back to myGroup2
select emaj.emaj_move_sequences('myschema2','.*','','myGroup2');

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 3;
select group_last_alter_time_id, group_nb_table, group_nb_sequence from emaj.emaj_group 
  where group_name in ('myGroup1','myGroup2','myGroup4') order by 1 desc ,2,3;

-----------------------------------
-- emaj_modify_table
-----------------------------------

-- error cases
-- table not in a group
select emaj.emaj_modify_table('dummySchema','mytbl1',null);
select emaj.emaj_modify_table('myschema1','dummyTable',null);

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
-- change a priority
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":1}');

-- change a log data tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":"tsp log''2"}');
-- change a log index tablespace
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_index_tablespace":"tsp log''2"}');

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-- revert changes
select emaj.emaj_modify_table('myschema1','mytbl2','{"priority":null}');
select emaj.emaj_modify_table('myschema1','mytbl1','{"log_data_tablespace":null, "log_index_tablespace":null}');

select rel_schema, rel_tblseq, rel_time_range, rel_priority from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2' order by 3 desc ,1,2 limit 2;
select rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' order by 3 desc ,1,2 limit 2;

-------------------------------------
---- emaj_modify_tables with array
-------------------------------------
-- error cases
-- table not in a group
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
-- empty tables array
select emaj.emaj_modify_tables('myschema2',null,null,null::jsonb);
select emaj.emaj_modify_tables('myschema2','','',null::jsonb);
select emaj.emaj_modify_tables('myschema2','mytbl1','mytbl1',null::jsonb);

-- ok and revert the previous changes
select emaj.emaj_modify_tables('myschema2','mytbl.*','','{"priority":null,"log_data_tablespace":null}'::jsonb);
select rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp from emaj.emaj_relation
  where rel_schema = 'myschema2' and rel_tblseq like 'mytbl%' order by 1,2,3;

select altr_time_id, altr_step, altr_schema, altr_tblseq, altr_group, altr_priority, altr_group_is_logging,
       altr_new_group, altr_new_group_is_logging from emaj.emaj_alter_plan
  order by 1 desc, 2,3,4 limit 14;

-- checks are performed by the alterLogging.sql script
