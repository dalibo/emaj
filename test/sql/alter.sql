-- Alter.sql : tests groups structure changes with groups in IDLE state.
-- Test emaj_remove_table(), emaj_remove_tables(), emaj_remove_sequence(), emaj_remove_sequences(),
--      emaj_move_table(), emaj_move_tables(), emaj_move_sequence(), emaj_move_sequences(),
--      emaj_modify_table() and emaj_modify_tables() functions.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Define the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/alter'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(8000);

-----------------------------
-- Stop, reset and drop and recreate groups.
-----------------------------
SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_reset_group('myGroup1');
SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_force_drop_group('myGroup2');
SELECT emaj.emaj_stop_group('phil''s group#3",', 'Simple stop mark');
SELECT emaj.emaj_drop_group('phil''s group#3",');
SELECT emaj.emaj_force_stop_group('myGroup4');
SELECT emaj.emaj_drop_group('myGroup4');
SELECT emaj.emaj_force_stop_group('emptyGroup');
SELECT emaj.emaj_drop_group('emptyGroup');
SELECT emaj.emaj_create_group('myGroup1');

-- Rebuild all original groups.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', ARRAY['myGroup1', 'myGroup2', 'emptyGroup', 'myGroup4', 'myGroup5'], TRUE);
-- Add a table without PK to the audit_only group.
SELECT emaj.emaj_assign_table('phil''s schema"3', 'myTbl2\', 'myGroup5');

-----------------------------------
-- emaj_remove_table.
-----------------------------------

-- Error cases.
-- Missing table.
SELECT emaj.emaj_remove_table('dummySchema', 'mytbl1');
SELECT emaj.emaj_remove_table('myschema1', 'dummyTable');
-- Table not in a group.
SELECT emaj.emaj_remove_table('myschema6', 'table_with_50_characters_long_name_____0_________0');
-- Bad relation kind.
SELECT emaj.emaj_remove_table('myschema1', 'mytbl2b_col20_seq');
-- Bad mark.
SELECT emaj.emaj_remove_table('myschema1', 'mytbl1', 'EMAJ_LAST_MARK');

-- Ok.
SELECT emaj.emaj_remove_table('myschema1', 'mytbl1');

-----------------------------------
-- emaj_remove_tables with array.
-----------------------------------
-- Error cases.
-- Empty tables array.
SELECT emaj.emaj_remove_tables('myschema1', ARRAY[]::TEXT[]);
SELECT emaj.emaj_remove_tables('myschema1', NULL);
SELECT emaj.emaj_remove_tables('myschema1', ARRAY['']);
-- Missing table.
SELECT emaj.emaj_remove_tables('myschema1', ARRAY['dummyTable', 'mytbl1', 'mytbl2']);

-- Ok (with a duplicate table name).
SELECT emaj.emaj_remove_tables('myschema1', ARRAY['mytbl2', 'mytbl2b', 'mytbl2']);

-----------------------------------
-- emaj_remove_tables with filters.
-----------------------------------
-- Bad schema.
SELECT emaj.emaj_remove_tables('dummySchema', NULL, NULL);

-- Empty tables array.
SELECT emaj.emaj_remove_tables('myschema1', NULL, NULL);
SELECT emaj.emaj_remove_tables('myschema1', '', '');
SELECT emaj.emaj_remove_tables('myschema1', 'mytbl1', 'mytbl1');

-- Ok.
SELECT emaj.emaj_remove_tables('myschema1', 'my(t|T)bl\d$', 'mytbl2');

SELECT group_last_alter_time_id, group_nb_table, group_nb_sequence FROM emaj.emaj_group WHERE group_name = 'myGroup1';

-----------------------------------
-- emaj_remove_sequence.
-----------------------------------

-- Error cases.
-- Missing sequence.
SELECT emaj.emaj_remove_sequence('dummySchema', 'myseq1');
SELECT emaj.emaj_remove_sequence('myschema2', 'dummySequence');
-- Sequence not in a group.
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq2');
-- Bad relation kind.
SELECT emaj.emaj_remove_sequence('myschema2', 'mytbl1');
-- Bad mark.
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq1', 'EMAJ_LAST_MARK');

-- Ok.
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq1');

-----------------------------------
-- emaj_remove_sequences with array.
-----------------------------------
-- Error cases.
-- Empty sequences array.
SELECT emaj.emaj_remove_sequences('myschema2', ARRAY[]::TEXT[]);
SELECT emaj.emaj_remove_sequences('myschema2', NULL);
SELECT emaj.emaj_remove_sequences('myschema2', ARRAY['']);
-- Missing sequence.
SELECT emaj.emaj_remove_sequences('myschema2', ARRAY['dummyTable', 'myseq2']);

-- Ok (with a duplicate sequence name).
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq2', 'myGroup2');
SELECT emaj.emaj_remove_sequences('myschema2', ARRAY['myseq2', 'myseq2']);

-----------------------------------
-- emaj_remove_sequences with filters.
-----------------------------------
-- Bad schema.
SELECT emaj.emaj_remove_sequences('dummySchema', NULL, NULL);

-- Empty tables array.
SELECT emaj.emaj_remove_sequences('myschema2', NULL, NULL);
SELECT emaj.emaj_remove_sequences('myschema2', '', '');
SELECT emaj.emaj_remove_sequences('myschema2', 'myseq1', 'myseq1');

-- Ok.
SELECT emaj.emaj_remove_sequences('myschema2', '.*', '');

SELECT group_last_alter_time_id, group_nb_table, group_nb_sequence FROM emaj.emaj_group WHERE group_name = 'myGroup2';

-- Rebuild all original groups.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', ARRAY['myGroup1', 'myGroup2', 'emptyGroup', 'myGroup4'], TRUE);

-- Check for emaj_remove_table() and emaj_remove_sequence() functions family.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 8000 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 8000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 8000 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(8200);

-----------------------------------
-- emaj_move_table.
-----------------------------------

-- Error cases.
-- Missing table.
SELECT emaj.emaj_move_table('dummySchema', 'mytbl1', 'myGroup2');
SELECT emaj.emaj_move_table('myschema1', 'dummyTable', 'myGroup2');
-- Table not in a group.
SELECT emaj.emaj_move_table('myschema6', 'table_with_50_characters_long_name_____0_________0', 'myGroup1');
-- Bad relation kind.
SELECT emaj.emaj_move_table('myschema1', 'mytbl2b_col20_seq', 'myGroup2');

-- Bad new group.
SELECT emaj.emaj_move_table('myschema1', 'mytbl1', 'dummyGroup');
-- Bad mark.
SELECT emaj.emaj_move_table('myschema1', 'mytbl1', 'myGroup2', 'EMAJ_LAST_MARK');

-- Bad table type for target rollbackable group.
SELECT emaj.emaj_move_table('myschema5', 'myunloggedtbl', 'myGroup1');
SELECT emaj.emaj_move_table('phil''s schema"3', 'myTbl2\', 'myGroup1');

-- Move to the same group.
SELECT emaj.emaj_move_table('myschema1', 'mytbl1', 'myGroup1');

-- Ok.
SELECT emaj.emaj_move_table('myschema1', 'mytbl1', 'myGroup2');

-----------------------------------
-- emaj_move_tables with array.
-----------------------------------
-- Error cases.
-- Missing table.
SELECT emaj.emaj_move_tables('myschema1', ARRAY['dummyTable', 'mytbl1', 'mytbl2'], 'myGroup1');
-- Empty tables array.
SELECT emaj.emaj_move_tables('myschema1', ARRAY[]::TEXT[], 'myGroup1');
SELECT emaj.emaj_move_tables('myschema1', NULL, 'myGroup1');
SELECT emaj.emaj_move_tables('myschema1', ARRAY[''], 'myGroup1');

-- Bad table type for target rollbackable group.
SELECT emaj.emaj_move_tables('myschema5', ARRAY['myunloggedtbl'], 'myGroup1');

-- Move to the same group.
SELECT emaj.emaj_move_tables('myschema1', ARRAY['mytbl2', 'mytbl2b'], 'myGroup1');

-- Ok (with a duplicate table name).
SELECT emaj.emaj_move_tables('myschema1', ARRAY['mytbl2', 'mytbl2b', 'mytbl2'], 'myGroup2');

SELECT rel_schema, rel_tblseq, rel_group FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema1' AND rel_kind = 'r' AND upper_inf(rel_time_range)
  ORDER BY 1, 2, 3;

-----------------------------------
-- emaj_move_tables with filters.
-----------------------------------
-- Bad schema.
SELECT emaj.emaj_move_tables('dummySchema', NULL, NULL, 'myGroup2');

-- Empty tables array.
SELECT emaj.emaj_move_tables('myschema1', NULL, NULL, 'myGroup1');
SELECT emaj.emaj_move_tables('myschema1', '', '', 'myGroup1');
SELECT emaj.emaj_move_tables('myschema1', 'mytbl1', 'mytbl1', 'myGroup1');

-- Ok and go back to myGroup1.
SELECT emaj.emaj_move_tables('myschema1', 'my(t|T)bl.*', 'mytbl2$', 'myGroup1');
SELECT emaj.emaj_move_tables('myschema1', '.*', '', 'myGroup1');

-- Bad table type for target rollbackable group.
SELECT emaj.emaj_move_tables('myschema5', '.*', '', 'myGroup1');
SELECT emaj.emaj_move_tables('phil''s schema"3', 'bl2', '', 'myGroup1');  -- to select 'mytbl2\'

SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id > 8000 ORDER BY 1 DESC, 2, 3, 4 limit 6;
SELECT group_last_alter_time_id, group_nb_table, group_nb_sequence FROM emaj.emaj_group
  WHERE group_name IN ('myGroup1', 'myGroup2') ORDER BY 1 DESC, 2, 3;

-----------------------------------
-- emaj_move_sequence.
-----------------------------------

-- Error cases.
-- Missing sequence.
SELECT emaj.emaj_move_sequence('dummySchema', 'myseq1', 'myGroup1');
SELECT emaj.emaj_move_sequence('myschema2', 'dummySequence', 'myGroup1');
-- Sequence not in a group.
SELECT emaj.emaj_move_sequence('myschema2', 'myseq2', 'myGroup1');
-- Bad relation kind.
SELECT emaj.emaj_move_sequence('myschema2', 'mytbl1', 'myGroup1');
-- Bad new group.
SELECT emaj.emaj_move_sequence('myschema2', 'myseq1', 'dummyGroup');
-- Bad mark.
SELECT emaj.emaj_move_sequence('myschema2', 'myseq1', 'myGroup1', 'EMAJ_LAST_MARK');

-- Move to the same group.
SELECT emaj.emaj_move_sequence('myschema2', 'myseq1', 'myGroup2');

-- Ok.
SELECT emaj.emaj_move_sequence('myschema2', 'myseq1', 'myGroup1');

-----------------------------------
-- emaj_move_sequences with array.
-----------------------------------
-- Error cases.
-- Missing sequence.
SELECT emaj.emaj_move_sequences('myschema2', ARRAY['dummySequence', 'myseq1'], 'myGroup1');
-- Empty tables array.
SELECT emaj.emaj_move_sequences('myschema2', ARRAY[]::TEXT[], 'myGroup1');
SELECT emaj.emaj_move_sequences('myschema2', NULL, 'myGroup1');
SELECT emaj.emaj_move_sequences('myschema2', ARRAY[''], 'myGroup1');

-- Move to the same group.
SELECT emaj.emaj_move_sequences('myschema2', ARRAY['myseq1', 'myTbl3_col31_seq'], 'myGroup2');
SELECT emaj.emaj_move_sequences('myschema2', ARRAY['myseq1', 'myTbl3_col31_seq'], 'myGroup2');

-- Ok (with a duplicate sequence name).
SELECT emaj.emaj_move_sequences('myschema2', ARRAY['myseq1', 'myseq1'], 'myGroup4');

SELECT rel_schema, rel_tblseq, rel_group FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema2' AND rel_kind = 'S' AND upper_inf(rel_time_range)
  ORDER BY 1, 2, 3;

-----------------------------------
-- emaj_move_sequences with filters.
-----------------------------------
-- Bad schema.
SELECT emaj.emaj_move_sequences('dummySchema', NULL, NULL, 'myGroup2');

-- Empty sequences array.
SELECT emaj.emaj_move_sequences('myschema2', NULL, NULL, 'myGroup2');
SELECT emaj.emaj_move_sequences('myschema2', '', '', 'myGroup2');
SELECT emaj.emaj_move_sequences('myschema2', 'myseq1', 'myseq1', 'myGroup2');

-- Ok and go back to myGroup2.
SELECT emaj.emaj_move_sequences('myschema2', '.*', '', 'myGroup2');

SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id > 8000 ORDER BY 1 DESC, 2, 3, 4 limit 3;
SELECT group_last_alter_time_id, group_nb_table, group_nb_sequence FROM emaj.emaj_group
  WHERE group_name IN ('myGroup1', 'myGroup2', 'myGroup4') ORDER BY 1 DESC, 2, 3;

-- Check for emaj_move_table() and emaj_move_sequence() functions family.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 8200 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 8200 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 8200 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(8400);

-----------------------------------
-- emaj_modify_table.
-----------------------------------

-- Error cases.
-- Missing table.
SELECT emaj.emaj_modify_table('dummySchema', 'mytbl1', NULL);
SELECT emaj.emaj_modify_table('myschema1', 'dummyTable', NULL);
-- Table not in a group.
SELECT emaj.emaj_modify_table('myschema6', 'table_with_50_characters_long_name_____0_________0', NULL);
-- Bad relation kind.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2b_col20_seq', NULL);

-- Invalid priority.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority":"not_numeric"}'::JSONB);
-- Invalid tablespace.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"log_data_tablespace":"dummytsp"}'::JSONB);
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"log_index_tablespace":"dummytsp"}'::JSONB);
-- Unknown property.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"unknown_property":null}'::JSONB);

-- Bad mark.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', NULL, 'EMAJ_LAST_MARK');

-- Ok.
-- No property.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', NULL);
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{ }');

-- Change a priority.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"priority":1}');

-- Change a log data tablespace.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"log_data_tablespace":"tsp log''2"}');
-- Change a log index tablespace.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"log_index_tablespace":"tsp log''2"}');

-- Change a triggers list, by adding 1 trigger.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":["mytbl2trg1"]}');
SELECT rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_ignored_triggers IS NOT NULL ORDER BY 1, 2, 3;
-- Add the same.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":["mytbl2trg1"]}');
-- Add all triggers for a table.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers_profiles":[".*"]}');
SELECT rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_ignored_triggers IS NOT NULL ORDER BY 1, 2, 3;

SELECT rel_schema, rel_tblseq, rel_time_range, rel_priority FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl2' ORDER BY 3 DESC, 1, 2 LIMIT 2;
SELECT rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' ORDER BY 3 DESC, 1, 2 LIMIT 2;

-- Revert changes.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"priority":null}');
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"log_data_tablespace":null, "log_index_tablespace":null}');
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":null}');

SELECT rel_schema, rel_tblseq, rel_time_range, rel_priority FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl2' ORDER BY 3 DESC, 1, 2 LIMIT 2;
SELECT rel_schema, rel_tblseq, rel_time_range, rel_log_dat_tsp, rel_log_idx_tsp FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' ORDER BY 3 DESC, 1, 2 LIMIT 2;

-------------------------------------
---- emaj_modify_tables with array.
-------------------------------------
-- Error cases.
-- Missing table.
SELECT emaj.emaj_modify_tables('myschema2', ARRAY['dummyTable', 'mytbl1', 'mytbl2'], NULL);
-- Empty tables array.
SELECT emaj.emaj_modify_tables('myschema2', ARRAY[]::TEXT[], NULL);
SELECT emaj.emaj_modify_tables('myschema2', NULL, NULL);
SELECT emaj.emaj_modify_tables('myschema2', ARRAY[''], NULL);

---- ok (with a duplicate table name).
SELECT emaj.emaj_modify_tables('myschema2', ARRAY['mytbl1', 'mytbl2', 'mytbl2'], '{"priority":10, "log_data_tablespace":"tsplog1"}');
SELECT rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema2' AND rel_tblseq in ('mytbl1', 'mytbl2') ORDER BY 1, 2, 3;

-----------------------------------
-- emaj_modify_tables with filters.
-----------------------------------
-- Bad schema.
SELECT emaj.emaj_modify_tables('dummySchema', NULL, NULL, NULL::JSONB);

-- Empty tables array.
SELECT emaj.emaj_modify_tables('myschema2', NULL, NULL, NULL::JSONB);
SELECT emaj.emaj_modify_tables('myschema2', '', '', NULL::JSONB);
SELECT emaj.emaj_modify_tables('myschema2', 'mytbl1', 'mytbl1', NULL::JSONB);

-- Ok and revert the previous changes.
SELECT emaj.emaj_modify_tables('myschema2', 'mytbl.*', '', '{"priority":null, "log_data_tablespace":null}'::JSONB);
SELECT rel_schema, rel_tblseq, rel_time_range, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp FROM emaj.emaj_relation
  WHERE rel_schema = 'myschema2' AND rel_tblseq LIKE 'mytbl%' ORDER BY 1, 2, 3;

-- Check for emaj_modify_table() functions family.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 8400 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 8400 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 8400 ORDER BY hist_id;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
