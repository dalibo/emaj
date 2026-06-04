-- AlterLogging.sql : test groups structure changes with groups in LOGGING state.
--
-- It follows alter.sql that tests the same functions but with groups in IDLE state.
-- It uses the same tables and groups, and the same sequences range.
-- It includes the final checks for both alter.sql and alterLogging.sql scenarios.

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(9000);

-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/alter_logging'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

SELECT emaj.emaj_remove_tables('myschema2', ARRAY['mytbl7', 'mytbl8']);
SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'Mk1');

-- Save the groups configuration.
SELECT emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json');

-----------------------------
-- Change attributes.
-----------------------------

-- Bad mark (duplicate name).
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority":30}'::JSONB, emaj.emaj_get_previous_mark_group('myGroup1', now()));

-- Change the priority.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority":30}'::JSONB, 'Priority Changed');

-- Change the log data tablespace and the log index tablespace for different tables.
set default_tablespace = tspemaj_renamed;
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2b', '{"log_data_tablespace":null}'::JSONB, 'Attributes_changed_1');
SELECT emaj.emaj_modify_table('myschema2', 'mytbl6', '{"log_index_tablespace":"tsplog1"}'::JSONB, 'Attributes_changed_2');
reset default_tablespace;

-- Change the triggers to ignore at rollback time.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers_profiles":["trg1", "trg2"]}');
SELECT rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_ignored_triggers IS NOT NULL ORDER BY 1, 2, 3;

-- Perform some operations: set an intermediate mark, update myTbl3 and rollback.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Mk2');
INSERT INTO myschema1."myTbl3" VALUES (11, now(), 11.0);
UPDATE myschema2."myTbl3" SET col33 = 11.0 WHERE col31 = 10;
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk2');

-- Revert the properties changes.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1", "myGroup2"}', TRUE);

-- Checks for attributes changes.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 9000 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 9000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 9000 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(9100);

-----------------------------
-- Add sequences.
-----------------------------

-- Add myschema2.mySeq2 to the myGroup2 group.
-- First perform changes and set marks.
SELECT emaj.emaj_set_mark_group('myGroup2', 'ADD_SEQ test');
SELECT nextval('myschema2.myseq2');
SELECT emaj.emaj_set_mark_group('myGroup2', 'Before ADD_SEQ');
SELECT nextval('myschema2.myseq2');

-- Then add the sequence to the group.
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq2', 'myGroup2', 'Add myseq2');

SELECT group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  FROM emaj.emaj_group WHERE group_name = 'myGroup2';
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'myseq2' ORDER BY rel_time_range;
SELECT * FROM emaj.emaj_sequence WHERE sequ_name = 'myseq2';

-- Perform some other updates.
SELECT nextval('myschema2.myseq2');
SELECT emaj.emaj_set_mark_group('myGroup2', 'After ADD_SEQ');
SELECT nextval('myschema2.myseq2');
SELECT nextval('myschema2.myseq2');

-- Get statistics.
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'ADD_SEQ test', NULL)
  ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments,
       stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq2', 'myGroup2', 'Mk1');

--testing snap, dump changes and sql generation.
\! mkdir $EMAJTESTTMPDIR/snap
SELECT emaj.emaj_snap_group('myGroup2', :'EMAJTESTTMPDIR' || '/snap', '');
\! cat $EMAJTESTTMPDIR/snap/myschema2_myseq2.snap
\! rm $EMAJTESTTMPDIR/snap/*

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mk1', 'After ADD_SEQ', 'SEQUENCES_ONLY', NULL);
SELECT sql_text FROM emaj_temp_sql WHERE sql_line_number = 0;

SELECT emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, :'EMAJTESTTMPDIR' || '/myFile');
\! grep myseq2 $EMAJTESTTMPDIR/myFile
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, :'EMAJTESTTMPDIR' || '/myFile', ARRAY['myschema2.myseq2']);
\! rm $EMAJTESTTMPDIR/myFile

-- Testing mark deletions.
BEGIN;
  -- testing DELETE a single mark set after the sequence addition
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'After ADD_SEQ');
  SELECT 'Should not exist' FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'After ADD_SEQ';
  -- testing DELETE all marks up TO a mark set after the alter_group
  SELECT emaj.emaj_delete_before_mark_group('myGroup2', 'EMAJ_LAST_MARK');
  SELECT * FROM emaj.emaj_mark WHERE mark_group = 'myGroup2';
  SELECT * FROM emaj.emaj_sequence WHERE sequ_name = 'myseq2' ORDER BY sequ_time_id;
ROLLBACK;

BEGIN;
  -- testing the alter_group mark deletion
  SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add myseq2';
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'Add myseq2');
  SELECT * FROM emaj.emaj_sequence WHERE sequ_name = 'myseq2' ORDER BY sequ_time_id LIMIT 1;
  SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
         stat_last_time_id, stat_increments, stat_has_structure_changed
    FROM emaj.emaj_sequence_stat_group('myGroup2', 'ADD_SEQ test', NULL)
    ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments,
         stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq2', 'myGroup2', 'Mk1');
ROLLBACK;

-- Test sequences at bounds following a rollback targeting a mark set before an ADD_SEQ.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', NULL);
  SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add myseq2';
-- Beware, the sequence changes generated by the E-Maj rollback will not be rollback by the transaction rollback.
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup2', 'Before ADD_SEQ', TRUE);
  SELECT * FROM emaj.emaj_sequence WHERE sequ_schema = 'myschema2' AND sequ_name = 'myseq2';
ROLLBACK;

-- Estimate and then perform a logged rollback to reach a mark prior the ADD_SEQ operation and check.
DELETE FROM emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
SELECT * FROM emaj.emaj_estimate_rollback_group('myGroup2', 'Before ADD_SEQ', TRUE);
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_logged_rollback_group('myGroup2', 'Before ADD_SEQ', TRUE);
SELECT * FROM myschema2.myseq2;
SELECT * FROM emaj.emaj_sequence WHERE sequ_name = 'myseq2' ORDER BY sequ_time_id;

-- Consolidate this logged rollback.
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'Logged_Rlbk_End');
SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add myseq2';
SELECT * FROM emaj.emaj_get_consolidable_rollbacks() WHERE cons_group = 'myGroup2'; -- should report 1 row TO consolidate
SELECT * FROM emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
SELECT * FROM emaj.emaj_sequence WHERE sequ_schema = 'myschema2' AND sequ_name = 'myseq2' ORDER BY sequ_schema, sequ_name, sequ_time_id;

-- Remove mySeq2 from the group, then re-add it.
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq2', 'Remove myseq2');
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq2', 'myGroup2', 'Re-add myseq2');

SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'myseq2' ORDER BY rel_time_range;

-- Finaly remove mySeq2 from the group.
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq2', 'Really remove myseq2');

-- And perform a full rollback (that do not process mySeq2).
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_group('myGroup2', 'ADD_SEQ test', TRUE);
SELECT * FROM myschema2.myseq2;

-- Verify that all time range bounds of emaj_relation has its corresponding row into emaj_sequence.
SELECT * FROM emaj.emaj_sequence WHERE sequ_schema = 'myschema2' AND sequ_name = 'myseq2' AND sequ_time_id IN
  (SELECT lower(rel_time_range) FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'myseq2' UNION
   SELECT upper(rel_time_range) FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'myseq2')
  ORDER BY sequ_time_id;

-- Checks for add sequences test.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 9100 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 9100 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 9100 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(9300);

-----------------------------
-- Remove sequences.
-----------------------------

SELECT emaj.emaj_set_mark_group('myGroup1', 'Mk2b');
SELECT emaj.emaj_set_mark_group('myGroup1', 'Before_remove');

SELECT group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  FROM emaj.emaj_group WHERE group_name = 'myGroup1';
SELECT emaj.emaj_remove_sequence('myschema1', 'myTbl3_col31_seq', 'Sequence_removed');
SELECT group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  FROM emaj.emaj_group WHERE group_name = 'myGroup1';
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq = 'myTbl3_col31_seq' ORDER BY rel_time_range;
SELECT * FROM emaj.emaj_verify_all() AS t(msg) WHERE msg NOT LIKE '%foreign key%';

--testing snap, dump changes and sql generation.
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR' || '/snap', '');
\! ls $EMAJTESTTMPDIR/snap
\! rm -R $EMAJTESTTMPDIR/snap/*

SELECT emaj.emaj_set_mark_group('myGroup1', 'After_remove');

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Mk1', 'EMAJ_LAST_MARK', 'SEQUENCES_ONLY', NULL);
SELECT sql_text FROM emaj_temp_sql WHERE sql_line_number = 0;

-- Testing statistics.
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup1', 'Before_remove', NULL)
  ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments,
       stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema1', 'myTbl3_col31_seq', 'myGroup1', 'Mk2b');

-- Testing sql script generation.
SELECT emaj.emaj_gen_sql_group('myGroup1', 'Mk1', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/script.sql', ARRAY['myschema1.myTbl3_col31_seq']);
\! grep -v 'generated by E-Maj at' $EMAJTESTTMPDIR/script.sql
\! rm $EMAJTESTTMPDIR/script.sql

BEGIN;
  -- testing the alter_group mark deletion
  SELECT emaj.emaj_delete_mark_group('myGroup1', 'Sequence_removed');
  SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_increments, stat_has_structure_changed
    FROM emaj.emaj_sequence_stat_group('myGroup1', 'Before_remove', NULL)
    ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments,
         stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema1', 'myTbl3_col31_seq', 'myGroup1', 'Mk2b');
  SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
    FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq = 'myTbl3_col31_seq' ORDER BY rel_time_range;
  SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id,
         mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
    FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' ORDER BY mark_time_id DESC LIMIT 2;
ROLLBACK;

BEGIN;
  -- testing DELETE a single mark set before the sequence removal
  SELECT emaj.emaj_delete_mark_group('myGroup1', 'Before_remove');
  SELECT * FROM emaj.emaj_sequence WHERE sequ_schema = 'myschema1'
    AND sequ_time_id NOT IN (SELECT distinct mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup1');
  -- testing ROLLBACK
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_logged_rollback_group('myGroup1', 'Mk2b', TRUE) ORDER BY 1, 2;
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup1', 'Mk2b', TRUE) ORDER BY 1, 2;
  SAVEPOINT svp1;
  -- testing group's reset
    SELECT emaj.emaj_stop_group('myGroup1');
    SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range) ORDER BY 1, 2, 3;
    SELECT emaj.emaj_reset_group('myGroup1');
    SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range);
  ROLLBACK TO svp1;
  -- testing marks deletion
  SELECT emaj.emaj_set_mark_group('myGroup1', 'Mk2d');
  SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'Mk2b');
  SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
    FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range) ORDER BY 1, 2, 3;
  SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'Mk2d');
  SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range);
  -- testing the sequence DROP
  DROP SEQUENCE mySchema1."myTbl3_col31_seq" CASCADE;
ROLLBACK;

-- Revert the changes.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1"}', TRUE, 'Revert_sequences_removal');

SELECT emaj.emaj_cleanup_rollback_state();

-- Checks for remove sequences test.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 9300 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 9300 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 9300 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(9500);

-----------------------------
-- Add tables.
-----------------------------

-- Add myschema2.mytbl7 to the myGroup2 group.
-- First perform updates and set marks.
SELECT emaj.emaj_set_mark_group('myGroup2', 'ADD_TBL test');
INSERT INTO myschema2.mytbl7 VALUES (1), (2), (3);
SELECT emaj.emaj_set_mark_group('myGroup2', 'Before ADD_TBL');
DELETE FROM myschema2.mytbl7 WHERE col71 = 3;

-- Then add the table to the group.
SELECT emaj.emaj_assign_table('myschema2', 'mytbl7', 'myGroup2', NULL, 'Add mytbl7');

SELECT group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  FROM emaj.emaj_group WHERE group_name = 'myGroup2';
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'mytbl7' ORDER BY rel_time_range;
-- Perform some other updates.
INSERT INTO myschema2.mytbl7 VALUES (4), (5);

-- Remove the alter group mark just after the alter operation and then get statistics and set a mark.
BEGIN;
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'Add mytbl7');
  SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
    FROM emaj.emaj_log_stat_group('myGroup2', 'Before ADD_TBL', NULL);
  SELECT emaj.emaj_set_mark_group('myGroup2', 'Just after ADD_TBL');
  SELECT mark_log_rows_before_next FROM emaj.emaj_mark WHERE mark_name = 'Before ADD_TBL';
ROLLBACK;

-- Set marks and look at statistics.
SELECT emaj.emaj_set_mark_group('myGroup2', 'Just after ADD_TBL');
UPDATE myschema2.mytbl7 SET col71 = 6 WHERE col71 = 2;
SELECT col71, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl7_log ORDER BY emaj_gid;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'ADD_TBL test', NULL);
SELECT emaj.emaj_set_mark_group('myGroup2', 'After ADD_TBL');
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'ADD_TBL test', 'After ADD_TBL');
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl7', 'myGroup2', 'Mk1');

--testing snap, dump changes and sql generation.
SELECT emaj.emaj_snap_group('myGroup2', :'EMAJTESTTMPDIR' || '/snap', '');
\! ls $EMAJTESTTMPDIR/snap
\! rm $EMAJTESTTMPDIR/snap/*

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mk1', 'After ADD_TBL', 'TABLES_ONLY', NULL);
SELECT sql_text FROM emaj_temp_sql WHERE sql_line_number = 0;

SELECT emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_TBL', NULL, :'EMAJTESTTMPDIR' || '/script.sql', ARRAY['myschema2.mytbl7']);
\! rm $EMAJTESTTMPDIR/script.sql

-- Testing mark deletions.
BEGIN;
-- Testing delete a single mark set after the table addition.
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'After ADD_TBL');
  SELECT 'Should NOT exist' FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'After ADD_TBL';
-- Testing delete all marks up TO a mark set after the alter_group.
  SELECT emaj.emaj_delete_before_mark_group('myGroup2', 'EMAJ_LAST_MARK');
  SELECT * FROM emaj.emaj_mark WHERE mark_group = 'myGroup2';
  SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
    WHERE tbl_name = 'mytbl7' ORDER BY tbl_time_id;
ROLLBACK;

BEGIN;
  -- testing the add table mark deletion
  SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add mytbl7';
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'Add mytbl7');
  SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
    WHERE tbl_name = 'mytbl7' ORDER BY tbl_time_id LIMIT 1;
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl7', 'myGroup2', 'Mk1');
ROLLBACK;

-- Test sequences and holes at bounds following a rollback targeting a mark set before an ADD_TBL.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', NULL);
  SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add mytbl7';
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup2', 'Before ADD_TBL', TRUE);
  SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
    WHERE tbl_schema = 'myschema2' AND tbl_name = 'mytbl7' ORDER BY tbl_time_id;
  SELECT * FROM emaj.emaj_seq_hole WHERE sqhl_schema = 'myschema2' AND sqhl_table = 'mytbl7';
  SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
    FROM emaj.emaj_log_stat_group('myGroup2', 'ADD_TBL test', NULL);
ROLLBACK;

-- Estimate and then perform a logged rollback to reach a mark prior the ADD_TBL operation and check.
DELETE FROM emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
SELECT * FROM emaj.emaj_estimate_rollback_group('myGroup2', 'Before ADD_TBL', TRUE);
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_logged_rollback_group('myGroup2', 'Before ADD_TBL', TRUE);
SELECT col71, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl7_log ORDER BY emaj_gid;
SELECT * FROM myschema2.mytbl7 ORDER BY col71;

-- Consolidate this logged rollback.
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'Logged_Rlbk_End');
SELECT mark_time_id FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'Add mytbl7';
SELECT * FROM emaj.emaj_get_consolidable_rollbacks() WHERE cons_group = 'myGroup2'; -- should report 1 row TO consolidate
SELECT * FROM emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
  WHERE tbl_schema = 'myschema2' AND tbl_name = 'mytbl7' ORDER BY tbl_time_id;

-- Perform some other updates.
INSERT INTO myschema2.myTbl7 VALUES (7), (8), (9);

-- Verify the group and detect the lack of the new log table.
BEGIN;
  SELECT emaj.emaj_disable_protection_by_event_triggers();
  DROP TABLE emaj_myschema2.mytbl7_log;
  SELECT * FROM emaj.emaj_verify_all() as t(msg) WHERE msg NOT LIKE '%foreign key%';
  SELECT emaj.emaj_enable_protection_by_event_triggers();
ROLLBACK;

-- Remove myTbl7 from the group, then re-add it.
SELECT emaj.emaj_remove_table('myschema2', 'mytbl7', 'Remove mytbl7');
SELECT emaj.emaj_assign_table('myschema2', 'mytbl7', 'myGroup2', NULL, 'Re-add mytbl7');

SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq = 'mytbl7' ORDER BY rel_time_range;
DELETE FROM myschema2.myTbl7 WHERE col71 = 9;

-- Get statistics.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'ADD_TBL test', NULL);
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'ADD_TBL test', NULL);

-- Dump changes.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mk1', 'EMAJ_LAST_MARK', 'TABLES_ONLY', NULL);
SELECT sql_text FROM emaj_temp_sql WHERE sql_line_number = 0;

-- And finaly revert the group configuration changes.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup2"}', TRUE);

-- And perform a full rollback (that do not process myTbl7).
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_group('myGroup2', 'ADD_TBL test', TRUE);
SELECT * FROM myschema2.mytbl7 ORDER BY col71;

-- Checks for add tables tests.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 9500 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 9500 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 9500 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(9700);

-----------------------------
-- Remove tables.
-----------------------------
INSERT INTO myschema1."myTbl3" (col33) VALUES (1.);
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mk2c');
INSERT INTO myschema1."myTbl3" (col33) VALUES (1.);

SELECT emaj.emaj_remove_tables('myschema1', '^(myTbl3|mytbl2b)$', NULL, '2 tables removed from myGroup1');

SELECT group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  FROM emaj.emaj_group WHERE group_name = 'myGroup1';
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND (rel_tblseq = 'myTbl3' OR rel_tblseq = 'mytbl2b') ORDER BY 1, 2, 3;

DELETE FROM myschema1."myTbl3" WHERE col33 = 1.;
SELECT count(*) FROM emaj_myschema1."myTbl3_log_1";
SELECT * FROM emaj.emaj_verify_all() AS t(msg) WHERE msg NOT LIKE '%foreign key%';

-- Test statistics following a rollback targeting a mark set before a REMOVE_TBL.
BEGIN;
  SELECT emaj.emaj_set_param('dblink_user_password', NULL);
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup1', 'Mk2c', TRUE);
  SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
    FROM emaj.emaj_log_stat_group('myGroup1', 'Mk2b', NULL) ORDER BY 1, 2, 3, 4;
ROLLBACK;

-- Get statistics.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup1', 'Mk1', NULL);
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Mk1', NULL);
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema1', 'myTbl3', 'myGroup1', 'Revert_sequences_removal');

--testing snap, dump changes and sql generation.
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR' || '/snap', '');
\! ls $EMAJTESTTMPDIR/snap
\! rm $EMAJTESTTMPDIR/snap/*

SELECT emaj.emaj_set_mark_group('myGroup1', 'After_2_tables_removed');
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Mk1', 'EMAJ_LAST_MARK', 'TABLES_ONLY', NULL);
SELECT sql_text FROM emaj_temp_sql WHERE sql_line_number = 0;

SELECT emaj.emaj_gen_sql_group('myGroup1', 'Mk1', NULL, :'EMAJTESTTMPDIR' || '/script.sql', ARRAY['myschema1.myTbl3']);
\! rm $EMAJTESTTMPDIR/script.sql

-- Test marks deletions.
BEGIN;
-- Testing delete a single mark set before the table removal.
  SELECT emaj.emaj_delete_mark_group('myGroup1', 'Mk2c');
  SELECT * FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' AND mark_name = 'Mk2b';
-- Testing marks deletion (delete all marks before the alter_group).
  SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'EMAJ_LAST_MARK');
  SELECT 'should not exist' FROM pg_class, pg_namespace
    WHERE relnamespace = pg_namespace.oid AND nspname = 'emaj_myschema1' AND relname IN ('myTbl3_log', 'mytbl2b_log');
  SELECT count(*) FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq IN ('myTbl3', 'mytbl2b');
  SELECT * FROM emaj.emaj_rel_hist WHERE relh_schema = 'myschema1' AND relh_tblseq IN ('myTbl3', 'mytbl2b') ORDER BY 1, 2, 3;
ROLLBACK;

BEGIN;
-- Testing the alter group mark deletion.
  SELECT emaj.emaj_delete_mark_group('myGroup1', '2 tables removed from myGroup1');
  SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND (rel_tblseq = 'myTbl3' OR rel_tblseq = 'mytbl2b') ORDER BY rel_tblseq;
  SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
    WHERE tbl_name IN ('mytbl2b', 'myTbl3') ORDER BY tbl_time_id DESC, tbl_name LIMIT 2;
  SELECT * FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' AND mark_name = '2 tables removed from myGroup1';
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema1', 'myTbl3', 'myGroup1', 'Revert_sequences_removal');
ROLLBACK;

BEGIN;
-- Testing marks deletion (other cases).
  SELECT emaj.emaj_set_mark_group('myGroup1', 'Mk2d');
  SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'Mk2b');
  SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range) ORDER BY 1, 2, 3;
  SELECT * FROM emaj.emaj_rel_hist WHERE relh_schema = 'myschema1' AND relh_tblseq = 'mytbl2b' ORDER BY 1, 2, 3;
  SELECT 'found' FROM pg_class, pg_namespace WHERE relnamespace = pg_namespace.oid AND relname = 'mytbl2b_log_1' AND nspname = 'emaj_myschema1';
  SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'Mk2d');
  SELECT 'should not exist' FROM pg_class, pg_namespace
    WHERE relnamespace = pg_namespace.oid AND relname = 'mytbl2b_log' AND nspname = 'emaj_myschema1';
  SELECT count(*) FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range);
  SELECT * FROM emaj.emaj_rel_hist WHERE relh_schema = 'myschema1' AND relh_tblseq = 'mytbl2b' ORDER BY 1, 2, 3;
ROLLBACK;

-- Testing rollback and consolidation.
INSERT INTO myschema1.mytbl1 VALUES (100, 'Alter_logg', E'\\000'::bytea);
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_logged_rollback_group('myGroup1', 'Mk2b', TRUE) ORDER BY 1, 2;
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'Logged_Rlbk_End');
SELECT emaj.emaj_remove_table('myschema1', 'mytbl1', '2nd remove_tbl');

SELECT * FROM emaj.emaj_get_consolidable_rollbacks() WHERE cons_group = 'myGroup1';
SELECT * FROM emaj.emaj_consolidate_rollback_group('myGroup1', 'Logged_Rlbk_End');
SELECT count(*) FROM emaj_myschema1.mytbl1_log_1;   -- the log table should be empty
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_group('myGroup1', 'Mk2b', TRUE) ORDER BY 1, 2;

-- Testing group's reset.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_reset_group('myGroup1');
  SELECT * FROM emaj.emaj_rel_hist WHERE relh_schema = 'myschema1' AND upper(relh_time_range) > 6050 ORDER BY 1, 2, 3;
ROLLBACK;

-- Testing group's stop and start.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_start_group('myGroup1');
  SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    FROM emaj.emaj_relation WHERE rel_group = 'myGroup1' AND NOT upper_inf(rel_time_range) ORDER BY 1, 2, 3;
  SELECT 'should not exist' FROM pg_class, pg_namespace WHERE relnamespace = pg_namespace.oid AND relname = 'mytbl2b_log' AND nspname = 'emaj_myschema1';
ROLLBACK;

-- Testing the table drop (remove first the sequence linked to the table, otherwise an event triger fires).
BEGIN;
  SELECT emaj.emaj_remove_sequence('myschema1', 'myTbl3_col31_seq', 'remove_seq_before_drop_mytbl3');
  DROP TABLE mySchema1."myTbl3";
ROLLBACK;

-- Testing an ALTER TABLE leading to a table rewrite.
BEGIN;
  ALTER TABLE mySchema1."myTbl3" ALTER COLUMN col33 type decimal (10, 1);
ROLLBACK;

-- Re-add all removed tables.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', ARRAY['myGroup1'], TRUE, 'Revert_tables_removal');

SELECT emaj.emaj_cleanup_rollback_state();

-- Set an intermediate mark.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Mk3');
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;

-- Estimate a rollback crossing alter group operations.
DELETE FROM emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
SELECT emaj.emaj_estimate_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk1', FALSE);

-- Execute a rollback not crossing any alter group operation.
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk3', FALSE) ORDER BY 1, 2;

-- Execute rollbacks crossing alter group operations.
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk2');
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk2', FALSE) ORDER BY 1, 2;
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk2', TRUE) ORDER BY 1, 2;

SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Mk1', TRUE, 'Rollback 2 groups crossing an ALTER group') ORDER BY 1, 2;

-- Checks for remove tables tests.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 9700 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 9700 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 9700 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(10000);

-----------------------------
-- Change the group ownership of a table and a sequence (and some other attributes).
-----------------------------

-- Case when the source group is idle and the destination group is logging.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup2');
  SELECT emaj.emaj_move_table('myschema2', 'myTbl3', 'myGroup1', 'move table to myGroup1 while logging');
  SELECT emaj.emaj_move_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup1', 'move seq to myGroup1 while logging');
  SELECT emaj.emaj_modify_table('myschema2', 'myTbl3', '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1"}'::JSONB,
                                'modify table TO myGroup1 while logging');
  SELECT hist_function, hist_event, hist_object, hist_wording FROM emaj.emaj_hist
    WHERE hist_function LIKE 'MO%' ORDER BY hist_id DESC LIMIT 13;
ROLLBACK;

-- Case when both groups are logging.
-- Generates updates before the group change.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'before move');
UPDATE myschema2."myTbl3" set col32 = col32 + '1 day' WHERE col31 >= 8;

SELECT emaj.emaj_move_table('myschema2', 'myTbl3', 'myGroup1', 'move table to myGroup1');
SELECT emaj.emaj_move_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup1', 'move seq to myGroup1');

BEGIN;
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup1', 'before move', FALSE);
ROLLBACK;
BEGIN;
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup2', 'before move', FALSE);
ROLLBACK;
BEGIN;
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup1', 'before move', TRUE);
ROLLBACK;
BEGIN;
  SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
    FROM emaj.emaj_rollback_group('myGroup2', 'before move', TRUE);
ROLLBACK;

SELECT emaj.emaj_modify_table('myschema2', 'myTbl3', '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1", "priority":100}'::JSONB,
                              'modify table');
SELECT * FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND (rel_tblseq = 'myTbl3' OR rel_tblseq = 'myTbl3_col31_seq') ORDER BY 1, 2, 3, 4;

-- Perform various tasks on the groups.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'after move');
UPDATE myschema2."myTbl3" SET col33 = 12.0 WHERE col31 >= 9;

-- Groups statistics.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_groups('{"myGroup1", "myGroup2"}', 'before move', NULL)
  WHERE stat_schema = 'myschema2' AND stat_table = 'myTbl3';
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_verb, stat_role, stat_rows
  FROM emaj.emaj_detailed_log_stat_groups('{"myGroup1", "myGroup2"}', 'before move', NULL)
  WHERE stat_schema = 'myschema2' AND stat_table = 'myTbl3';

-- Table and sequence statistics.
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'myTbl3', 'myGroup2', 'Mk1');
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup2', 'Mk1');
-- Delete both move marks.
BEGIN;
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'move table to myGroup1');
  SELECT emaj.emaj_delete_mark_group('myGroup1', 'move table to myGroup1');
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'move seq to myGroup1');
  SELECT emaj.emaj_delete_mark_group('myGroup1', 'move seq to myGroup1');
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'myTbl3', 'myGroup2', 'Mk1');
  SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup2', 'Mk1');
ROLLBACK;

-- Generate a sql script crossing the move (before rollback).
SELECT emaj.emaj_gen_sql_groups('{"myGroup1", "myGroup2"}', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script1.sql',
                                ARRAY['myschema2.myTbl3', 'myschema2.myTbl3_col31_seq']);

-- Rollback to a mark set after the group change.
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'after move', FALSE, 'Logged rollback on 2 groups');

-- Rollback to a mark set before the group change.
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'before move', TRUE);

-- Generate a sql script crossing the move (after rollback).
SELECT emaj.emaj_gen_sql_group('myGroup1', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script2.sql',
  ARRAY['myschema2.myTbl3', 'myschema2.myTbl3_col31_seq']);
SELECT emaj.emaj_gen_sql_groups('{"myGroup1", "myGroup2"}', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script3.sql',
  ARRAY['myschema2.myTbl3', 'myschema2.myTbl3_col31_seq']);

\! rm $EMAJTESTTMPDIR/script*

-- Revert the changes and apply with a destination group in idle state.

SELECT emaj.emaj_stop_group('myGroup2');
-- This first import try fails because myGroup1 is missing in the groups to process list.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', ARRAY['myGroup2'], TRUE);
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1", "myGroup2"}', TRUE);

-----------------------------
-- Some extra tests with tables and sequences removal and addition.
-----------------------------
SELECT emaj.emaj_start_group('myGroup2', 'Start');
INSERT INTO myschema2.mytbl1 VALUES (100, 'Started', E'\\000'::bytea);
SELECT nextval('myschema2.myseq1');

-- Remove and assign somes tables and sequences in a single transaction.
BEGIN;
  SELECT emaj.emaj_remove_tables('myschema2', '{"mytbl1", "mytbl2", "myTbl3"}'::TEXT[], 'REMOVE_3_TABLES');
  SELECT emaj.emaj_remove_sequence('myschema2', 'myseq1', 'REMOVE_1_SEQUENCES');

  SELECT emaj.emaj_assign_tables('myschema2', '{"mytbl1", "mytbl2", "myTbl3"}'::TEXT[], 'myGroup2', NULL, 'ASSIGN_3_TABLES');
  SELECT emaj.emaj_assign_sequences('myschema2', '{"myseq1", "myseq2"}'::TEXT[], 'myGroup2', 'ASSIGN_2_SEQUENCES');
  INSERT INTO myschema2.mytbl1 VALUES (110, 'Assigned', E'\\000'::bytea);
  SELECT nextval('myschema2.myseq1');

  -- what if some (all but the emaj sequence) emaj components are missing at remove_table time ?
  SELECT emaj.emaj_disable_protection_by_event_triggers();
  DROP TABLE emaj_myschema2.mytbl1_log;
  DROP FUNCTION emaj_myschema2.mytbl1_log_fnct() CASCADE;
  SELECT emaj.emaj_enable_protection_by_event_triggers();
  SELECT emaj.emaj_remove_tables('myschema2', '{"mytbl1", "mytbl2", "myTbl3"}');
  SELECT emaj.emaj_remove_sequences('myschema2', '{"myseq1", "myseq2"}');
  SELECT emaj.emaj_assign_tables('myschema2', '{"mytbl1", "mytbl2", "myTbl3"}', 'myGroup2',
                                 '{"priority":1, "log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1"}'::JSONB);
  SELECT emaj.emaj_assign_sequences('myschema2', '{"myseq1", "myseq2"}'::TEXT[], 'myGroup2', 'Mark_assign_sequences');
COMMIT;

-- Remove tables and sequences with bad marks.
SELECT emaj.emaj_remove_table('myschema2', 'mytbl1', 'Mark_assign_sequences');
SELECT emaj.emaj_remove_sequence('myschema2', 'myseq1', 'Mark_assign_sequences');

-- Alter a table in a logging group.
SELECT emaj.emaj_remove_table('myschema1', 'mytbl4');
ALTER TABLE myschema1.mytbl4 ALTER COLUMN col45 TYPE VARCHAR(15);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl4', 'myGroup1');

BEGIN;
  SELECT emaj.emaj_remove_table('myschema1', 'mytbl4');
  ALTER TABLE myschema1.mytbl4 ALTER COLUMN col45 TYPE VARCHAR(10);
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl4', 'myGroup1', NULL, 'Assign_mytbl4');
COMMIT;

-- Checks.
SELECT count(*) FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND (mark_name LIKE 'REMOVE%' OR mark_name LIKE 'ASSIGN%');
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema2' AND rel_tblseq IN ('mytbl1', 'mytbl2', 'myTbl3', 'myseq1', 'myseq2')
  ORDER BY rel_schema, rel_tblseq, rel_time_range;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence
  WHERE sequ_schema = 'myschema2' AND sequ_name LIKE 'myseq%' ORDER BY sequ_name, sequ_time_id;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table
  WHERE tbl_schema = 'myschema2' AND tbl_name = 'mytbl1' ORDER BY tbl_time_id;

-- Referential integrity violation attempt while rollbacking to a mark set before an ADD_TBL with a FK involved in the operation.
INSERT INTO myschema1.mytbl2 VALUES (1000, 'a', current_date);
INSERT INTO myschema1.mytbl4 VALUES (100, 'b', 1000, 2, 'ABC');
BEGIN;
  SELECT emaj.emaj_remove_table('myschema1', 'mytbl4', 'Remove_mytbl4');
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl4', 'myGroup1', NULL, 'Reassign_mytbl4');
-- Deactivate the dblink usage to avoid deadlock, the assign_table and the rollback being executed in the same transaction,
--   They conflict on the emaj_myschema1.mytbl4_log_seq log sequence.
  SELECT emaj.emaj_set_param('dblink_user_password', NULL);
-- The rollback should fail at FK check.
  SELECT * FROM emaj.emaj_rollback_group('myGroup1', emaj.emaj_get_previous_mark_group('myGroup1', 'Remove_mytbl4'), TRUE);
ROLLBACK;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

-----------------------------
-- Some extra tests with group ownership change for some tables and sequences.
-----------------------------
-- From an idle to a logging group.
SELECT emaj.emaj_move_tables('myschema4', '.*', '', 'myGroup1', 'MOVE_TBL_idle_to_logging');
SELECT emaj.emaj_move_sequences('myschema2', '.*', '', 'myGroup1', 'MOVE_SEQ_idle_to_logging');

-- Bad mark.
SELECT emaj.emaj_move_tables('myschema4', '.*', '', 'myGroup2', emaj.emaj_get_previous_mark_group('myGroup1', now()));
SELECT emaj.emaj_move_sequences('myschema2', '.*', '', 'myGroup2', emaj.emaj_get_previous_mark_group('myGroup1', now()));

-- From a logging group to another logging group.
SELECT emaj.emaj_move_tables('myschema4', '.*', '', 'myGroup2', 'MOVE_TBL_logging_to_logging');
SELECT emaj.emaj_move_sequences('myschema2', '.*', '', 'myGroup2', 'MOVE_SEQ_logging_to_logging');

-- Reset an idle group to check the emaj_sequence cleaning.
BEGIN;
  SELECT emaj.emaj_reset_group('myGroup4');
  SELECT rel_group, count(*) FROM emaj.emaj_sequence, emaj.emaj_relation
    WHERE sequ_schema = rel_schema AND sequ_name = rel_tblseq AND sequ_time_id <@ rel_time_range
      AND sequ_schema = 'myschema2' AND sequ_name = 'myseq1'
    group BY 1;
ROLLBACK;

-- And back to the idle group.
SELECT emaj.emaj_move_tables('myschema4', '.*', '', 'myGroup4', 'MOVE_TBL_logging_to_idle');
SELECT emaj.emaj_move_sequences('myschema2', '.*', '', 'myGroup4', 'MOVE_SEQ_logging_to_idle');

-- Move a table with fkey and verify all.
SELECT emaj.emaj_move_table('myschema2', 'mytbl2', 'myGroup1', 'Move a table with fkey');
SELECT * FROM emaj.emaj_verify_all() AS t(msg) WHERE msg NOT LIKE '%foreign key%';
SELECT emaj.emaj_move_table('myschema2', 'mytbl2', 'myGroup2', 'Move a table with fkey back');

-- Rename a table and/or change its schema.
ALTER TABLE myschema1.mytbl1 RENAME TO mytbl1_new_name;
SELECT emaj.emaj_remove_table('myschema1', 'mytbl1');
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1_new_name', 'myGroup1');

ALTER TABLE myschema1.mytbl1_new_name SET SCHEMA public;
SELECT emaj.emaj_remove_table('myschema1', 'mytbl1_new_name');
SELECT emaj.emaj_assign_table('public', 'mytbl1_new_name', 'myGroup1');

ALTER TABLE public.mytbl1_new_name RENAME TO mytbl1;
ALTER TABLE public.mytbl1 SET SCHEMA myschema1;
SELECT emaj.emaj_remove_table('public', 'mytbl1_new_name');
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');

-----------------------------
-- Test end: check.
-----------------------------
SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_stop_group('myGroup2');

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Mk1', 'EMAJ_LAST_MARK', NULL, NULL);
SELECT regexp_replace(sql_text, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '<timestamp>', 'g') FROM emaj_temp_sql WHERE sql_line_number = 0;

SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_drop_group('myGroup2');
SELECT emaj.emaj_drop_group('myGroup4');
SELECT emaj.emaj_drop_group('myGroup5');
SELECT emaj.emaj_drop_group('emptyGroup');

-- Checks for group ownership change tests.
SELECT * FROM emaj.emaj_relation_change WHERE rlchg_time_id >= 10000 ORDER BY 1, 2, 3, 4;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 10000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 10000 ORDER BY hist_id;

SELECT nspname FROM pg_namespace WHERE nspname LIKE 'emaj%' ORDER BY nspname;
SELECT * FROM emaj.emaj_schema ORDER BY 1;
SELECT * FROM emaj.emaj_rel_hist ORDER BY 1, 2, 3;

SELECT rlbk_id, rlbk_status, rlbk_comment FROM emaj.emaj_rlbk WHERE rlbk_comment IS NOT NULL ORDER BY 1;

TRUNCATE emaj.emaj_hist;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
