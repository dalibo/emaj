-- Mark.sql : test emaj_set_mark_group(), emaj_set_mark_groups(), emaj_does_exist_mark_group(),
--                 emaj_comment_mark_group(), emaj_rename_mark_group(), emaj_get_previous_mark_group(),
--                 emaj_delete_mark_group(), emaj_protect_mark_group() and emaj_unprotect_mark_group() functions.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(3000);

SELECT emaj.emaj_start_group('myGroup1', 'Mark1');
SELECT emaj.emaj_start_group('myGroup2', 'Mark2');
SELECT emaj.emaj_start_group('emptyGroup', 'MarkInit');

-----------------------------
-- emaj_set_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_set_mark_group(NULL, NULL);
SELECT emaj.emaj_set_mark_group('unknownGroup', NULL);

-- Reserved mark name.
SELECT emaj.emaj_set_mark_group('myGroup1', 'EMAJ_LAST_MARK');

-- Should be OK.
SELECT emaj.emaj_set_mark_group('myGroup1', 'SM1');
SELECT emaj.emaj_set_mark_group('myGroup2', 'SM1', 'comment recorded at mark''s set');
SELECT emaj.emaj_set_mark_group('myGroup2', 'phil''s mark #1');
SELECT emaj.emaj_set_mark_group('emptyGroup', 'SM1');

-- Check mark existence.
SELECT emaj.emaj_does_exist_mark_group('unknownGroup', 'unknownMark');
SELECT emaj.emaj_does_exist_mark_group('myGroup1', 'unknownMark');
SELECT emaj.emaj_does_exist_mark_group('myGroup1', 'SM1');

-- Duplicate mark name.
SELECT emaj.emaj_set_mark_group('myGroup1', 'SM1');
SELECT emaj.emaj_set_mark_group('myGroup1', 'SM1') WHERE NOT emaj.emaj_does_exist_mark_group('myGroup1', 'SM1');

-- Mark with generated name and in a single transaction.
BEGIN TRANSACTION;
  SELECT emaj.emaj_set_mark_group('myGroup1', NULL);
  SELECT emaj.emaj_set_mark_group('myGroup2', '');
COMMIT;

-- Default value for mark name.
SELECT pg_sleep(0.001);
SELECT emaj.emaj_set_mark_group('myGroup2');

-- Use of % in mark name.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Foo%Bar');

-- Multiple emaj_set_mark_group() using the same generated start mark name => fails.
-- This test is commented because the generated error message differs from one run to another.
--BEGIN;
--  SELECT emaj.emaj_start_group('myGroup4');
--  SELECT emaj.emaj_set_mark_group('myGroup4');
--  SELECT emaj.emaj_set_mark_group('myGroup4');
--ROLLBACK;

-----------------------------
-- emaj_set_mark_groups() tests.
-----------------------------
-- NULL group names array.
SELECT emaj.emaj_set_mark_groups(NULL, NULL);

-- Groups array is unknown.
SELECT emaj.emaj_set_mark_groups('{"unknownGroup", ""}', NULL);

-- Reserved mark name.
SELECT emaj.emaj_set_mark_groups('{"myGroup1"}', 'EMAJ_LAST_MARK');

-- Should be OK.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'SM3');

-- Duplicate mark name and warning on group names array content.
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', NULL, 'myGroup2', '', 'myGroup2', 'emptyGroup', 'myGroup2', 'myGroup1'], 'SM3');

-- Generated mark name.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', '');
SELECT pg_sleep(0.001);
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', NULL);
SELECT pg_sleep(0.001);
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}');

-- Use of % in mark name.
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Bar%Foo', 'comment recorded at mark''s set');

-- Check for emaj_set_mark_group() and emaj_set_mark_groups().
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 3000 ORDER BY time_id;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user FROM emaj.emaj_hist WHERE hist_id >= 3000 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(3200);

-----------------------------
-- emaj_comment_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_comment_mark_group(NULL, NULL, NULL);
SELECT emaj.emaj_comment_mark_group('unknownGroup', NULL, NULL);

-- Mark is unknown.
SELECT emaj.emaj_comment_mark_group('myGroup1', 'unknownMark', NULL);

-- Should be OK.
SELECT emaj.emaj_comment_mark_group('myGroup1', 'SM1', 'a first comment for group #1');
SELECT emaj.emaj_comment_mark_group('myGroup1', 'SM1', 'a better comment for group #1');
SELECT emaj.emaj_comment_mark_group('myGroup2', 'SM1', 'a first comment for group #2');
SELECT emaj.emaj_comment_mark_group('myGroup2', 'SM1', NULL);
SELECT emaj.emaj_comment_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'a comment for group #2');
SELECT emaj.emaj_comment_mark_group('myGroup2', 'phil''s mark #1', 'a good phil''s comment!');
SELECT emaj.emaj_comment_mark_group('emptyGroup', 'SM1', 'a comment on a mark for an empty group');

-----------------------------
-- emaj_get_previous_mark_group().
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_get_previous_mark_group(NULL, NULL::timestamptz);
SELECT emaj.emaj_get_previous_mark_group('unknownGroup', NULL::timestamptz);
SELECT emaj.emaj_get_previous_mark_group(NULL, NULL::TEXT);
SELECT emaj.emaj_get_previous_mark_group('unknownGroup', NULL::TEXT);

-- Mark is unknown in emaj_mark.
SELECT emaj.emaj_get_previous_mark_group('myGroup2', 'unknownMark');

-- Should be OK.
SELECT emaj.emaj_get_previous_mark_group('myGroup2',
                   (SELECT time_clock_timestamp FROM emaj.emaj_mark, emaj.emaj_time_stamp
                      WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_name = 'SM1'));
SELECT emaj.emaj_get_previous_mark_group('myGroup2',
                   (SELECT time_clock_timestamp FROM emaj.emaj_mark, emaj.emaj_time_stamp
                      WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_name = 'SM1')+'0.000001 SECOND'::interval);
SELECT emaj.emaj_get_previous_mark_group('myGroup1',
                   (SELECT min(time_clock_timestamp) FROM emaj.emaj_mark, emaj.emaj_time_stamp
                      WHERE time_id = mark_time_id AND mark_group = 'myGroup1'));

SELECT emaj.emaj_get_previous_mark_group('myGroup2', 'SM1');
SELECT emaj.emaj_get_previous_mark_group('emptyGroup', 'SM1');
SELECT coalesce(emaj.emaj_get_previous_mark_group('myGroup2', 'Mark2'), 'No previous mark');
SELECT emaj.emaj_get_previous_mark_group('myGroup2',
                   (SELECT emaj.emaj_get_previous_mark_group('myGroup2',
                                       (SELECT emaj.emaj_get_previous_mark_group('myGroup2',
                                                           (SELECT emaj.emaj_get_previous_mark_group('myGroup2', 'EMAJ_LAST_MARK')))))));

-----------------------------
-- emaj_rename_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_rename_mark_group(NULL, NULL, NULL);
SELECT emaj.emaj_rename_mark_group('unknownGroup', NULL, NULL);

-- Unknown mark name.
SELECT emaj.emaj_rename_mark_group('myGroup1', 'DummyMark', 'new mark');

-- Invalid new mark name.
SELECT emaj.emaj_rename_mark_group('myGroup1', 'Mark1', 'EMAJ_LAST_MARK');

-- New mark name already exists.
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'SM1');

-- Should be OK.
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', NULL);
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'SM2');
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'SM2');
SELECT emaj.emaj_rename_mark_group('myGroup2', 'phil''s mark #1', 'john''s mark #1');
SELECT emaj.emaj_rename_mark_group('emptyGroup', 'EMAJ_LAST_MARK', 'SM2');

-- Simulate SM2 is the end mark of a logged rollback operations on both myGroup1 and myGroup2 groups.
UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = 'Mark1' WHERE mark_name = 'SM2';
SELECT emaj.emaj_rename_mark_group('myGroup1', 'Mark1', 'First Mark');

-- Check for emaj_comment_mark_group() and emaj_rename_mark_group().
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 3200 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user FROM emaj.emaj_hist WHERE hist_id >= 3200 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(3400);

-----------------------------
-- emaj_delete_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_delete_mark_group(NULL, NULL);
SELECT emaj.emaj_delete_mark_group('unknownGroup', NULL);

-- Unknown mark name.
SELECT emaj.emaj_delete_mark_group('myGroup2', NULL);
SELECT emaj.emaj_delete_mark_group('myGroup2', 'DummyMark');

-- Next attempts should be OK.
SELECT emaj.emaj_delete_mark_group('myGroup1', 'EMAJ_LAST_MARK');

-- Simulate SM3 is an end mark of a logged rollback operations on myGroup1 group.
UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = 'SM1' WHERE mark_group = 'myGroup1' AND mark_name = 'SM3';
SELECT emaj.emaj_delete_mark_group('myGroup1', 'SM1');
SELECT mark_group, mark_name, mark_logged_rlbk_target_mark FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' AND mark_name = 'SM3';

SELECT emaj.emaj_delete_mark_group('emptyGroup', 'MarkInit');

SELECT emaj.emaj_delete_mark_group('myGroup1', 'First Mark');

SELECT sum(emaj.emaj_delete_mark_group('myGroup1', mark_name))
  FROM (SELECT mark_name FROM emaj.emaj_mark
          WHERE mark_group = 'myGroup1' AND (mark_name ~ E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d')
          ORDER BY mark_time_id) as t;

SELECT emaj.emaj_delete_mark_group('myGroup2', 'john''s mark #1');

-- Error: at least 1 mark should remain.
SELECT emaj.emaj_delete_mark_group('myGroup1', 'SM3');

-----------------------------
-- emaj_delete_before_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_delete_before_mark_group(NULL, NULL);
SELECT emaj.emaj_delete_before_mark_group('unknownGroup', NULL);

-- Unknown mark name.
SELECT emaj.emaj_delete_before_mark_group('myGroup2', 'DummyMark');

-- NULL input for mark name returns NULL.
SELECT emaj.emaj_delete_before_mark_group('myGroup2', NULL);

-- Should be OK.
SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'SM3');
SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT emaj.emaj_set_mark_group('emptyGroup', 'EGM3');
SELECT emaj.emaj_set_mark_group('emptyGroup', 'EGM4');
SELECT emaj.emaj_delete_before_mark_group('emptyGroup', 'SM2');

-- Simulate SM2 is an end mark of a logged rollback operations on myGroup2 group.
UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = 'SM1' WHERE mark_group = 'myGroup2' AND mark_name = 'SM2';
-- And delete marks including SM1.
SELECT emaj.emaj_delete_before_mark_group('myGroup2',
      (SELECT emaj.emaj_get_previous_mark_group('myGroup2',
             (SELECT time_clock_timestamp FROM emaj.emaj_mark, emaj.emaj_time_stamp
                WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_group = 'myGroup2' AND mark_name = 'SM2')+'0.000001 SECOND'::interval)));
SELECT mark_group, mark_name, mark_logged_rlbk_target_mark FROM emaj.emaj_mark WHERE mark_group = 'myGroup2' AND mark_name = 'SM2';

-- Check emaj_delete_before_mark_group() also cleans up the emaj_hist table.
SELECT emaj.emaj_set_param('history_retention', '0 second');
SELECT emaj.emaj_set_mark_group('phil''s group#3",', 'Mark4');
SELECT emaj.emaj_set_mark_group('phil''s group#3",', 'Mark5');
SELECT emaj.emaj_delete_before_mark_group('phil''s group#3",', 'Mark4');
SELECT emaj.emaj_set_param('history_retention', NULL);

-- Check for emaj_delete_mark_group() and emaj_delete_before_mark_group().
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 3400 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user FROM emaj.emaj_hist WHERE hist_id >= 3400 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(3600);

-----------------------------
-- emaj_protect_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_protect_mark_group(NULL, NULL);
SELECT emaj.emaj_protect_mark_group('unknownGroup', NULL);
-- Group is not rollbackable.
SELECT emaj.emaj_protect_mark_group('phil''s group#3",', NULL);
-- Mark is unknown.
SELECT emaj.emaj_protect_mark_group('myGroup1', NULL);
SELECT emaj.emaj_protect_mark_group('myGroup1', 'unknownMark');
-- Should be ok.
SELECT emaj.emaj_protect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT mark_time_id, mark_name, mark_group, mark_is_rlbk_protected FROM emaj.emaj_mark WHERE mark_group = 'myGroup1';
SELECT emaj.emaj_protect_mark_group('emptyGroup', 'EMAJ_LAST_MARK');

-- Protect an already protected group.
SELECT emaj.emaj_protect_mark_group('myGroup1', 'SM3');
SELECT mark_time_id, mark_name, mark_group, mark_is_rlbk_protected FROM emaj.emaj_mark WHERE mark_group = 'myGroup1';

-----------------------------
-- emaj_unprotect_mark_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_unprotect_mark_group(NULL, NULL);
SELECT emaj.emaj_unprotect_mark_group('unknownGroup', NULL);
-- Group is not rollbackable.
SELECT emaj.emaj_unprotect_mark_group('phil''s group#3",', NULL);
-- Mark is unknown.
SELECT emaj.emaj_unprotect_mark_group('myGroup1', NULL);
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'unknownMark');
-- Should be ok.
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT mark_time_id, mark_name, mark_group, mark_is_rlbk_protected FROM emaj.emaj_mark WHERE mark_group = 'myGroup1';

SELECT emaj.emaj_unprotect_mark_group('emptyGroup', 'EMAJ_LAST_MARK');

-- Protect an already protected group.
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'SM3');
SELECT mark_time_id, mark_name, mark_group, mark_is_rlbk_protected FROM emaj.emaj_mark WHERE mark_group = 'myGroup1';

-- Check mark protections is removed by stop_group functions.
SELECT emaj.emaj_protect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_is_rlbk_protected
  FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' ORDER BY mark_time_id, mark_group;

SELECT emaj.emaj_stop_group('myGroup1');
SELECT regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_is_rlbk_protected
  FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' ORDER BY mark_time_id, mark_group;

-----------------------------
-- Test functions with group not in logging state.
-----------------------------
-- MyGroup1 is already stopped.
SELECT emaj.emaj_stop_group('myGroup2');

SELECT emaj.emaj_set_mark_group('myGroup1', 'SM1');
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'SM1');
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'RENAMED');
SELECT emaj.emaj_protect_mark_group('myGroup1', 'RENAMED');
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'EMAJ_LAST_MARK');

-- Check marks state.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;

-----------------------------
-- Test end: check.
-----------------------------
SELECT * FROM emaj.emaj_log_session ORDER BY lses_group, lses_time_range;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 3600 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event,
       regexp_replace(hist_object, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 3600 ORDER BY hist_id;
