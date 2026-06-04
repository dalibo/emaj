-- Stat.sql : test statistics functions.
--   emaj_log_stat_group(), emaj_log_stat_groups().
--   emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups(),
--   emaj_sequence_stat_group() and emaj_sequence_stat_groups().
--   _get_sequences_last_value().
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5000);

-----------------------------
-- Log updates on myschema2 between 3 mono-group and multi-groups marks.
-----------------------------
SET search_path TO public, myschema2;

-- Set a multi-groups mark.
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1');

-- Inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger) and increment myseq1.
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::bytea FROM generate_series (1, 10100) AS i;
UPDATE myTbl1 SET col13=E'\\034'::bytea WHERE col11 <= 500;
DELETE FROM myTbl1 WHERE col11 > 10000;
INSERT INTO myTbl2 SELECT i, 'DEF', current_date FROM generate_series (1, 900) AS i;
SELECT nextval('myschema2.myseq1');
SELECT nextval('myschema2.myseq1');

-- Set marks.
SELECT emaj.emaj_set_mark_group('myGroup2', 'Mark22');
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-2');

-- Inserts/updates/deletes in myTbl3 and myTbl4and increment and alter myseq1.
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
INSERT INTO myTbl4 SELECT i, 'FK...', i, 1, 'ABC' FROM generate_series (1, 100) AS i;
SELECT nextval('myschema2.myseq1');
ALTER SEQUENCE myschema2.myseq1 MAXVALUE 10000;

-- Set marks.
SELECT emaj.emaj_set_mark_group('myGroup2', 'Mark23');
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-3');

-- Reset the sequence alter.
ALTER SEQUENCE myschema2.myseq1 MAXVALUE 2000;

-----------------------------
-- emaj_log_stat_group(), emaj_log_stat_groups(), emaj_detailled_log_stat_group() and emaj_detailled_log_stat_groups() test.
-----------------------------
-- Group is unknown.
SELECT * FROM emaj.emaj_log_stat_group(NULL, NULL, NULL);
SELECT * FROM emaj.emaj_log_stat_groups(ARRAY['unknownGroup'], NULL, NULL);
SELECT * FROM emaj.emaj_detailed_log_stat_group(NULL, NULL, NULL);
SELECT * FROM emaj.emaj_detailed_log_stat_groups(ARRAY['unknownGroup'], NULL, NULL);

-- Invalid marks.
SELECT * FROM emaj.emaj_log_stat_group('myGroup2', NULL, NULL);
SELECT * FROM emaj.emaj_log_stat_group('myGroup2', '', NULL);
SELECT * FROM emaj.emaj_log_stat_group('myGroup2', 'dummyStartMark', NULL);
SELECT * FROM emaj.emaj_log_stat_group('myGroup2', 'Mark22', 'dummyEndMark');
SELECT * FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'dummyStartMark', NULL);
SELECT * FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark22', 'dummyEndMark');

SELECT * FROM emaj.emaj_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], NULL, NULL);
SELECT * FROM emaj.emaj_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'dummyEndMark');
SELECT * FROM emaj.emaj_detailed_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], NULL, NULL);
SELECT * FROM emaj.emaj_detailed_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'dummyEndMark');

-- Start mark > end mark.
-- Original test (uncomment for unit test).
--  SELECT * FROM emaj.emaj_log_stat_group('myGroup2', 'Mark23', 'Mark22');
--  SELECT * FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark23', 'Mark22');

-- Just check the error is trapped, because the error message contains timestamps.
CREATE FUNCTION test_log(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) returns void language plpgsql as
$$
BEGIN
  BEGIN
    PERFORM count(*) FROM emaj.emaj_log_stat_group(v_groupName, v_firstMark, v_lastMark);
    RETURN;
  EXCEPTION WHEN raise_exception THEN
    RAISE NOTICE 'Error trapped on emaj_log_stat_group() call';
  END;
  BEGIN
    PERFORM count(*) FROM emaj.emaj_detailed_log_stat_group(v_groupName, v_firstMark, v_lastMark);
    RETURN;
  EXCEPTION WHEN raise_exception THEN
    RAISE NOTICE 'Error trapped on emaj_detailed_log_stat_group() call';
  END;
  RETURN;
END;
$$;
SELECT test_log('myGroup2', 'Mark23', 'Mark22');
SELECT test_log('myGroup2', 'EMAJ_LAST_MARK', 'Mark22');
DROP FUNCTION test_log(TEXT, TEXT, TEXT);

-- Should be ok.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'Mark21', NULL)
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'Mark22', 'Mark22')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'Mark22', 'Mark23')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'EMAJ_LAST_MARK', '')
  ORDER BY stat_group, stat_schema, stat_table;

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL)
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'Multi-3')
  ORDER BY stat_group, stat_schema, stat_table;

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark21', NULL)
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark22', 'Mark22')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark22', 'Mark23')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'EMAJ_LAST_MARK', '')
  ORDER BY stat_group, stat_schema, stat_table;

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id,
       stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL)
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id,
       stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'Multi-3')
  ORDER BY stat_group, stat_schema, stat_table;

-- Empty group.
SELECT * FROM emaj.emaj_log_stat_group('emptyGroup', 'SM2', NULL);
SELECT * FROM emaj.emaj_detailed_log_stat_group('emptyGroup', 'SM2', NULL);

-- Warning on marks range too wide to be contained by a single log session.
SELECT emaj.emaj_stop_group('myGroup4', 'myGroup4_stop');
SELECT emaj.emaj_start_group('myGroup4', 'myGroup4_restart', FALSE);
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup4', 'myGroup4_start', 'myGroup4_restart')
  ORDER BY stat_group, stat_schema, stat_table;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup4', 'myGroup4_start', '')
  ORDER BY stat_group, stat_schema, stat_table;

-----------------------------
-- emaj_sequence_stat_group(), emaj_sequence_stat_groups() test.
-----------------------------

-- Group is unknown.
SELECT * FROM emaj.emaj_sequence_stat_group('dummy', NULL, NULL);

-- Start mark is null or unknown.
SELECT * FROM emaj.emaj_sequence_stat_group('myGroup1', NULL, NULL);
SELECT * FROM emaj.emaj_sequence_stat_groups(ARRAY['myGroup1'], 'dummy', NULL);

-- End mark is unknown.
SELECT * FROM emaj.emaj_sequence_stat_group('myGroup1', 'EMAJ_LAST_MARK', 'dummy');

-- End mark is prior start mark (not tested as this is the same piece of code as for emaj_log_stat_group().

-- Empty group.
SELECT * FROM emaj.emaj_sequence_stat_group('emptyGroup', 'SM2', NULL);

-- Should be ok.
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'Mark21', NULL)
  ORDER BY stat_group, stat_schema, stat_sequence;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK')
  ORDER BY stat_group, stat_schema, stat_sequence;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'Mark22', 'Mark22')
  ORDER BY stat_group, stat_schema, stat_sequence;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'Mark22', 'Mark23')
  ORDER BY stat_group, stat_schema, stat_sequence;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'EMAJ_LAST_MARK', '')
  ORDER BY stat_group, stat_schema, stat_sequence;

SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL)
  ORDER BY stat_group, stat_schema, stat_sequence;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'Multi-3')
  ORDER BY stat_group, stat_schema, stat_sequence;

----------------------------------------------------------
-- emaj_log_stat_table() and emaj_log_stat_sequence() test.
----------------------------------------------------------
SELECT emaj.emaj_set_mark_group('myGroup2', 'before_log_stat_tblseq');

-- Test errors with input parameters.
-- Schema is unknown.
SELECT * FROM emaj.emaj_log_stat_table('dummy', 'mytbl1');
SELECT * FROM emaj.emaj_log_stat_sequence('dummy', 'mytbl1');

-- Table/sequence is unknown.
SELECT * FROM emaj.emaj_log_stat_table('myschema2', NULL);
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'dummy', 'myGroup1', 'M1');
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', NULL);
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'dummy', 'myGroup1', 'M1');

-- Bad timestamp or marks interval.
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', '2030/01/01'::TIMESTAMPTZ, '2020/01/01'::TIMESTAMPTZ);
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', '2030/01/01'::TIMESTAMPTZ, '2020/01/01'::TIMESTAMPTZ);
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23', 'myGroup2', 'Mark22');
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23', 'myGroup2', 'Mark22');

-- Bad group or mark names.
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'dummy_group', 'M1', 'myGroup2', 'M3');
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'dummy_mark', 'myGroup2', 'M3');
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', NULL);
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'M1', 'dummy_group', 'M3');
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'M1', 'myGroup2', 'dummy_mark');

SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'dummy_group', 'M1', 'myGroup2', 'M3');
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'dummy_mark', 'myGroup2', 'M3');
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'mytbl1', 'myGroup2', NULL);
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'M1', 'dummy_group', 'M3');
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'M1', 'myGroup2', 'dummy_mark');

-- Should be OK.

-- No bounds.
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1');

-- Limit the timeframe with marks range.
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23');

SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', NULL, NULL, 'myGroup2', 'Mark22');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', NULL, NULL, 'myGroup2', 'Mark22');

SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark23');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark23');

-- Marks range from another group.
SELECT emaj.emaj_set_mark_group('emptyGroup', 'log_stat_1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'emptyGroup', 'log_stat_1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'emptyGroup', 'log_stat_1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'emptyGroup', 'log_stat_1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'emptyGroup', 'log_stat_1');

-- Same mark as lower and upper bounds.
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark22');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark22', 'myGroup2', 'Mark22');

-- No data on the requested time or marks interval.
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', '2020/01/01'::TIMESTAMPTZ, '2020/01/02'::TIMESTAMPTZ);
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', '2020/01/01'::TIMESTAMPTZ, '2020/01/02'::TIMESTAMPTZ);
SELECT * FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', NULL, NULL, 'emptyGroup', 'SM2');    -- very old upper bound mark
SELECT * FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', NULL, NULL, 'emptyGroup', 'SM2');

-- Limit the timeframe with timestamps.
BEGIN TRANSACTION;
  SELECT emaj.emaj_set_mark_group('myGroup2', 'tmp_mark');
  INSERT INTO myTbl1 SELECT i, 'TMP', 'TMP' FROM generate_series (100000, 100005) AS i;
  SELECT nextval('myschema2.myseq1');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', transaction_timestamp(), NULL);
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', transaction_timestamp(), NULL);

-- Same timestamp as lower and upper bounds.
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', transaction_timestamp(), transaction_timestamp());
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', transaction_timestamp(), transaction_timestamp());
COMMIT;

-- Perform rollbacks.
--   A logged rollback ...
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'Multi-1');

SELECT emaj.emaj_rename_mark_group('myGroup2', emaj.emaj_get_previous_mark_group('myGroup2', 'EMAJ_LAST_MARK'), 'RLBK_START');
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'RLBK_DONE');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1');

--    ... to consolidate.
BEGIN TRANSACTION;
  SELECT * FROM emaj.emaj_consolidate_rollback_group('myGroup2', 'RLBK_DONE');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1');
ROLLBACK;

--   An unlogged rollback.
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'RLBK_DONE');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Mark23');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Mark23');

-- Delete an intermediate mark (the mark and table/sequence state are deleted).
SELECT emaj.emaj_delete_mark_group('myGroup2', 'tmp_mark');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'before_log_stat_tblseq', 'myGroup2', 'RLBK_START');
SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
       stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'before_log_stat_tblseq', 'myGroup2', 'RLBK_START');

-- Perform group stop and restart.
BEGIN TRANSACTION;
  SELECT emaj.emaj_stop_group('myGroup2', 'stop');
  SELECT emaj.emaj_start_group('myGroup2', 'restart', FALSE);
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1', 'myGroup2', 'Multi-3');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1', 'myGroup2', 'Multi-3');

-- Delete oldest marks and marks at log session boundaries.
  SELECT emaj.emaj_set_mark_group('myGroup2', '1 after start');
  SELECT emaj.emaj_delete_before_mark_group('myGroup2', 'before_log_stat_tblseq');
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'stop');
  SELECT emaj.emaj_delete_mark_group('myGroup2', 'restart');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_changes, stat_rollbacks
    FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1');
  SELECT stat_group, stat_first_mark, stat_first_time_id, stat_is_log_start, stat_last_mark, stat_last_time_id,
         stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
    FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1');
ROLLBACK;

SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'before_log_stat_tblseq');
SELECT * FROM emaj.emaj_delete_mark_group('myGroup2', 'before_log_stat_tblseq');

-----------------------------
-- _get_sequences_last_value() test.
-----------------------------
-- Get all groups, tables and sequences.
SELECT * FROM emaj._get_sequences_last_value(NULL, NULL, NULL, NULL, NULL, NULL)
  WHERE p_key <> 'current_epoch' ORDER BY 1;
-- Filter on groups.
SELECT p_key FROM emaj._get_sequences_last_value('Group', NULL, NULL, NULL, NULL, NULL) ORDER BY 1;
SELECT p_key FROM emaj._get_sequences_last_value(NULL, 'Group', NULL, NULL, NULL, NULL) ORDER BY 1;
-- Filter on tables and sequences.
SELECT p_key FROM emaj._get_sequences_last_value(NULL, NULL, 'tbl2|tbl4', '2b', 'col', 'myschema2') ORDER BY 1;


-- Check for statistics functions family.
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5000 ORDER BY hist_id;
