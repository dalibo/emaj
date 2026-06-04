-- Rollback.sql : test changes log, emaj_rollback_group(), emaj_logged_rollback_group(),
--                emaj_rollback_groups(), emaj_logged_rollback_groups(),
--                emaj_cleanup_rollback_state(), emaj_rollback_activity(),
--                emaj_comment_rollback(),
--                emaj_consolidate_rollback_group() and emaj_get_consolidable_rollbacks() functions.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(4000);

-- Set the triggers state.
ALTER TABLE mySchema1.myTbl2 ENABLE TRIGGER myTbl2trg2;
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":"mytbl2trg2"}'::JSONB);
-- Switch the myTbl2trg1 as an ALWAYS trigger.
ALTER TABLE mySchema1.myTbl2 DISABLE TRIGGER myTbl2trg1, ENABLE ALWAYS TRIGGER myTbl2trg1;

-- Disable event triggers to test cases with missing components.
SELECT emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- Rollback nothing tests.
-----------------------------

-- Group or groups is NULL.
SELECT * FROM emaj.emaj_rollback_group(NULL, NULL);
SELECT * FROM emaj.emaj_logged_rollback_group(NULL, NULL);

SELECT * FROM emaj.emaj_rollback_groups(NULL, NULL);
SELECT * FROM emaj.emaj_logged_rollback_groups(NULL, NULL);

-- Group is unknown.
SELECT * FROM emaj.emaj_rollback_group('unknownGroup', NULL);
SELECT * FROM emaj.emaj_logged_rollback_group('unknownGroup', NULL);

SELECT * FROM emaj.emaj_rollback_groups('{"unknownGroup"}', NULL);
SELECT * FROM emaj.emaj_logged_rollback_groups('{"unknownGroup", "myGroup1"}', NULL);
BEGIN;
  SELECT emaj.emaj_start_group('myGroup1', '');
  SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "unknownGroup"}', 'EMAJ_LAST_MARK');
ROLLBACK;

-- Group not in logging state.
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'EMAJ_LAST_MARK');

SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'EMAJ_LAST_MARK');

SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', NULL);
BEGIN;
  SELECT emaj.emaj_start_group('myGroup1', '');
  SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'EMAJ_LAST_MARK');
ROLLBACK;

-- Start groups and set some marks.
SELECT emaj.emaj_start_group('myGroup1', 'Mark11');
SELECT emaj.emaj_start_group('myGroup2', 'Mark21');

SELECT emaj.emaj_set_mark_group('myGroup1', 'Different_Mark');
SELECT emaj.emaj_set_mark_group('myGroup2', 'Different_Mark');

-- Log tables are empty.
SELECT count(*) FROM emaj_myschema1.mytbl1_log;
SELECT count(*) FROM emaj_myschema1.mytbl2_log;
SELECT count(*) FROM emaj_myschema1.mytbl2b_log;
SELECT count(*) FROM emaj_myschema1."myTbl3_log";
SELECT count(*) FROM emaj_myschema1.mytbl4_log;
SELECT count(*) FROM emaj_myschema2.mytbl1_log;
SELECT count(*) FROM emaj_myschema2.mytbl2_log;
SELECT count(*) FROM emaj_myschema2."myTbl3_log";
SELECT count(*) FROM emaj_myschema1.myTbl4_log;

-- Protected group.
SELECT emaj.emaj_protect_group('myGroup1');
SELECT emaj.emaj_protect_group('myGroup2');
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup2", "myGroup1"}', NULL);
SELECT emaj.emaj_unprotect_group('myGroup1');
SELECT emaj.emaj_unprotect_group('myGroup2');
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

-- Unknown mark name.
SELECT * FROM emaj.emaj_rollback_group('myGroup1', NULL);
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'DummyMark');

SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', NULL);
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'DummyMark');

SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2", ""}', NULL);
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2", NULL}', NULL);
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'DummyMark');

SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2", "myGroup2"}', NULL);
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mark11');

-- Mark name referencing different points in time.
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Different_Mark');
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'EMAJ_LAST_MARK');

-- Attempt to rollback 'audit_only' groups.
SELECT emaj.emaj_create_group('auditOnlyEmptyGroup', FALSE);
SELECT * FROM emaj.emaj_rollback_group('phil''s group#3",', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_logged_rollback_groups(ARRAY['phil''s group#3",', 'auditOnlyEmptyGroup'], 'M1_audit_only');
SELECT emaj.emaj_drop_group('auditOnlyEmptyGroup');

-- Attempt to rollback to a stop mark.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_start_group('myGroup1', 'StartMark', FALSE);
  SELECT emaj.emaj_rename_mark_group('myGroup1', (SELECT emaj.emaj_get_previous_mark_group('myGroup1', 'StartMark')), 'GeneratedStopMark');
  SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'GeneratedStopMark');
ROLLBACK;

-- Attempt to rollback over protected marks.
BEGIN;
  SELECT emaj.emaj_set_mark_group('myGroup1', 'Protected Mark 1');
  SELECT emaj.emaj_protect_mark_group('myGroup1', 'Protected Mark 1');
  SELECT emaj.emaj_set_mark_group('myGroup1', 'Protected Mark 2');
  SELECT emaj.emaj_protect_mark_group('myGroup1', 'Protected Mark 2');
  SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Mark11');
ROLLBACK;

-- Attempt to rollback several groups over one single-mark protected marks.
BEGIN;
  SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Common Mark 1');
  SELECT emaj.emaj_set_mark_group('myGroup1', 'Protected Mark 1');
  SELECT emaj.emaj_protect_mark_group('myGroup1', 'Protected Mark 1');
  SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Common Mark 1');
ROLLBACK;

-- Attempt to logged-rollback over a protected mark.
BEGIN;
  SELECT emaj.emaj_protect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
  SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'Mark11');
ROLLBACK;

-- Rollback to a protected mark.
BEGIN;
  SELECT emaj.emaj_protect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
  SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');
ROLLBACK;

-- Missing application table and mono-group rollback.
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Mark21');
ROLLBACK;
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'Mark21');
ROLLBACK;

-- Internal error when the row corresponding to an application sequence to rollback is missing in emaj_sequence.
BEGIN;
  DELETE FROM emaj.emaj_sequence WHERE sequ_schema = 'myschema2' AND sequ_name = 'myseq1';
  SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Mark21');
ROLLBACK;

-- Should be OK, with different cases of dblink status.
-- Hide dblink_connect functions.
ALTER FUNCTION public.dblink_connect(TEXT, TEXT) RENAME TO renamed_dblink_connect;
ALTER FUNCTION public.dblink_connect(TEXT) RENAME TO renamed_dblink_connect;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Mark21');
ALTER FUNCTION public.renamed_dblink_connect(TEXT, TEXT) RENAME TO dblink_connect;
ALTER FUNCTION public.renamed_dblink_connect(TEXT) RENAME TO dblink_connect;

SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Mark1B');

-- No user/password defined in emaj_param.
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'EMAJ_LAST_MARK');

-- Bad user/password defined in emaj_param.
SELECT emaj.emaj_set_param('dblink_user_password', 'user=<user> password=<password>');
SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Mark1B');

-- Transaction not in READ COMMITTED isolation level => the dblink connection is not possible.
BEGIN;
  SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
  SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'EMAJ_LAST_MARK');
COMMIT;

-- Dblink connection should now be ok (missing right on dblink functions is tested in adm1.sql).
SELECT emaj.emaj_set_param('dblink_user_password', 'user=postgres password=postgres');
SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mark1B');

-- Missing application table and multi-groups rollback.
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT * FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Mark1B');
ROLLBACK;
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT * FROM emaj.emaj_logged_rollback_groups('{"myGroup1", "myGroup2"}', 'Mark1B');
ROLLBACK;

-- Restart groups.
SELECT emaj.emaj_stop_groups('{"myGroup1", "myGroup2"}');
SELECT emaj.emaj_start_group('myGroup1', 'Mark11');
SELECT emaj.emaj_start_group('myGroup2', 'Mark21');

-----------------------------
-- Log phase #1 with 2 unlogged rollbacks.
-----------------------------
-- Populate application tables.
SET search_path TO public, myschema1;
-- Inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger).
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::bytea FROM generate_series (1, 11) AS i;
BEGIN transaction;
  UPDATE myTbl1 SET col13=E'\\034'::bytea WHERE col11 <= 3;
  INSERT INTO myTbl2 values (1, 'ABC', current_date);
COMMIT;
DELETE FROM myTbl1 WHERE col11 > 10;
INSERT INTO myTbl2 values (2, 'DEF', NULL);
INSERT INTO myTbl2 values (3, 'GHI', NULL);
UPDATE myTbl2 set col22 = NULL WHERE col23 IS NULL;
DELETE FROM myTbl2 WHERE col21 = 3 AND col22 IS NULL;
SELECT count(*) FROM mytbl1;
SELECT count(*) FROM mytbl2;
SELECT count(*) FROM mytbl2b;
SELECT count(*) FROM "myTbl3";
SELECT count(*) FROM myTbl4;

-- Set a mark.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark12');
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT * FROM emaj.emaj_log_session WHERE lses_group = 'myGroup1' AND upper_inf(lses_time_range) ORDER BY lses_group, lses_time_range;

-- Inserts/updates/deletes in myTbl3 and myTbl4.
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
UPDATE myTbl4 set col44 = NULL WHERE col41 = 1;
SELECT count(*) FROM "myTbl3";
SELECT count(*) FROM myTbl4;

-- Set a mark.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark13');
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Rollback #1.
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Mark12');
-- Check impact.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33 FROM myschema1."myTbl3" ORDER BY col31;
SELECT col41, col42, col43, col44, col45 FROM myschema1.myTbl4 ORDER BY col41, col43;

-- Rollback #2 (and stop).
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Mark11', FALSE, 'Rollback set by the ''rollback.sql'' script');
SELECT emaj.emaj_stop_group('myGroup1');
-- Check impact.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col11, col12, col13 FROM myschema1.myTbl1 ORDER BY col11, col12;
SELECT col21, col22 FROM myschema1.myTbl2 ORDER BY col21;
SELECT col20, col21 FROM myschema1.myTbl2b ORDER BY col20;

-- Restart group.
SELECT emaj.emaj_start_group('myGroup1', 'Mark11');

-- Check the logs are empty again.
SELECT count(*) FROM emaj_myschema1.mytbl1_log;
SELECT count(*) FROM emaj_myschema1.mytbl2_log;
SELECT count(*) FROM emaj_myschema1.mytbl2b_log;
SELECT count(*) FROM emaj_myschema1."myTbl3_log";
SELECT count(*) FROM emaj_myschema1.mytbl4_log;

-----------------------------
-- Log phase #2 with 2 logged rollbacks.
-----------------------------
-- Populate application tables.
SET search_path TO public, myschema1;
-- Inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger).
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::bytea FROM generate_series (1, 11) AS i;
BEGIN transaction;
  UPDATE myTbl1 SET col13=E'\\034'::bytea WHERE col11 <= 3;
  INSERT INTO myTbl2 VALUES (1, 'ABC', current_date);
COMMIT;
DELETE FROM myTbl1 WHERE col11 > 10;
INSERT INTO myTbl2 VALUES (2, 'DEF', NULL);
SELECT count(*) FROM mytbl1;
SELECT count(*) FROM mytbl2;
SELECT count(*) FROM mytbl2b;

-- Set a mark.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark12');

-- Inserts/updates/deletes in myTbl3 and myTbl4.
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
SELECT count(*) FROM "myTbl3";
SELECT count(*) FROM myTbl4;

-- Set a mark.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark13');
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.mytbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Logged rollback #1.
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'Mark12');
-- Check impact.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33 FROM myschema1."myTbl3" ORDER BY col31;
SELECT col41, col42, col43, col44, col45 FROM myschema1.myTbl4 ORDER BY col41, col43;

-- Logged rollback #2.
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'Mark11', FALSE, 'Logged rollback set by the rollback.sql script');
-- Check impact.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col11, col12, col13 FROM myschema1.myTbl1 ORDER BY col11, col12;
SELECT col21, col22 FROM myschema1.myTbl2 ORDER BY col21;
SELECT col20, col21 FROM myschema1.myTbl2b ORDER BY col20;
-- Check application triggers state.
SELECT nspname, relname, tgname, tgenabled FROM pg_trigger, pg_class, pg_namespace
  WHERE relnamespace = pg_namespace.oid AND tgrelid = pg_class.oid AND tgname LIKE 'mytbl2trg%'
  ORDER BY 1, 2, 3;

-----------------------------
-- Unlogged rollback of logged rollbacks #3.
-----------------------------
ALTER TABLE mySchema1.myTbl2 DISABLE TRIGGER myTbl2trg1;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Mark13');
ALTER TABLE mySchema1.myTbl2 ENABLE TRIGGER myTbl2trg1;
-- Check impact.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col11, col12, col13 FROM myschema1.myTbl1 ORDER BY col11, col12;
SELECT col21, col22 FROM myschema1.myTbl2 ORDER BY col21;
SELECT col20, col21 FROM myschema1.myTbl2b ORDER BY col20;

-----------------------------
-- Rollback an empty group.
-----------------------------
SELECT * FROM emaj.emaj_rollback_group('emptyGroup', 'EGM4');
SELECT * FROM emaj.emaj_logged_rollback_group('emptyGroup', 'EGM3');

-----------------------------
-- Test use of partitionned tables.
-----------------------------
INSERT INTO mySchema4.myTblR1 VALUES (-2), (-1), (0), (1), (2);
SELECT emaj.emaj_assign_table('myschema4', 'mytblr2', 'myGroup4');
SELECT emaj.emaj_start_group('myGroup4', 'myGroup4_start');

INSERT INTO myschema4.myTblM VALUES ('2001-09-11', 0, 'abc'), ('2011-09-11', 10, 'def'), ('2021-09-11', 20, 'ghi');
INSERT INTO mySchema4.myTblP VALUES (-2, 'A', 'Stored in partition 1A'), (-1, 'P', 'Stored in partition 1B'),
                                    (0, 'Q', 'Stored in partition 2'), (1, 'X', 'Stored in partition 2'),
                                    (2, 'Y', 'Also stored in partition 2');

SELECT emaj.emaj_set_mark_group('myGroup4', 'mark1');
TRUNCATE myschema4.myTblM;

UPDATE myschema4.myTblP SET col3 = 'Updated column' WHERE col1 = 0;
UPDATE myschema4.myTblP SET col1 = -2, col3 = 'Moved to partition 1B' WHERE col1 = 2;
INSERT into myschema4.mytblr2 (col2, col3) values (1, 'X');

BEGIN;
-- Failing rollback because mytblr1 is outside the 'myGroup4' and the FK on it is immediate and has a ON DELETE CASCADE clause.
  SELECT * FROM emaj.emaj_logged_rollback_group('myGroup4', 'mark1');
ROLLBACK;

BEGIN;
-- Failing rollback because mytblr1 is outside the 'myGroup4' and the FK on it is immediate.
  ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1);
  SELECT * FROM emaj.emaj_logged_rollback_group('myGroup4', 'mark1');
ROLLBACK;

-- Should be OK.
ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1)
  DEFERRABLE INITIALLY IMMEDIATE;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup4', 'mark1');

SELECT col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mytblm_log;
SELECT col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mytblc1_log;
SELECT col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mytblc2_log;
SELECT col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mypartp1a_log;
SELECT col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mypartp1b_log;
SELECT col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mypartp2_log;

-- Use the functions dedicated to emaj_web.
-- For an equivalent of "select * from emaj.emaj_rollback_group('myGroup4', 'myGroup4_start', true, 'my comment');".
SELECT * FROM emaj._rlbk_async(emaj._rlbk_init(ARRAY['myGroup4'], 'myGroup4_start', FALSE, 1, FALSE, TRUE, 'my comment'), FALSE);
-- And check the result.
SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
       rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id,
       rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_messages
  FROM emaj.emaj_rlbk ORDER BY rlbk_id DESC limit 1;

-----------------------------
-- Test emaj_rollback_activity().
-----------------------------
-- Insert necessary rows into rollback tables in order to test various cases of emaj_rollback_activity() reports.
-- These tests ares performed inside transactions that are then rolled back.
BEGIN;
-- 1 rollback operation in EXECUTING state, but no rollback steps have started yet.
  INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) overriding system value
    VALUES (-1, now()-'0.1 seconds'::INTERVAL);
  INSERT INTO emaj.emaj_time_stamp (time_id, time_clock_timestamp) overriding system value
    VALUES (-2, '2000-01-01 01:00:00');
  INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_status)
    VALUES (1232, ARRAY['group1232'], 'mark1232', -2, -1, TRUE, FALSE,
             1, 5, 4, 3, 1, now()-'0.1 seconds'::INTERVAL, 'EXECUTING');
  INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_estimated_quantity, rlbp_start_datetime, rlbp_duration)
    VALUES (1232, 'RLBK_TABLE', 'schema', 't1', '', '50 seconds'::INTERVAL, NULL, NULL, NULL),
           (1232, 'RLBK_TABLE', 'schema', 't2', '', '30 seconds'::INTERVAL, NULL, NULL, NULL),
           (1232, 'RLBK_TABLE', 'schema', 't3', '', '20 seconds'::INTERVAL, NULL, NULL, NULL),
           (1232, 'CTRL+DBLINK', '',     '',  '', '0.3 second'::INTERVAL, 3,   NULL, NULL);
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
-- The first RLBK_TABLE has started, but the elapse of the step is not yet > estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '11 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '11 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't1';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - ((20.0 + 30.0)+(50.0 - 11.0) + 2*0.3/3) / (11.1 + (20.0 + 30.0)+(50.0 - 11.0) + 2*0.3/3); -- the completion % should be 11%
-- The first RLBK_TABLE is completed, and the step duration < the estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '45.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '45 seconds'::INTERVAL,
                                 rlbp_duration = '45 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't1';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - (20.0 + 30.0 + 2*0.3/3) / (45.1 + (20.0 + 30.0) + 2*0.3/3);  -- the completion % should be 47%
-- The second RLBK_TABLE has started, but the elapse of the step is not yet > estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '73.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '28 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't2';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - (20.0 + (30.0 - 28.0) + 1*0.3/3) / (73.1 + (20.0 + (30.0 - 28.0) + 1*0.3/3));  -- the completion % should be 77%
-- The second RLBK_TABLE has started, but the elapse of the step is already > estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '85.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '40 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't2';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - (20.0 + 1*0.3/3) / (85.1 + (20.0 + 1*0.3/3));  -- the completion % should be 81%
-- The second RLBK_TABLE has started, but the elapse of the step is already > estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '105.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '60 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't2';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - (20.0 + 1*0.3/3) / (105.1 + (20.0 + 1*0.3/3 ));  -- the completion % should be 84%
-- The second RLBK_TABLE is completed, and the step duration > the estimated duration.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '110.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '65 seconds'::INTERVAL,
                                 rlbp_duration = '65 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't2';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - (20.0 + 1*0.3/3) / (110.1 + (20.0 + 1*0.3/3));  -- the completion % should be 85%
-- The third RLBK_TABLE has started, and is almost completed.
  UPDATE emaj.emaj_rlbk SET rlbk_start_datetime = now() - '124.1 seconds'::INTERVAL WHERE rlbk_id = 1232;
  UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = now() - '18.8 seconds'::INTERVAL
    WHERE rlbp_rlbk_id = 1232 AND rlbp_table = 't3';
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct FROM emaj._rollback_activity();
  SELECT 1.0 - ((20.0 - 18.8)) / (124.1 + (20.0 - 18.8));  -- the completion % should be 99%
ROLLBACK;

BEGIN;
-- 1 rollback operation in LOCKING state and without step other than LOCK_TABLE.
  INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) overriding system value
    VALUES (-1, now() - '2 minutes'::INTERVAL);
  INSERT INTO emaj.emaj_time_stamp (time_id, time_clock_timestamp) overriding system value
    VALUES (-2, '2000-01-01 01:00:00');
  INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_status)
    VALUES (1233, ARRAY['group1233'], 'mark1233', -2, -1, TRUE, FALSE,
             'Comment', 1, 5, 4, 3, 1, now() - '2 minutes'::INTERVAL, now() - '1 minutes 59 second'::INTERVAL, 'LOCKING');
  INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
    VALUES (1233, 'LOCK_TABLE', 'schema', 't1', '', NULL, NULL, NULL),
           (1233, 'LOCK_TABLE', 'schema', 't2', '', NULL, NULL, NULL),
           (1233, 'LOCK_TABLE', 'schema', 't3', '', NULL, NULL, NULL);
  SELECT rlbk_id, rlbk_status, rlbk_comment, rlbk_planning_duration, date_trunc('second', rlbk_elapse) as elapse,
         date_trunc('second', rlbk_remaining) as remaining, rlbk_completion_pct
    FROM emaj._rollback_activity();
-- The rollback operation in LOCKING state now has RLBK_TABLE steps.
  INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
    VALUES (1233, 'RLBK_TABLE', 'schema', 't1', '', '0:20:00'::INTERVAL, NULL, NULL),
           (1233, 'RLBK_TABLE', 'schema', 't2', '', '0:02:00'::INTERVAL, NULL, NULL),
           (1233, 'RLBK_TABLE', 'schema', 't3', '', '0:00:20'::INTERVAL, NULL, NULL);
  SELECT rlbk_id, rlbk_status, date_trunc('second', rlbk_elapse) as elapse, date_trunc('second', rlbk_remaining) AS remaining,
         rlbk_completion_pct FROM emaj._rollback_activity();
-- +1 rollback operation in PLANNING state.
  INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) OVERRIDING SYSTEM VALUE
    VALUES (-3, now() - '1 minute'::INTERVAL);
  INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_status)
    VALUES (1234, ARRAY['group1234'], 'mark1234', -2, -3, TRUE, FALSE,
             1, 5, 4, 3, 1, now() - '1 minute'::INTERVAL, 'PLANNING');
  SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence,
         rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, date_trunc('second', rlbk_elapse) AS elapse,
         date_trunc('second', rlbk_remaining) AS remaining, rlbk_completion_pct
    FROM emaj._rollback_activity();
ROLLBACK;

-----------------------------
-- Test emaj_comment_rollback(),
-----------------------------

-- Unknown rollback id.
SELECT emaj.emaj_comment_rollback(NULL, NULL);
SELECT emaj.emaj_comment_rollback(-1, NULL);

-- Set a comment.
SELECT emaj.emaj_comment_rollback(4000, 'First comment');
SELECT rlbk_comment FROM emaj.emaj_rlbk WHERE rlbk_id = 4000;

-- Update a comment.
SELECT emaj.emaj_comment_rollback(4000, 'Updated comment');
SELECT rlbk_comment FROM emaj.emaj_rlbk WHERE rlbk_id = 4000;

-- Delete a comment.
SELECT emaj.emaj_comment_rollback(4000, NULL);
SELECT rlbk_comment FROM emaj.emaj_rlbk WHERE rlbk_id = 4000;

-----------------------------
-- Test emaj_consolidate_rollback_group() and emaj_get_consolidable_rollbacks().
-----------------------------
-- Group is NULL or unknown.
SELECT emaj.emaj_consolidate_rollback_group(NULL, NULL);
SELECT emaj.emaj_consolidate_rollback_group('unknownGroup', NULL);

-- Mark is unknown.
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', NULL);
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'unknown mark');

-- Mark is known but is not an end rollback mark.
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'Mark12');

-- Mark is an end rollback mark but the rollback target mark is invalid.
BEGIN;
  UPDATE emaj.emaj_mark SET mark_logged_rlbk_target_mark = 'dummy' WHERE mark_name = 'Mark12';
  SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'Mark12');
ROLLBACK;

-- Should be ok.
SET search_path TO public, myschema1;

-- Rollback without log rows to delete.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Conso_M1');
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'Conso_M1');
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

-- Mark Conso_M2, updates, mark Conso_M3, updates, logged rlbk back to Conso_M2, updates, rename both marks, consolidate, check and cancel.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Conso_M2');
INSERT INTO myTbl1 SELECT i, 'Test', 'Conso' FROM generate_series (1000, 1011) AS i;
INSERT INTO myTbl2 VALUES (1000, 'TC1', NULL);
DELETE FROM myTbl1 WHERE col11 > 1010;

SELECT emaj.emaj_set_mark_group('myGroup1', 'Conso_M3');
UPDATE myTbl2 set col22 = 'TC2' WHERE col22 ='TC1';

SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'Conso_M2');
INSERT INTO myTbl2 VALUES (1000, 'TC3', NULL);

SELECT emaj.emaj_rename_mark_group('myGroup1', 'Conso_M2', 'Renamed_conso_M2');
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'Renamed_last_mark');
SELECT cons_group, cons_target_rlbk_mark_name, cons_target_rlbk_mark_time_id, cons_end_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_rows, cons_marks
  FROM emaj.emaj_get_consolidable_rollbacks();

SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'Renamed_last_mark');
SELECT cons_group, cons_target_rlbk_mark_name, cons_target_rlbk_mark_time_id, cons_end_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_rows, cons_marks
  FROM emaj.emaj_get_consolidable_rollbacks();

-- Consolidate a rollback already consolidated.
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'Renamed_last_mark');

SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence
  WHERE sequ_schema = 'emaj_myschema1' ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole WHERE sqhl_schema = 'myschema1' ORDER BY 1, 2, 3;

SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Conso_M1');
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":null}'::JSONB);

-- Consolidate a stopped (and empty) group.
BEGIN;
  SELECT emaj.emaj_rename_mark_group('emptyGroup', 'EMAJ_LAST_MARK', 'end_rlbk_mark');
  SELECT emaj.emaj_stop_group('emptyGroup', 'stop');
  SELECT * FROM emaj.emaj_get_consolidable_rollbacks();
  SELECT emaj.emaj_consolidate_rollback_group('emptyGroup', 'end_rlbk_mark');
  SELECT * FROM emaj.emaj_mark WHERE mark_group = 'emptyGroup' ORDER BY mark_time_id, mark_group;
ROLLBACK;

-- Check that dropping the group deletes rows from emaj_sequence and emaj_seq_hole.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_drop_group('myGroup1');
  SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence
    WHERE sequ_name LIKE 'myschema1%' ORDER BY sequ_time_id, sequ_schema, sequ_name;
  SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_schema, tbl_name, tbl_time_id;
  SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole
    WHERE sqhl_schema = 'myschema1' ORDER BY 1, 2, 3;
ROLLBACK;

-----------------------------
-- Test emaj_cleanup_rollback_state().
-----------------------------
-- Rollback a transaction with an E-Maj rollback to generate an ABORTED rollback event.
BEGIN;
  SELECT * FROM emaj.emaj_rollback_group('myGroup4', 'myGroup4_start');
ROLLBACK;
SELECT rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session FROM emaj.emaj_rlbk
  WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED') ORDER BY rlbk_id;
SELECT emaj.emaj_cleanup_rollback_state();

-- Finaly add mytblr1 to the group.
SELECT emaj.emaj_assign_table('myschema4', 'mytblr1', 'myGroup4', NULL, 'Assign mytblr1');

-----------------------------
-- Check application triggers state.
-----------------------------
SELECT nspname, relname, tgname, tgenabled FROM pg_trigger, pg_class, pg_namespace
  WHERE relnamespace = pg_namespace.oid AND tgrelid = pg_class.oid AND tgname LIKE 'mytbl2trg%'
  ORDER BY 1, 2, 3;

-----------------------------
-- Check rollback tables.
-----------------------------
SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_comment,
       rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id,
       rlbk_dblink_schema, rlbk_is_dblink_used,
       CASE WHEN rlbk_end_datetime IS NULL THEN 'NULL' ELSE '[ts]' END AS "end_datetime", rlbk_messages
  FROM emaj.emaj_rlbk ORDER BY rlbk_id;
SELECT rlbs_rlbk_id, rlbs_session,
       CASE WHEN rlbs_end_datetime IS NULL THEN 'NULL' ELSE '[ts]' END AS "end_datetime"
  FROM emaj.emaj_rlbk_session ORDER BY rlbs_rlbk_id, rlbs_session;
SELECT rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_object_def, rlbp_app_trg_type, rlbp_is_repl_role_replica, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  FROM emaj.emaj_rlbk_plan ORDER BY rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object;
SELECT rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id, rlbt_quantity FROM emaj.emaj_rlbk_stat
  ORDER BY rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

-----------------------------
-- Test end: reset history and force sequences id.
-----------------------------
SELECT emaj.emaj_enable_protection_by_event_triggers();
SELECT * FROM emaj.emaj_log_session WHERE lower(lses_time_range) >= 4000 ORDER BY lses_group, lses_time_range;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 4000 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user FROM emaj.emaj_hist WHERE hist_id >= 4000 ORDER BY hist_id;
