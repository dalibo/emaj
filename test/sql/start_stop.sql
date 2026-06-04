-- Start_stop.sql : test emaj_start_group(), emaj_start_groups(),
--                       emaj_is_logging_group(), emaj_get_logging_groups(), emaj_get_idle_groups(),
--                       emaj_stop_group(), emaj_stop_groups(), emaj_force_stop_group(),
--                       emaj_protect_group() and emaj_unprotect_group() functions.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/create_drop'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

SELECT public.handle_emaj_sequences(2000);

-- Build original groups.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', ARRAY['myGroup1', 'myGroup2', 'emptyGroup'], TRUE);

-- Disable event triggers.
-- This is done to allow tests with missing or renamed or altered components.
SELECT emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- emaj_start_group(), emaj_is_logging_group(), emaj_get_logging_groups() and emaj_get_idle_groups() tests.
-----------------------------
-- Group is unknown in emaj_group.
SELECT emaj.emaj_start_group(NULL, NULL);
SELECT emaj.emaj_start_group('unknownGroup', NULL, NULL);
SELECT emaj.emaj_start_groups(ARRAY['unknownGroup1', 'unknownGroup2'], NULL, NULL);
-- Reserved mark name.
SELECT emaj.emaj_start_group('myGroup1', 'EMAJ_LAST_MARK');

-- Detection of a missing application schema.
SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA myschema1 CASCADE;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
RESET client_min_messages;
-- Detection of a missing application relation.
BEGIN;
  DROP TABLE myschema1.mytbl4;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of relation type change (a table is now a sequence!).
BEGIN;
  UPDATE emaj.emaj_relation SET rel_kind = 'S' WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' AND upper_inf(rel_time_range);
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a missing E-Maj log schema.
SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA emaj_myschema1 CASCADE;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
RESET client_min_messages;
-- Detection of a missing log trigger.
BEGIN;
  DROP TRIGGER emaj_log_trg ON myschema1.mytbl1;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a missing log function.
BEGIN;
  DROP FUNCTION emaj_myschema1.mytbl1_log_fnct() CASCADE;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a missing truncate trigger.
BEGIN;
  DROP TRIGGER emaj_trunc_trg ON myschema1.mytbl1;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a missing log table.
BEGIN;
  DROP table emaj_myschema1.mytbl1_log;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a change in the application table structure (new column).
BEGIN;
  ALTER TABLE myschema1.mytbl1 ADD COLUMN newcol int;
  ALTER TABLE myschema1.mytbl1 ADD COLUMN othernewcol TEXT;
  ALTER TABLE myschema1.mytbl2 ADD COLUMN newcol int;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a change in the application table structure (column type change).
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP COLUMN col42;
  ALTER TABLE myschema1.mytbl4 ALTER COLUMN col45 TYPE varchar(15);
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a missing primary key.
BEGIN;
  alter table myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a table altered as UNLOGGED.
BEGIN;
  ALTER TABLE myschema1."myTbl3" SET unlogged;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a primary key structure change.
BEGIN;
  ALTER TABLE myschema1.mytbl4 DROP CONSTRAINT mytbl4_pkey;
  ALTER TABLE myschema1.mytbl4 ADD PRIMARY KEY (col41, col42);
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of STORED generated column whose expression has been dropped.
BEGIN;
  ALTER TABLE myschema1.mytbl2b ALTER COLUMN col22 DROP EXPRESSION;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a column transformed into generated column.
BEGIN;
  ALTER TABLE myschema1.mytbl2b DROP COLUMN col24, ADD COLUMN col24 BOOLEAN GENERATED ALWAYS AS (col21 > 3) STORED;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a bad trigger in a "triggers to ignore at rollback time" array.
BEGIN;
  UPDATE emaj.emaj_relation SET rel_ignored_triggers = '{"dummy1", "dummy2"}'
    WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl1' AND upper_inf(rel_time_range);
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;
-- Detection of a log table missing a technical column.
BEGIN;
  ALTER TABLE emaj_myschema1.mytbl1_log DROP COLUMN emaj_verb;
  SELECT emaj.emaj_start_group('myGroup1', 'M1');
ROLLBACK;

-- Should be OK.
-- Use the first correct emaj_start_group() function call to test the emaj_hist purge.
SELECT emaj.emaj_set_param('history_retention', '0.1 second');
SELECT pg_sleep(0.2);

SELECT emaj.emaj_start_group('myGroup1', 'Mark1');
-- Check old events are deleted.
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist ORDER BY hist_id;

-- Test the smallest history_retention value that means infinity.
SELECT emaj.emaj_set_param('history_retention', '100 years');
SELECT emaj.emaj_start_group('myGroup2', 'Mark2', TRUE);

SELECT emaj.emaj_set_param('history_retention', NULL);
SELECT emaj.emaj_start_group('phil''s group#3",', 'Mark3', FALSE);
SELECT emaj.emaj_start_group('emptyGroup', 'Mark1');

SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_stop_group('myGroup2');

-- Check group state.
SELECT emaj.emaj_is_logging_group('unknownGroup');
SELECT emaj.emaj_is_logging_group('emptyGroup');
SELECT emaj.emaj_is_logging_group('myGroup1');

-- Get group names array.
SELECT emaj.emaj_get_logging_groups();
SELECT emaj.emaj_get_logging_groups('Group');
SELECT emaj.emaj_get_logging_groups(NULL, 'empty');
SELECT emaj.emaj_get_logging_groups('\d', 'my');

SELECT emaj.emaj_get_idle_groups();
SELECT emaj.emaj_get_idle_groups('Group');
SELECT emaj.emaj_get_idle_groups(NULL, 'my');
SELECT emaj.emaj_get_idle_groups('\d', '2');

-- Warnings on FK.

-- Warning on fkey between tables from different groups.
BEGIN;
  ALTER TABLE myschema2.myTbl4 DROP CONSTRAINT mytbl4_col44_fkey, ADD CONSTRAINT mytbl4_col44_fkey
    FOREIGN KEY (col44, col45) REFERENCES myschema1.myTbl1 (col11, col12) ON DELETE CASCADE ON UPDATE SET NULL;
  SELECT emaj.emaj_start_group('myGroup2', 'Mark2');
ROLLBACK;

-- Warning on non deferrable fkey set on partitionned table.
BEGIN;
  ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1);
  SELECT emaj.emaj_start_group('myGroup4', 'Mark1');
ROLLBACK;

-- Warning on fkey set on partitionned table with ON UPDATE|DELETE clause.
BEGIN;
  ALTER TABLE myschema4.myTblP DROP CONSTRAINT mytblp_col1_fkey, ADD FOREIGN KEY (col1) REFERENCES myschema4.mytblr1(col1)
    on UPDATE set NULL;
  SELECT emaj.emaj_start_group('myGroup4', 'Mark1');
ROLLBACK;

-- Start with generated mark name.
SELECT emaj.emaj_start_group('myGroup1', '%abc%', TRUE);
SELECT emaj.emaj_start_group('myGroup2', '', FALSE);

-- Group already started.
SELECT emaj.emaj_start_group('myGroup2', 'Mark3');
SELECT emaj.emaj_start_group('myGroup2', 'Mark3') WHERE NOT emaj.emaj_is_logging_group('myGroup2');
-- Do not stop on error.
SELECT emaj.emaj_start_group('myGroup2', '', TRUE, TRUE);

-- Use of % in start mark name.
SELECT emaj.emaj_start_group('myGroup1', 'Foo%Bar');
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;

-- Multiple emaj_start_group() using the same generated start mark name => fails.
-- This test is commented because the generated error message differs from one run to another.
--BEGIN;
--  SELECT emaj.emaj_start_group('myGroup4');
--  SELECT emaj.emaj_stop_group('myGroup4');
--  SELECT emaj.emaj_start_group('myGroup4', NULL, FALSE);
--ROLLBACK;

-- Check for emaj_start_group().
SELECT group_name, group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group ORDER BY group_name;
SELECT * FROM emaj.emaj_log_session ORDER BY lses_group, lses_time_range;
SELECT * FROM emaj.emaj_group_hist ORDER BY grph_group, grph_time_range;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark WHERE mark_time_id >= 2000 ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 2000 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
  regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
  hist_user FROM emaj.emaj_hist WHERE hist_id >= 2000 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(2200);


-----------------------------
-- emaj_stop_group() tests.
-----------------------------
-- Unknown group.
SELECT emaj.emaj_stop_group(NULL);
SELECT emaj.emaj_stop_group('unknownGroup');
SELECT emaj.emaj_stop_group(NULL, NULL);
SELECT emaj.emaj_stop_group('unknownGroup', NULL);
-- Invalid mark.
SELECT emaj.emaj_stop_group('myGroup1', 'EMAJ_LAST_MARK');
-- Already existing mark.
SELECT emaj.emaj_stop_group('phil''s group#3",', 'Mark3');
-- Missing application table.
BEGIN;
  DROP table mySchema2."myTbl3" cascade;
  SELECT emaj.emaj_stop_group('myGroup2');
ROLLBACK;

-- Should be OK.
SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_stop_group('emptyGroup');

-- Should be OK with a stop mark and logs reset.
SELECT emaj.emaj_stop_group('myGroup2', 'Stop mark', TRUE);

-- Error, group already stopped.
SELECT emaj.emaj_stop_group('myGroup2');
-- Warning, group already stopped but idle groups allowed.
SELECT emaj.emaj_stop_group('myGroup2', NULL, FALSE, TRUE);
SELECT emaj.emaj_stop_group('myGroup2', 'Stop mark 2', FALSE, TRUE);
SELECT emaj.emaj_stop_group('myGroup2') WHERE emaj.emaj_is_logging_group('myGroup2');

-- Start with auto-mark in a single transaction.
BEGIN TRANSACTION;
  SELECT emaj.emaj_start_group('myGroup1');
  SELECT emaj.emaj_start_group('myGroup2', '');
COMMIT;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;

BEGIN TRANSACTION;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_stop_group('myGroup2', '');
COMMIT;

-- Multiple emaj_stop_group() using the same generated start mark name => fails.
-- This test is commented because the generated error message differs from one run to another.
--BEGIN;
--  SELECT emaj.emaj_start_group('myGroup4', 'a_first_start_mark');
--  SELECT emaj.emaj_stop_group('myGroup4', '%');
--  SELECT emaj.emaj_start_group('myGroup4', 'another_start_mark', FALSE);
--  SELECT emaj.emaj_stop_group('myGroup4', '%');
--ROLLBACK;

-- Check for emaj_stop_group().
SELECT group_name, group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group ORDER BY group_name;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark WHERE mark_time_id >= 2200 ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 2200 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
  regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
  hist_user FROM emaj.emaj_hist WHERE hist_id >= 2200 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(2400);

-----------------------------
-- emaj_start_groups() tests.
-----------------------------
SELECT emaj.emaj_stop_group('myGroup1', NULL, FALSE, TRUE);
-- NULL group names array.
SELECT emaj.emaj_start_groups(NULL, NULL, NULL);

-- At least one group is unknown.
SELECT emaj.emaj_start_groups('{""}', NULL);
SELECT emaj.emaj_start_groups('{"unknownGroup", ""}', NULL, TRUE);
SELECT emaj.emaj_start_groups('{"myGroup1", "unknownGroup"}', NULL, FALSE);

-- Reserved mark name.
SELECT emaj.emaj_start_groups('{"myGroup1"}', 'EMAJ_LAST_MARK');

-- OK.
SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'Mark1', TRUE);
-- 2 groups already started and onErrorStop set to TRUE.
SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'Mark2', FALSE, FALSE);
-- 1 group already started and p_loggingGroupsAllowed set to TRUE.
SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'Mark2', FALSE, TRUE);
-- 2 groups already started and p_loggingGroupsAllowed set to TRUE.
SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'Mark3', FALSE, TRUE);

SELECT emaj.emaj_stop_groups('{"myGroup1", "myGroup2"}');

-- Missing application table.
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Mark1', TRUE);
ROLLBACK;

-- Should be OK, with a warning on fkey between tables from different groups and warning on group names array content.
BEGIN;
  ALTER TABLE myschema2.myTbl4 DROP CONSTRAINT mytbl4_col44_fkey;
  ALTER TABLE myschema2.myTbl4 ADD CONSTRAINT mytbl4_col44_fkey
    FOREIGN KEY (col44, col45) REFERENCES myschema1.myTbl1 (col11, col12) ON DELETE CASCADE ON UPDATE SET NULL;
  SELECT emaj.emaj_start_groups(ARRAY['myGroup1', NULL, 'myGroup2', '', 'myGroup2', 'myGroup2', 'myGroup1'], 'Mark1');
ROLLBACK;

-- Check for emaj_start_groups().
SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Mark1', TRUE);
SELECT group_name, group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group ORDER BY group_name;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark WHERE mark_time_id >= 2400 ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 2400 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
  regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
  hist_user FROM emaj.emaj_hist WHERE hist_id >= 2400 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(2500);

-----------------------------
-- emaj_stop_groups() tests.
-----------------------------
-- NULL group names array.
SELECT emaj.emaj_stop_groups(NULL);

-- At least one group is unknown.
SELECT emaj.emaj_stop_groups('{""}');
SELECT emaj.emaj_stop_groups('{"unknownGroup", ""}');
SELECT emaj.emaj_stop_groups('{"myGroup1", "unknownGroup"}');

-- Missing application table.
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2']);
ROLLBACK;

-- Should be OK.
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2'], 'Global Stop at %');

SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;

-- Error, restop but idle groups are not allowed.
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2'], 'STOP');

-- With warning about group names array content and idle groups allowed.
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', NULL, 'myGroup2', '', 'myGroup2', 'myGroup2', 'myGroup1'], 'STOP', FALSE, TRUE);

-- Re-stop with logs reset and using the previous mark name.
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', NULL, 'myGroup2', '', 'myGroup2', 'myGroup2', 'myGroup1'], 'STOP', TRUE, TRUE);

-----------------------------
-- emaj_force_stop_group() tests.
-----------------------------
SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Mark1', TRUE);
-- Unknown group.
SELECT emaj.emaj_force_stop_group(NULL);
SELECT emaj.emaj_force_stop_group('unknownGroup');

-- Should be OK.
-- Missing application schema.
SET client_min_messages TO WARNING;
BEGIN;
  DROP SCHEMA mySchema2 CASCADE;
  SELECT emaj.emaj_force_stop_group('myGroup2');
ROLLBACK;
RESET client_min_messages;
-- Missing application table.
BEGIN;
  DROP TABLE mySchema2."myTbl3" CASCADE;
  SELECT emaj.emaj_force_stop_group('myGroup2');
ROLLBACK;
-- Missing log trigger.
BEGIN;
  DROP TRIGGER emaj_log_trg ON myschema2.mytbl4;
  SELECT emaj.emaj_force_stop_group('myGroup2');
ROLLBACK;
-- Missing truncate trigger.
BEGIN;
  DROP TRIGGER emaj_trunc_trg ON myschema2.mytbl4;
  SELECT emaj.emaj_force_stop_group('myGroup2');
ROLLBACK;
-- Sane group.
SELECT emaj.emaj_force_stop_group('myGroup2');
SELECT emaj.emaj_force_stop_group('myGroup1');

-- Error, group already stopped.
SELECT emaj.emaj_force_stop_group('myGroup2');

-- Check for emaj_stop_groups() and emaj_force_stop_group().
-- Impact of stopped groups.
SELECT group_name, group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group ORDER BY group_name;
SELECT * FROM emaj.emaj_log_session ORDER BY lses_group, lses_time_range;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark WHERE mark_time_id >= 2500 ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 2500 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
  regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
  hist_user FROM emaj.emaj_hist WHERE hist_id >= 2500 ORDER BY hist_id;

SELECT public.handle_emaj_sequences(2600);

-----------------------------
-- emaj_protect_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_protect_group(NULL);
SELECT emaj.emaj_protect_group('unknownGroup');
-- Group is not rollbackable.
SELECT emaj.emaj_protect_group('phil''s group#3",');
-- Group is not in logging state.
SELECT emaj.emaj_protect_group('myGroup1');
-- Should be ok.
SELECT emaj.emaj_start_group('myGroup1', 'M1');
SELECT emaj.emaj_protect_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';
-- Protect an already protected group.
SELECT emaj.emaj_protect_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';
-- Stop should reset the protection.
SELECT emaj.emaj_stop_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';

-----------------------------
-- emaj_unprotect_group() tests.
-----------------------------
-- Group is unknown.
SELECT emaj.emaj_unprotect_group(NULL);
SELECT emaj.emaj_unprotect_group('unknownGroup');
-- Group is not rollbackable.
SELECT emaj.emaj_unprotect_group('phil''s group#3",');
-- Group is not in logging state.
SELECT emaj.emaj_unprotect_group('myGroup1');
-- Should be ok.
SELECT emaj.emaj_start_group('myGroup1', 'M1');
SELECT emaj.emaj_protect_group('myGroup1');
SELECT emaj.emaj_unprotect_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';
-- Unprotect an already unprotected group.
SELECT emaj.emaj_unprotect_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';
SELECT emaj.emaj_stop_group('myGroup1');
SELECT group_is_logging, group_is_rlbk_protected FROM emaj.emaj_group WHERE group_name = 'myGroup1';

SELECT emaj.emaj_enable_protection_by_event_triggers();
-- Check for emaj_protect_group() and emaj_unprotect_group().
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 2600 ORDER BY time_id;
SELECT hist_id, hist_function, hist_event, hist_object,
  regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
  hist_user FROM emaj.emaj_hist WHERE hist_id >= 2600 ORDER BY hist_id;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
