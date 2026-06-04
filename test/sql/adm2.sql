-- Adm2.sql : complex scenario executed by an emaj_adm role.
--            Follows adm1.sql, and includes more specific test cases.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14000);

-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/adm2'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

SET session_authorization TO _regress_emaj_adm1;

-- Before going on, save and reload parameters.
SELECT emaj.emaj_import_parameters_configuration(emaj.emaj_export_parameters_configuration());

-----------------------------
-- Step 8 : use of multi-group functions, start_group(s) without log reset and use old marks (i.e. set before the latest group start).
-----------------------------
-- Stop both groups.
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2']);

-- Start both groups.
SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', FALSE);

-- Set a mark for both groups.
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-2');

-- Logged rollback to Multi-1.
SELECT * FROM emaj.emaj_logged_rollback_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', FALSE) ORDER BY 1, 2;

-- Rollback to Multi-2.
SELECT * FROM emaj.emaj_rollback_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-2', FALSE) ORDER BY 1, 2;

-- Rollback and stop to Multi-1.
SELECT * FROM emaj.emaj_rollback_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1', 'myGroup2'], 'Stop after ROLLBACK');

-- Try to start both groups, but with an old mark name.
SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', FALSE);

-- Really start both groups.
SELECT emaj.emaj_start_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1b', FALSE);

-- Try to rollback several groups to an old common mark.
SELECT * FROM emaj.emaj_rollback_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', FALSE);

-- Set again a mark for both groups.
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-2');

-- Delete the mark for only 1 group and get detailed statistics for the other group.
SELECT emaj.emaj_delete_mark_group('myGroup1', 'Multi-2');

-- Use this mark for the other group before delete it.
SELECT * FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-2', NULL);
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'Multi-2', NULL)
  ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Multi-2', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_delete_mark_group('myGroup2', 'Multi-2');

-- Get statistics using old marks.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'M1', 'M2');
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark,
       stat_last_time_id, stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup2', 'M1', 'M2')
  ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;

-- Delete intermediate old marks.
SELECT emaj.emaj_delete_mark_group('myGroup1', 'Multi-1');
SELECT emaj.emaj_delete_mark_group('myGroup2', 'Multi-1');
-- ... and reuse mark names for parallel rollback test.
SELECT emaj.emaj_rename_mark_group('myGroup1', 'Multi-1b', 'Multi-1');
SELECT emaj.emaj_rename_mark_group('myGroup2', 'Multi-1b', 'Multi-1');

-- Rename an old mark.
SELECT emaj.emaj_rename_mark_group('myGroup2', 'M2', 'Deleted M2');

-- Use emaj_get_previous_mark_group and delete an initial old mark.
SELECT emaj.emaj_delete_before_mark_group('myGroup2',
      (SELECT emaj.emaj_get_previous_mark_group('myGroup2',
             (SELECT time_clock_timestamp FROM emaj.emaj_mark, emaj.emaj_time_stamp
                WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_group = 'myGroup2' AND mark_name = 'M3')+'0.000001 SECOND'::interval)));

-- Comment an old mark.
SELECT emaj.emaj_comment_mark_group('myGroup2', 'M3', 'This mark is old');

-- Try to get a rollback duration estimate on an old mark.
SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'M3', TRUE);

-- Try to rollback on an old mark.
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'M3', FALSE) ORDER BY 1, 2;

-----------------------------
-- Checking step 8.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14000 ORDER BY hist_id;

-- Log tables.
RESET session_authorization;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col51, col52, col53, col54, col55, col56, col57, col58, emaj_verb, emaj_tuple, emaj_gid
  FROM emaj_myschema2.mytbl5_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl6_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14200);

-----------------------------
-- Step 9 : for phil''s group#3", recreate the group as rollbackable, update tables,
--          Rename a mark, then delete 2 marks then delete all before a mark.
-----------------------------
-- Prepare phil's group#3, group.
--
RESET session_authorization;
ALTER TABLE "phil's schema""3"."myTbl2\" add primary key (col21);
SET session_authorization TO _regress_emaj_adm2;

SELECT emaj.emaj_create_group('phil''s group#3",');
SELECT emaj.emaj_assign_tables('phil''s schema"3', '.*', 'my"tbl4', 'phil''s group#3",');
SELECT emaj.emaj_assign_sequences('phil''s schema"3', '.*', NULL, 'phil''s group#3",');

SELECT emaj.emaj_start_group('phil''s group#3",', 'M1_rollbackable');
--
RESET session_authorization;
SET search_path TO public, "phil's schema""3";
--
INSERT INTO "phil's tbl1" SELECT i, 'AB''C', E'\\014'::BYTEA FROM generate_series (1, 31) AS i;
UPDATE "phil's tbl1" SET "phil\s""col13" = E'\\034'::BYTEA WHERE "phil's col11" <= 3;
UPDATE "phil's tbl1" SET "phil\s""col13" = E'\\034'''::BYTEA WHERE "phil's col11" between 18 AND 22;
INSERT INTO "my""tbl4" (col41) VALUES (1);
INSERT INTO "my""tbl4" (col41) VALUES (2);
INSERT INTO "myTbl2\" VALUES (1, 'ABC', '2010-12-31');
DELETE FROM "phil's tbl1" WHERE "phil's col11" > 20;
INSERT INTO "myTbl2\" VALUES (2, 'DEF', NULL);
SELECT nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
--

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('phil''s group#3",', 'M2_rollbackable');
SELECT emaj.emaj_set_mark_group('phil''s group#3",', 'M2_again!');
--
RESET session_authorization;
DELETE FROM "phil's tbl1" WHERE "phil's col11" = 10;
UPDATE "phil's tbl1" SET "phil's col12" = 'DEF' WHERE "phil's col11" <= 2;

SELECT nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_groups(ARRAY['phil''s group#3",'], 'phil''s mark #1', 'Third mark set');
--
SELECT emaj.emaj_rename_mark_group('phil''s group#3",', 'phil''s mark #1', 'phil''s mark #3');
--
SELECT emaj.emaj_delete_mark_group('phil''s group#3",', 'M2_again!');
--
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('phil''s group#3",', 'M1_rollbackable', '');
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'phil''s mark #3', '');
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_changes, stat_rollbacks
  FROM emaj.emaj_log_stat_table('phil''s schema"3', 'phil''s tbl1');
SELECT stat_group, stat_first_mark, stat_is_log_start, stat_last_mark, stat_is_log_stop, stat_increments, stat_has_structure_changed, stat_rollbacks
  FROM emaj.emaj_log_stat_sequence('phil''s schema"3', 'phil''s"seq\1');

--
SELECT * FROM emaj.emaj_logged_rollback_group('phil''s group#3",', 'phil''s mark #3', FALSE) ORDER BY 1, 2;
SELECT * FROM emaj.emaj_rollback_group('phil''s group#3",', 'phil''s mark #3', FALSE) ORDER BY 1, 2;

-----------------------------
-- Checking step 9.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity FROM emaj.emaj_rlbk_stat
  ORDER BY rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14200 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14200 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM "phil's schema""3"."phil's tbl1" ORDER BY "phil's col11", "phil's col12";
SELECT * FROM "phil's schema""3"."myTbl2\" ORDER BY col21;
-- Log tables.
SET session_authorization TO _regress_emaj_adm2;
SELECT "phil's col11", "phil's col12", "phil\s""col13", emaj_verb, emaj_tuple, emaj_gid
  FROM "emaj_phil's schema""3"."phil's tbl1_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM "emaj_phil's schema""3"."myTbl2\_log" ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14300);

-----------------------------
-- Step 10 : for myGroup1, in a transaction, update tables and rollback the transaction,
--           Then rollback to previous mark.
-----------------------------
RESET session_authorization;
SET search_path TO public, myschema1;
--
BEGIN TRANSACTION;
  DELETE FROM mytbl1;
ROLLBACK;
--

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'EMAJ_LAST_MARK', FALSE) ORDER BY 1, 2;

-----------------------------
-- Checking step 10.
-----------------------------
-- emaj tables.
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_quantity FROM emaj.emaj_rlbk_stat
  ORDER BY rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14300 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14300 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14400);

-----------------------------
-- Step 11 : tests snaps and script generation functions.
-----------------------------
-- First perform changes in a table with generated columns.
reset session_authorization;
SET search_path TO public, myschema1;
INSERT INTO mytbl2b (col21) VALUES (10), (11);
UPDATE mytbl2b SET col21 = 12 WHERE col21 = 11;
DELETE FROM mytbl2b WHERE col21 >= 10;

-- Add some updates for tables with unusual types (arrays, geometric).
SET search_path TO public, myschema2;
INSERT INTO myTbl5 VALUES (10, '{"abc", "def", "ghi"}', '{1, 2, 3}', NULL, '{}', NULL, '{"id":1000}', '[2020-01-01, 2021-01-01)', NULL);
INSERT INTO myTbl5 VALUES (20, ARRAY['abc', 'def', 'ghi'], ARRAY[3, 4, 5], ARRAY['2000/02/01'::DATE,
                           '2000/02/28'::DATE], '{"id":1001, "c1":"abc"}', NULL, '{"id":1001}', NULL, XMLPARSE (CONTENT '<foo>bar</foo>'));
UPDATE myTbl5 SET col54 = '{"2010/11/28", "2010/12/03"}', col55 = '{"id":1001, "c2":"def"}', col57 = '{"id":1001, "c3":"ghi"}' WHERE col54 is NULL;
INSERT INTO myTbl6 SELECT i+10, point(i, 1.3), box(point(i, 2), point(i+0.2, 3)), circle(point(5, 5), i), '((-2, -2), (3, 0), (1, 4))',
                          '10.20.30.40/27', 'EXECUTING', (i+10, point(i, 1.3))::mycomposite FROM generate_series (1, 8) AS i;
UPDATE myTbl6 SET col64 = '<(5, 6), 3.5>', col65 = NULL, col67 = 'COMPLETED' WHERE col61 <= 13;

-- Also add rows with unusual text content.
INSERT INTO mytbl2 VALUES (10, E'row 1 \r... AND row 2 with a '' (quote) character', NULL);
INSERT INTO mytbl2 VALUES (11, E'row 1 with a TRUE \\ character\r... AND row 2 with two \\n TRUE AND a '' (quote) characters', NULL);
-- And manupulate NULL characters.
INSERT INTO mytbl1 VALUES (200, E'Start\tEnd ', E'A\\000B'::BYTEA);
UPDATE mytbl1 SET col12 = E' Start\tEnd' WHERE col11 = 200;
DELETE FROM mytbl1 WHERE col13 = E'A\\000B'::BYTEA;

-- Also apply some changes in sequence characteristics.
ALTER SEQUENCE myschema2.myseq1 MINVALUE 1 MAXVALUE 100 INCREMENT 10 START 21 RESTART 11 CACHE 2 CYCLE;

-- Create the directory for the first snaps set.
\! mkdir -p $EMAJTESTTMPDIR/snaps1
-- ... and snap all groups.
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR' || '/snaps1', 'CSV HEADER');
SELECT emaj.emaj_snap_group('myGroup2', :'EMAJTESTTMPDIR' || '/snaps1', 'CSV HEADER');
SELECT emaj.emaj_snap_group('phil''s group#3",', :'EMAJTESTTMPDIR' || '/snaps1', 'CSV HEADER');

\! ls $EMAJTESTTMPDIR/snaps1

-- Generate a sql script for each active group (and check the result with detailed log statistics + number of sequences).
SELECT emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myGroup1.sql');
SELECT coalesce(sum(stat_rows), 0) + 1 /* sequence change */ as check FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myGroup2.sql', ARRAY[
     'myschema2.mytbl1', 'myschema2.mytbl2', 'myschema2.myTbl3', 'myschema2.mytbl4',
     'myschema2.mytbl5', 'myschema2.mytbl6', 'myschema2.myseq1', 'myschema2.myTbl3_col31_seq']);
SELECT sum(stat_rows) + 1 /* sequence change */ AS check FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Multi-1', NULL);
SELECT emaj.emaj_gen_sql_group('phil''s group#3",', 'M1_rollbackable', NULL, :'EMAJTESTTMPDIR' || '/Group3.sql');
SELECT sum(stat_rows) + 1 /* sequence change*/ AS check FROM emaj.emaj_detailed_log_stat_group('phil''s group#3",', 'M1_rollbackable', NULL);

-- Generate another sql script for myGroup1 but with a manual export and check both scripts are the same.
SELECT emaj.emaj_gen_sql_group('myGroup1', 'Multi-1', NULL, NULL);
\setenv FILE1 :EMAJTESTTMPDIR'/myGroup1_2.sql'
\copy (SELECT * FROM emaj_sql_script) TO PROGRAM 'cat >$FILE1'
-- Mask timestamp in initial comment and compare.
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'
\! diff $EMAJTESTTMPDIR/myGroup1.sql $EMAJTESTTMPDIR/myGroup1_2.sql

-- Process \\ characters in script files.
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i_s -s 's/\\\\/\\/g'
-- Comment transaction commands for the need of the current test.
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
-- Mask timestamp in initial comment.
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'

\! ls $EMAJTESTTMPDIR

-- Create the directory for the second snaps set.
\! mkdir $EMAJTESTTMPDIR/snaps2
-- In a single transaction and as superuser:
--   Rollback groups, replay updates with generated scripts, snap groups again and cancel the transaction.
BEGIN;
  SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Multi-1', FALSE) ORDER BY 1, 2;
  SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Multi-1', FALSE) ORDER BY 1, 2;
  SELECT * FROM emaj.emaj_rollback_group('phil''s group#3",', 'M1_rollbackable', FALSE) ORDER BY 1, 2;

  \! cat $EMAJTESTTMPDIR/myGroup1.sql
  RESET session_authorization;
  \set FILE1 :EMAJTESTTMPDIR '/myGroup1.sql'
  \i :FILE1
  \set FILE2 :EMAJTESTTMPDIR '/myGroup2.sql'
  \i :FILE2
  \set FILE3 :EMAJTESTTMPDIR '/Group3.sql'
  \i :FILE3

  SET session_authorization TO _regress_emaj_adm1;
  SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR' || '/snaps2', 'CSV HEADER');
  SELECT emaj.emaj_snap_group('myGroup2', :'EMAJTESTTMPDIR' || '/snaps2', 'CSV HEADER');
  SELECT emaj.emaj_snap_group('phil''s group#3",', :'EMAJTESTTMPDIR' || '/snaps2', 'CSV HEADER');
ROLLBACK;

-- Mask timestamp in _INFO files.
\! sed -i_s -s 's/at .*/at [ts]/' $EMAJTESTTMPDIR/snaps1/_INFO $EMAJTESTTMPDIR/snaps2/_INFO
-- And compare both snaps sets.
-- Sequences are detected as different because of :
-- - the effect of RESTART on is_called and next_val attributes,
-- - internal log_cnt value being reset.
\! diff --exclude _INFO_s $EMAJTESTTMPDIR/snaps1 $EMAJTESTTMPDIR/snaps2

-- Reset the sequence myschema2.myseq1 to its previous characteristics.
RESET session_authorization;
ALTER sequence myschema2.myseq1 restart 1004 start 1000 increment 1 maxvalue 9223372036854775807 minvalue 1000 cache 1 no cycle;

SET session_authorization TO _regress_emaj_adm2;

-----------------------------
-- Checking step 11.
-----------------------------
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14400 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14400 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14500);

-----------------------------
-- Step 12 : test use of a table with a very long name (63 characters long).
-----------------------------
SELECT emaj.emaj_stop_group('phil''s group#3",');

-- Remove the "phil's tbl1" table, rename it and reassign it to its group.
SELECT emaj.emaj_remove_table('phil''s schema"3', 'phil''s tbl1');

RESET session_authorization;
ALTER table "phil's schema""3"."phil's tbl1" RENAME TO table_with_very_looooooooooooooooooooooooooooooooooooooong_name;

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_assign_table('phil''s schema"3', 'table_with_very_looooooooooooooooooooooooooooooooooooooong_name', 'phil''s group#3",');

-- Use the table and its group.
SELECT emaj.emaj_start_group('phil''s group#3",', 'M1_after_table_rename');

RESET session_authorization;
UPDATE "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name SET "phil's col12" = 'GHI' WHERE "phil's col11" between 6 AND 9;

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('phil''s group#3",', 'M2');

RESET session_authorization;
DELETE FROM "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name WHERE "phil's col11" > 18;

SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj.emaj_rollback_group('phil''s group#3",', 'M1_after_table_rename', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_stop_group('phil''s group#3",');
SELECT emaj.emaj_drop_group('phil''s group#3",');

--
RESET session_authorization;
ALTER TABLE "phil's schema""3".table_with_very_looooooooooooooooooooooooooooooooooooooong_name RENAME TO "phil's tbl1";

-----------------------------
-- Checking step 12.
-----------------------------
SET session_authorization TO _regress_emaj_adm1;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14500 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14500 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14600);

-----------------------------
-- Step 13 : test use of groups or marks protection.
-----------------------------
SET session_authorization TO _regress_emaj_adm2;
-- Try to rollback a protected group.
SELECT emaj.emaj_protect_group('myGroup2');
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'M3', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_unprotect_group('myGroup2');

-- Try to rollback over a protected mark.
SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark_to_protect');
SELECT emaj.emaj_protect_mark_group('myGroup1', 'Mark_to_protect');
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Multi-1', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'Mark_to_protect');
SELECT emaj.emaj_delete_mark_group('myGroup1', 'Mark_to_protect');

-----------------------------
-- Checking step 13.
-----------------------------
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14600 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14600 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(14700);

-----------------------------
-- Step 14 : test complex use of rollbacks consolidations, with an application trigger kept enabled.
-----------------------------
SET search_path TO public, myschema1;
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":["mytbl2trg2"]}');

-- 2 consolidations of 2 logged rollbacks.
-- Multi-1 MC1 MC2 RMC1S RMC1D      MC3 MC4 MC5 RMC3S RMC3D.
--         ^       lrg(MC1)         ^           lrg(MC3).
--         Xxxxxxxxxxxxxxx conso(RMC1D).
--                                        Xxxxxxxxxxxxxxxxxxx conso(RMC3D).

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC1');

RESET session_authorization;
INSERT INTO myTbl1 SELECT i, 'Test', 'Conso' FROM generate_series (2000, 2012) as i;
INSERT INTO myTbl2 VALUES (2000, 'TC1', NULL);
DELETE FROM myTbl1 WHERE col11 > 2010;

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC2');

RESET session_authorization;
UPDATE myTbl2 SET col22 = 'TC2' WHERE col22 ='TC1';

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC1', FALSE) ORDER BY 1, 2;

RESET session_authorization;
INSERT INTO myTbl2 VALUES (2000, 'TC3', NULL);

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'RLBK_MC1_DONE');
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC3');

RESET session_authorization;
INSERT INTO "myTbl3" (col33) SELECT generate_series(2000, 2039, 4)/100;
INSERT INTO myTbl4 VALUES (2000, 'FK...', 1, 10, 'ABC');
UPDATE myTbl4 SET col44 = NULL WHERE col41 = 2000;

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC4');
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC5');

SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC3', FALSE) ORDER BY 1, 2;
SELECT cons_group, regexp_replace(cons_end_rlbk_mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_target_rlbk_mark_time_id, cons_rows, cons_marks, cons_marks
  FROM emaj.emaj_get_consolidable_rollbacks();

-- Consolidate both logged rollback at once.
SELECT cons_target_rlbk_mark_name, regexp_replace(cons_end_rlbk_mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
       emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark_name)
  FROM emaj.emaj_get_consolidable_rollbacks() WHERE cons_group = 'myGroup1';

SELECT cons_group, regexp_replace(cons_end_rlbk_mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
       cons_target_rlbk_mark_name, cons_end_rlbk_mark_time_id, cons_target_rlbk_mark_time_id, cons_rows, cons_marks
  FROM emaj.emaj_get_consolidable_rollbacks();

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);

-- 2 consolidations of 2 nested logged rollbacks
-- MC6 MC7 MC8  RMC8S RMC8D M9 RMC6S RMC6D
--         ^    lrg(MC8)
--         Xxxxxxxxxxxx conso(RMC8D)
-- ^                           lrb(MC6)
-- Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx conso(RMC6D)

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC6');

reset session_authorization;
INSERT INTO myTbl1 SELECT i, 'Test', 'Conso' FROM generate_series (2000, 2012) AS i;
INSERT INTO myTbl2 VALUES (3000, 'TC6', NULL);
DELETE FROM myTbl1 WHERE col11 > 2010;

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC7');

RESET session_authorization;
UPDATE myTbl2 SET col22 = 'TC7' WHERE col22 ='TC6';

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC8');

RESET session_authorization;
INSERT INTO myTbl2 VALUES (3001, 'TC8', NULL);

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC8', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC9');

RESET session_authorization;
INSERT INTO "myTbl3" (col33) SELECT generate_series(2000, 2039, 4)/100;
INSERT INTO myTbl4 VALUES (2000, 'FK...', 1, 10, 'ABC');
UPDATE myTbl4 SET col44 = NULL WHERE col41 = 2000;

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC6', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);

-- Consolidation of 2 logged rollbacks referencing the same mark
-- MC10  RMC10S RMC10D M11 RMC10S RMC10D
-- ^     lrg(MC10)
-- ^                       lrb(MC10)
-- Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx conso(RMC10D)

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC10');

RESET session_authorization;
INSERT INTO myTbl1 SELECT i, 'Test', 'Conso' FROM generate_series (3000, 3010) AS i;
DELETE FROM myTbl1 WHERE col11 > 3005;

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC10', FALSE) ORDER BY 1, 2;

RESET session_authorization;
UPDATE myTbl2 SET col22 = 'TC7' WHERE col22 ='TC6';

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC11');

RESET session_authorization;
INSERT INTO myTbl4 VALUES (3000, 'FK...', 1, 10, 'ABC');
UPDATE myTbl4 SET col44 = NULL WHERE col41 = 3000;

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC10', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'EMAJ_LAST_MARK');

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);

-- Consolidation of 1 from 2 overlapping logged rollbacks
-- MC15 MC16 RMC15S RMC15D MC17 RMC16S RMC16D
-- ^         lrg(MC15)
--      ^                       lrg(MC16)
-- Xxxxxxxxxxxxxxxxx conso(RMC15D)

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC15');

RESET session_authorization;
INSERT INTO myTbl1 SELECT i, 'Test', 'Conso' FROM generate_series (4000, 4012) as i;
INSERT INTO myTbl2 VALUES (4000, 'TC15', NULL);
DELETE FROM myTbl1 WHERE col11 > 4010;

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'MC16');

RESET session_authorization;
UPDATE myTbl2 SET col22 = 'TC16' WHERE col22 ='TC15';

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC15', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'RLBK_MC15_DONE');

SELECT emaj.emaj_set_mark_group('myGroup1', 'MC17');

RESET session_authorization;
INSERT INTO myTbl2 VALUES (4001, 'TC15', NULL);

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'MC16', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'RLBK_MC15_DONE');

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Multi-1', NULL);

RESET session_authorization;
SELECT * FROM myTbl1 ORDER BY col11;
SELECT * FROM myTbl2 ORDER BY col21;

-----------------------------
-- Checking step 14.
-----------------------------
SET session_authorization TO _regress_emaj_adm2;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark WHERE mark_group = 'myGroup1' ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence WHERE sequ_schema = 'emaj_myschema1' ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table WHERE tbl_schema = 'myschema1' ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size
  FROM emaj.emaj_seq_hole WHERE sqhl_schema = 'myschema1' ORDER BY 1, 2, 3;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 14700 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 14700 ORDER BY hist_id;

SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Multi-1', TRUE) ORDER BY 1, 2;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
RESET session_authorization;
