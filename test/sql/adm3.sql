-- Adm3.sql : complex scenario executed by an emaj_adm role.
--            Follows adm1.sql and adm2.sql.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(17000);

-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/adm3'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

SET session_authorization TO _regress_emaj_adm1;
SET search_path TO public, myschema1, emaj;

-----------------------------
-- Step 15 : export / import configurations.
-----------------------------
-- Save parameters on a file and reload them.
SELECT emaj_export_parameters_configuration(:'EMAJTESTTMPDIR' || '/param_config.json');
SELECT emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/param_config.json', TRUE);

-- Also save the groups configuration on a file.
SELECT emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json');

-----------------------------
-- Checking step 15.
-----------------------------
-- emaj tables.
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 17000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 17000 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(17100);

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 16 : test transactions with several emaj operations.
-----------------------------
SELECT emaj_create_group('myGroup4');
SELECT emaj_assign_tables('myschema4', '.*', NULL, 'myGroup4');

-- Several similar operations in a single transaction, using different mark names.
BEGIN;
  SELECT emaj_start_group('myGroup4', 'M1');
  SELECT emaj_set_mark_group('myGroup4', 'M2');
  SELECT emaj_set_mark_group('myGroup4', 'M3');
  SELECT emaj_stop_group('myGroup4', 'M4');
  SELECT emaj_start_group('myGroup4', 'M5', FALSE);
  SELECT emaj_stop_group('myGroup4', 'M6');
COMMIT;

-----------------------------
-- Checking step 16.
-----------------------------
-- emaj tables.
SELECT * FROM emaj.emaj_mark WHERE mark_group = 'myGroup4' ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 17100 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 17100 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(17200);

-----------------------------
-- Step 17 : test partition attach and detach.
-----------------------------
-- Needs postgres 10+.

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj_start_group('myGroup4', 'Start');

RESET session_authorization;
INSERT INTO mySchema4.myTblR1 VALUES (-2), (-1), (0), (1), (2), (11);
INSERT INTO mySchema4.myTblP VALUES (-2, 'A', 'Stored in partition 1A'), (-1, 'P', 'Stored in partition 1B'),
                                    (0, 'Q', 'Stored in partition 2'), (1, 'X', 'Stored in partition 2'),
                                    (2, 'Y', 'Also stored in partition 2');
----INSERT INTO mySchema4.myTblP VALUES (-1, 'Stored in partition 1'), (1, 'Stored in partition 2');

SELECT emaj_set_mark_group('myGroup4', 'M1');
----UPDATE mySchema4.myTblP SET col1 = 2 WHERE col1 = 1;
UPDATE myschema4.myTblP SET col1 = -2, col3 = 'Moved to partition 1B' WHERE col1 = 2;

-- Create a new partition and add it into the group ; in passing also add the sequence linked to the serial column of the mother table.
CREATE TABLE mySchema4.myPartP3 PARTITION OF mySchema4.myTblP FOR VALUES FROM (10) TO (19);
-- Create the table with PG 9.6- so that next scripts do not abort.
CREATE TABLE IF NOT EXISTS mySchema4.myPartP3 () INHERITS (mySchema4.myTblP);
-- Add a PK (will fail with PG12+ because of the global PK).
ALTER TABLE mySchema4.myPartP3 ADD PRIMARY KEY (col1);
GRANT ALL ON mySchema4.myPartP3 TO _regress_emaj_adm1, _regress_emaj_adm2;

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_assign_table('myschema4', 'mypartp3', 'myGroup4', NULL, 'Add partition 3');
SELECT emaj_assign_sequence('myschema4', 'mytblp_col4_seq', 'myGroup4', 'Add partition 3_seq');

RESET session_authorization;
INSERT INTO mySchema4.myTblP VALUES (11, 'A', 'Stored in partition 3');

-- Remove obsolete partitions ; in passing also remove the sequence linked to the serial column of the mother table.
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_remove_tables('myschema4', ARRAY['mypartp1a', 'mypartp1b'], 'Remove partition 1a & 1b');
SELECT emaj_remove_sequence('myschema4', 'mytblp_col4_seq', 'Remove partition 1_seq');

RESET session_authorization;
DROP TABLE mySchema4.myPartP1a, mySchema4.myPartP1b CASCADE;
SET session_authorization TO _regress_emaj_adm2;

-- Verify that emaj_adm has the proper grants to delete old marks leading to an old log table drop.
BEGIN TRANSACTION;
  SELECT emaj_delete_before_mark_group('myGroup4', 'EMAJ_LAST_MARK');
ROLLBACK;

-- Look at statistics and log content.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj_log_stat_group('myGroup4', 'Start', NULL) ORDER BY 1, 2, 3, 4;
SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table,
       rel_emaj_verb_attnum, rel_has_always_ident_col, rel_log_seq_last_value
  FROM emaj.emaj_relation WHERE rel_schema = 'myschema4' AND rel_tblseq LIKE 'mypar%' ORDER BY rel_tblseq, rel_time_range;

RESET session_authorization;
SELECT col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mypartP3_log ORDER BY emaj_gid;
SELECT col1, col2, col3, col4, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema4.mypartP1a_log_1 ORDER BY emaj_gid;

-- Rollback to a mark set before the first changes.
SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj_rollback_group('myGroup4', 'Start');
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj_rollback_group('myGroup4', 'Start', TRUE);

SELECT emaj_stop_group('myGroup4');
SELECT emaj_drop_group('myGroup4');

-----------------------------
-- Checking step 17.
-----------------------------
-- emaj tables.
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 17200 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 17200 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(17400);

-----------------------------
-- Step 18 : test defect with application table or sequence.
--           Also test some changes on the unlogged and the with oids tables.
-----------------------------
SELECT emaj_create_group('phil''s group#3",', FALSE);
SELECT emaj_assign_tables('phil''s schema"3', '.*', NULL, 'phil''s group#3",');
SELECT emaj_assign_sequences('phil''s schema"3', '.*', NULL, 'phil''s group#3",');
SELECT emaj_assign_tables('myschema5', '.*', NULL, 'phil''s group#3",');

SELECT emaj_start_group('phil''s group#3",', 'start');

-----------------------------
-- Test changes on the unlogged table.
-----------------------------
RESET session_authorization;
INSERT INTO myschema5.myUnloggedTbl VALUES (10), (11), (12);
UPDATE myschema5.myUnloggedTbl SET col1 = 13 WHERE col1 = 12;
DELETE FROM myschema5.myUnloggedTbl WHERE col1 = 10;

SET session_authorization TO _regress_emaj_adm2;
SELECT col1, emaj_verb, emaj_tuple, emaj_gid, emaj_user FROM emaj_myschema5.myUnloggedTbl_log ORDER BY emaj_gid;

-- Disable event triggers for this step and change an application table structure.
SELECT emaj_disable_protection_by_event_triggers();

-----------------------------
-- Test remove_and_add operations to repair an application table.
-----------------------------
RESET session_authorization;
ALTER TABLE "phil's schema""3"."my""tbl4" ALTER COLUMN col45 TYPE CHAR(11);

SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj_verify_all() as t(msg) WHERE msg NOT LIKE '%foreign key%';
SELECT emaj_remove_table('phil''s schema"3', 'my"tbl4', 'remove_the_damaged_table');
SELECT emaj_assign_table('phil''s schema"3', 'my"tbl4', 'phil''s group#3",', NULL, 're_add_the_table');

SELECT * FROM emaj.emaj_relation WHERE rel_schema = 'phil''s schema"3' AND rel_tblseq = 'my"tbl4' ORDER BY rel_time_range;

-----------------------------
-- Test a remove operation to fix the case of a dropped log table, log sequence or log function.
-----------------------------
RESET session_authorization;
DROP TABLE "emaj_phil's schema""3"."my""tbl4_log";

SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj_verify_all() as t(msg) WHERE msg NOT LIKE '%foreign key%';
SELECT emaj_remove_table('phil''s schema"3', 'my"tbl4', 'remove_the_damaged_table_2');
SELECT emaj_assign_table('phil''s schema"3', 'my"tbl4', 'phil''s group#3",', NULL, 're_add_the_table_2');

SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  FROM emaj.emaj_relation WHERE rel_schema = 'phil''s schema"3' AND rel_tblseq = 'my"tbl4' ORDER BY rel_time_range;

-----------------------------
-- Test a remove operation to fix the case of a dropped application table or sequence.
-----------------------------
-- In fact just rename the table and the sequence.
RESET session_authorization;
ALTER TABLE "phil's schema""3"."my""tbl4" RENAME TO mytbl4_sav;
ALTER SEQUENCE "phil's schema""3"."phil's""seq\1" RENAME TO "phil's""seq\1_sav";

SET session_authorization TO _regress_emaj_adm2;

-- Try to set a mark.
BEGIN;
  SELECT emaj_set_mark_group('phil''s group#3",', 'should fails');
ROLLBACK;

SELECT emaj_remove_table('phil''s schema"3', 'my"tbl4', 'remove_the_dropped_table');
SELECT emaj_remove_sequence('phil''s schema"3', 'phil''s"seq\1', 'remove_the_dropped_seq');

-- Check that the log table does not exist anymore.
SELECT count(*) FROM "emaj_phil's schema3"."my""tbl4_log";

-- Revert the changes.
RESET session_authorization;
ALTER TABLE "phil's schema""3".mytbl4_sav RENAME TO "my""tbl4";
ALTER TABLE "phil's schema""3"."my""tbl4" ALTER COLUMN col45 TYPE CHAR(10);
ALTER SEQUENCE "phil's schema""3"."phil's""seq\1_sav" RENAME TO "phil's""seq\1";

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_assign_table('phil''s schema"3', 'my"tbl4', 'phil''s group#3",', NULL, 'revert_last_changes_tbl');
SELECT emaj_assign_sequence('phil''s schema"3', 'phil''s"seq\1', 'phil''s group#3",', 'revert_last_changes_seq');

-- Ree-nable the event triggers and drop the group.
SELECT emaj_enable_protection_by_event_triggers();
SELECT emaj_stop_group('phil''s group#3",');
SELECT emaj_drop_group('phil''s group#3",');

-----------------------------
-- Checking step 18.
-----------------------------
-- emaj tables.
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 17400 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 17400 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(17600);

-----------------------------
-- Step 19 : test use of dynamic tables group management (assign, move, remove, change).
-----------------------------

-- Create, start and populate groups.
-- Grp_tmp is empty, grp_tmp_3 and grp_tmp_4 contains tables and sequences frop respectively phil''s schema3 and myschema4.
-- Grp_tmp_4 is started before being populated.
SELECT emaj_create_group('grp_tmp');
SELECT emaj_create_group('grp_tmp_3');
SELECT emaj_create_group('grp_tmp_4');
SELECT emaj_start_groups('{"grp_tmp", "grp_tmp_4"}', 'Start');
BEGIN;
  SELECT emaj_assign_tables('phil''s schema"3', '.*', '', 'grp_tmp_3');
  SELECT emaj_assign_sequences('phil''s schema"3', '.*', '', 'grp_tmp_3');
  SELECT emaj_assign_tables('myschema4', '.*', '', 'grp_tmp_4');
  SELECT emaj_assign_sequences('myschema4', '.*', '', 'grp_tmp_4');
  SELECT emaj_modify_table('myschema4', 'mytblm', '{"ignored_triggers": "mytblm_insert_trigger"}'::JSONB);
COMMIT;
SELECT emaj_start_group('grp_tmp_3', 'Start');
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk1');

-- Export the initial groups configuration.
SELECT emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json', ARRAY['grp_tmp', 'grp_tmp_3', 'grp_tmp_4']);

-- Perform some changes and set marks.
RESET session_authorization;
INSERT INTO "phil's schema""3"."my""tbl4" (col41)
  SELECT i FROM generate_series(3, 8) i;
DELETE FROM "phil's schema""3"."myTbl2\";
INSERT INTO "phil's schema""3"."myTbl2\" (col22, col23)
  SELECT 'After Mk1', '12-31-2020' FROM generate_series(1, 3);

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk2');

RESET session_authorization;
INSERT INTO "phil's schema""3"."myTbl2\" (col22, col23)
  SELECT 'After Mk2', '12-31-2030' FROM generate_series(1, 3);
SELECT nextval(E'"phil''s schema""3"."phil''s""seq\\1"');
INSERT INTO myschema4.mytblm
  SELECT '2006-06-30'::DATE + ('1 year'::interval) * i, i, 'After Mk2'
    FROM generate_series(0, 9) i;

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk3');

RESET session_authorization;
DELETE FROM myschema4.mytblm
  WHERE col1 = '2006-06-30';
UPDATE myschema4.mytblm SET col3 = 'After Mk2 AND updated after Mk3'
  WHERE col1 > '2013-01-01';

-- Rollback to the previous mark.
SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj_rollback_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk3');

-- Change some priority and log tablespaces.
SELECT emaj_modify_tables('phil''s schema"3', '.*tbl1', '', '{"priority":-1, "log_data_tablespace":"tsp log''2"}'::JSONB, 'Modify 1 table');

-- Move all tables and sequences into grp_tmp and set a common mark.
SELECT emaj_move_tables('phil''s schema"3', '.*', '', 'grp_tmp', 'Move_tbl_3_to_tmp');
SELECT * FROM emaj_verify_all() as t(msg) WHERE msg NOT LIKE '%foreign key%';
SELECT emaj_move_sequences('phil''s schema"3', '.*', '', 'grp_tmp', 'Move_seq_3_to_tmp');
SELECT emaj_move_tables('myschema4', '.*', '', 'grp_tmp', 'Move_tbl_4_to_tmp');
SELECT emaj_move_sequences('myschema4', '.*', '', 'grp_tmp', 'Move_seq_4_to_tmp');

SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk4');

-- Perform some other changes and set marks.
RESET session_authorization;
UPDATE "phil's schema""3"."my""tbl4" SET col42 = 'Updated after Mk4'
  WHERE col41 > 5;
DELETE FROM "phil's schema""3"."myTbl2\"
  WHERE col21 = 4;
DELETE FROM "phil's schema""3"."my""tbl4"
  WHERE col41 = 4;

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk5');

RESET session_authorization;
INSERT INTO myschema4.mytblm
  SELECT '2017-06-30'::date + ('1 year'::INTERVAL) * i, 5, 'After Mk5'
    FROM generate_series(0, 3) i;
SELECT nextval(E'"phil''s schema""3"."phil''s""seq\\1"');

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk6');

RESET session_authorization;
UPDATE myschema4.mytblm SET col3 = 'After Mk5 AND updated after Mk6'
  WHERE col1 > '2017-01-01';

-- Remove the table mytblm and the sequence phil's seq\1.
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_remove_table('myschema4', 'mytblc1', 'Remove_mytblc1');
SELECT emaj_remove_sequence('phil''s schema"3', 'phil''s"seq\1', 'Remove_myseq1');

-- Logged rollback to Mk5.
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj_logged_rollback_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk5', TRUE);
SELECT emaj_rename_mark_group(group_name, 'EMAJ_LAST_MARK', 'End_logged_rollback')
  FROM (VALUES ('grp_tmp_3'), ('grp_tmp_4'), ('grp_tmp')) as t(group_name);

-- Perform some other changes and set marks.
RESET session_authorization;
DELETE FROM myschema4.mytblm
  WHERE col1 = '2018-06-30';

SET session_authorization TO _regress_emaj_adm1;
SELECT emaj_set_mark_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk7');

RESET session_authorization;
DELETE FROM mySchema4.mytblm;

-- Consolidate the logged rollback.
SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj_get_consolidable_rollbacks() ORDER BY 1, 2;
SELECT emaj_consolidate_rollback_group('grp_tmp', 'End_logged_rollback');

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj_log_stat_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk1', NULL)
  ORDER BY stat_first_mark_datetime, stat_schema, stat_table;
SELECT mark_time_id, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_group,
       mark_log_rows_before_next
  FROM emaj.emaj_mark WHERE mark_group in ('grp_tmp_3', 'grp_tmp_4', 'grp_tmp')
  ORDER BY 1, 2, 3;

-- Generate sql script.
SELECT emaj_gen_sql_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk1', NULL, :'EMAJTESTTMPDIR' || '/allGroups.sql');
--  \! grep -iP '(insert|update|delete|alter)' $EMAJTESTTMPDIR/allGroups.sql.

-- Revert the priority and log tablespaces changes.
SELECT emaj_modify_tables('phil''s schema"3', '.*tbl1', '', '{"priority":null, "log_data_tablespace":null}'::JSONB, 'revert changes for 1 table');

-- Rollback to a mark set before the tables and sequences move.
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj_rollback_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk3', FALSE);
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj_rollback_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk3', TRUE);

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj_log_stat_groups('{"grp_tmp_3", "grp_tmp_4", "grp_tmp"}', 'Mk1', NULL)
  ORDER BY stat_first_mark_datetime, stat_schema, stat_table;

-- Delete all marks before Mk3.
SELECT emaj_delete_before_mark_group(group_name, 'Mk3')
  FROM (VALUES ('grp_tmp_3'), ('grp_tmp_4'), ('grp_tmp')) AS t(group_name);

-- Test a remove_table following log sequence deletion.
RESET session_authorization;
SELECT emaj_disable_protection_by_event_triggers();
DROP SEQUENCE "emaj_phil's schema""3"."my""tbl4_log_seq";
SELECT emaj_enable_protection_by_event_triggers();

SET session_authorization TO _regress_emaj_adm2;
-- Note that the warning about the mytblp_col3_seq sequence is normal.
SELECT * FROM emaj_verify_all() AS t(msg) WHERE msg NOT LIKE '%foreign key%';
--     A removal while the group is LOGGING fails.
SELECT emaj_remove_table('phil''s schema"3', 'my"tbl4');
SELECT emaj_force_stop_group('grp_tmp');
SELECT emaj_remove_table('phil''s schema"3', 'my"tbl4');
SELECT emaj_assign_table('phil''s schema"3', 'my"tbl4', 'grp_tmp');
SELECT * FROM emaj_verify_all() AS t(msg) WHERE msg NOT LIKE '%foreign key%';
SELECT emaj_start_group('grp_tmp', 'Group restart');

-- Import the groups configuration.
SELECT emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/groups_config.json', NULL, TRUE, 'REVERT_CHANGES');

-- Reset groups at their initial state.
SELECT emaj_stop_group('grp_tmp_3');
SELECT emaj_drop_group('grp_tmp_3');
SELECT emaj_set_mark_group('grp_tmp_4', 'Last_mark');
SELECT emaj_force_drop_group('grp_tmp_4');
SELECT emaj_stop_group('grp_tmp');
SELECT emaj_drop_group('grp_tmp');

SELECT * FROM emaj.emaj_rel_hist ORDER BY 1, 2, 3;
SELECT count(*) FROM emaj.emaj_relation WHERE rel_schema in ('phil''s schema"3', 'myschema4');

-----------------------------
-- Checking step 19.
-----------------------------
-- emaj tables.
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 17600 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 17600 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(18000);

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 20 : test emaj_gen_sql_dump_changes_group() and emaj_dump_changes_group().
-----------------------------

SELECT emaj_gen_sql_dump_changes_group('myGroup1', 'M4', 'M5', 'Consolidation=FULL', NULL, :'EMAJTESTTMPDIR' || '/dump_changes.sql');
\! wc -l $EMAJTESTTMPDIR/dump_changes.sql

SELECT emaj_dump_changes_group('myGroup1', 'M4', 'M5', NULL, '{"myschema1.myTbl3"}', :'EMAJTESTTMPDIR');
\! wc -l $EMAJTESTTMPDIR/myschema1_myTbl3.changes $EMAJTESTTMPDIR/_INFO

-- Build the table/sequence names array using the emaj_log_stat_group() function result.
SELECT emaj_gen_sql_dump_changes_group('myGroup1', 'M4', 'M5',
                                            'EMAJ_COLUMNS=(emaj_tuple, emaj_gid, emaj_verb), CONSOLIDATION=FULL, ORDER_BY=TIME',
                                            (SELECT array_agg(distinct stat_schema || '.' || stat_table) FROM emaj_log_stat_group('myGroup1', 'M4', 'M5'))
                                           );
SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number;

-- Create a table and a view from a consolidated vision of a log table.
DO $$
DECLARE
  v_stmt TEXT;
BEGIN
  PERFORM emaj_gen_sql_dump_changes_group('myGroup1', 'M4', 'M5', 'CONSOLIDATION=FULL, EMAJ_COLUMNS=MIN', ARRAY['myschema1.myTbl3']);
  SELECT sql_text INTO v_stmt FROM emaj_temp_sql WHERE sql_schema = 'myschema1' AND sql_tblseq = 'myTbl3' AND sql_line_number = 1;
  EXECUTE 'CREATE TABLE public.myTbl3_cons_log_table_M4_M5 AS ' || v_stmt;
  EXECUTE 'CREATE VIEW public.myTbl3_cons_log_view_M4_M5 AS ' || v_stmt;
END;
$$;
SELECT emaj_tuple, col33 FROM public.myTbl3_cons_log_table_M4_M5 WHERE col31 = 13 ORDER BY emaj_tuple DESC;
SELECT emaj_tuple, col33 FROM public.myTbl3_cons_log_view_M4_M5 WHERE col31 = 13;   -- already sorted

DROP TABLE IF EXISTS public.myTbl3_cons_log_table_M4_M5;
DROP VIEW IF EXISTS public.myTbl3_cons_log_view_M4_M5;

\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- Step 21 : test TRUNCATE (log, statistics, rollback, sql generation and replay).
-----------------------------

SELECT emaj_create_group('truncateTestGroup');
SELECT emaj_assign_tables('phil''s schema"3', '.*', '', 'truncateTestGroup');
SELECT emaj_assign_tables('myschema4', '.*', '', 'truncateTestGroup');
SELECT emaj_start_group('truncateTestGroup', 'M1');

RESET session_authorization;
TRUNCATE "phil's schema""3"."phil's tbl1" CASCADE;
TRUNCATE myschema4.myTblC2;
TRUNCATE myschema4.myPartP3;

SELECT count(*) FROM "phil's schema""3"."phil's tbl1";

SET session_authorization TO _regress_emaj_adm2;
SELECT count(*) FROM "emaj_phil's schema""3"."phil's tbl1_log";
SELECT is_called, last_value FROM "emaj_phil's schema""3"."phil's tbl1_log_seq";

SELECT emaj_set_mark_group('truncateTestGroup', 'M2');

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj_detailed_log_stat_group('truncateTestGroup', 'M1', NULL);

SELECT emaj_dump_changes_group('truncateTestGroup', 'M1', 'M2', 'CONSOLIDATION=NONE, EMAJ_COLUMNS=MIN', '{"myschema4.mypartp3"}', :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema4_mypartp3.changes
SELECT emaj_dump_changes_group('truncateTestGroup', 'M1', 'M2', 'CONSOLIDATION=PARTIAL', '{"myschema4.mypartp3"}', :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema4_mypartp3.changes
\! rm $EMAJTESTTMPDIR/*

SELECT * FROM emaj_logged_rollback_group('truncateTestGroup', 'M1', FALSE);

SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj_detailed_log_stat_group('truncateTestGroup', 'M1', NULL);

SELECT emaj_gen_sql_group('truncateTestGroup', 'M1', 'M2', :'EMAJTESTTMPDIR' || '/gensql.sql');
-- TODO: replace the previous statement by the next one, once the issue with the emaj_logged_rollback_group() and emaj_gen_sql_groups() functions will be fixed.
--SELECT emaj_gen_sql_group('truncateTestGroup', 'M1', null, :'EMAJTESTTMPDIR' || '/gensql.sql');

SELECT * FROM emaj_rollback_group('truncateTestGroup', 'M1', FALSE);

\! sed -i -s 's/at .*$/at [ts]$/' $EMAJTESTTMPDIR/gensql.sql
\! sed -i -s 's/\\\\/\\/g' $EMAJTESTTMPDIR/gensql.sql
\! sed -i -s 's/^COMMIT/ROLLBACK/' $EMAJTESTTMPDIR/gensql.sql

\set FILE1 :EMAJTESTTMPDIR '/gensql.sql'
RESET session_authorization;
\i :FILE1

\! rm $EMAJTESTTMPDIR/*

SET session_authorization TO _regress_emaj_adm2;
SELECT emaj_stop_group('truncateTestGroup');
SELECT emaj_drop_group('truncateTestGroup');

-- First set all rollback events state.
SELECT emaj_cleanup_rollback_state();

-----------------------------
-- Checking step 21.
-----------------------------
-- emaj tables.
SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_nb_table,
       rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
       case WHEN rlbk_end_datetime is NULL THEN 'NULL' else '[ts]' END AS "end_datetime",
       regexp_replace(array_to_string(rlbk_messages, '#'), E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rlbk WHERE rlbk_id >= 10000 ORDER BY rlbk_id;
SELECT rlbs_rlbk_id, rlbs_session,
       case WHEN rlbs_end_datetime is NULL THEN 'NULL' else '[ts]' END AS "end_datetime"
  FROM emaj.emaj_rlbk_session WHERE rlbs_rlbk_id >= 10000  ORDER BY rlbs_rlbk_id, rlbs_session;
SELECT rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_object_def, rlbp_app_trg_type, rlbp_is_repl_role_replica, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id >= 10000  ORDER BY rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object;
SELECT rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id, rlbt_quantity
  FROM emaj.emaj_rlbk_stat WHERE rlbt_rlbk_id >= 10000 ORDER BY rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 18000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 18000 ORDER BY hist_id;

SELECT * FROM emaj.emaj_log_session WHERE lower(lses_time_range) >= 12000 ORDER BY lses_group, lses_time_range;
SELECT * FROM emaj.emaj_group_hist ORDER BY grph_group, grph_time_range;

-----------------------------
-- Check grants on other functions to emaj_adm role.
-----------------------------
SELECT emaj_get_previous_mark_group('dummyGroup', 'EMAJ_LAST_MARK');
SELECT emaj_estimate_rollback_groups(ARRAY['dummyGroup'], 'dummyMark', FALSE);
SELECT * FROM emaj_rollback_activity();
SELECT substr(pg_size_pretty(pg_database_size(current_database())), 1, 0);
SELECT emaj_forget_group('dummyGroup');

--
RESET session_authorization;

-- The groups are left in their current state for the parallel rollback test.
SELECT count(*) FROM mySchema1.myTbl4;
SELECT count(*) FROM mySchema1.myTbl1;
SELECT count(*) FROM mySchema1.myTbl2;
SELECT count(*) FROM mySchema1."myTbl3";
SELECT count(*) FROM mySchema1.myTbl2b;
SELECT count(*) FROM mySchema2.myTbl4;
SELECT count(*) FROM mySchema2.myTbl1;
SELECT count(*) FROM mySchema2.myTbl2;
SELECT count(*) FROM mySchema2."myTbl3";
SELECT count(*) FROM mySchema2.myTbl5;
SELECT count(*) FROM mySchema2.myTbl6;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
