-- After_upg_while_logging.sql : complex scenario executed by an emaj_adm role.
-- The E-Maj version is changed while groups are in logging state.
-- This script is the part of operations performed after the upgrade.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Revoke rights granted before the upgrade.
REVOKE ALL ON SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  FROM _regress_emaj_adm1, _regress_emaj_adm2;
REVOKE ALL ON ALL TABLES IN SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  FROM _regress_emaj_adm1, _regress_emaj_adm2;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  FROM _regress_emaj_adm1, _regress_emaj_adm2;

-----------------------------
-- Step 1 : check the E-Maj installation.
-----------------------------
SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_verify_all();

-- New or modified table contents.

SELECT * FROM emaj.emaj_install_conf;

SELECT * FROM emaj.emaj_param ORDER BY param_key;

SELECT rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions FROM emaj.emaj_relation ORDER BY rel_schema, rel_tblseq, rel_time_range;

-----------------------------
-- Step 2 : for both groups, rollback to the common mark just set before the upgrade, after having unprotected the first group.
-----------------------------
SELECT emaj.emaj_unprotect_group('myGroup1');
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_rollback_groups('{"myGroup1", "myGroup2"}', 'Common', TRUE) ORDER BY 1, 2;

-----------------------------
-- Step 3 : for myGroup1, update tables, then unprotect, logged_rollback, rename the end rollback mark and consolidate the rollback.
-----------------------------
RESET session_authorization;
SET search_path TO myschema1;
--
UPDATE "myTbl3" SET col33 = col33 / 2;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M2', FALSE) ORDER BY 1, 2;
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'M3');
SELECT rlbk_severity, regexp_replace(rlbk_message, E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g')
  FROM emaj.emaj_logged_rollback_group('myGroup1', 'M2', TRUE) ORDER BY 1, 2;
--
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'End_rollback_to_M2');
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'End_rollback_to_M2');

-----------------------------
-- Step 4 : for myGroup1, update tables and sequences again then set 3 marks and get stats.
-----------------------------
RESET session_authorization;
INSERT INTO myTbl1 SELECT i, 'DEF', E'\\000'::BYTEA FROM generate_series (100, 110) as i;
INSERT INTO myTbl2 VALUES (3, 'GHI', '2010-01-02');
DELETE FROM myTbl1 WHERE col11 = 1;
SELECT nextval('myschema1."myTbl3_col31_seq"');
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M4');
--
RESET session_authorization;
UPDATE "myTbl3" SET col33 = col33 / 2;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M5');
--
RESET session_authorization;
UPDATE myTbl1 SET col11 = 99 WHERE col11 = 1;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M6');
--
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup1', 'M2', 'M6') ORDER BY 1, 2, 3, 4;
SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id,
       stat_increments, stat_has_structure_changed
  FROM emaj.emaj_sequence_stat_group('myGroup1', 'M2', 'M6')
  ORDER BY stat_group, stat_schema, stat_sequence, stat_first_mark_datetime;

-----------------------------
-- Step 5 : for myGroup2, logged rollback again then unlogged rollback.
-----------------------------
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M2', FALSE) ORDER BY 1, 2;
--
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'M3', FALSE) ORDER BY 1, 2;

-----------------------------
-- Step 6 : for myGroup1, update tables, rollback, other updates, then logged rollback.
-----------------------------
RESET session_authorization;
SET search_path TO myschema1;
--
INSERT INTO myTbl1 VALUES (1, 'Step 6', E'\\000'::BYTEA);
INSERT INTO myTbl4 VALUES (11, 'FK...', 1, 1, 'Step 6');
INSERT INTO myTbl4 VALUES (12, 'FK...', 1, 1, 'Step 6');
TRUNCATE myTbl2 CASCADE;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M5', FALSE) ORDER BY 1, 2;
--
RESET session_authorization;
INSERT INTO myTbl1 VALUES (1, E'Step\t6', E'\\001'::BYTEA);
COPY myTbl4 FROM stdin CSV;
11,,1,1,"Step	6"
12,,1,1,"Step	6"
\.
--
SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'M4', FALSE) ORDER BY 1, 2;

-----------------------------
-- Step 7 : for myGroup1, generate a sql script on the whole time frame.
-----------------------------
SELECT emaj.emaj_gen_sql_group('myGroup1', 'M1', NULL, NULL, ARRAY['myschema1.mytbl1', 'myschema1.mytbl2', 'myschema1.mytbl4']);
SELECT regexp_replace(scr_sql, '^-- SQL script generated by E-Maj at .*', '-- SQL script generated by E-Maj at %') FROM emaj_sql_script;
SELECT emaj.emaj_gen_sql_group('myGroup1', 'M1', NULL, '/dev/null', ARRAY['myschema1.myTbl3']);

-----------------------------
-- Step 8 : for myGroup1, update tables, rename a mark, then delete 2 marks then delete all before a mark.
-----------------------------
RESET session_authorization;
SET search_path TO myschema1;
--
DELETE FROM "myTbl3" WHERE col31 between 14 AND 18;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_rename_mark_group('myGroup1', mark_name, 'Before logged rollback to M4') FROM emaj.emaj_mark WHERE mark_comment LIKE '%to mark M4 start';
SELECT emaj.emaj_delete_mark_group('myGroup1', mark_name) FROM emaj.emaj_mark WHERE mark_comment LIKE '%to mark M4 end';
SELECT emaj.emaj_delete_mark_group('myGroup1', 'M1');
SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'M4');

-----------------------------
-- Step 9 : for myGroup6, perform a table change and verify that no log trigger has been called.
--          And finaly drop the group.
-----------------------------
RESET session_authorization;
INSERT INTO mySchema6.table_with_51_characters_long_name_____0_________0a VALUES (1), (2);
SELECT count(*) FROM emaj_myschema6."table_with_51_characters_long_name_____0_________0#1_log";
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_start_group('myGroup6', 'Start G6');
--
RESET session_authorization;
INSERT INTO mySchema6.table_with_51_characters_long_name_____0_________0a VALUES (3), (4);
DELETE FROM mySchema6.table_with_51_characters_long_name_____0_________0a;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup6', 'Start G6', NULL) ORDER BY 1, 2, 3, 4;
SELECT * FROM emaj.emaj_rollback_group('myGroup6', 'Start G6');
SELECT emaj.emaj_stop_group('myGroup6');
SELECT emaj.emaj_drop_group('myGroup6');

-----------------------------
-- Test end: check and reset history.
-----------------------------
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d(\\d?)', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist
  ORDER BY hist_id;
--
-- Set sequence restart value.
TRUNCATE emaj.emaj_hist;
SELECT public.handle_emaj_sequences(30000);

-- The groups are left in their current state for the parallel rollback test.
-- Perform some updates to prepare the parallel rollback test.
-- Set a mark for both groups.
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1');
--
RESET session_authorization;
SELECT count(*) FROM mySchema1.myTbl4;
SELECT count(*) FROM mySchema1.myTbl1;
SELECT count(*) FROM mySchema1.myTbl2;
SELECT count(*) FROM mySchema1."myTbl3";
SELECT count(*) FROM mySchema1.myTbl2b;
SELECT count(*) FROM mySchema2.myTbl4;
SELECT count(*) FROM mySchema2.myTbl1;
SELECT count(*) FROM mySchema2.myTbl2;
SELECT count(*) FROM mySchema2."myTbl3";
DELETE FROM mySchema1.myTbl4;
DELETE FROM mySchema1.myTbl1;
DELETE FROM mySchema1.myTbl2;
DELETE FROM mySchema1."myTbl3";
DELETE FROM mySchema1.myTbl2b;
DELETE FROM mySchema2.myTbl4;
DELETE FROM mySchema2.myTbl1;
DELETE FROM mySchema2.myTbl2;
DELETE FROM mySchema2."myTbl3";
ALTER SEQUENCE mySchema2.mySeq1 restart 9999;
