-- Viewer.sql : test emaj data access and functions calls by an emaj_viewer role.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(11000);

-----------------------------
-- Grant emaj_viewer role.
-----------------------------
GRANT emaj_viewer TO _regress_emaj_viewer;

-----------------------------
-- Prepare groups for the test.
-----------------------------
DELETE FROM emaj.emaj_rlbk_stat;    -- for ROLLBACK duration estimates stability
SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_assign_tables('myschema1', '.*', NULL, 'myGroup1');
SELECT emaj.emaj_assign_sequences('myschema1', '.*', NULL, 'myGroup1');
SELECT emaj.emaj_start_group('myGroup1', 'Start');

SELECT emaj.emaj_create_group('myGroup2');
SELECT emaj.emaj_assign_tables('myschema2', '.*', 'mytbl[7,8]', 'myGroup2');
SELECT emaj.emaj_assign_sequences('myschema2', '.*', 'myseq2', 'myGroup2');

SELECT emaj.emaj_create_group('emptyGroup');
SELECT emaj.emaj_start_group('emptyGroup');

--
SET session_authorization TO _regress_emaj_viewer;
--
-----------------------------
-- Authorized table or view accesses.
-----------------------------
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_version_hist) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_visible_param) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_hist) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_time_stamp) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_group) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_group_hist) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_schema) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_relation) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rel_hist) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_log_session) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_mark) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_sequence) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_table) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_seq_hole) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_session) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_plan) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_stat) AS t;
SELECT 'SELECT ok' AS result FROM (SELECT count(*) FROM emaj_mySchema1.myTbl1_log) AS t;

-----------------------------
-- Authorized functions.
-----------------------------
SELECT emaj.emaj_get_version();
SELECT emaj.emaj_does_exist_group('myGroup1');
SELECT emaj.emaj_get_groups();
SELECT emaj.emaj_is_logging_group('myGroup1');
SELECT emaj.emaj_get_logging_groups();
SELECT emaj.emaj_get_idle_groups();
SELECT emaj.emaj_get_assigned_group_table('myschema2', 'mytbl1');
SELECT emaj.emaj_get_assigned_group_sequence('myschema2', 'myseq1');
SELECT emaj.emaj_does_exist_mark_group('myGroup1', 'Mark');
SELECT * FROM emaj.emaj_get_current_log_table('myschema1', 'mytbl1');
SELECT * FROM emaj.emaj_verify_all();
SELECT emaj.emaj_get_previous_mark_group('myGroup1', current_timestamp);
SELECT emaj.emaj_get_previous_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT emaj.emaj_cleanup_rollback_state();
SELECT count(*) FROM emaj.emaj_log_stat_group('myGroup1', 'Start', NULL);
SELECT count(*) FROM emaj.emaj_log_stat_groups(ARRAY['myGroup1'], 'Start', NULL);
SELECT count(*) FROM emaj.emaj_detailed_log_stat_group('myGroup1', 'Start', NULL);
SELECT count(*) FROM emaj.emaj_detailed_log_stat_groups(ARRAY['myGroup1'], 'Start', NULL);
SELECT count(*) FROM emaj.emaj_sequence_stat_group('myGroup1', 'Start', NULL);
SELECT count(*) FROM emaj.emaj_sequence_stat_groups(ARRAY['myGroup1'], 'Start', NULL);
SELECT count(*) FROM emaj.emaj_log_stat_table('myschema2', 'mytbl1');
SELECT count(*) FROM emaj.emaj_log_stat_sequence('myschema2', 'myseq1');
SELECT count(*) FROM emaj._get_sequences_last_value(NULL, NULL, NULL, NULL, NULL, NULL);
SELECT emaj.emaj_estimate_rollback_group('myGroup1', emaj.emaj_get_previous_mark_group('myGroup1', current_timestamp), FALSE);
SELECT emaj.emaj_estimate_rollback_groups(ARRAY['myGroup1'], emaj.emaj_get_previous_mark_group('myGroup1', current_timestamp), FALSE);
SELECT * FROM emaj.emaj_rollback_activity();
SELECT * FROM emaj.emaj_get_consolidable_rollbacks();
SELECT substr(pg_size_pretty(pg_database_size(current_database())), 1, 0);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Start', 'Start', NULL, NULL);

-----------------------------
-- Forbiden tables or views accesses.
-----------------------------
DELETE FROM emaj.emaj_version_hist;
DELETE FROM emaj.emaj_default_param;
SELECT * FROM emaj.emaj_param;
SELECT * FROM emaj.emaj_all_param;
DELETE FROM emaj.emaj_hist;
DELETE FROM emaj.emaj_group;
DELETE FROM emaj.emaj_relation;
DELETE FROM emaj.emaj_rel_hist;
DELETE FROM emaj.emaj_mark;
DELETE FROM emaj.emaj_sequence;
DELETE FROM emaj.emaj_table;
DELETE FROM emaj.emaj_seq_hole;
DELETE FROM emaj.emaj_relation_change;
DELETE FROM emaj.emaj_rlbk;
DELETE FROM emaj.emaj_rlbk_session;
DELETE FROM emaj.emaj_rlbk_plan;
DELETE FROM emaj.emaj_rlbk_stat;
DELETE FROM emaj_mySchema1.myTbl1_log;

-----------------------------
-- Forbiden functions.
-----------------------------
SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_force_drop_group('myGroup1');
SELECT emaj.emaj_forget_group('myGroup1');
SELECT emaj.emaj_export_groups_configuration();
SELECT emaj.emaj_import_groups_configuration('{}'::JSON);
SELECT emaj.emaj_start_group('myGroup1', 'mark');
SELECT emaj.emaj_start_groups(ARRAY['myGroup1'], 'mark');
SELECT emaj.emaj_stop_group('myGroup1');
SELECT emaj.emaj_stop_group('myGroup1', NULL);
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1']);
SELECT emaj.emaj_stop_groups(ARRAY['myGroup1'], NULL);
SELECT emaj.emaj_protect_group('myGroup1');
SELECT emaj.emaj_unprotect_group('myGroup1');
SELECT emaj.emaj_set_mark_group('myGroup1', 'mark');
SELECT emaj.emaj_set_mark_groups(ARRAY['myGroup1'], 'mark');
SELECT emaj.emaj_comment_mark_group('myGroup1', 'mark', NULL);
SELECT emaj.emaj_delete_mark_group('myGroup1', 'mark');
SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'mark');
SELECT emaj.emaj_rename_mark_group('myGroup1', 'EMAJ_LAST_MARK', 'mark');
SELECT emaj.emaj_protect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT emaj.emaj_unprotect_mark_group('myGroup1', 'EMAJ_LAST_MARK');
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'mark');
SELECT * FROM emaj.emaj_rollback_groups(ARRAY['myGroup1'], 'mark');
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup1', 'mark');
SELECT * FROM emaj.emaj_logged_rollback_groups(ARRAY['myGroup1'], 'mark');
SELECT emaj.emaj_comment_rollback(1, 'comment');
SELECT emaj.emaj_consolidate_rollback_group('myGroup1', 'mark');
SELECT emaj.emaj_reset_group('myGroup1');
SELECT emaj.emaj_snap_group('myGroup1', '/tmp', NULL);
SELECT emaj.emaj_dump_changes_group('myGroup1', 'Start', 'Start', NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Start', 'Start', NULL, NULL, '/tmp/dummy');
SELECT emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/dummy');
SELECT emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/dummy', ARRAY['']);
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1'], NULL, NULL, '/tmp/dummy');
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1'], NULL, NULL, '/tmp/dummy', ARRAY['']);
SELECT emaj.emaj_export_parameters_configuration();
SELECT emaj.emaj_export_parameters_configuration('/tmp/dummy/location/file');
SELECT emaj.emaj_import_parameters_configuration('{}'::JSON);
SELECT emaj.emaj_import_parameters_configuration('/tmp/dummy/location/file');

--
RESET session_authorization;
