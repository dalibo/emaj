-- viewer.sql : test use of functions by an emaj_viewer role
--
-----------------------------
-- grant emaj_viewer role
-----------------------------
grant emaj_viewer to emaj_regression_tests_viewer_user;
--
set role emaj_regression_tests_viewer_user;
--
-----------------------------
-- authorized table accesses
-----------------------------
select count(*) from emaj.emaj_param;
select count(*) from emaj.emaj_hist;
select count(*) from emaj.emaj_group_def;
select count(*) from emaj.emaj_group;
select count(*) from emaj.emaj_relation;
select count(*) from emaj.emaj_mark;
select count(*) from emaj.emaj_sequence;
select count(*) from emaj.emaj_seq_hole;
select count(*) from emaj.emaj_rlbk;
select count(*) from emaj.emaj_rlbk_session;
select count(*) from emaj.emaj_rlbk_plan;
select count(*) from emaj.emaj_rlbk_stat;
select count(*) from emaj.mySchema1_myTbl1_log;

-----------------------------
-- authorized functions
-----------------------------
select * from emaj.emaj_verify_all();
select emaj.emaj_get_previous_mark_group('myGroup1', current_timestamp);
select emaj.emaj_get_previous_mark_group('myGroup1', 'EMAJ_LAST_MARK');
select emaj.emaj_cleanup_rollback_state();
select * from emaj.emaj_log_stat_group('myGroup1',NULL,NULL);
select * from emaj.emaj_detailed_log_stat_group('myGroup1',NULL,NULL);
select emaj.emaj_estimate_rollback_group('myGroup1',emaj.emaj_get_previous_mark_group('myGroup1',current_timestamp),FALSE);
select emaj.emaj_estimate_rollback_groups(array['myGroup1'],emaj.emaj_get_previous_mark_group('myGroup1',current_timestamp),FALSE);
select * from emaj.emaj_rollback_activity();
select substr(pg_size_pretty(pg_database_size(current_database())),1,0);

-----------------------------
-- forbiden table accesses
-----------------------------
delete from emaj.emaj_param;
delete from emaj.emaj_hist;
delete from emaj.emaj_group_def;
delete from emaj.emaj_group;
delete from emaj.emaj_relation;
delete from emaj.emaj_mark;
delete from emaj.emaj_sequence;
delete from emaj.emaj_seq_hole;
delete from emaj.emaj_rlbk;
delete from emaj.emaj_rlbk_session;
delete from emaj.emaj_rlbk_plan;
delete from emaj.emaj_rlbk_stat;
delete from emaj.mySchema1_myTbl1_log;

-----------------------------
-- forbiden functions
-----------------------------
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup1',true);
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup1');
select emaj.emaj_alter_group('myGroup1');
select emaj.emaj_start_group('myGroup1','mark');
select emaj.emaj_start_groups(array['myGroup1'],'mark');
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_stop_group('myGroup1',NULL);
select emaj.emaj_stop_groups(array['myGroup1']);
select emaj.emaj_stop_groups(array['myGroup1'],NULL);
select emaj.emaj_set_mark_group('myGroup1','mark');
select emaj.emaj_set_mark_groups(array['myGroup1'],'mark');
select emaj.emaj_comment_mark_group('myGroup1','mark',NULL);
select emaj.emaj_delete_mark_group('myGroup1','mark'); 
select emaj.emaj_delete_before_mark_group('myGroup1','mark');
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','mark');
select emaj.emaj_rollback_group('myGroup1','mark'); 
select emaj.emaj_rollback_groups(array['myGroup1'],'mark'); 
select emaj.emaj_logged_rollback_group('myGroup1','mark');
select emaj.emaj_logged_rollback_groups(array['myGroup1'],'mark');
select emaj.emaj_reset_group('myGroup1');
select emaj.emaj_snap_group('myGroup1','/tmp',NULL);
select emaj.emaj_snap_log_group('myGroup1',NULL,NULL,'/tmp',NULL);
select emaj.emaj_gen_sql_group('myGroup1',NULL,NULL,'/tmp/dummy');
select emaj.emaj_gen_sql_group('myGroup1',NULL,NULL,'/tmp/dummy',array['']);
select emaj.emaj_gen_sql_groups(array['myGroup1'],NULL,NULL,'/tmp/dummy');
select emaj.emaj_gen_sql_groups(array['myGroup1'],NULL,NULL,'/tmp/dummy',array['']);
--
reset role;

