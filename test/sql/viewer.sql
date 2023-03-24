-- viewer.sql : test use of functions by an emaj_viewer role
--

-- set sequence restart value
select public.handle_emaj_sequences(11000);

-----------------------------
-- grant emaj_viewer role
-----------------------------
grant emaj_viewer to emaj_regression_tests_viewer_user;

-----------------------------
-- prepare groups for the test
-----------------------------
delete from emaj.emaj_rlbk_stat;    -- for rollback duration estimates stability
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_assign_tables('myschema1','.*',null,'myGroup1');
select emaj.emaj_assign_sequences('myschema1','.*',null,'myGroup1');
select emaj.emaj_start_group('myGroup1','Start');

select emaj.emaj_create_group('myGroup2');
select emaj.emaj_assign_tables('myschema2','.*','mytbl[7,8]','myGroup2');
select emaj.emaj_assign_sequences('myschema2','.*','myseq2','myGroup2');

select emaj.emaj_create_group('emptyGroup');
select emaj.emaj_start_group('emptyGroup');

--
set role emaj_regression_tests_viewer_user;
--
-----------------------------
-- authorized table or view accesses
-----------------------------
select 'select ok' as result from (select count(*) from emaj.emaj_visible_param) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_hist) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_time_stamp) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_group) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_schema) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_relation) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rel_hist) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_mark) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_sequence) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_table) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_seq_hole) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_session) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_plan) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_stat) as t;
select 'select ok' as result from (select count(*) from emaj_mySchema1.myTbl1_log) as t;

-----------------------------
-- authorized functions
-----------------------------
select * from emaj.emaj_get_current_log_table('myschema1', 'mytbl1');
select * from emaj.emaj_verify_all();
select emaj.emaj_get_previous_mark_group('myGroup1', current_timestamp);
select emaj.emaj_get_previous_mark_group('myGroup1', 'EMAJ_LAST_MARK');
select emaj.emaj_cleanup_rollback_state();
select count(*) from emaj.emaj_log_stat_group('myGroup1',NULL,NULL);
select count(*) from emaj.emaj_log_stat_groups(array['myGroup1'],NULL,NULL);
select count(*) from emaj.emaj_detailed_log_stat_group('myGroup1',NULL,NULL);
select count(*) from emaj.emaj_detailed_log_stat_groups(array['myGroup1'],NULL,NULL);
select emaj.emaj_estimate_rollback_group('myGroup1',emaj.emaj_get_previous_mark_group('myGroup1',current_timestamp),FALSE);
select emaj.emaj_estimate_rollback_groups(array['myGroup1'],emaj.emaj_get_previous_mark_group('myGroup1',current_timestamp),FALSE);
select * from emaj.emaj_rollback_activity();
select * from emaj.emaj_get_consolidable_rollbacks();
select substr(pg_size_pretty(pg_database_size(current_database())),1,0);

-----------------------------
-- forbiden table accesses
-----------------------------
select count(*) from emaj.emaj_param;
delete from emaj.emaj_param;
delete from emaj.emaj_hist;
delete from emaj.emaj_group;
delete from emaj.emaj_relation;
delete from emaj.emaj_rel_hist;
delete from emaj.emaj_mark;
delete from emaj.emaj_sequence;
delete from emaj.emaj_table;
delete from emaj.emaj_seq_hole;
delete from emaj.emaj_relation_change;
delete from emaj.emaj_rlbk;
delete from emaj.emaj_rlbk_session;
delete from emaj.emaj_rlbk_plan;
delete from emaj.emaj_rlbk_stat;
delete from emaj_mySchema1.myTbl1_log;

-----------------------------
-- forbiden functions
-----------------------------
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup1');
select emaj.emaj_export_groups_configuration();
select emaj.emaj_import_groups_configuration('{}'::json);
select emaj.emaj_start_group('myGroup1','mark');
select emaj.emaj_start_groups(array['myGroup1'],'mark');
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_stop_group('myGroup1',NULL);
select emaj.emaj_stop_groups(array['myGroup1']);
select emaj.emaj_stop_groups(array['myGroup1'],NULL);
select emaj.emaj_protect_group('myGroup1');
select emaj.emaj_unprotect_group('myGroup1');
select emaj.emaj_set_mark_group('myGroup1','mark');
select emaj.emaj_set_mark_groups(array['myGroup1'],'mark');
select emaj.emaj_comment_mark_group('myGroup1','mark',NULL);
select emaj.emaj_delete_mark_group('myGroup1','mark'); 
select emaj.emaj_delete_before_mark_group('myGroup1','mark');
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','mark');
select emaj.emaj_protect_mark_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_unprotect_mark_group('myGroup1','EMAJ_LAST_MARK');
select * from emaj.emaj_rollback_group('myGroup1','mark'); 
select * from emaj.emaj_rollback_groups(array['myGroup1'],'mark'); 
select * from emaj.emaj_logged_rollback_group('myGroup1','mark');
select * from emaj.emaj_logged_rollback_groups(array['myGroup1'],'mark');
select emaj.emaj_consolidate_rollback_group('myGroup1','mark');
select emaj.emaj_reset_group('myGroup1');
select emaj.emaj_snap_group('myGroup1','/tmp',NULL);
select emaj.emaj_snap_log_group('myGroup1',NULL,NULL,'/tmp',NULL);
select emaj.emaj_gen_sql_group('myGroup1',NULL,NULL,'/tmp/dummy');
select emaj.emaj_gen_sql_group('myGroup1',NULL,NULL,'/tmp/dummy',array['']);
select emaj.emaj_gen_sql_groups(array['myGroup1'],NULL,NULL,'/tmp/dummy');
select emaj.emaj_gen_sql_groups(array['myGroup1'],NULL,NULL,'/tmp/dummy',array['']);
select emaj.emaj_export_parameters_configuration();
select emaj.emaj_export_parameters_configuration('/tmp/dummy/location/file');
select emaj.emaj_import_parameters_configuration('{}'::json);
select emaj.emaj_import_parameters_configuration('/tmp/dummy/location/file');

--
reset role;
