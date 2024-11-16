-- client.sql: Test client tools

--------------------------------------------------------------
--
-- Parallel Rollback clients
--
--------------------------------------------------------------

--------------------------------------------
-- Prepare data for emajParallelRollback.php
--------------------------------------------
-- set sequence restart value
truncate emaj.emaj_hist;
select public.handle_emaj_sequences(20000);

delete from mySchema1.myTbl4;
delete from mySchema1.myTbl1;
delete from mySchema1.myTbl2; 
delete from mySchema1."myTbl3";
delete from mySchema1.myTbl2b;
delete from mySchema2.myTbl4;
delete from mySchema2.myTbl1;
delete from mySchema2.myTbl2; 
delete from mySchema2."myTbl3";
delete from mySchema2.myTbl5;
delete from mySchema2.myTbl6 where col61 <> 0;
alter sequence mySchema2.mySeq1 restart 9999;

--------------------------------------------
-- Call emajParallelRollback.php
--------------------------------------------
-- parallel rollback, but with disabled dblink connection
delete from emaj.emaj_param where param_key = 'dblink_user_password';
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- unlogged rollback for 2 groups in strict mode, after having performed a group configuration change
select emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority": 2}'::jsonb);
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- unlogged rollback for 2 groups in unstrict mode
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- logged rollback for a single group and a single session
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g myGroup1 -m Multi-1 -s 1 -a -c "Revert aborted ABC chain"

--------------------------------------------
-- Prepare data for emajParallelRollback.pl
--------------------------------------------
-- set sequence restart value
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 20200;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 20200;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 20200;

delete from mySchema1.myTbl4;
delete from mySchema1.myTbl1;
delete from mySchema1.myTbl2; 
delete from mySchema1."myTbl3";
delete from mySchema1.myTbl2b;
delete from mySchema2.myTbl4;
delete from mySchema2.myTbl1;
delete from mySchema2.myTbl2; 
delete from mySchema2."myTbl3";
delete from mySchema2.myTbl5;
delete from mySchema2.myTbl6 where col61 <> 0;
alter sequence mySchema2.mySeq1 restart 9999;

--------------------------------------------
-- Call emajParallelRollback.pl
--------------------------------------------
-- parallel rollback, but with disabled dblink connection
delete from emaj.emaj_param where param_key = 'dblink_user_password';
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- unlogged rollback for 2 groups in strict mode, after having performed a group configuration change
select emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority": 1}'::jsonb);
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- unlogged rollback for 2 groups in unstrict mode
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- logged rollback for a single group and a single session
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g myGroup1 -m Multi-1 -s 1 -a -c "Revert aborted DEF chain"

--------------------------------------------------------------
--
-- Rollback monitor clients
--
--------------------------------------------------------------

--------------------------------------------
-- Prepare data for both emajRollbackMonitor.php and emajRollbackMonitor.pl
--------------------------------------------

-- 1st rollback, in EXECUTING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) overriding system value
  values (-1, now()-'110.1 seconds'::interval);
insert into emaj.emaj_time_stamp (time_id, time_clock_timestamp) overriding system value
  values (-2, '2000-01-01 01:00:00');
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_end_locking_datetime, rlbk_status)
  values (20101,array['group20101'],'mark20101',-2,-1,true,false,1,
           5,4,3,1,now()-'110.1 seconds'::interval,now()-'110.0 seconds'::interval,now()-'109.8 seconds'::interval,'EXECUTING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (20101, 'RLBK_TABLE','schema','t1','','50 seconds'::interval,null,null),
         (20101, 'RLBK_TABLE','schema','t2','','30 seconds'::interval,null,null),
         (20101, 'RLBK_TABLE','schema','t3','','20 seconds'::interval,null,null);
-- the first RLBK_TABLE is completed, and the step duration < the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '45 seconds'::interval,
                               rlbp_duration = '45 seconds'::interval
  where rlbp_rlbk_id = 20101 and rlbp_table = 't1';
-- the second RLBK_TABLE is completed, and the step duration > the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '65 seconds'::interval,
                               rlbp_duration = '65 seconds'::interval
  where rlbp_rlbk_id = 20101 and rlbp_table = 't2';

-- 2nd rollback, in LOCKING state with RLBK_TABLE steps
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) overriding system value
  values (-3, now()-'2 minutes'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_status)
  values (20102,array['group20102'],'mark20102',-2,-3,true,false,1,
           5,4,3,NULL,now()-'2 minutes'::interval,now()-'1 minute 59.8 seconds'::interval,'LOCKING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (20102, 'LOCK_TABLE','schema','t1','',null,null,null),
         (20102, 'LOCK_TABLE','schema','t2','',null,null,null),
         (20102, 'LOCK_TABLE','schema','t3','',null,null,null),
         (20102, 'RLBK_TABLE','schema','t1','','0:20:00'::interval,null,null),
         (20102, 'RLBK_TABLE','schema','t2','','0:02:00'::interval,null,null),
         (20102, 'RLBK_TABLE','schema','t3','','0:00:20'::interval,null,null);

-- 3rd rollback, in PLANNING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) overriding system value
  values (-4, now()-'1 minute'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session,
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime,rlbk_status)
  values (20103,array['group20103'],'mark20103',-2,-4,true,false,1,
           5,4,3,NULL,now()-'1 minute'::interval,'PLANNING');

--------------------------------------------
-- call emajRollbackMonitor.php using an emaj_viewer role
--------------------------------------------
\! ${EMAJ_DIR}/client/emajRollbackMonitor.php -h localhost -d regression -U emaj_regression_tests_viewer_user -i 0.1 -n 2 -l 2 -a 12 -v -r

--------------------------------------------
-- call emajRollbackMonitor.pl using an emaj_viewer role
--------------------------------------------
\! ${EMAJ_DIR}/client/emajRollbackMonitor.pl -h localhost -d regression -U emaj_regression_tests_viewer_user -i 0.1 -n 2 -l 2 -a 12 -v -r

--------------------------------------------------------------
--
-- emajStat client
--
--------------------------------------------------------------
-- Rename the latest mark of myGroup2 for output stability
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','latest_mark');

-- 2 displays with an emaj_viewer role with various options
\! ${EMAJ_DIR}/client/emajStat.pl -h localhost -d regression -U emaj_regression_tests_viewer_user --regression-test --no-cls --interval 0.1 --iter 2 --include-group '1|2' --max-table 5 --exclude-sequence 'col31' --max-relation-name-length 23
