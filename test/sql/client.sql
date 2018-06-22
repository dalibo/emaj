-- client.sql: Test client tools

--------------------------------------------------------------
-- PHP SCRIPTS
--
-- Test emajParallelRollback.php and emajRollbackMonitor.php
--------------------------------------------------------------

-- set sequence restart value
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 20000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 20000;
alter sequence emaj.emaj_mark_mark_id_seq restart 20000;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 20000;

--------------------------------------------
-- Prepare data for emajParallelRollback.php
--------------------------------------------
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
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- unlogged rollback for 2 groups in strict mode, after having performed an alter group operation
update emaj.emaj_group_def set grpdef_priority = 1 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- unlogged rollback for 2 groups in unstrict mode
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- logged rollback for a single group and a single session
\! ${EMAJ_DIR}/client/emajParallelRollback.php -h localhost -d regression -g myGroup1 -m Multi-1 -s 1 -a

--------------------------------------------
-- Prepare data for emajRollbackMonitor.php
--------------------------------------------

-- 1st rollback, in EXECUTING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-1, now()-'110.1 seconds'::interval);
insert into emaj.emaj_time_stamp (time_id, time_clock_timestamp) values (-2, '2000-01-01 01:00:00');
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20101,array['group20101'],'mark20101',-2,-1,true,false,1,
           5,4,3,'EXECUTING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
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
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-3, now()-'2 minutes'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20102,array['group20102'],'mark20102',-2,-3,true,false,1,
           5,4,3,'LOCKING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (20102, 'LOCK_TABLE','schema','t1','',null,null,null),
         (20102, 'LOCK_TABLE','schema','t2','',null,null,null),
         (20102, 'LOCK_TABLE','schema','t3','',null,null,null),
         (20102, 'RLBK_TABLE','schema','t1','','0:20:00'::interval,null,null),
         (20102, 'RLBK_TABLE','schema','t2','','0:02:00'::interval,null,null),
         (20102, 'RLBK_TABLE','schema','t3','','0:00:20'::interval,null,null);

-- 3rd rollback, in PLANNING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-4, now()-'1 minute'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20103,array['group20103'],'mark20103',-2,-4,true,false,1,
           5,4,3,'PLANNING');

--------------------------------------------
-- call emajRollbackMonitor.php using an emaj_viewer role
--------------------------------------------
\! ${EMAJ_DIR}/client/emajRollbackMonitor.php -h localhost -d regression -U emaj_regression_tests_viewer_user -i 0.1 -n 2 -l 2 -a 12 -vr

--------------------------------------------------------------
-- PERL SCRIPTS
--
-- Test emajParallelRollback.pl and emajRollbackMonitor.pl
--------------------------------------------------------------

-- set sequence restart value
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 20200;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 20200;
alter sequence emaj.emaj_mark_mark_id_seq restart 20200;
alter sequence emaj.emaj_rlbk_rlbk_id_seq restart 20200;

--------------------------------------------
-- Prepare data for emajParallelRollback.pl
--------------------------------------------
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
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- unlogged rollback for 2 groups in strict mode, after having performed an alter group operation
update emaj.emaj_group_def set grpdef_priority = 1 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1');
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- unlogged rollback for 2 groups in unstrict mode
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- logged rollback for a single group and a single session
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -h localhost -d regression -g myGroup1 -m Multi-1 -s 1 -a

--------------------------------------------
-- Prepare data for emajRollbackMonitor.pl
--------------------------------------------

-- 1st rollback, in EXECUTING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-5, now()-'110.1 seconds'::interval);
insert into emaj.emaj_time_stamp (time_id, time_clock_timestamp) values (-6, '2000-01-01 01:00:00');
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20301,array['group20301'],'mark20301',-2,-1,true,false,1,
           5,4,3,'EXECUTING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (20301, 'RLBK_TABLE','schema','t1','','50 seconds'::interval,null,null),
         (20301, 'RLBK_TABLE','schema','t2','','30 seconds'::interval,null,null),
         (20301, 'RLBK_TABLE','schema','t3','','20 seconds'::interval,null,null);
-- the first RLBK_TABLE is completed, and the step duration < the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '45 seconds'::interval,
                               rlbp_duration = '45 seconds'::interval
  where rlbp_rlbk_id = 20301 and rlbp_table = 't1';
-- the second RLBK_TABLE is completed, and the step duration > the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '65 seconds'::interval,
                               rlbp_duration = '65 seconds'::interval
  where rlbp_rlbk_id = 20301 and rlbp_table = 't2';

-- 2nd rollback, in LOCKING state with RLBK_TABLE steps
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-7, now()-'2 minutes'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20302,array['group20302'],'mark20302',-2,-3,true,false,1,
           5,4,3,'LOCKING');
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (20302, 'LOCK_TABLE','schema','t1','',null,null,null),
         (20302, 'LOCK_TABLE','schema','t2','',null,null,null),
         (20302, 'LOCK_TABLE','schema','t3','',null,null,null),
         (20302, 'RLBK_TABLE','schema','t1','','0:20:00'::interval,null,null),
         (20302, 'RLBK_TABLE','schema','t2','','0:02:00'::interval,null,null),
         (20302, 'RLBK_TABLE','schema','t3','','0:00:20'::interval,null,null);

-- 3rd rollback, in PLANNING state
insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-8, now()-'1 minute'::interval);
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
  values (20303,array['group20303'],'mark20303',-2,-4,true,false,1,
           5,4,3,'PLANNING');

--------------------------------------------
-- call emajRollbackMonitor.pl using an emaj_viewer role
--------------------------------------------
\! ${EMAJ_DIR}/client/emajRollbackMonitor.pl -h localhost -d regression -U emaj_regression_tests_viewer_user -i 0.1 -n 2 -l 2 -a 12 -v -r

