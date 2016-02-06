-- client.sql: Test client tools, namely emajParallelRollback.php and emajRollbackMonitor.php

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
\! ../../php/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- unlogged rollback for 2 groups
\! ../../php/emajParallelRollback.php -h localhost -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- logged rollback for a single group and a single session
\! ../../php/emajParallelRollback.php -h localhost -d regression -g myGroup1 -m Multi-1 -s 1

--------------------------------------------
-- Prepare data for emajRollbackMonitor.php
--------------------------------------------

delete from emaj.emaj_rlbk_plan where rlbp_rlbk_id > 1000;
delete from emaj.emaj_rlbk where rlbk_id > 1000;

-- 1st rollback, in EXECUTING state
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_start_datetime)
  values (1232,array['group1232'],'mark1232','2000-01-01 01:00:00',true,1,
           5,4,3,'EXECUTING',now()-'110.1 seconds'::interval);
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (1232, 'RLBK_TABLE','schema','t1','','50 seconds'::interval,null,null),
         (1232, 'RLBK_TABLE','schema','t2','','30 seconds'::interval,null,null),
         (1232, 'RLBK_TABLE','schema','t3','','20 seconds'::interval,null,null);
-- the first RLBK_TABLE is completed, and the step duration < the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '45 seconds'::interval,
                               rlbp_duration = '45 seconds'::interval
  where rlbp_rlbk_id = 1232 and rlbp_table = 't1';
-- the second RLBK_TABLE is completed, and the step duration > the estimated duration 
update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '65 seconds'::interval,
                               rlbp_duration = '65 seconds'::interval
  where rlbp_rlbk_id = 1232 and rlbp_table = 't2';

-- 2nd rollback, in LOCKING state with RLBK_TABLE steps
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_start_datetime)
  values (1233,array['group1233'],'mark1233','2000-01-01 01:00:00',true,1,
           5,4,3,'LOCKING',now()-'2 minutes'::interval);
insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  values (1233, 'LOCK_TABLE','schema','t1','',null,null,null),
         (1233, 'LOCK_TABLE','schema','t2','',null,null,null),
         (1233, 'LOCK_TABLE','schema','t3','',null,null,null),
         (1233, 'RLBK_TABLE','schema','t1','','0:20:00'::interval,null,null),
         (1233, 'RLBK_TABLE','schema','t2','','0:02:00'::interval,null,null),
         (1233, 'RLBK_TABLE','schema','t3','','0:00:20'::interval,null,null);

-- 3rd rollback, in PLANNING state
insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session, 
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_start_datetime)
  values (1234,array['group1234'],'mark1234','2000-01-01 01:00:00',true,1,
           5,4,3,'PLANNING',now()-'1 minute'::interval);

--------------------------------------------
-- call emajRollbackMonitor.php using an emaj_viewer role
--------------------------------------------

\! ../../php/emajRollbackMonitor.php -h localhost -d regression -U emaj_regression_tests_viewer_user -i 0.1 -n 2 -l 2 -a 12 -vr

