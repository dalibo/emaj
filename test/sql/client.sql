-- Client.sql: Test client tools.

--------------------------------------------------------------
--
-- Parallel Rollback client.
--
--------------------------------------------------------------

-- Set sequence restart value.
TRUNCATE emaj.emaj_hist;
SELECT public.handle_emaj_sequences(20000);

--------------------------------------------
-- Prepare data.
--------------------------------------------

DELETE FROM mySchema1.myTbl4;
DELETE FROM mySchema1.myTbl1;
DELETE FROM mySchema1.myTbl2;
DELETE FROM mySchema1."myTbl3";
DELETE FROM mySchema1.myTbl2b;
DELETE FROM mySchema2.myTbl4;
DELETE FROM mySchema2.myTbl1;
DELETE FROM mySchema2.myTbl2;
DELETE FROM mySchema2."myTbl3";
DELETE FROM mySchema2.myTbl5;
DELETE FROM mySchema2.myTbl6 WHERE col61 <> 0;
ALTER SEQUENCE mySchema2.mySeq1 RESTART 9999;

--------------------------------------------
-- Call emajParallelRollback.pl.
--------------------------------------------

-- Parallel rollback attempt with an unauthorized role.
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_viewer -W viewer -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- Parallel rollback, but with disabled dblink connection.
SELECT emaj.emaj_set_param('dblink_user_password', NULL);
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_adm1 -W adm -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a
SELECT emaj.emaj_set_param('dblink_user_password', 'user=postgres password=postgres');

-- Unlogged rollback for 2 groups in strict mode, after having performed a group configuration change.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority": 1}'::JSONB);
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_adm1 -W adm -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l

-- Unlogged rollback for 2 groups in unstrict mode.
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_adm1 -W adm -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l -a

-- Logged rollback for a single group and a single session.
\! ${EMAJ_DIR}/client/emajParallelRollback.pl -d regression -U _regress_emaj_adm1 -W adm -g myGroup1 -m Multi-1 -s 1 -a -c "Revert aborted DEF chain"

--------------------------------------------------------------
--
-- Rollback monitor client.
--
--------------------------------------------------------------

--------------------------------------------
-- Prepare data.
--------------------------------------------

-- 1st rollback, in EXECUTING state.
INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) OVERRIDING SYSTEM VALUE
  VALUES (-1, now() - '110.1 seconds'::INTERVAL);
INSERT INTO emaj.emaj_time_stamp (time_id, time_clock_timestamp) OVERRIDING SYSTEM VALUE
  VALUES (-2, '2000-01-01 01:00:00');
INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session,
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_end_locking_datetime, rlbk_status)
  VALUES (20101, ARRAY['group20101'], 'mark20101', -2, -1, TRUE, FALSE, 1,
           5, 4, 3, 1, now() - '110.1 seconds'::INTERVAL, now() - '110.0 seconds'::INTERVAL, now() - '109.8 seconds'::INTERVAL, 'EXECUTING');
INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  VALUES (20101, 'RLBK_TABLE', 'schema', 't1', '', '50 seconds'::INTERVAL, NULL, NULL),
         (20101, 'RLBK_TABLE', 'schema', 't2', '', '30 seconds'::INTERVAL, NULL, NULL),
         (20101, 'RLBK_TABLE', 'schema', 't3', '', '20 seconds'::INTERVAL, NULL, NULL);
-- The first RLBK_TABLE is completed, and the step duration < the estimated duration.
UPDATE emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '45 seconds'::INTERVAL,
                               rlbp_duration = '45 seconds'::INTERVAL
  WHERE rlbp_rlbk_id = 20101 AND rlbp_table = 't1';
-- The second RLBK_TABLE is completed, and the step duration > the estimated duration.
UPDATE emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '65 seconds'::INTERVAL,
                               rlbp_duration = '65 seconds'::INTERVAL
  WHERE rlbp_rlbk_id = 20101 AND rlbp_table = 't2';

-- 2nd rollback, in LOCKING state with RLBK_TABLE steps.
INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) OVERRIDING SYSTEM VALUE
  VALUES (-3, now() - '2 minutes'::INTERVAL);
INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session,
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_end_planning_datetime, rlbk_status)
  VALUES (20102, ARRAY['group20102'], 'mark20102', -2, -3, TRUE, FALSE, 1,
           5, 4, 3, NULL, now() - '2 minutes'::INTERVAL, now() - '1 minute 59.8 seconds'::INTERVAL, 'LOCKING');
INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
           rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
  VALUES (20102, 'LOCK_TABLE', 'schema', 't1', '', NULL, NULL, NULL),
         (20102, 'LOCK_TABLE', 'schema', 't2', '', NULL, NULL, NULL),
         (20102, 'LOCK_TABLE', 'schema', 't3', '', NULL, NULL, NULL),
         (20102, 'RLBK_TABLE', 'schema', 't1', '', '0:20:00'::INTERVAL, NULL, NULL),
         (20102, 'RLBK_TABLE', 'schema', 't2', '', '0:02:00'::INTERVAL, NULL, NULL),
         (20102, 'RLBK_TABLE', 'schema', 't3', '', '0:00:20'::INTERVAL, NULL, NULL);

-- 3rd rollback, in PLANNING state.
INSERT INTO emaj.emaj_time_stamp (time_id, time_tx_timestamp) OVERRIDING SYSTEM VALUE
  VALUES (-4, now() - '1 minute'::INTERVAL);
INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session,
           rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_start_datetime, rlbk_status)
  VALUES (20103, ARRAY['group20103'], 'mark20103', -2, -4, TRUE, FALSE, 1,
           5, 4, 3, NULL, now() - '1 minute'::INTERVAL, 'PLANNING');

--------------------------------------------
-- Call emajRollbackMonitor.pl.
--------------------------------------------
-- Call emajRollbackMonitor.pl using an unauthorized role.
\! ${EMAJ_DIR}/client/emajRollbackMonitor.pl -d regression -U _regress_emaj_anonym -W anonym -i 0.1 -n 2 -l 2 -a 12 -v -r

-- Call emajRollbackMonitor.pl using an emaj_viewer role.
\! ${EMAJ_DIR}/client/emajRollbackMonitor.pl -d regression -U _regress_emaj_viewer -W viewer -i 0.1 -n 2 -l 2 -a 12 -v -r

--------------------------------------------------------------
--
-- emajStat client.
--
--------------------------------------------------------------

-- Call emajStat.pl using an unauthorized role.
\! ${EMAJ_DIR}/client/emajStat.pl -d regression -U _regress_emaj_anonym -W anonym --regression-test --no-cls --INTERVAL 0.1 --iter 2

-- Rename the latest mark of myGroup2 for output stability.
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'latest_mark');

-- 2 displays with an emaj_viewer role with various options.
\! ${EMAJ_DIR}/client/emajStat.pl -d regression -U _regress_emaj_viewer -W viewer --regression-test --no-cls --INTERVAL 0.1 --iter 2 --include-group '1|2' --max-table 5 --exclude-sequence 'col31' --max-relation-name-length 23
