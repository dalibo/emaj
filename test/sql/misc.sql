-- Misc.sql : test miscellaneous functions.
--   emaj_set_param(),
--   emaj_reset_group(),
--   emaj_estimate_rollback_group() and emaj_estimate_rollback_groups(),
--   emaj_snap_group(),
--   emaj_get_current_log_table(),
--   emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group(),
--   emaj_gen_sql_group() and emaj_gen_sql_groups(),
--   table reclustering,
--   emaj_import_parameters_configuration() and emaj_export_parameters_configuration(),
--   emaj_purge_histories().
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5200);

-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/misc'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

-----------------------------
-- emaj_set_param() test.
-----------------------------
-- Illegal key.
SELECT emaj.emaj_set_param('A key', 'a value');

-- Invalid interval format.
SELECT emaj.emaj_set_param('history_retention', 'not an interval value');

-- Update a key with a real value change.
SELECT emaj.emaj_set_param('history_retention', '1 MONTHS');
-- Update a key with the same value.
SELECT emaj.emaj_set_param('history_retention', '1 MONTHS');
-- Reset a key (here in upper case) to its default value.
SELECT emaj.emaj_set_param('HISTORY_RETENTION', NULL);
-- Reset a key with no change.
SELECT emaj.emaj_set_param('history_retention', NULL);

-----------------------------
-- emaj_reset_group() test.
-----------------------------

-- Group is unknown.
SELECT emaj.emaj_reset_group(NULL);
SELECT emaj.emaj_reset_group('unknownGroup');

-- Group still in logging state.
SELECT emaj.emaj_reset_group('myGroup1');

-- Log tables are not yet empty.
SELECT count(*) FROM emaj_myschema1.mytbl1_log;
SELECT count(*) FROM emaj_myschema1.mytbl2_log;
SELECT count(*) FROM emaj_myschema1.mytbl2b_log;
SELECT count(*) FROM emaj_myschema1."myTbl3_log";
SELECT count(*) FROM emaj_myschema1.mytbl4_log;

-- Should be OK.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_reset_group('myGroup1');
  SELECT count(*) FROM emaj_myschema1.mytbl1_log;
  SELECT count(*) FROM emaj_myschema1.mytbl2_log;
  SELECT count(*) FROM emaj_myschema1.mytbl2b_log;
  SELECT count(*) FROM emaj_myschema1."myTbl3_log";
  SELECT count(*) FROM emaj_myschema1.mytbl4_log;
ROLLBACK;

BEGIN;
  SELECT emaj.emaj_stop_group('emptyGroup');
  SELECT emaj.emaj_reset_group('emptyGroup');
ROLLBACK;

-----------------------------
-- emaj_estimate_rollback_group() and emaj_estimate_rollback_groups() tests.
--
-- When emaj is not created as an extension - i.e. is created with the psql script - the estimate durations are different.
-- This is normal as the FK are not processed the same way.
-----------------------------

-- Group is unknown.
SELECT emaj.emaj_estimate_rollback_group(NULL, NULL, FALSE);
SELECT emaj.emaj_estimate_rollback_group('unknownGroup', NULL, TRUE);
SELECT emaj.emaj_estimate_rollback_groups('{"myGroup2", "unknownGroup"}', NULL, TRUE);

-- Invalid marks.
SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'dummyMark', TRUE);

SELECT emaj.emaj_set_mark_group('myGroup1', 'Mark21');
SELECT emaj.emaj_estimate_rollback_groups(ARRAY['myGroup1', 'myGroup2'], 'Mark21', TRUE);

-- Group not in logging state.
BEGIN;
  SELECT emaj.emaj_stop_group('myGroup1');
  SELECT emaj.emaj_estimate_rollback_group('myGroup1', 'Mark11', FALSE);
ROLLBACK;

-- Estimate a rollback of an empty group.
SELECT emaj.emaj_estimate_rollback_group('emptyGroup', 'EGM4', TRUE);

-- Insert 1 timing parameters (=> so use 5 default values).

SELECT emaj.emaj_set_param('fixed_step_rollback_duration', '2.5 millisecond');

-- Analyze tables to get proper reltuples statistics.
VACUUM ANALYZE myschema2.mytbl4;
SELECT reltuples FROM pg_class, pg_namespace WHERE relnamespace=pg_namespace.oid AND relname = 'mytbl4' AND nspname = 'myschema2';

-- Estimate with empty rollback statistics and default parameters.
DELETE FROM emaj.emaj_rlbk_stat;

-- Estimates with empty rollback statistics but 1 temporarily modified parameter ; no table to rollback.
-- Check in passing that the simulation is not blocked by protections set on groups or marks.
BEGIN;
  SELECT emaj.emaj_protect_group('myGroup2');
  SELECT emaj.emaj_protect_mark_group('myGroup2', 'EMAJ_LAST_MARK');
  SELECT emaj.emaj_set_param('fixed_table_rollback_duration', '1.4 millisecond');
  SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'EMAJ_LAST_MARK', FALSE);
-- Should return 0.0205 sec.
  SELECT emaj.emaj_unprotect_mark_group('myGroup2', 'EMAJ_LAST_MARK');
  SELECT emaj.emaj_unprotect_group('myGroup2');
ROLLBACK;

SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'Mark21', FALSE);
-- Should return 1.4086 sec (or 1.4291 sec is emaj is not an extension).

-- Estimates with empty rollback statistics but temporarily modified parameters.
BEGIN;
  SELECT emaj.emaj_set_param('avg_row_rollback_duration', '150 microsecond');
  SELECT emaj.emaj_set_param('avg_row_delete_log_duration', '12 microsecond');
  SELECT emaj.emaj_set_param('avg_fkey_check_duration', '27 microsecond');
  SELECT emaj.emaj_set_param('fixed_step_rollback_duration', '7 millisecond');
  SELECT emaj.emaj_set_param('fixed_dblink_rollback_duration', '2.5 millisecond');
  SELECT emaj.emaj_estimate_rollback_groups('{"myGroup2"}', 'Mark21', TRUE);
-- Should return 1.814 sec (or 1.8285 sec is emaj is not an extension).
ROLLBACK;

-- Estimate with added rollback statistics about fkey drops, recreations and checks, and rollback sequences.
--   Drop the foreign key on emaj_rlbk_stat to easily temporarily insert dummy rows.
ALTER TABLE emaj.emaj_rlbk_stat DROP CONSTRAINT emaj_rlbk_stat_rlbt_rlbk_id_fkey;
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DROP_FK', '', '', '', 1, 1, '0.003 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('ADD_FK', 'myschema2', 'mytbl4', 'mytbl4_col44_fkey', 1, 300, '0.036 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('SET_FK_IMM', 'myschema2', 'mytbl4', 'mytbl4_col43_fkey', 1, 2000, '0.030 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DROP_FK', '', '', '', 2, 1, '0.0042 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('ADD_FK', 'myschema2', 'mytbl4', 'mytbl4_col44_fkey', 2, 200, '0.020 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('SET_FK_IMM', 'myschema2', 'mytbl4', 'mytbl4_col43_fkey', 2, 1200, '0.015 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_SEQUENCES', '', '', '', 1, 2, '0.003 SECONDS'::INTERVAL);

SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'Mark21', FALSE);
-- Should return 1.4071 sec (or 1.435306 sec is emaj is not an extension).

-- Estimate with added statistics about tables rollbacks.
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'mytbl1', '', 1, 5350, '1.000 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'mytbl2', '', 1, 100, '0.004 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'mytbl2', '', 2, 200, '0.010 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'mytbl2', '', 3, 20000, '1.610 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'myTbl3', '', 1, 99, '0.004 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'myTbl3', '', 2, 101, '0.008 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('RLBK_TABLE', 'myschema2', 'mytbl4', '', 1, 50000, '3.600 SECONDS'::INTERVAL);
SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'Mark21', FALSE);
-- Should return 2.275704 sec (or 2.30391 sec is emaj is not an extension).

-- Estimate with added statistics about log deletes and CTRLxDBLINK pseudo steps.
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'mytbl1', '', 1, 5350, '0.250 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'mytbl2', '', 1, 150, '0.001 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'mytbl2', '', 2, 200, '0.003 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'mytbl2', '', 3, 20000, '1.610 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'myTbl3', '', 1, 99, '0.001 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'myTbl3', '', 2, 151, '0.002 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('DELETE_LOG', 'myschema2', 'mytbl4', '', 1, 50000, '0.900 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('CTRL+DBLINK', '', '', '', 1, 10, '0.005 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('CTRL-DBLINK', '', '', '', 2, 10, '0.035 SECONDS'::INTERVAL);
INSERT INTO emaj.emaj_rlbk_stat VALUES
  ('CTRL-DBLINK', '', '', '', 3, 10, '0.025 SECONDS'::INTERVAL);
SELECT emaj.emaj_estimate_rollback_group('myGroup2', 'Mark21', FALSE);
-- Should return 2.643791 sec (or 2.668997 sec is emaj is not an extension).

-- Estimate with 2 groups and a SET_FK_DEF step.
VACUUM ANALYZE myschema1.mytbl4;
SELECT reltuples FROM pg_class, pg_namespace WHERE relnamespace=pg_namespace.oid AND relname = 'mytbl4' AND nspname = 'myschema1';
BEGIN;
-- Temporarily insert new rows into myTbl4 of myschema1.
  INSERT INTO myschema1.myTbl4 SELECT i, 'FK...', 2, 1, 'ABC' FROM generate_series (10, 20) AS i;
  SELECT emaj.emaj_estimate_rollback_groups('{"myGroup1", "myGroup2"}', 'Multi-1', FALSE);
-- Should return 2.675001 sec (or 2.724307 sec is emaj is not an extension).
ROLLBACK;

-- Delete all manualy inserted rollback statistics, cleanup the statistics table and recreate its foreign key.
DELETE FROM emaj.emaj_rlbk_stat;
VACUUM emaj.emaj_rlbk_stat;
ALTER table emaj.emaj_rlbk_stat add FOREIGN KEY (rlbt_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id);

-- Check for emaj_estimate_rollback_group().
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5200 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5300);

-----------------------------
-- emaj_snap_group() tests.
-----------------------------

-- Group is unknown.
SELECT emaj.emaj_snap_group(NULL, NULL, NULL);
SELECT emaj.emaj_snap_group('unknownGroup', NULL, NULL);

-- Invalid directory.
SELECT emaj.emaj_snap_group('myGroup1', NULL, NULL);
SELECT emaj.emaj_snap_group('myGroup1', 'unknown_directory', NULL);
SELECT emaj.emaj_snap_group('myGroup1', '/unknown_directory', NULL);

-- Invalid COPY TO options.
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR', 'dummy_option');

-- SQL injection attempt.
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR', '; CREATE ROLE fake LOGIN PASSWORD '''' SUPERUSER');

-- Should be OK (even when executed twice, files being overwriten).
SELECT emaj.emaj_snap_group('emptyGroup', :'EMAJTESTTMPDIR', '');
\! ls $EMAJTESTTMPDIR
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR', '');
SELECT emaj.emaj_snap_group('myGroup1', :'EMAJTESTTMPDIR', 'CSV HEADER DELIMITER '';'' ');
\! ls $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_snap_group('phil''s group#3",', :'EMAJTESTTMPDIR', '');
\! ls $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

-----------------------------
-- emaj_get_current_log_table() tests.
-----------------------------
-- Not found.
SELECT * FROM emaj.emaj_get_current_log_table('myschema1', 'dummy_table');
-- Found.
SELECT * FROM emaj.emaj_get_current_log_table('myschema1', 'mytbl1');
SELECT 'SELECT count(*) FROM ' || quote_ident(log_schema) || '.' || quote_ident(log_table)
  FROM emaj.emaj_get_current_log_table('myschema1', 'mytbl1');

-- Check for emaj_snap_group().
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5300 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5400);

-----------------------------
-- emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group() tests.
-----------------------------

--
-- Test errors with input parameters.
--

-- Group is unknown.
SELECT emaj.emaj_gen_sql_dump_changes_group('dummy', NULL, NULL, NULL, NULL);

-- Start mark is null or unknown.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', NULL, NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'dummy', NULL, NULL, NULL);

-- End mark is null or unknown.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'dummy', NULL, NULL);

-- End mark is prior start mark.
-- Just check the error is trapped, because the error message contents timestamps.
do language plpgsql
$$
BEGIN
  BEGIN
    PERFORM emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark23', 'Mark21', NULL, NULL);
    RETURN;
  EXCEPTION WHEN raise_exception THEN
    RAISE NOTICE 'Error trapped on emaj_gen_sql_dump_changes_group() call';
  END;
END;
$$;

-- Invalid options.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER = dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'COLS_ORDER = PK', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=()', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION = dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION = FULL', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION = partial', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = ()', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS = (emaj_tuple , dummy)', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'NO_EMPTY_FILES', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'Tables_Only, Sequences_Only', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY = dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'ORDER_BY = pk', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SQL_FORMAT = dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), SQL_FORMAT=PRETTY', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=dummy', NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=(format csv)', NULL);
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=dummy', NULL, :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_OPTIONS=()', NULL, :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=dummy', NULL, :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=(dummy)', NULL, :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COPY_OPTIONS=(;)', NULL, :'EMAJTESTTMPDIR');

-- Invalid relations array.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY['dummy']);

-- Invalid output location.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, 'dummy');
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, '/dummy');
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, NULL);
SELECT emaj.emaj_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, NULL, 'dummy');

--
-- Various options influencing the generated SQL.
--

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', '', NULL);
SELECT * FROM emaj_temp_sql;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=NONE, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=PARTIAL, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'CONSOLIDATION=FULL, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=ALL, CONSOLIDATION=FULL, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=MIN, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'EMAJ_COLUMNS=(emaj_tuple, emaj_gid, emaj_txid), SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY=PK, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'ORDER_BY = TIME,
                                                                             Consolidation = partial,
                                                                             Sql_Format = Pretty', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq in ('mytbl1', 'myseq1') AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER=LOG_TABLE, CONSOLIDATION=FULL, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq = 'mytbl1' AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'COLS_ORDER=PK, SQL_FORMAT=PRETTY', NULL);
SELECT sql_tblseq, sql_text FROM emaj_temp_sql WHERE sql_tblseq = 'mytbl1' AND sql_line_number >= 1 ORDER BY sql_stmt_number, sql_line_number;

SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SEQUENCES_ONLY', NULL);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 GROUP BY sql_rel_kind ORDER BY sql_rel_kind;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'TABLES_ONLY', NULL);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 GROUP BY sql_rel_kind ORDER BY sql_rel_kind;

-- Test the tables/sequences array filter.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY[]::TEXT[]);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY[NULL, '']::TEXT[]);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', NULL, ARRAY['myschema2.mytbl1', 'myschema2.myseq1']);
SELECT sql_rel_kind, count(*) FROM emaj_temp_sql WHERE sql_line_number = 1 GROUP BY 1;
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SEQUENCES_ONLY', ARRAY['myschema2.mytbl1']);
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'TABLES_ONLY', ARRAY['myschema2.myseq1']);

-- Test output as a sql/psql script.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'SQL_FORMAT=PRETTY', ARRAY['myschema2.mytbl1', 'myschema2.myseq1'],
                                            :'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23', 'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || ')', ARRAY['myschema2.mytbl1', 'myschema2.myseq1'],
                                            :'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup2', 'Mark21', 'Mark23',
                                            'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), PSQL_COPY_OPTIONS=(format csv, delimiter '';'', header)',
                                            ARRAY['myschema2.mytbl1', 'myschema2.myseq1'], :'EMAJTESTTMPDIR' || '/sql_script');
\! cat $EMAJTESTTMPDIR/sql_script
\! rm $EMAJTESTTMPDIR/*

-- Perform logged changes.

SELECT emaj.emaj_set_mark_group('myGroup1', 'Dump_changes_tests_M1');

-- Build the base content of the test table.
UPDATE myschema1.myTbl4 SET col42 = 'Initial row' WHERE col41 = 1 AND col43 = 1;
INSERT INTO myschema1.myTbl4
  SELECT i, 'Initial row', 1, 1, 'ABC' FROM generate_series(2, 9) i;

SELECT emaj.emaj_set_mark_group('myGroup1', 'Dump_changes_tests_M2');

-- Record changes.

INSERT INTO myschema1.myTbl4 VALUES (10, 'Inserted row', 1, 1, 'ABC');

DELETE FROM myschema1.myTbl4 WHERE col41 = 1 AND col43 = 1;

UPDATE myschema1.myTbl4 SET col43 = 2, col42 = 'PK col changed' WHERE col41 = 2 AND col43 = 1;

UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed' WHERE col41 = 3 AND col43 = 1;

UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed' WHERE col41 = 4 AND col43 = 1;
UPDATE myschema1.myTbl4 SET col42 = 'Non PK col changed twice' WHERE col41 = 4 AND col43 = 1;

UPDATE myschema1.myTbl4 SET col44 = 1 WHERE col41 = 5 AND col43 = 1;  -- actually no change

SELECT emaj.emaj_set_mark_group('myGroup1', 'Dump_changes_tests_M3');

INSERT INTO myschema1.myTbl4 VALUES (11, 'Inserted and deleted row', 2, 1, 'ABC');
DELETE FROM myschema1.myTbl4 WHERE col41 = 11 AND col43 = 2;

DELETE FROM myschema1.myTbl4 WHERE col41 = 6 AND col43 = 1;
INSERT INTO myschema1.myTbl4 VALUES (6, 'Deleted and inserted row', 1, 1, 'ABC');

DELETE FROM myschema1.myTbl4 WHERE col41 = 7 AND col43 = 1;
INSERT INTO myschema1.myTbl4 VALUES (7, 'Initial row', 1, 1, 'ABC');    -- totaly unchanged row

INSERT INTO myschema1.myTbl4 VALUES (12, 'Inserted, updated and deleted row', 2, 1, 'ABC');
UPDATE myschema1.myTbl4 SET col44 = 2 WHERE col41 = 12 AND col43 = 2;
DELETE FROM myschema1.myTbl4 WHERE col41 = 12 AND col43 = 2;

SELECT nextval('myschema1."myTbl3_col31_seq"');

SELECT * FROM emaj.emaj_set_mark_group('myGroup1', 'Dump_changes_tests_M4');

-- Directly dump changes.
SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M3',
                                    'NO_EMPTY_FILES, EMAJ_COLUMNS=MIN',
                                    NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M3', 'Dump_changes_tests_M4',
                                    'NO_EMPTY_FILES, COPY_OPTIONS=(format csv, header), EMAJ_COLUMNS=(emaj_verb, emaj_tuple, emaj_gid)',
                                    NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! grep -v '  started at ' $EMAJTESTTMPDIR/_INFO
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
                                    'CONSOLIDATION=FULL, COPY_OPTIONS=(format csv, delimiter '';'', force_quote *, header)',
                                    NULL, :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
                                    'CONSOLIDATION=PARTIAL',
                                    NULL, :'EMAJTESTTMPDIR');
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

-- Dump changes for tables with unusual data types and consolidation.

SELECT emaj.emaj_dump_changes_group('myGroup2', 'Multi-2', 'Multi-3',
                                    'CONSOLIDATION=PARTIAL',
                                    '{"myschema2.mytbl5", "myschema2.mytbl6"}', :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

SELECT emaj.emaj_dump_changes_group('myGroup2', 'Multi-2', 'Multi-3',
                                    'CONSOLIDATION=FULL',
                                    '{"myschema2.mytbl5", "myschema2.mytbl6"}', :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! rm $EMAJTESTTMPDIR/*

-- Generate SQL statements with the PSQL_COPY_DIR option and execute the generated script.
SELECT emaj.emaj_gen_sql_dump_changes_group('myGroup1', 'Dump_changes_tests_M2', 'Dump_changes_tests_M4',
                                            'PSQL_COPY_DIR=(' || :'EMAJTESTTMPDIR' || '), CONSOLIDATION=FULL',
                                            NULL, :'EMAJTESTTMPDIR' || '/sql_script');
\i :EMAJTESTTMPDIR/sql_script
\! ls -1sS $EMAJTESTTMPDIR
\! cat $EMAJTESTTMPDIR/myschema1_mytbl4.changes
\! cat $EMAJTESTTMPDIR/myschema1_myTbl3_col31_seq.changes
\! rm $EMAJTESTTMPDIR/*

SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'Dump_changes_tests_M1');
SELECT emaj.emaj_cleanup_rollback_state();
SELECT emaj.emaj_delete_mark_group('myGroup1', 'EMAJ_LAST_MARK');

-- Test using quotes in schema, table or group names.
SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=NONE', NULL, :'EMAJTESTTMPDIR');
\! ls -1sS $EMAJTESTTMPDIR
\! grep -v '  started at ' $EMAJTESTTMPDIR/_INFO
\! cat $EMAJTESTTMPDIR/phil_s_schema_3_myTbl2__col21_seq.changes

SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=PARTIAL', '{"phil''s schema\"3.phil''s tbl1"}', :'EMAJTESTTMPDIR');
SELECT emaj.emaj_dump_changes_group('phil''s group#3",', 'Mark4', 'Mark5', 'CONSOLIDATION=FULL', '{"phil''s schema\"3.phil''s tbl1"}', :'EMAJTESTTMPDIR');
\! rm $EMAJTESTTMPDIR/*

-- Checks for emaj_dump_changes_group() and emaj_gen_sql_dump_changes_group().
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5400 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5500);

-----------------------------
-- emaj_gen_sql_group() and emaj_gen_sql_groups() tests.
-----------------------------

-- Group is unknown.
SELECT emaj.emaj_gen_sql_group(NULL, NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_group('unknownGroup', NULL, NULL, NULL, NULL);

SELECT emaj.emaj_gen_sql_groups(NULL, NULL, NULL, NULL);
SELECT emaj.emaj_gen_sql_groups('{"myGroup1", "unknownGroup"}', NULL, NULL, NULL);

-- Invalid start mark.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'unknownMark', NULL, NULL);
SELECT emaj.emaj_gen_sql_groups('{"myGroup1", "myGroup2"}', 'Mark11', NULL, NULL, NULL);

-- Invalid end mark.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Multi-1', 'unknownMark', NULL);
SELECT emaj.emaj_gen_sql_groups('{"myGroup1", "myGroup2"}', 'Multi-1', 'Mark11', NULL);

-- End mark is prior start mark.
-- (mark timestamps are temporarily changed so that regression test can return a stable error message).
BEGIN;
  UPDATE emaj.emaj_time_stamp SET time_clock_timestamp = '2000-01-01 12:00:00+00'
    FROM emaj.emaj_mark
    WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_name = 'Mark21';
  UPDATE emaj.emaj_time_stamp SET time_clock_timestamp = '2000-01-01 13:00:00+00'
    FROM emaj.emaj_mark
    WHERE time_id = mark_time_id AND mark_group = 'myGroup2' AND mark_name = 'Mark22';
  SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark22', 'Mark21', NULL);
ROLLBACK;
BEGIN;
  UPDATE emaj.emaj_time_stamp SET time_clock_timestamp = '2000-01-01 12:00:00+00'
    FROM emaj.emaj_mark
    WHERE time_id = mark_time_id AND mark_name = 'Multi-2';
  UPDATE emaj.emaj_time_stamp SET time_clock_timestamp = '2000-01-01 13:00:00+00'
    FROM emaj.emaj_mark
    WHERE time_id = mark_time_id AND mark_name = 'Multi-3';
  SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-3', 'Multi-2', NULL);
ROLLBACK;

-- Start mark with the same name but that doesn't correspond to the same point in time.
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Mark21', 'Multi-2', NULL);
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], NULL, 'Multi-2', NULL, NULL);

-- End mark with the same name but that doesn't correspond to the same point in time.
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'Mark21', NULL);

-- Empty table/sequence names array.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY['']);

-- Unknown table/sequence names in the tables filter.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile', ARRAY['foo']);
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema1.mytbl1', 'myschema2.myTbl3_col31_seq', 'phil''s schema3.phil''s tbl1']);
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema1.mytbl1', 'foo', 'myschema2.myTbl3_col31_seq', 'phil''s schema3.phil''s tbl1']);

-- The tables group contains a table without pkey.
SELECT emaj.emaj_gen_sql_group('phil''s group#3",', 'Mark4', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/Group3');

-- Invalid location path name.
SELECT emaj.emaj_gen_sql_group('myGroup1', 'Mark21', NULL, '/tmp/unknownDirectory/myFile');
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL, '/tmp/unknownDirectory/myFile');

-- Should be ok.
-- Generated files content is checked later in adm2.sql scenario.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', NULL, :'EMAJTESTTMPDIR' || '/myFile');
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'Mark22', :'EMAJTESTTMPDIR' || '/myFile');
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', NULL, :'EMAJTESTTMPDIR' || '/myFile');
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-2', 'Multi-3', :'EMAJTESTTMPDIR' || '/myFile');
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile');

-- Should be ok with no output file supplied.
SELECT emaj.emaj_gen_sql_group('myGroup1', 'Mark21', NULL, NULL);
\copy (select * from emaj_sql_script) to '/dev/null'
DROP TABLE emaj_temp_script CASCADE;

-- Should be ok, with tables and sequences filtering.
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_time_id, stat_last_mark, stat_last_time_id, stat_role, stat_verb, stat_rows
  FROM emaj.emaj_detailed_log_stat_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK');
-- All tables and sequences.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema2.mytbl1', 'myschema2.mytbl2', 'myschema2.myTbl3', 'myschema2.mytbl4',
     'myschema2.mytbl5', 'myschema2.mytbl6', 'myschema2.myseq1', 'myschema2.myTbl3_col31_seq']);
-- Minus 1 sequence.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema2.mytbl1', 'myschema2.mytbl2', 'myschema2.myTbl3', 'myschema2.mytbl4',
     'myschema2.mytbl5', 'myschema2.mytbl6', 'myschema2.myseq1']);
-- Minus 1 table.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema2.mytbl1', 'myschema2.mytbl2', 'myschema2.myTbl3',
     'myschema2.mytbl5', 'myschema2.mytbl6', 'myschema2.myseq1']);
-- Only 1 sequence.
SELECT emaj.emaj_gen_sql_group('myGroup2', 'Mark21', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema2.myTbl3_col31_seq']);
-- Only 1 table (with a strange name and belonging to a group having another table without pkey).
SELECT emaj.emaj_gen_sql_group('phil''s group#3",', 'Mark4', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'phil''s schema"3.phil''s tbl1']);
-- Several groups and 1 table of each, with redondancy in the tables array.
SELECT emaj.emaj_gen_sql_groups(ARRAY['myGroup1', 'myGroup2'], 'Multi-1', 'Multi-3', :'EMAJTESTTMPDIR' || '/myFile', ARRAY[
     'myschema1.mytbl4', 'myschema2.mytbl4', 'myschema1.mytbl4', 'myschema2.mytbl4']);
\! grep 'only for' $EMAJTESTTMPDIR/myFile

-- Generate a sql script after a table structure change but on a time frame prior the change.
BEGIN;
  SELECT emaj.emaj_set_mark_group('myGroup2', 'Test sql generation');
  INSERT INTO mySchema2.myTbl2 VALUES (1000, 'Text', '01/01/2000');
  UPDATE mySchema2.myTbl2 SET col22 = 'New TEXT' WHERE col21 = 1000;
  DELETE FROM mySchema2.myTbl2 WHERE col21 = 1000;
  SELECT emaj.emaj_remove_table('myschema2', 'mytbl2', 'Before ALTER mytbl2');
  ALTER table mySchema2.myTbl2 RENAME COLUMN col21 TO id;
  SELECT emaj.emaj_gen_sql_group('myGroup2', 'Test sql generation', 'Before ALTER mytbl2', :'EMAJTESTTMPDIR' || '/altered_tbl.sql');
ROLLBACK;
-- Comment transaction commands and mask the timestamp in the initial comment for the need of the current test.
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/^BEGIN/--BEGIN/;s/^COMMIT/--COMMIT/'
\! find $EMAJTESTTMPDIR -name '*.sql' -type f -print0 | xargs -0 sed -i -s 's/at .*$/at [ts]$/'
-- And execute the generated script.
\set SQLSCRIPT :EMAJTESTTMPDIR '/altered_tbl.sql'
BEGIN;
  \i :SQLSCRIPT
ROLLBACK;

\! rm $EMAJTESTTMPDIR/*

-- Check for emaj_gen_sql_group().
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5500 ORDER BY hist_id;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(5600);

-----------------------------
-- Test a table reclustering (it will use the pkey index as clustered index) and a vacuum full.
-----------------------------
CLUSTER emaj_myschema1.mytbl1_log;
REPACK (ANALYZE, CONCURRENTLY) emaj_myschema1.mytbl1_log;    -- PG19+

VACUUM FULL emaj_myschema1.mytbl1_log;

-----------------------------
-- Try forbiden actions on parameters tables.
-----------------------------
TRUNCATE emaj.emaj_param;
TRUNCATE emaj.emaj_default_param;

-----------------------------
-- emaj_export_parameters_configuration() and emaj_import_parameters_configuration() tests.
-----------------------------
SELECT emaj.emaj_set_param('fixed_step_rollback_duration', '3.5 millisecond');

-- Direct export.
--   OK.
SELECT json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');
SELECT json_array_length(emaj.emaj_export_parameters_configuration(TRUE)->'parameters');

-- Export in file.
--   Error.
SELECT emaj.emaj_export_parameters_configuration('/tmp/dummy/location/file');
--   OK.
SELECT emaj.emaj_export_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config');
\! wc -l $EMAJTESTTMPDIR/orig_param_config
\! grep -v ', at ' $EMAJTESTTMPDIR/orig_param_config

-- Direct import.
--   Error.
--     No "parameters" array.
SELECT emaj.emaj_import_parameters_configuration('{ "dummy_json": null }'::JSON);
--     Unknown attributes.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "unknown_attribute_1": null, "unknown_attribute_2": null} ] }'::JSON);
--     Missing or null "key" attributes.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "value": "no_key"} ] }'::JSON);
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": null} ] }'::JSON);
--     Invalid key.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "unknown_param" } ] }'::JSON);
--     Duplicate key.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention" }, { "key": "history_retention" } ] }'::JSON);
--     Bad interval format.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": "NOT an interval" } ] }'::JSON);

--   Ok.
--     New local value.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": "1 day"} ] }'::JSON);
--     Modified local value.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": "2 days"} ] }'::JSON);
--     "null" "value" attribute.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention", "value": null} ] }'::JSON);
--     Missing "value" attribute.
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ { "key": "history_retention"} ] }'::JSON);
SELECT json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');
--   Reset other parameters.
SELECT emaj.emaj_set_param('history_retention', '1 day');
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ ] }'::JSON, FALSE);
SELECT * FROM emaj.emaj_param WHERE param_key = 'history_retention';
SELECT emaj.emaj_import_parameters_configuration('{ "parameters": [ ] }'::JSON, TRUE);
SELECT * FROM emaj.emaj_param WHERE param_key = 'history_retention';

-- Import from file.
--   Error.
SELECT emaj.emaj_import_parameters_configuration('/tmp/dummy/location/file');
\! echo 'not a json content' >$EMAJTESTTMPDIR/bad_param_config
SELECT emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');
\! echo '{ "dummy_json": null }' >$EMAJTESTTMPDIR/bad_param_config
SELECT emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');
\! echo '{ "parameters": [ { "key": "bad_key", "value": null} ] }' >$EMAJTESTTMPDIR/bad_param_config
SELECT emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/bad_param_config');

--   Ok.
SELECT emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config', TRUE);
SELECT json_array_length(emaj.emaj_export_parameters_configuration()->'parameters');

SELECT emaj.emaj_import_parameters_configuration(:'EMAJTESTTMPDIR' || '/orig_param_config', FALSE);

-----------------------------
-- emaj_purge_histories() tests.
-----------------------------
-- Use the default retention delay.
SELECT emaj.emaj_purge_histories(NULL);
-- Overload the retention delay.
SELECT emaj.emaj_purge_histories('0 SECOND');
-- Use an infinite default retention delay without overload.
SELECT emaj.emaj_set_param('history_retention', '101 years');
SELECT emaj.emaj_purge_histories();
SELECT emaj.emaj_set_param('history_retention', NULL);

-- Check for emaj_import_parameters_configuration() and emaj_purge_histories().
SELECT hist_id, hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 5600 ORDER BY hist_id;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
