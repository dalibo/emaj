-- After_restore.sql: test the emaj environment restored from a dump taken at the end of schedule reg tests.
-- All operations are executed by a super-user.
--

-----------------------------
-- Checking restore.
-----------------------------

-- Compare the number of rows for each emaj table and the properties of each emaj sequence with their saved state before the dump.

DO LANGUAGE PLPGSQL $$
DECLARE
  r             RECORD;
  keeptable     BOOLEAN = FALSE;
  delta         SMALLINT;
  expected_val  BIGINT;
  returned_val  BIGINT;
BEGIN
-- Comparing the number of rows in each table.
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 'r' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest'
     ORDER BY 1, 2
  LOOP
    SELECT tbl_tuple INTO expected_val FROM emaj.emaj_regtest_dump_tbl WHERE tbl_schema = r.nspname AND tbl_name = r.relname;
    EXECUTE 'SELECT count(*) FROM '||quote_ident(r.nspname)||'.'||quote_ident(r.relname) INTO returned_val;
    IF NOT (expected_val = returned_val OR (r.nspname || '.' || r.relname = 'emaj.emaj_hist' AND expected_val = returned_val - 1)) THEN
-- The emaj_hist table may contain 1 more row created at extension creation.
      RAISE WARNING 'Error, the table %.% contains % rows instead of %', quote_ident(r.nspname), quote_ident(r.relname), returned_val, expected_val;
      keeptable = TRUE;
    END IF;
  END LOOP;
-- Comparing the properties of each sequence.
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 's' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' AND relname ~ '_seq$'
     ORDER BY 1, 2
  LOOP
    EXECUTE 'SELECT * FROM emaj.emaj_regtest_dump_seq WHERE sequ_schema = ' || quote_literal(r.nspname) || ' AND sequ_name = ' || quote_literal(r.relname)
         || ' EXCEPT SELECT * FROM emaj._get_current_seq(' || quote_literal(r.nspname) || ', ' || quote_literal(r.relname) || ', 0)';
    GET DIAGNOSTICS delta = ROW_COUNT;
    IF delta > 0 THEN
      SELECT sequ_last_val INTO expected_val FROM emaj.emaj_regtest_dump_seq WHERE sequ_schema = r.nspname AND sequ_name = r.relname;
      EXECUTE 'SELECT sequ_last_val FROM emaj._get_current_seq(' || quote_literal(r.nspname) || ', ' || quote_literal(r.relname) || ', 0)'
        INTO returned_val;
      IF expected_val <> returned_val THEN
        RAISE WARNING 'Error, the sequence %.% has last_val equal to % instead of %',
                      quote_ident(r.nspname), quote_ident(r.relname), returned_val, expected_val;
      ELSE
        RAISE WARNING 'Error, the properties of the sequence %.% are not the expected ones',
                      quote_ident(r.nspname), quote_ident(r.relname);
      END IF;
      keeptable = TRUE;
    END IF;
  END loop;
-- If everything is ok, drop both control tables created just before the database dump.
  IF NOT keeptable THEN
    DROP table emaj.emaj_regtest_dump_tbl, emaj.emaj_regtest_dump_seq;
  END IF;
END $$;

-- Let E-maj check its environment.
SELECT * FROM emaj.emaj_verify_all();

-----------------------------
-- Let's use the E-Maj environment.
-----------------------------
-----------------------------
-- Step 1 : for myGroup2, update tables and set a mark.
-----------------------------
SET search_path TO myschema2;
INSERT INTO myTbl1 SELECT 100+i, 'KLM', E'\\000\\014'::bytea FROM generate_series (1, 11) as i;
UPDATE myTbl1 SET col13=E'\\000\\034'::BYTEA WHERE col11 >105;
INSERT INTO myTbl2 VALUES (100, 'KLM', '2012-12-31');
DELETE FROM myTbl1 WHERE col11 > 110;
SELECT nextval('myschema2.myseq1');
--
SELECT emaj.emaj_set_mark_group('myGroup2', 'After restore mark');
--
-----------------------------
-- Checking step 1.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;

-- User tables.
SELECT * FROM mySchema2.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema2.myTbl2 ORDER BY col21;
SELECT col31, col33 FROM mySchema2."myTbl3" ORDER BY col31;
SELECT * FROM mySchema2.myTbl4 ORDER BY col41;
-- Log tables.
SELECT col11, col12, col13, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl1_log ORDER BY col11, col12, emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl2_log ORDER BY col21, emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple FROM emaj_myschema2."myTbl3_log" ORDER BY col31, emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl4_log ORDER BY col41, emaj_gid, emaj_tuple DESC;
--
-----------------------------
-- Step 2 : for myGroup2, rollback to mark Multi-1 (set before dump/restore).
-----------------------------
SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  FROM emaj.emaj_log_stat_group('myGroup2', 'Multi-1', NULL);
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'Multi-1');
--
-----------------------------
-- Checking step 2.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp ORDER BY time_id;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
-- User tables.
SELECT * FROM mySchema2.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema2.myTbl2 ORDER BY col21;
SELECT col31, col33 FROM mySchema2."myTbl3" ORDER BY col31;
SELECT * FROM mySchema2.myTbl4 ORDER BY col41;
-- Log tables.
SELECT col11, col12, col13, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl1_log ORDER BY col11, col12, emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl2_log ORDER BY col21, emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple FROM emaj_myschema2."myTbl3_log" ORDER BY col31, emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple FROM emaj_myschema2.mytbl4_log ORDER BY col41, emaj_gid, emaj_tuple DESC;
--
-----------------------------
-- Step 3 : stop myGroup2.
-----------------------------
SELECT emaj.emaj_stop_group('myGroup2');
--
-----------------------------
-- Test end: check and reset history.
-----------------------------
SELECT count(*) FROM emaj.emaj_hist;
--SELECT hist_id, hist_function, hist_event, hist_object,
--       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'), hist_user
--  FROM emaj.emaj_hist order by hist_id;
--
TRUNCATE emaj.emaj_hist;
ALTER SEQUENCE emaj.emaj_hist_hist_id_seq restart 30000;
