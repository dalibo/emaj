-- after_restore.sql: test the emaj environment restored from a dump taken at the end of schedule reg tests
-- All operations are executed by a super-user
--

-----------------------------
-- Checking restore
-----------------------------

DO LANGUAGE plpgsql $$
DECLARE
  r             RECORD;
  keepTable     BOOLEAN = FALSE;
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
     ORDER BY 1,2
  LOOP
    SELECT tbl_tuple INTO expected_val FROM emaj.emaj_regtest_dump_tbl WHERE tbl_schema = r.nspname AND tbl_name = r.relname;
    EXECUTE 'SELECT count(*) FROM '||quote_ident(r.nspname)||'.'||quote_ident(r.relname) INTO returned_val;
    IF NOT (expected_val = returned_val OR (r.nspname || '.' || r.relname = 'emaj.emaj_hist' AND expected_val = returned_val - 1)) THEN
-- the emaj_hist table may contain 1 more row created at extension creation.
      RAISE WARNING 'Error, the table %.% contains % rows instead of %', quote_ident(r.nspname), quote_ident(r.relname), returned_val, expected_val;
      keepTable = TRUE;
    END IF;
  END LOOP;
-- Comparing the properties of each sequence.
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 'S' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' AND relname ~ '_seq$'
     ORDER BY 1,2
  LOOP
    EXECUTE 'SELECT * FROM emaj.emaj_regtest_dump_seq WHERE sequ_schema = ' || quote_literal(r.nspname) || ' AND sequ_name = ' || quote_literal(r.relname)
         || ' EXCEPT SELECT * FROM emaj.tmp_get_current_sequence_state(' || quote_literal(r.nspname) || ',' || quote_literal(r.relname) || ',0)';
    GET DIAGNOSTICS delta = ROW_COUNT;
    IF delta > 0 THEN
      SELECT sequ_last_val INTO expected_val FROM emaj.emaj_regtest_dump_seq WHERE sequ_schema = r.nspname AND sequ_name = r.relname;
      EXECUTE 'SELECT sequ_last_val FROM emaj.tmp_get_current_sequence_state(' || quote_literal(r.nspname) || ',' || quote_literal(r.relname) || ',0)'
        INTO returned_val;
      IF expected_val <> returned_val THEN
        RAISE WARNING 'Error, the sequence %.% has last_val equal to % instead of %', quote_ident(r.nspname), quote_ident(r.relname), returned_val, expected_val;
      ELSE
        RAISE WARNING 'Error, the properties of the sequence %.% are not the expected ones', quote_ident(r.nspname), quote_ident(r.relname);
      END IF;
      keepTable = TRUE;
    END IF;
  END LOOP;
-- if everything is OK, drop both control tables created just before the database dump.
  IF NOT keepTable THEN
    DROP TABLE emaj.emaj_regtest_dump_tbl, emaj.emaj_regtest_dump_seq;
  END IF;
-- drop the specific function created just before the database dump.
  DROP FUNCTION emaj.tmp_get_current_sequence_state(TEXT, TEXT, BIGINT);
END $$;

-----------------------------
-- Let's use the E-Maj environment
-----------------------------
-----------------------------
-- Step 1 : for myGroup2, update tables and set a mark 
-----------------------------
set search_path=myschema2;
insert into myTbl1 select 100+i, 'KLM', E'\\000\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\000\\034'::bytea where col11 >105;
insert into myTbl2 values (100,'KLM','2012-12-31');
delete from myTbl1 where col11 > 110;
select nextval('myschema2.myseq1');
--
select emaj.emaj_set_mark_group('myGroup2','After restore mark');
--
-----------------------------
-- Checking step 1
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;

-- user tables
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple from emaj_myschema2.mytbl1_log order by col11, col12, emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple from emaj_myschema2.mytbl2_log order by col21, emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple from emaj_myschema2."myTbl3_log" order by col31, emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple from emaj_myschema2.mytbl4_log order by col41, emaj_gid, emaj_tuple desc;
--
-----------------------------
-- Step 2 : for myGroup2, rollback to mark Multi-1 (set before dump/restore) 
-----------------------------
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows from emaj.emaj_log_stat_group('myGroup2','Multi-1',NULL);
select * from emaj.emaj_rollback_group('myGroup2','Multi-1');
--
-----------------------------
-- Checking step 2
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
-- user tables
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple from emaj_myschema2.mytbl1_log order by col11, col12, emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple from emaj_myschema2.mytbl2_log order by col21, emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple from emaj_myschema2."myTbl3_log" order by col31, emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple from emaj_myschema2.mytbl4_log order by col41, emaj_gid, emaj_tuple desc;
--
-----------------------------
-- Step 3 : stop myGroup2
-----------------------------
select emaj.emaj_stop_group('myGroup2');
--
-----------------------------
-- test end: check and reset history
-----------------------------
select count(*) from emaj.emaj_hist;
--select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
--  (select * from emaj.emaj_hist order by hist_id) as t;
--
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 30000;
