-- before_dump.sql: create control tables into the emaj schema before dumping the regression database.
-- Tables and sequences are checked in the after_restore.sql script.
--
\set ON_ERROR_STOP on
SET client_min_messages TO WARNING;

-- Create a function to help reading sequences.
CREATE OR REPLACE FUNCTION emaj.tmp_get_current_sequence_state(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql AS
$tmp$
  DECLARE
    r_sequ                   emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Read the sequence.
    EXECUTE format('SELECT nspname, relname, %s, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                   '  FROM %I.%I sq,'
                   '       pg_catalog.pg_sequence s'
                   '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                   '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                   '  WHERE nspname = %L AND relname = %L',
                   coalesce(p_timeId, 0), p_schema, p_sequence, p_schema, p_sequence)
      INTO STRICT r_sequ;
    RETURN r_sequ;
  END;
$tmp$;

DO LANGUAGE plpgsql $$
DECLARE
  r  RECORD;
BEGIN
-- Create a table to store the number of rows in tables (emaj internals tables + log tables)
  DROP TABLE IF EXISTS emaj.emaj_regtest_dump_tbl;
  CREATE TABLE emaj.emaj_regtest_dump_tbl (tbl_schema TEXT NOT NULL, tbl_name TEXT NOT NULL, tbl_tuple BIGINT);
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 'r' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' 
     ORDER BY 1,2
  LOOP
    EXECUTE 'INSERT INTO emaj.emaj_regtest_dump_tbl VALUES (' || quote_literal(r.nspname) || ',' || quote_literal(r.relname)
         || ', (SELECT count(*) FROM ' || quote_ident(r.nspname) || '.' || quote_ident(r.relname) || '))';
  END LOOP;
-- Create a table to store properties of sequences (internals + logs)
  DROP TABLE IF EXISTS emaj.emaj_regtest_dump_seq;
  CREATE TABLE emaj.emaj_regtest_dump_seq AS ( SELECT * FROM emaj.emaj_sequence) WITH NO DATA;
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 'S' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' AND relname ~ '_seq$'
     ORDER BY 1,2
  LOOP
    INSERT INTO emaj.emaj_regtest_dump_seq SELECT * FROM emaj.tmp_get_current_sequence_state(r.nspname,r.relname, 0);
  END LOOP;
END $$;
