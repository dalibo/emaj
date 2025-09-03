-- before_dump.sql: create control tables into the emaj schema before dumping the regression database.
-- Tables and sequences are checked in the after_restore.sql script.
--
\set ON_ERROR_STOP on

SET client_min_messages TO WARNING;
DROP TABLE IF EXISTS emaj.emaj_regtest_dump_tbl;
DROP TABLE IF EXISTS emaj.emaj_regtest_dump_seq;
RESET client_min_messages;

-- Create a table to store the number of rows in tables (emaj internals tables + log tables)
CREATE TABLE emaj.emaj_regtest_dump_tbl (tbl_schema TEXT NOT NULL, tbl_name TEXT NOT NULL, tbl_tuple BIGINT);
-- Create a table to store sequences properties (internals + logs)
CREATE TABLE emaj.emaj_regtest_dump_seq AS (SELECT * FROM emaj.emaj_sequence) WITH NO DATA;

-- Populate both tables
DO LANGUAGE plpgsql $$
DECLARE
  r  RECORD;
BEGIN
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
  FOR r IN
    SELECT nspname, relname
      FROM pg_catalog.pg_class, pg_catalog.pg_namespace
     WHERE relnamespace = pg_namespace.oid
       AND relkind = 'S' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' AND relname ~ '_seq$'
     ORDER BY 1,2
  LOOP
    INSERT INTO emaj.emaj_regtest_dump_seq SELECT * FROM emaj._get_current_seq(r.nspname,r.relname, 0);
  END LOOP;
END $$;
