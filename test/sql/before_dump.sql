-- before_dump.sql: Create control tables in emaj schema before dump regression database.
--                   Tables are read in after_restore.sql script.
--
\set ON_ERROR_STOP on

DO LANGUAGE plpgsql $$
DECLARE
r  RECORD;
BEGIN
-- Creating a table to store the number of rows in tables (internals + logs)
DROP TABLE IF EXISTS emaj.emaj_regtest_dump_tbl;
CREATE TABLE emaj.emaj_regtest_dump_tbl (tbl_schema TEXT NOT NULL, tbl_name TEXT NOT NULL, tbl_tuple bigint);
FOR r IN
  SELECT nspname, relname
    FROM pg_catalog.pg_class, pg_catalog.pg_namespace
   WHERE relnamespace = pg_namespace.oid
     AND relkind = 'r' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' 
   ORDER BY 1,2
LOOP
  EXECUTE 'INSERT INTO emaj.emaj_regtest_dump_tbl VALUES ('||quote_literal(r.nspname)||','||quote_literal(r.relname)||',(SELECT count(*) FROM '||quote_ident(r.nspname)||'.'||quote_ident(r.relname)||'))';
END LOOP;
-- Creating a table to store properties of sequences (internals + logs)
DROP TABLE IF EXISTS emaj.emaj_regtest_dump_seq;
CREATE TABLE emaj.emaj_regtest_dump_seq AS ( SELECT * FROM emaj.emaj_sequence) WITH NO DATA;
FOR r IN
  SELECT nspname, relname
    FROM pg_catalog.pg_class, pg_catalog.pg_namespace
   WHERE relnamespace = pg_namespace.oid
     AND relkind = 'S' AND nspname ~ '^emaj' AND relname !~ '^emaj_regtest' AND relname ~ '_seq$'
   ORDER BY 1,2
LOOP
  INSERT INTO emaj.emaj_regtest_dump_seq SELECT * FROM emaj._get_current_sequence_state(r.nspname,r.relname, 0);
END LOOP;
END $$;
