-- instExt.sql : install E-Maj as an extension (for postgres version 9.1+)
--               First install with psql script and then transform emaj objects as extension
--               Then directly install as extension
--

-----------------------------
-- install dblink
-----------------------------
CREATE EXTENSION IF NOT EXISTS dblink;

-----------------------------
-- for postgres cluster 9.1 and 9.4, temporarily rename tspemaj tablespace to test both cases
-----------------------------
DO LANGUAGE plpgsql 
$$
  DECLARE
  BEGIN
    IF substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)') IN ('9.1', '9.4') THEN
      ALTER TABLESPACE tspemaj RENAME TO tspemaj_renamed;
    END IF;
  END;
$$;

-----------------------------
-- emaj installation as extension
-----------------------------
CREATE EXTENSION emaj VERSION 'next_version';

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

-- check table list
\d emaj.*

-----------------------------
-- count all functions in emaj schema and all function usable by users (emaj_xxx)
-----------------------------
select count(*) from pg_proc, pg_namespace 
  where pg_namespace.oid=pronamespace and nspname = 'emaj';

select count(*) from pg_proc, pg_namespace 
  where pg_namespace.oid=pronamespace and nspname = 'emaj' and proname LIKE E'emaj\\_%';

