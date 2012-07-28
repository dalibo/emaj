-- instExt.sql : install E-Maj as an extension (for postgres version 9.1+)
--               First install with psql script and then transform emaj objects as extension
--               Then directly install as extension
--

-----------------------------
-- for postgres cluster 8.3 and 9.1, temporarily rename tspemaj tablespace 
-----------------------------
CREATE or REPLACE FUNCTION public.emaj_tmp() 
RETURNS VOID LANGUAGE plpgsql AS 
$tmp$
  DECLARE
  BEGIN
    IF substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)') IN ('8.3', '9.1') THEN
      ALTER TABLESPACE tspemaj RENAME TO tspemaj_renamed;
    END IF;
    RETURN; 
  END;
$tmp$;
SELECT public.emaj_tmp();
DROP FUNCTION public.emaj_tmp();

-----------------------------
-- emaj installation using psql script
-----------------------------
\i sql/emaj.sql

-- check the extension is available in the right version 
select * from pg_available_extension_versions where name = 'emaj';

-- transform emaj object as extension
CREATE EXTENSION emaj FROM unpackaged;

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- drop the extension
DROP EXTENSION emaj;

-- verify that all emaj tables, types and functions have been included in the extension and then deleted 
-- both tables and functions list should be empty 

select relname from pg_class,pg_namespace where relnamespace = pg_namespace.oid and nspname = 'emaj';
select proname from pg_proc,pg_namespace where pronamespace = pg_namespace.oid and nspname = 'emaj';

-----------------------------
-- emaj installation as extension
-----------------------------
CREATE EXTENSION emaj;

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

