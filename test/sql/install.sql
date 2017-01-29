-- install.sql : install E-Maj as an extension (for postgres version 9.1+)
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
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

-- check table list
\d emaj.*

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
-- wait during half a second to let the statistics collector aggregate the latest stats
select pg_sleep(0.5);
select count(*) from 
  (select pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
    where (funcname like E'emaj\\_%' or funcname like E'\\_%')) as t;

