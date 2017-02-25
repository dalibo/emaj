-- install_upgrade.sql : Upgrade from E-Maj 2.0.1 to next_version while groups are not yet created.
-- install E-Maj as an extension 
--

------------------------------------------------------------
-- install dblink
------------------------------------------------------------
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
-- check the extension's availability
-----------------------------

-- check the extension is available in the right version 
select * from pg_available_extension_versions where name = 'emaj';

-- look at all available update paths
select * from pg_extension_update_paths('emaj') order by 1,2;;

-----------------------------------------------------------
-- emaj update to next_version
-----------------------------------------------------------
CREATE EXTENSION emaj VERSION '2.0.1';

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO 'next_version';

-----------------------------------------------------------
-- check installation
-----------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check the emaj environment, just after creation
select emaj.emaj_verify_all();

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
-- wait during half a second to let the statistics collector aggregate the latest stats
select pg_sleep(0.5);
select count(*) from 
  (select pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
    where (funcname like E'emaj\\_%' or funcname like E'\\_%')) as t;

