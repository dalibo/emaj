-- install_upgrade.sql
-- Install the last stable E-Maj version and immediately upgrade to the devel version, while groups are not yet created.
-- install E-Maj as an extension 
--

-----------------------------
-- check the extension's availability
-----------------------------

-- check the extension is available in the right version 
select * from pg_available_extension_versions where name = 'emaj' order by version desc limit 2;

-- look at all available update paths
select * from pg_extension_update_paths('emaj') order by 1,2;;

-----------------------------------------------------------
-- emaj update to next_version
-----------------------------------------------------------
CREATE EXTENSION emaj VERSION '4.6.0' CASCADE;

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO 'devel';

-----------------------------------------------------------
-- check installation
-----------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- check the emaj_version_hist content
select verh_version from emaj.emaj_version_hist order by verh_time_range;
select emaj.emaj_get_version();

-- check the emaj environment, just after creation
select emaj.emaj_verify_all();

-- check history
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
-- wait during half a second to let the statistics collector aggregate the latest stats
select pg_sleep(1.2);
with reset as (select funcid, pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
                 where (funcname like E'emaj\\_%' or funcname like E'\\_%') )
  select * from reset where funcid is null;
