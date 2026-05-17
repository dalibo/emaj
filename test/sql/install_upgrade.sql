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
CREATE EXTENSION emaj VERSION '4.7.1' CASCADE;

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

-- check the emaj_version_hist and emaj_install_conf content
select verh_version from emaj.emaj_version_hist order by verh_time_range;
select * from emaj.emaj_install_conf;
select emaj.emaj_get_version();

-- check the emaj environment, just after creation
select emaj.emaj_verify_all();

-- check history and parameters
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;
select * from emaj.emaj_all_param order by param_rank;
