-- install.sql : install E-Maj as an extension
--

-----------------------------
-- emaj installation as extension
-----------------------------
CREATE EXTENSION emaj VERSION 'devel' CASCADE;

-----------------------------
-- verify that dropping the extension is blocked by event trigger
-----------------------------
BEGIN;
  DROP EXTENSION emaj CASCADE;
ROLLBACK;

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- check the emaj_version_hist and emaj_install_conf content
select verh_version from emaj.emaj_version_hist;
select * from emaj.emaj_install_conf;
select emaj.emaj_get_version();

-- check history and parameters
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;
select * from emaj.emaj_all_param order by param_rank;
