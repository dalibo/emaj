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

-- check the emaj_version_hist content
select verh_version from emaj.emaj_version_hist;

-- check history
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;

-- check table list
\d emaj.*

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
with reset as (select funcid, pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
                 where (funcname like E'emaj\\_%' or funcname like E'\\_%') )
  select * from reset where funcid is null;
