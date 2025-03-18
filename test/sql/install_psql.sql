-- install_psql.sql : install E-Maj with a psql script
--
-----------------------------
-- emaj installation (not as extension)
-----------------------------
\i sql/emaj-devel.sql

-- Test the dblink extension lack (this cannot be easily simulated in the verify.sql unit tests script when emaj is installed as an extension)
begin;
  drop extension dblink;
  select * from emaj.emaj_verify_all();
  rollback;
end;

-----------------------------
-- check installation
-----------------------------

-- check the emaj_version_hist content
select verh_version from emaj.emaj_version_hist;
select emaj.emaj_get_version();

-- check history
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
-- wait during half a second to let the statistics collector aggregate the latest stats
select pg_sleep(1.2);
with reset as (select funcid, pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
                 where (funcname like E'emaj\\_%' or funcname like E'\\_%') )
  select * from reset where funcid is null;
