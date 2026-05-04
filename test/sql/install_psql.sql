-- install_psql.sql : install E-Maj with a psql script
--
-----------------------------
-- emaj installation (not as extension)
-----------------------------
\i sql/emaj-devel.sql

-----------------------------
-- check installation
-----------------------------

-- check the emaj_version_hist and emaj_install_conf content
select verh_version from emaj.emaj_version_hist;
select * from emaj.emaj_install_conf;
select emaj.emaj_get_version();

-- check history and parameters
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;
select * from emaj.emaj_all_param order by param_rank;

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
-- first look at pg_stat_activity to force the statistics collector aggregate the latest stats
select null as stats_gathered from pg_stat_activity limit 1;
with reset as (select funcid, pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
                 where (funcname like E'emaj\\_%' or funcname like E'\\_%') )
  select * from reset where funcid is null;
