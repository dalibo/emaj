-- install_psql.sql : install E-Maj with a psql script
--
-----------------------------
-- emaj installation (not as extension)
-----------------------------
\i sql/emaj-4.3.1.sql

-- Test the dblink extension lack (this cannot be easily simulated in the verify.sql unit tests script when emaj is installed as an extension)
begin;
  drop extension dblink;
  select * from emaj.emaj_verify_all();
  rollback;
end;

-----------------------------
-- check installation
-----------------------------

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_hist;

-- check table list
\d emaj.*

-- reset function calls statistics (so the check.sql output is stable with all installation paths)
with reset as (select funcid, pg_stat_reset_single_function_counters(funcid) from pg_stat_user_functions
                 where (funcname like E'emaj\\_%' or funcname like E'\\_%') )
  select * from reset where funcid is null;
