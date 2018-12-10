-- install_psql.sql : install E-Maj with a psql script
--
-----------------------------
-- install dblink and btree_gist
-----------------------------
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-----------------------------
-- emaj installation as extension
-----------------------------
\i sql/emaj-devel.sql

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
