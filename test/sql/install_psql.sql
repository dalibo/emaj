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
