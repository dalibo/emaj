-- install_psql.sql : install E-Maj with a psql script
--
-----------------------------
-- emaj installation (not as extension).
-----------------------------
\i sql/emaj-5.0.0.sql

-----------------------------
-- Check installation.
-----------------------------

-- Check the emaj_version_hist and emaj_install_conf content
SELECT verh_version FROM emaj.emaj_version_hist;
SELECT * FROM emaj.emaj_install_conf;
SELECT emaj.emaj_get_version();

-- Check history and parameters
SELECT hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
DELETE FROM emaj.emaj_hist;
SELECT * FROM emaj.emaj_all_param ORDER BY param_rank;
