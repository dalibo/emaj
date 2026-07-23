-- install.sql : install E-Maj as an extension
--

-----------------------------
-- Install emaj as extension.
-----------------------------
CREATE EXTENSION emaj VERSION '5.0.0' CASCADE;

-----------------------------
-- Verify that dropping the extension is blocked by event trigger.
-----------------------------
BEGIN;
  DROP EXTENSION emaj CASCADE;
ROLLBACK;

-----------------------------
-- Check installation.
-----------------------------
-- Check impact in catalog.
SELECT extname, extversion FROM pg_extension WHERE extname = 'emaj';
SELECT relname FROM pg_catalog.pg_class, 
                    (SELECT unnest(extconfig) AS oid FROM pg_catalog.pg_extension WHERE extname = 'emaj') AS t 
  WHERE t.oid = pg_class.oid
  ORDER BY 1;

-- Check the emaj_version_hist and emaj_install_conf content.
SELECT verh_version FROM emaj.emaj_version_hist;
SELECT * FROM emaj.emaj_install_conf;
SELECT emaj.emaj_get_version();

-- Check history and parameters.
SELECT hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
DELETE FROM emaj.emaj_hist;
SELECT * FROM emaj.emaj_all_param ORDER BY param_rank;
