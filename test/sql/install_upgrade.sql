-- install_upgrade.sql
-- Install the last stable E-Maj version and immediately upgrade to the devel version, while groups are not yet created.
-- install E-Maj as an extension 
--

-----------------------------
-- Check the extension's availability.
-----------------------------

-- Check the extension is available in the right version.
SELECT * FROM pg_available_extension_versions WHERE name = 'emaj' ORDER BY version DESC LIMIT 2;

-- Look at all available update paths.
SELECT * FROM pg_extension_update_paths('emaj') ORDER BY 1, 2;

------------------------------------------------------------
-- Install emaj in its previous version as an extension.
------------------------------------------------------------
CREATE EXTENSION emaj VERSION '4.7.1' CASCADE;

-- Check.
SELECT extname, extversion FROM pg_extension WHERE extname = 'emaj';

-----------------------------------------------------------
-- Update emaj to the next version.
-----------------------------------------------------------
ALTER EXTENSION emaj UPDATE TO '5.0.0';

-----------------------------------------------------------
-- Check installation.
-----------------------------------------------------------
-- Check impact in catalog.
SELECT extname, extversion FROM pg_extension WHERE extname = 'emaj';
SELECT relname FROM pg_catalog.pg_class, 
                    (SELECT unnest(extconfig) AS oid FROM pg_catalog.pg_extension WHERE extname = 'emaj') AS t 
  WHERE t.oid = pg_class.oid
  ORDER BY 1;

-- Check the emaj_version_hist and emaj_install_conf content.
SELECT verh_version FROM emaj.emaj_version_hist ORDER BY verh_time_range;
SELECT * FROM emaj.emaj_install_conf;
SELECT emaj.emaj_get_version();

-- Check the emaj environment, just after its creation.
SELECT emaj.emaj_verify_all();

-- Check history and parameters.
SELECT hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
DELETE FROM emaj.emaj_hist;
SELECT * FROM emaj.emaj_all_param ORDER BY param_rank;
