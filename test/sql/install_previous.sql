-- install_previous.sql : install previous version of E-Maj as an extension
--

-----------------------------
-- Set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace.
-----------------------------
ALTER TABLESPACE tspemaj_renamed RENAME TO tspemaj;
SET default_tablespace TO tspemaj;

------------------------------------------------------------
-- Install emaj in its previous version as an extension.
------------------------------------------------------------
CREATE EXTENSION emaj VERSION '5.0.0' CASCADE;

------------------------------------------------------------
-- Check installation.
------------------------------------------------------------
-- Check impact in catalog.
SELECT extname, extversion FROM pg_extension WHERE extname = 'emaj';

-- Check the emaj version.
SELECT emaj.emaj_get_version();

-- Check the history.
SELECT hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user FROM emaj.emaj_hist ORDER BY hist_id;
