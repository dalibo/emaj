-- install_previous.sql : install previous version of E-Maj as an extension
--

-----------------------------
-- set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace
-----------------------------
ALTER TABLESPACE tspemaj_renamed RENAME TO tspemaj;
SET default_tablespace TO tspemaj;

------------------------------------------------------------
-- emaj installation in its previous version as an extension
------------------------------------------------------------
CREATE EXTENSION emaj VERSION '4.3.1' CASCADE;

------------------------------------------------------------
-- check installation
------------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, hist_wording, hist_user from emaj.emaj_hist order by hist_id;
