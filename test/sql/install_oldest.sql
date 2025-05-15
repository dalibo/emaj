-- install_oldest.sql : install the oldest E-Maj version that is compatible with the oldest currently supported Postgres version
--

-----------------------------
-- set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace
-----------------------------
ALTER TABLESPACE tspemaj_renamed RENAME TO tspemaj;
SET default_tablespace TO tspemaj;

------------------------------------------------------------
-- emaj installation in its oldest version as an extension
------------------------------------------------------------
CREATE EXTENSION emaj VERSION '3.1.0' CASCADE;

------------------------------------------------------------
-- check installation
------------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';
