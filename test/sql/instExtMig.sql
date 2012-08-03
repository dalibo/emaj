-- instExtMig.sql : Migrate from E-Maj 0.10.1 to 0.12.0 while groups are not yet created.
-- install E-Maj as an extension (for postgres version 9.1+)
--
-----------------------------
-- emaj update to 0.12.0
-----------------------------
\! cp /usr/local/pg912/share/postgresql/extension/emaj.control.0.12.0 /usr/local/pg912/share/postgresql/extension/emaj.control

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj';

-- process the extension migration
ALTER EXTENSION emaj UPDATE TO '0.12.0';

-- drop the extension after having detached all emaj objects from it
--\i ../../sql/emaj--0.10.1--unpackaged.sql
--DROP EXTENSION emaj;

-- process the common emaj migration 
--\i ../../sql/emaj-0.10.1-to-0.12.0.sql

-- transform emaj objects as extension 
--CREATE EXTENSION emaj FROM unpackaged;

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

