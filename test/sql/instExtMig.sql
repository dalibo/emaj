-- instExtMig.sql : Migrate from E-Maj 1.3.0 to next_version while groups are not yet created.
-- install E-Maj as an extension 
--

-- check the extension is available in the right version 
select * from pg_available_extension_versions where name = 'emaj';

-- look at all available update paths
select * from pg_extension_update_paths('emaj');

-----------------------------
-- test that a 1.3.0 version moved as an extension can be dropped
-----------------------------
-- emaj 1.3.0 installation using the psql script
\i ../../../emaj-1.3.0/sql/emaj.sql

-- transform emaj object as extension
CREATE EXTENSION emaj VERSION '1.3.0' FROM unpackaged;

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- drop the extension
DROP EXTENSION emaj;

-- verify that all emaj tables, types and functions have been included in the extension and then deleted 
-- both tables and functions list should be empty 

select relname from pg_class,pg_namespace where relnamespace = pg_namespace.oid and nspname = 'emaj';
select proname from pg_proc,pg_namespace where pronamespace = pg_namespace.oid and nspname = 'emaj';


-----------------------------
-- emaj update to next_version
-----------------------------
--\! cp /usr/local/pg912/share/postgresql/extension/emaj.control.0.12.0 /usr/local/pg912/share/postgresql/extension/emaj.control

-- emaj 1.3.0 installation using the psql script
\i ../../../emaj-1.3.0/sql/emaj.sql

-- transform emaj object as extension
CREATE EXTENSION emaj VERSION '1.3.0' FROM unpackaged;

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- process the extension migration
ALTER EXTENSION emaj UPDATE TO 'next_version';

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

