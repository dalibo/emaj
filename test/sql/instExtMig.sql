-- instExtMig.sql : Upgrade from E-Maj 1.3.0 to next_version while groups are not yet created.
-- install E-Maj as an extension 
--

------------------------------------------------------------
-- install dblink
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS dblink;

-----------------------------
-- for postgres cluster 9.1 and 9.4, temporarily rename tspemaj tablespace to test both cases
-----------------------------
DO LANGUAGE plpgsql 
$$
  DECLARE
  BEGIN
    IF substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)') IN ('9.1', '9.4') THEN
      ALTER TABLESPACE tspemaj RENAME TO tspemaj_renamed;
    END IF;
  END;
$$;

-----------------------------
-- check the extension's availability
-----------------------------

-- check the extension is available in the right version 
select * from pg_available_extension_versions where name = 'emaj';

-- look at all available update paths
select * from pg_extension_update_paths('emaj');

-----------------------------------------------------------
-- test that a 1.3.1 version moved as an extension can be dropped
-----------------------------------------------------------

-- emaj 1.3.1 installation using the psql script
\unset ECHO
\i ../../../emaj-1.3.1/sql/emaj.sql
\set ECHO all

-- transform emaj object as extension
CREATE EXTENSION emaj VERSION '1.3.1' FROM unpackaged;

-- check impact in catalog
select extname, extversion from pg_extension;

-- drop the extension
DROP EXTENSION emaj;

-- verify that all emaj tables, views, types and functions have been included in the extension and then deleted 
-- both tables and functions list should be empty 

select relname from pg_class,pg_namespace where relnamespace = pg_namespace.oid and nspname = 'emaj';
select proname from pg_proc,pg_namespace where pronamespace = pg_namespace.oid and nspname = 'emaj';

-----------------------------------------------------------
-- emaj update to next_version
-----------------------------------------------------------

-- emaj 1.3.1 installation using the psql script
\unset ECHO
\i ../../../emaj-1.3.1/sql/emaj.sql
\set ECHO all

-- transform emaj object as extension
CREATE EXTENSION emaj VERSION '1.3.1' FROM unpackaged;

-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO 'next_version';

-----------------------------------------------------------
-- check installation
-----------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

