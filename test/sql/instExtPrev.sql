-- instExtPrev.sql : install previous version of E-Maj as an extension
--
------------------------------------------------------------
-- install dblink
------------------------------------------------------------
CREATE EXTENSION dblink;

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

------------------------------------------------------------
-- emaj installation in 1.3.1 (with the psql script)
------------------------------------------------------------
\i ../../../emaj-1.3.1/sql/emaj.sql

------------------------------------------------------------
-- transform emaj 1.3.1 into extension
------------------------------------------------------------

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj';

CREATE EXTENSION emaj VERSION "1.3.1" FROM "unpackaged";

------------------------------------------------------------
-- check installation
------------------------------------------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

