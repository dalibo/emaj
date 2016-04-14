-- instPsqlPrev.sql: Install previous version of E-Maj as simple psql script
-- It installs the previous E-Maj version for migration test.
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

-----------------------------
-- emaj installation with previous version
-----------------------------
\i ../../../emaj-1.3.0/sql/emaj.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-- check history
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

