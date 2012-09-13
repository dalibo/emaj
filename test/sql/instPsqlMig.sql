-- instPsqlMig.sql: Migrate from E-Maj 0.11.1 to 0.12.0 while groups are not yet created. 
-- Install E-Maj as simple psql script (mandatory for postgres version prior 9.1)
--
------------------------------------------------------------
-- install dblink
------------------------------------------------------------
-- this 8.4.8 version seems compatible with 8.2 to 9.0 pg version
-- for future use...
--\i ~/postgresql-8.4.8/contrib/dblink/dblink.sql

-----------------------------
-- for postgres cluster 8.3 and 9.1, temporarily rename tspemaj tablespace to test both cases
-----------------------------
--CREATE or REPLACE FUNCTION public.emaj_tmp() 
--RETURNS VOID LANGUAGE plpgsql AS 
--$tmp$
--  DECLARE
--  BEGIN
--    IF substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)') IN ('8.3', '9.1') THEN
--      ALTER TABLESPACE tspemaj RENAME TO tspemaj_renamed;
--    END IF;
--    RETURN; 
--  END;
--$tmp$;
--SELECT public.emaj_tmp();
--DROP FUNCTION public.emaj_tmp();

-----------------------------
-- migrate to the target version
-----------------------------
\i ../../sql/emaj-0.11.1-to-0.12.0.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

