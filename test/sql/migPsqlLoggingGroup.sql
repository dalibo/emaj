-- migPsqlLoggingGroup.sql: Migrate from E-Maj 0.11.1 to 0.12.0 while groups are in logging state. 
-- Install E-Maj as simple psql script (mandatory for postgres version prior 9.1)
--
------------------------------------------------------------
-- install dblink
------------------------------------------------------------
-- this 8.4.8 version seems compatible with 8.2 to 9.0 pg version
-- for future use...
--\i ~/postgresql-8.4.8/contrib/dblink/dblink.sql

-----------------------------
-- migrate to the target version
-----------------------------
\i ../../sql/emaj-0.11.1-to-0.12.0.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

