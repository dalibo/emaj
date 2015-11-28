-- migPsqlLoggingGroup.sql: Migrate from previous E-Maj version while groups are in logging state. 
-- Install E-Maj as simple psql script
--

-----------------------------
-- migrate to the target version
-----------------------------
\i ../../sql/emaj-1.1.0-to-1.2.0.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

