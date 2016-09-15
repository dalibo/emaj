-- instPsqlMig.sql: Migrate from previous to next E-Maj vesrion while groups are not yet created. 
-- Install E-Maj as simple psql script
--

-----------------------------
-- migrate to the target version
-----------------------------
\i ../../sql/emaj-1.3.0-to-1.3.1.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

