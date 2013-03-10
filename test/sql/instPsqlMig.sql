-- instPsqlMig.sql: Migrate from previous to next E-Maj vesrion while groups are not yet created. 
-- Install E-Maj as simple psql script (mandatory for postgres version prior 9.1)
--
------------------------------------------------------------
-- install dblink
------------------------------------------------------------
-- this 8.4.8 version seems compatible with 8.2 to 9.0 pg version
\i ~/postgresql-8.4.8/contrib/dblink/dblink.sql

-----------------------------
-- migrate to the target version
-----------------------------
\i ../../sql/emaj-1.0.2-to-next.sql

-----------------------------
-- check installation
-----------------------------
-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

