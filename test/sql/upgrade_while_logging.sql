-- upgrade_while_logging.sql : Upgrade from E-Maj 1.3. to next_version while groups are in logging state.
--
-----------------------------
-- emaj update to next_version
-----------------------------
--\! cp /usr/local/pg912/share/postgresql/extension/emaj.control.0.12.0 /usr/local/pg912/share/postgresql/extension/emaj.control

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO 'next_version';

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

-----------------------------
-- Check the tables and sequences after upgrade
-----------------------------
-- emaj tables and sequences

-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
--
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;

