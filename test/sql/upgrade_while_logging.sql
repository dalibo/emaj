-- upgrade_while_logging.sql : Upgrade from the previous to the next version while groups are in logging state.
--
-----------------------------
-- set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace
-----------------------------
SET default_tablespace TO tspemaj;

-----------------------------
-- emaj update to the next version
-----------------------------

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj' order by version desc limit 2;
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

  select * from emaj.emaj_verify_all();

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO '4.3.1';

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

-- check tables list
\d emaj*.*

-- check technical sequences position
select * from emaj.emaj_global_seq;
select * from emaj.emaj_hist_hist_id_seq;
select * from emaj.emaj_time_stamp_time_id_seq;
select * from emaj.emaj_rlbk_rlbk_id_seq;

-- check log sequences position
select * from emaj_myschema1.mytbl1_log_seq;
select * from emaj_myschema1.mytbl2_log_seq;
select * from emaj_myschema1.mytbl2b_log_seq;
select * from emaj_myschema1.mytbl4_log_seq;
select * from emaj_myschema2.mytbl1_log_seq;
select * from emaj_myschema2.mytbl2_log_seq;
select * from emaj_myschema2."myTbl3_log_seq";
select * from emaj_myschema2.mytbl4_log_seq;
select * from "emaj_phil's schema3"."phil's tbl1_log_seq";
select * from "emaj_phil's schema3"."myTbl2\_log_seq";

-----------------------------
-- Check the tables and sequences after upgrade
-----------------------------
-- emaj tables and sequences

-- technical tables
select * from emaj.emaj_sequence order by sequ_schema, sequ_name, sequ_time_id;
select * from emaj.emaj_table order by tbl_schema, tbl_name, tbl_time_id;

-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log_1" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;
--
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- check the environment integrity
select * from emaj.emaj_verify_all();
