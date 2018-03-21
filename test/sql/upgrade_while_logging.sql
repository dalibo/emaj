-- upgrade_while_logging.sql : Upgrade from the previous to the next version while groups are in logging state.
--
-----------------------------
-- set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace
-----------------------------
SET default_tablespace TO tspemaj;

-- specific to 2.2.2 to 2.2.3 upgrade
-- uncomment to let the risky sequence values check fail
--begin;
--  select emaj.emaj_stop_group('myGroup1');
--  select emaj.emaj_start_group('myGroup1','%');
--rollback;

-----------------------------
-- emaj update to the next version
-----------------------------

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj';
select relname from pg_catalog.pg_class, 
                    (select unnest(extconfig) as oid from pg_catalog.pg_extension where extname = 'emaj') as t 
  where t.oid = pg_class.oid
  order by 1;

-- process the extension upgrade
ALTER EXTENSION emaj UPDATE TO 'devel';

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
\d emaj.*
\d emajb.*

-- check technical sequences position
select * from emaj.emaj_global_seq;
select * from emaj.emaj_hist_hist_id_seq;
select * from emaj.emaj_time_stamp_time_id_seq;
select * from emaj.emaj_mark_mark_id_seq;
select * from emaj.emaj_rlbk_rlbk_id_seq;

-- check log sequences position
select * from emaj.myschema1_mytbl1_log_seq;
select * from emaj.myschema1_mytbl2_log_seq;
select * from emajb.myschema1_mytbl2b_log_seq;
select * from emaj.myschema1_mytbl4_log_seq;
select * from emaj.myschema2_mytbl1_log_seq;
select * from emaj.myschema2_mytbl2_log_seq;
select * from emaj."myschema2_myTbl3_log_seq";
select * from emaj.myschema2_mytbl4_log_seq;
select * from emaj."phil's schema3_phil's tbl1_log_seq";
select * from emaj."phil's schema3_myTbl2\_log_seq";

-----------------------------
-- Check the tables and sequences after upgrade
-----------------------------
-- emaj tables and sequences

-- technical tables
select sch_name from emaj.emaj_schema order by 1;
select * from emaj.emaj_relation order by 1,2 ;
select rlbk_messages from emaj.emaj_rlbk order by rlbk_id;

-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.mySchema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema1_myTbl3_log_1" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
--
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;
