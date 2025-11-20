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

-- check the emaj integrity before upgrading
select * from emaj.emaj_verify_all();

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

-- check the emaj_version_hist content
select verh_version, verh_installed_by_superuser from emaj.emaj_version_hist order by verh_time_range;
select emaj.emaj_get_version();

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
select * from "emaj_phil's schema""3"."phil's tbl1_log_seq";
select * from "emaj_phil's schema""3"."myTbl2\_log_seq";

-----------------------------
-- Check the changes specific to this version upgrade
-----------------------------

select rel_schema, rel_tblseq, rel_time_range, rel_pk_cols from emaj.emaj_relation order by 1,2,3;
