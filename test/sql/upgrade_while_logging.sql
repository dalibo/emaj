-- upgrade_while_logging.sql : Upgrade from the previous to the next version while groups are in logging state.
--

-----------------------------
-- Set the default_tablespace parameter to tspemaj to store new technical tables into this tablespace.
-----------------------------
SET default_tablespace TO tspemaj;

-----------------------------
-- Update emaj to the next version.
-----------------------------

-- Check the extension is available.
SELECT * FROM pg_available_extension_versions WHERE name = 'emaj' ORDER BY version DESC LIMIT 2;

SELECT relname FROM pg_catalog.pg_class, 
                    (SELECT unnest(extconfig) AS oid FROM pg_catalog.pg_extension WHERE extname = 'emaj') AS t 
  WHERE t.oid = pg_class.oid
  ORDER BY 1;

-- Check the emaj integrity before upgrading.
SELECT * FROM emaj.emaj_verify_all();

-- Process the extension update.
ALTER EXTENSION emaj UPDATE TO 'devel';

-----------------------------
-- Check installation.
-----------------------------
-- Check impact in catalog.
SELECT extname, extversion FROM pg_extension WHERE extname = 'emaj';
SELECT relname FROM pg_catalog.pg_class, 
                    (SELECT unnest(extconfig) AS oid FROM pg_catalog.pg_extension WHERE extname = 'emaj') AS t 
  WHERE t.oid = pg_class.oid
  ORDER BY 1;

-- Check the emaj_version_hist and emaj_install_conf content.
SELECT verh_version FROM emaj.emaj_version_hist ORDER BY verh_time_range;
SELECT * FROM emaj.emaj_install_conf;
SELECT emaj.emaj_get_version();

-- Check tables list.
\d emaj*.*

-- Check technical sequences position.
SELECT * FROM emaj.emaj_global_seq;
SELECT * FROM emaj.emaj_hist_hist_id_seq;
SELECT * FROM emaj.emaj_time_stamp_time_id_seq;
SELECT * FROM emaj.emaj_rlbk_rlbk_id_seq;

-- Check log sequences position.
SELECT * FROM emaj_myschema1.mytbl1_log_seq;
SELECT * FROM emaj_myschema1.mytbl2_log_seq;
SELECT * FROM emaj_myschema1.mytbl2b_log_seq;
SELECT * FROM emaj_myschema1.mytbl4_log_seq;
SELECT * FROM emaj_myschema2.mytbl1_log_seq;
SELECT * FROM emaj_myschema2.mytbl2_log_seq;
SELECT * FROM emaj_myschema2."myTbl3_log_seq";
SELECT * FROM emaj_myschema2.mytbl4_log_seq;
SELECT * FROM "emaj_phil's schema""3"."phil's tbl1_log_seq";
SELECT * FROM "emaj_phil's schema""3"."myTbl2\_log_seq";

-----------------------------
-- Check the changes specific to this version upgrade.
-----------------------------
