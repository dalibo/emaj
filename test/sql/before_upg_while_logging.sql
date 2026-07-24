-- Before_upg_while_logging.sql : complex scenario executed by an emaj_adm role.
-- The E-Maj version is changed while groups are in logging state.
-- This script is the part of operations performed before the upgrade.
--
SET datestyle TO ymd;
-----------------------------
-- Grant emaj_adm role.
-----------------------------
GRANT emaj_adm TO _regress_emaj_adm1, _regress_emaj_adm2;
-- Give rights while we are in an emaj version 4.5.0 or earlier.
GRANT ALL ON SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  TO _regress_emaj_adm1, _regress_emaj_adm2;
GRANT ALL ON ALL TABLES IN SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  TO _regress_emaj_adm1, _regress_emaj_adm2;
GRANT ALL ON ALL SEQUENCES IN SCHEMA mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  TO _regress_emaj_adm1, _regress_emaj_adm2;

-----------------------------
-- Create groups.
-----------------------------
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_create_group('myGroup2', TRUE);
SELECT emaj.emaj_create_group('phil''s group#3",', FALSE);
SELECT emaj.emaj_create_group('myGroup6');

-----------------------------
-- Prepare groups.
-----------------------------
SELECT emaj.emaj_assign_tables('myschema1', '.*', NULL, 'myGroup1');
SELECT emaj.emaj_modify_table('myschema1', 'mytbl1', '{"priority":20}'::JSONB);
SELECT emaj.emaj_modify_table('myschema1', 'myTbl3', '{"priority":10}'::JSONB);
SELECT emaj.emaj_modify_table('myschema1', 'mytbl4', '{"priority":20}'::JSONB);
SELECT emaj.emaj_assign_sequence('myschema1', 'myTbl3_col31_seq', 'myGroup1');

SELECT emaj.emaj_assign_tables('myschema2', '{"mytbl1", "mytbl2", "myTbl3", "mytbl4"}', 'myGroup2');
SELECT emaj.emaj_assign_sequences('myschema2', '{"myTbl3_col31_seq", "myseq1"}', 'myGroup2');

-- The third group name contains space, comma # and '.
SELECT emaj.emaj_assign_tables('phil''s schema"3', '.*', 'my"tbl4', 'phil''s group#3",');
SELECT emaj.emaj_assign_sequence('phil''s schema"3', E'phil''s"seq\\1', 'phil''s group#3",');

-- Group with long name tables.
SELECT emaj.emaj_assign_tables('myschema6', '.*', NULL, 'myGroup6');

-----------------------------
-- Set the default_tablespace parameter to tspemaj to log tables and indexes into this tablespace.
-----------------------------
SET default_tablespace TO tspemaj;

-----------------------------
-- Start groups in a single transaction.
-----------------------------
BEGIN;
  SELECT emaj.emaj_start_groups('{"myGroup1", "myGroup2"}', 'M1');
  SELECT emaj.emaj_start_group('phil''s group#3",', 'M1');
COMMIT;

-----------------------------
-- Step 1 : for myGroup1, update tables, set 2 marks, perform 2 unlogged rollbacks and protect the group and its last mark.
-----------------------------
RESET session_authorization;
SET search_path TO myschema1;
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::BYTEA FROM generate_series (1, 11) AS i;
UPDATE myTbl1 SET col13=E'\\034'::BYTEA WHERE col11 <= 3;
INSERT INTO myTbl2 VALUES (1, 'ABC', '2010-12-31');
DELETE FROM myTbl1 WHERE col11 > 10;
INSERT INTO myTbl2 VALUES (2, 'DEF', NULL);
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M2');
--
RESET session_authorization;
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
INSERT INTO myTbl4 VALUES (2, 'FK...', 1, 1, 'ABC');
UPDATE myTbl4 SET col43 = 2;
INSERT INTO myTbl4 VALUES (3, 'FK...', 1, 10, 'ABC');
DELETE FROM myTbl1 WHERE col11 = 10;
UPDATE myTbl1 SET col12='DEF' WHERE col11 <= 2;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M3');
SELECT emaj.emaj_comment_mark_group('myGroup1', 'M3', 'Third mark set');
--
RESET session_authorization;
DELETE FROM myTbl1 WHERE col11 > 3;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M3');
INSERT INTO myTbl2 VALUES (3, 'GHI', NULL);
UPDATE myTbl4 SET col43 = 3 WHERE col41 = 2;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M3');
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_protect_mark_group('myGroup1', 'M3');
SELECT emaj.emaj_protect_group('myGroup1');

-----------------------------
-- Step 2 : for myGroup2, start, update tables and set 2 marks.
-----------------------------
RESET session_authorization;
SET search_path TO myschema2;
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::BYTEA FROM generate_series (1, 11) AS i;
UPDATE myTbl1 SET col13=E'\\034'::BYTEA WHERE col11 <= 3;
INSERT INTO myTbl2 VALUES (1, 'ABC', '2010-01-01');
DELETE FROM myTbl1 WHERE col11 > 10;
SELECT nextval('myschema2.myseq1');
INSERT INTO myTbl2 VALUES (2, 'DEF', NULL);
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup2', 'M2');
--
RESET session_authorization;
SET search_path TO myschema2;
SELECT nextval('myschema2.myseq1');
SELECT nextval('myschema2.myseq1');
SELECT nextval('myschema2.myseq1');
--
ALTER SEQUENCE mySeq1 NO MAXVALUE NO CYCLE;
SET session_authorization TO _regress_emaj_adm1;
--
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
INSERT INTO myTbl4 VALUES (2, 'FK...', 1, 1, 'ABC');
UPDATE myTbl4 SET col43 = 2;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup2', 'M3');

-----------------------------
-- Step 3 : for myGroup2, double logged rollback.
-----------------------------
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M2');
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M3');

-----------------------------
-- Step 4 : for both myGroup1 and myGroup2, SET a common mark.
-----------------------------
SELECT emaj.emaj_set_mark_groups('{"myGroup1", "myGroup2"}', 'Common');

-----------------------------
-- Step 5 : alter group myGroup1 by removing a table.
-----------------------------

SELECT emaj.emaj_remove_table('myschema1', 'myTbl3');

-----------------------------
-- Step 6 : managing a group with long name tables.
-----------------------------
SELECT emaj.emaj_start_group('myGroup6', 'Start G6');
SELECT emaj.emaj_remove_table('myschema6', 'table_with_55_characters_long_name_____0_________0abcde');
SELECT emaj.emaj_stop_group('myGroup6');

-----------------------------
-- Checking steps 1 to 6.
-----------------------------
-- emaj tables.
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp ORDER BY time_id;

SELECT group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging,
       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  FROM emaj.emaj_group ORDER BY group_name;

SELECT * FROM emaj.emaj_group_hist ORDER BY grph_group, grph_time_range;

SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;

SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;

SELECT tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;

SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size
  FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;

SELECT * FROM emaj.emaj_relation_change ORDER BY 1, 2, 3, 4, 5;

-- Log tables.
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log_1" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
--
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-------------------------------
-- Specific tests for this upgrade.
-------------------------------

RESET session_authorization;
