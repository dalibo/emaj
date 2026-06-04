-- Adm1.sql : complex scenarios executed by an emaj_adm role.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

SET datestyle TO ymd;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12000);

TRUNCATE emaj.emaj_hist;

-----------------------------
-- Grant emaj_adm role.
-----------------------------
GRANT emaj_adm TO _regress_emaj_adm1, _regress_emaj_adm2;

SELECT emaj.emaj_set_param('dblink_user_password', 'user=_regress_emaj_adm1 password=adm');

--
SET session_authorization TO _regress_emaj_adm1;

-----------------------------
-- Authorized tables and views accesses.
-----------------------------
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_version_hist) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_all_param) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_visible_param) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_time_stamp) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_hist) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_group) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_schema) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_relation) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rel_hist) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_relation_change) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_log_session) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_mark) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_sequence) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_table) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_seq_hole) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_session) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_plan) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj.emaj_rlbk_stat) AS t;
SELECT 'select ok' AS result FROM (SELECT count(*) FROM emaj_mySchema1.myTbl1_log) AS t;

-----------------------------
-- Stop, reset and drop existing groups.
-----------------------------
SELECT emaj.emaj_stop_group('myGroup1', 'Simple stop mark');
SELECT emaj.emaj_force_stop_group('emptyGroup');
SELECT emaj.emaj_stop_groups(emaj.emaj_get_logging_groups());
SELECT emaj.emaj_reset_group('myGroup1');
SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_force_drop_group('myGroup2');
SELECT emaj.emaj_drop_group(grp) FROM unnest(emaj.emaj_get_groups()) AS grp;

-- emaj tables.
SELECT * FROM emaj.emaj_group;
SELECT * FROM emaj.emaj_schema;
SELECT * FROM emaj.emaj_relation;
SELECT * FROM emaj.emaj_mark;
SELECT * FROM emaj.emaj_sequence;
SELECT * FROM emaj.emaj_table;
SELECT * FROM emaj.emaj_seq_hole;
SELECT count(*) FROM emaj.emaj_rlbk;
SELECT count(*) FROM emaj.emaj_rlbk_session;
SELECT count(*) FROM emaj.emaj_rlbk_plan;
SELECT count(*) FROM emaj.emaj_rlbk_stat;

-----------------------------
-- Cleanup application tables.
-----------------------------
RESET session_authorization;
TRUNCATE mySchema1.myTbl1, mySchema1.myTbl2, mySchema1."myTbl3", mySchema1.myTbl4, mySchema1.myTbl2b;
TRUNCATE mySchema2.myTbl1, mySchema2.myTbl2, mySchema2."myTbl3", mySchema2.myTbl4, mySchema2.myTbl5, mySchema2.myTbl6, mySchema2.myTbl7, mySchema2.myTbl8;
INSERT INTO myschema2.myTbl7 SELECT i FROM generate_series(0, 100, 1) i;
INSERT INTO myschema2.myTbl6 (col61) VALUES (0);
INSERT INTO myschema2.myTbl8 (col81) VALUES (0);
ALTER SEQUENCE mySchema2.mySeq1 restart 1000;
TRUNCATE mySchema4.myTblM, mySchema4.myTblC1, mySchema4.myTblC2;
TRUNCATE mySchema4.myTblP, mySchema4.myPartP1a, mySchema4.myPartP1b, mySchema4.myPartP2, mySchema4.myTblR1, mySchema4.myTblR2 CASCADE;
-- Analyze to get some statistics.
ANALYZE;

SET session_authorization TO _regress_emaj_adm2;

-----------------------------
-- Explicitely purge the histories.
-----------------------------
SELECT emaj.emaj_purge_histories('0 SECOND');
SELECT hist_function, hist_event, hist_wording
  FROM emaj.emaj_hist ORDER BY hist_id;

-----------------------------
-- Recreate and start groups.
-----------------------------
-- Set the parameter to drop the emaj_user_port column and add an 'extra_col_appname' column.
SELECT emaj.emaj_set_param('alter_log_table',
  'ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr(), ADD COLUMN extra_col_appname TEXT DEFAULT current_setting(''application_name'')');

SELECT emaj.emaj_create_group('myGroup1') WHERE NOT emaj.emaj_does_exist_group('myGroup1');
SELECT emaj.emaj_comment_group('myGroup1', 'This is group #1');
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"priority":20}'::JSONB)
  WHERE emaj.emaj_get_assigned_group_table('myschema1', 'mytbl1') is NULL;
SELECT emaj.emaj_assign_table('myschema1', 'mytbl2', 'myGroup1', '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl2b', 'myGroup1', '{"log_data_tablespace":"tsp log''2", "log_index_tablespace":"tsp log''2"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'myTbl3', 'myGroup1', '{"priority":10, "log_data_tablespace":"tsplog1"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl4', 'myGroup1', '{"priority":20, "log_data_tablespace":"tsplog1", "log_index_tablespace":"tsp log''2"}'::JSONB);
SELECT emaj.emaj_assign_sequences('myschema1', '.*', NULL, 'myGroup1');

SELECT emaj.emaj_create_group('myGroup2', TRUE, 'This is group #2');
SELECT emaj.emaj_assign_tables('myschema2', '.*', 'mytbl[7,8]', 'myGroup2');
SELECT emaj.emaj_assign_sequences('myschema2', '.*', 'myseq2', 'myGroup2');
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq1', 'myGroup2')
  WHERE emaj.emaj_get_assigned_group_sequence('myschema2', 'myseq1') is NULL;

SELECT emaj.emaj_create_group('emptyGroup');

-- Try to rename the last mark for a group that has no mark.
SELECT emaj.emaj_rename_mark_group('myGroup2', 'EMAJ_LAST_MARK', 'new_mark_name');

-- Force a purge of the history, the alter and the rollback tables.
SELECT emaj.emaj_set_param('history_retention', '0.1 second');
SELECT pg_sleep(0.2);
SELECT emaj.emaj_start_group('myGroup1', 'M1') WHERE NOT emaj.emaj_is_logging_group('myGroup1') AND NOT emaj.emaj_does_exist_mark_group('myGroup1', 'M1');
SELECT emaj.emaj_set_param('history_retention', NULL);

SELECT emaj.emaj_start_group('myGroup2', 'M1');

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12200);

-----------------------------
-- Step 1 : for myGroup1, update tables and set 2 marks.
-----------------------------
--
RESET session_authorization;
SET search_path TO public, myschema1;
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::bytea FROM generate_series (1, 11) AS i;
UPDATE myTbl1 SET col13=E'\\034'::bytea WHERE col11 <= 3;
INSERT INTO myTbl2 VALUES (1, 'ABC', '2010-12-31');
DELETE FROM myTbl1 WHERE col11 > 10;
INSERT INTO myTbl2 VALUES (2, 'DEF', NULL);
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M2');
--
RESET session_authorization;
SET search_path TO public, myschema1;
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
INSERT INTO myTbl4 VALUES (2, 'FK...', 1, 1, 'ABC');
UPDATE myTbl4 SET col43 = 2;
INSERT INTO myTbl4 VALUES (3, 'FK...', 1, 10, 'ABC');
-- The 2 next statements activate fkey on delete and on update clauses.
DELETE FROM myTbl1 WHERE col11 = 10;
UPDATE myTbl1 SET col12='DEF' WHERE col11 <= 2;
--
SET session_authorization TO _regress_emaj_adm2;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M3');
SELECT emaj.emaj_comment_mark_group('myGroup1', 'M3', 'Third mark set');

-----------------------------
-- Checking step 1.
-----------------------------
-- emaj tables.
SELECT group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  FROM emaj.emaj_group ORDER BY group_name;
SELECT * FROM emaj.emaj_group_hist ORDER BY grph_group, grph_time_range;
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12000 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12000 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema1.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema1.myTbl2 ORDER BY col21;
SELECT * FROM mySchema1.myTbl2b ORDER BY col20;
SELECT col31, col33 FROM mySchema1."myTbl3" ORDER BY col31;
SELECT * FROM mySchema1.myTbl4 ORDER BY col41;
-- Log tables.
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12300);

-----------------------------
-- Step 2 : for myGroup2, start, update tables and set 2 marks.
-----------------------------
SET search_path TO public, myschema2;
INSERT INTO myTbl1 SELECT i, 'ABC', E'\\014'::BYTEA FROM generate_series (1, 11) AS i;
UPDATE myTbl1 SET col13=E'\\034'::BYTEA WHERE col11 <= 3;
INSERT INTO myTbl2 VALUES (1, 'ABC', '2010-01-01');
DELETE FROM myTbl1 WHERE col11 > 10;
SELECT nextval('myschema2.myseq1');
INSERT INTO myTbl2 VALUES (2, 'DEF', NULL);
INSERT INTO "myTbl3" (col33) SELECT generate_series(1000, 1039, 4)/100;
INSERT INTO myTbl5 VALUES (1, '{"abc", "def", "ghi"}', '{1, 2, 3}', NULL, '{}', NULL, '{"id":1000}', '[2020-01-01, 2021-01-01)', NULL);
INSERT INTO myTbl5 VALUES (2, ARRAY['abc', 'def', 'ghi'], ARRAY[3, 4, 5], ARRAY['2000/02/01'::DATE, '2000/02/28'::DATE], '{"id":1001, "c1":"abc"}',
                           NULL, '{"id":1001}', NULL, XMLPARSE (CONTENT '<foo>bar</foo>'));
UPDATE myTbl5 SET col54 = '{"2010/11/28", "2010/12/03"}', col55 = '{"id":1001, "c2":"def"}', col57 = '{"id":1001, "c3":"ghi"}' WHERE col54 IS NULL;
INSERT INTO myTbl6
  SELECT i, point(i, 1.3), box(point(i, 0), point(i+0.2, 1)), circle(point(5, 5), i), '((-2, -2), (3, 0), (1, 4))',
         '10.20.30.40/27', 'EXECUTING', (i, point(i, 1.3))::mycomposite
    FROM generate_series (1, 8) AS i;
UPDATE myTbl6 SET col64 = '<(5, 6), 3.5>', col65 = NULL, col67 = 'COMPLETED' WHERE col61 between 1 AND 3;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup2', 'M2');
--
reset session_authorization;
SET search_path TO public, myschema2;
SELECT nextval('myschema2.myseq1');
SELECT nextval('myschema2.myseq1');
SELECT nextval('myschema2.myseq1');
--
ALTER SEQUENCE mySeq1 NO MAXVALUE NO CYCLE;
--
INSERT INTO myTbl4 VALUES (1, 'FK...', 1, 1, 'ABC');
INSERT INTO myTbl4 VALUES (2, 'FK...', 1, 1, 'ABC');
UPDATE myTbl4 SET col43 = 2;
DELETE FROM mytbl5 WHERE 4 = ANY(col53);
DELETE FROM myTbl6 WHERE col65 IS NULL AND col61 <> 0;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup2', 'M3');
-----------------------------
-- Checking step 2.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12300 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12300 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema2.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema2.myTbl2 ORDER BY col21;
SELECT col31, col33 FROM mySchema2."myTbl3" ORDER BY col31;
SELECT * FROM mySchema2.myTbl4 ORDER BY col41;
SELECT * FROM mySchema2.myTbl5 ORDER BY col51;
SELECT * FROM mySchema2.myTbl6 ORDER BY col61;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl5_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col61, col62, col63, col64, col65, col66, col67, col68, col69, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl6_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12400);

-----------------------------
-- Step 3 : for myGroup2, double logged rollback.
-----------------------------
RESET session_authorization;
ANALYZE mytbl4;
-- Rollback without dblink.

ALTER FUNCTION public.dblink_connect(TEXT, TEXT) RENAME TO renamed_dblink_connect;
ALTER FUNCTION public.dblink_connect(TEXT) RENAME TO renamed_dblink_connect;

SET session_authorization TO _regress_emaj_adm2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M2', FALSE) ORDER BY 1, 2;
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M3', FALSE) ORDER BY 1, 2;

reset session_authorization;
ALTER FUNCTION public.renamed_dblink_connect(TEXT, TEXT) RENAME TO dblink_connect;
ALTER FUNCTION public.renamed_dblink_connect(TEXT) RENAME TO dblink_connect;

SET session_authorization TO _regress_emaj_adm1;

-----------------------------
-- Checking step 3.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12400 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12400 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema2.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema2.myTbl2 ORDER BY col21;
SELECT col31, col33 FROM mySchema2."myTbl3" ORDER BY col31;
SELECT * FROM mySchema2.myTbl4 ORDER BY col41;
SELECT * FROM mySchema2.myTbl5 ORDER BY col51;
SELECT * FROM mySchema2.myTbl6 ORDER BY col61;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl5_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl6_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12500);

-----------------------------
-- Step 4 : for myGroup1, rollback then update tables then set 3 marks.
-----------------------------
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M2', FALSE) ORDER BY 1, 2;
--
RESET session_authorization;
SET search_path TO public, myschema1;
INSERT INTO myTbl1 SELECT i, 'DEF', E'\\000'::bytea FROM generate_series (100, 110) AS i;
INSERT INTO myTbl2 VALUES (3, 'GHI', '2010-01-02');
DELETE FROM myTbl1 WHERE col11 = 1;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M4');
--
RESET session_authorization;
UPDATE "myTbl3" SET col33 = col33 / 2;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M5');
--
RESET session_authorization;
UPDATE myTbl1 SET col11 = 99 WHERE col11 = 1;
--
set session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_set_mark_group('myGroup1', 'M6');

-----------------------------
-- Checking step 4.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12500 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12500 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema1.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema1.myTbl2 ORDER BY col21;
SELECT * FROM mySchema1.myTbl2b ORDER BY col20;
SELECT col31, col33 FROM mySchema1."myTbl3" ORDER BY col31;
SELECT * FROM mySchema1.myTbl4 ORDER BY col41;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12600);

-----------------------------
-- Step 5 : for myGroup2, logged rollback again then unlogged rollback.
-----------------------------
SELECT * FROM emaj.emaj_logged_rollback_group('myGroup2', 'M2', FALSE) ORDER BY 1, 2;
--
SELECT * FROM emaj.emaj_rollback_group('myGroup2', 'M3', FALSE) ORDER BY 1, 2;
-----------------------------
-- Checking step 5.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12600 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12600 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema2.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema2.myTbl2 ORDER BY col21;
SELECT col31, col33 FROM mySchema2."myTbl3" ORDER BY col31;
SELECT * FROM mySchema2.myTbl4 ORDER BY col41;
SELECT * FROM mySchema2.myTbl5 ORDER BY col51;
SELECT * FROM mySchema2.myTbl6 ORDER BY col61;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl4_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema2.mytbl5_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema2.myTbl6_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12700);

-----------------------------
-- Step 6 : for myGroup1, update tables, rollback, other updates, then logged rollback.
-----------------------------
RESET session_authorization;
SET search_path TO public, myschema1;
--
INSERT INTO myTbl1 VALUES (1, 'Step 6', E'\\000'::BYTEA);
INSERT INTO myTbl4 VALUES (11, 'FK...', 1, 1, 'Step 6');
INSERT INTO myTbl4 VALUES (12, 'FK...', 1, 1, 'Step 6');
--
SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj.emaj_rollback_group('myGroup1', 'M5', FALSE) ORDER BY 1, 2;
--
RESET session_authorization;
INSERT INTO myTbl1 VALUES (1, 'Step 6', E'\\001'::BYTEA);
INSERT INTO myTbl4 VALUES (11, '', 1, 1, 'Step 6');
INSERT INTO myTbl4 VALUES (12, '', 1, 1, 'Step 6');
--
-- For an equivalent of "select * from emaj.emaj_logged_rollback_group('myGroup1', 'M4', true, 'my comment');".
SET session_authorization TO _regress_emaj_adm1;
SELECT * FROM emaj._rlbk_async(emaj._rlbk_init(ARRAY['myGroup1'], 'M4', TRUE, 1, FALSE, TRUE, 'my comment'), FALSE);

SELECT emaj.emaj_comment_rollback(12701, 'Updated comment');

-- And check the rollback result.
SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_comment,
       rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id,
       rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_messages
 FROM emaj.emaj_rlbk ORDER BY rlbk_id DESC LIMIT 1;

-----------------------------
-- Checking step 6.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
-- Check that mark_log_stat_before_next column is always equal to either NULL or the emaj_log_stat_rows() function's result.
-- This should always return 0 row.
SELECT * FROM
  (SELECT mark_time_id, mark_group, mark_name, mark_log_rows_before_next -
    (SELECT sum(stat_rows) FROM emaj.emaj_log_stat_group(mark_group, mark_name,
      (SELECT mark_name FROM emaj.emaj_mark m2 WHERE m2.mark_group = m1.mark_group AND m2.mark_time_id > m1.mark_time_id ORDER BY mark_time_id LIMIT 1))
    ) AS checked_stat_rows FROM emaj.emaj_mark m1 WHERE mark_log_rows_before_next IS NOT NULL
  ) AS t
  WHERE checked_stat_rows <> 0;
--
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12700 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12700 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema1.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema1.myTbl2 ORDER BY col21;
SELECT * FROM mySchema1.myTbl2b ORDER BY col20;
SELECT col31, col33 FROM mySchema1."myTbl3" ORDER BY col31;
SELECT * FROM mySchema1.myTbl4 ORDER BY col41;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(12800);

-----------------------------
-- Step 7 : for myGroup1, update tables, rename a mark, then delete 2 marks then delete all before a mark.
-----------------------------
SET search_path TO public, myschema1;
--
RESET session_authorization;
DELETE FROM "myTbl3" WHERE col31 = 14;
DELETE FROM "myTbl3" WHERE col31 = 15;
DELETE FROM "myTbl3" WHERE col31 = 16;
DELETE FROM "myTbl3" WHERE col31 = 17;
DELETE FROM "myTbl3" WHERE col31 = 18;
--
SET session_authorization TO _regress_emaj_adm1;
SELECT emaj.emaj_rename_mark_group('myGroup1', mark_name, 'Before logged rollback to M4') FROM emaj.emaj_mark WHERE mark_comment LIKE '%to mark M4 start';
--
SELECT emaj.emaj_delete_mark_group('myGroup1', mark_name) FROM emaj.emaj_mark WHERE mark_comment LIKE '%to mark M4 end';
SELECT emaj.emaj_delete_mark_group('myGroup1', 'M1');
--
SELECT emaj.emaj_delete_before_mark_group('myGroup1', 'M4');
-----------------------------
-- Checking step 7.
-----------------------------
-- emaj tables.
SELECT mark_group, regexp_replace(mark_name, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), mark_time_id, mark_is_rlbk_protected,
       mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark FROM emaj.emaj_mark ORDER BY mark_time_id, mark_group;
SELECT sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called
  FROM emaj.emaj_sequence ORDER BY sequ_time_id, sequ_schema, sequ_name;
SELECT tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val
  FROM emaj.emaj_table ORDER BY tbl_time_id, tbl_schema, tbl_name;
SELECT sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size FROM emaj.emaj_seq_hole ORDER BY 1, 2, 3;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp WHERE time_id >= 12800 ORDER BY time_id;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)', '<timestamp>)', 'g'),
            E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist WHERE hist_id >= 12800 ORDER BY hist_id;

-- User tables.
RESET session_authorization;
SELECT * FROM mySchema1.myTbl1 ORDER BY col11, col12;
SELECT * FROM mySchema1.myTbl2 ORDER BY col21;
SELECT * FROM mySchema1.myTbl2b ORDER BY col20;
SELECT col31, col33 FROM mySchema1."myTbl3" ORDER BY col31;
SELECT * FROM mySchema1.myTbl4 ORDER BY col41;
-- Log tables.
SET session_authorization TO _regress_emaj_adm1;
SELECT col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl1_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col20, col21, col22, col23, col24, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl2b_log ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col31, col33, emaj_verb, emaj_tuple, emaj_gid FROM emaj_myschema1."myTbl3_log" ORDER BY emaj_gid, emaj_tuple DESC;
SELECT col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid FROM emaj_mySchema1.myTbl4_log ORDER BY emaj_gid, emaj_tuple DESC;

--
RESET session_authorization;
