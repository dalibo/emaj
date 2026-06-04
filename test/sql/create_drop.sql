-- Create_drop.sql : test emaj_create_group(), emaj_does_exist_group(), emaj_get_groups(), emaj_comment_group(),
-- emaj_assign_table(), emaj_assign_tables(), emaj_assign_sequence(), emaj_assign_sequences(),
-- emaj_get_assigned_group_table(), emaj_get_assigned_group_sequence(),
-- emaj_export_groups_configuration(), emaj_import_groups_configuration(),
-- emaj_drop_group() and emaj_force_drop_group() functions.
--

-- Do not display DETAIL and CONTEXT outputs when an error is raised (\errverbose can be used to debug a statement).
\set VERBOSITY terse

-- Define and create the temp file directory to be used by the script.
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/create_drop'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

-- Set sequence restart value.
SELECT public.handle_emaj_sequences(1000);

-----------------------------
-- emaj_create_group(), emaj_does_exist_group() and emaj_get_groups() tests.
-----------------------------

-- Invalid group names.
SELECT emaj.emaj_create_group(NULL);
SELECT emaj.emaj_create_group('', FALSE);

-- Should be OK.
SELECT emaj.emaj_create_group('emptyGroup');
SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_create_group('myGroup2', TRUE, 'comment set at group''s creation');
SELECT emaj.emaj_create_group('phil''s group#3",', FALSE);
SELECT emaj.emaj_create_group('myGroup4');
SELECT emaj.emaj_create_group('myGroup5', FALSE);
SELECT emaj.emaj_create_group('myGroup6');

-- Check group existence.
SELECT emaj.emaj_does_exist_group('emptyGroup');
SELECT emaj.emaj_does_exist_group('unknownGroup');

-- Already created.
SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_create_group('myGroup1') WHERE NOT emaj.emaj_does_exist_group('myGroup1');

-- Build groups array.
SELECT emaj.emaj_get_groups();
SELECT emaj.emaj_get_groups('Group');
SELECT emaj.emaj_get_groups(NULL, 'my');
SELECT emaj.emaj_get_groups('\d', 'my');

-----------------------------
-- emaj_comment_group() tests.
-----------------------------

-- Unknown group.
SELECT emaj.emaj_comment_group(NULL, NULL);
SELECT emaj.emaj_comment_group('unknownGroup', NULL);

-- Should be OK.
SELECT emaj.emaj_comment_group('myGroup1', 'a first comment for group #1');
SELECT emaj.emaj_comment_group('myGroup1', 'a better comment for group #1');
SELECT emaj.emaj_comment_group('emptyGroup', 'an empty group');

SELECT group_name, group_comment FROM emaj.emaj_group WHERE group_name = 'myGroup1';
SELECT emaj.emaj_comment_group('myGroup1', NULL);
SELECT group_name, group_comment FROM emaj.emaj_group WHERE group_name = 'myGroup1';

-----------------------------------
-- emaj_assign_table.
-----------------------------------

-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'dummyGroup');
-- Bad schema.
SELECT emaj.emaj_assign_table('dummySchema', 'mytbl1', 'myGroup1');
SELECT emaj.emaj_assign_table('emaj', 'mytbl1', 'myGroup1');
-- Bad table.
SELECT emaj.emaj_assign_table('myschema1', 'dummyTable', 'myGroup1');
-- Partitionned table.
SELECT emaj.emaj_assign_table('myschema4', 'mytblp', 'myGroup1');
-- Temp table.
DO LANGUAGE plpgsql
$$
BEGIN
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  BEGIN
    SELECT emaj.emaj_assign_table(nspname, 'mytemptbl', 'myGroup1') FROM pg_class, pg_namespace
      WHERE relnamespace = pg_namespace.oid AND relname = 'mytemptbl';
    RETURN;
  EXCEPTION WHEN raise_exception THEN
    RAISE EXCEPTION 'Error trapped on emaj_assign_table() call';
  END;
END;
$$;

-- UNLOGGED table on a rollbackable group.
SELECT emaj.emaj_assign_table('myschema5', 'myunloggedtbl', 'myGroup1');

-- Table without PKEY on a rollbackable group.
SELECT emaj.emaj_assign_table('phil''s schema"3', 'myTbl2\', 'myGroup1');

-- Invalid priority.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"priority":"not_numeric"}'::JSONB);

-- Invalid tablespace.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"log_data_tablespace":1}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"log_data_tablespace":"dummytsp"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"log_index_tablespace":1}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"log_index_tablespace":"dummytsp"}'::JSONB);

-- Invalid ignored_triggers.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"ignored_triggers":1}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"ignored_triggers":"emaj_log_trg"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"ignored_triggers":["emaj_trunc_trg"]}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"ignored_triggers":["dummy"]}'::JSONB);

-- Invalid ignored_triggers_profiles.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"ignored_triggers_profiles":1}'::JSONB);

-- Unknown property.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', '{"unknown_property":null}'::JSONB);

-- Bad mark.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1', NULL, 'EMAJ_LAST_MARK');

-- Erroneously existing log schema.
BEGIN;
  CREATE schema emaj_myschema1;
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');
ROLLBACK;

-- Bad alter_log_table parameter.
BEGIN;
  SELECT emaj.emaj_set_param('alter_log_table', 'dummmy content');
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');
ROLLBACK;

-- Attempting to assign emaj objects: referencing an emaj schema.
SELECT emaj.emaj_assign_table('emaj', 'emaj_param', 'myGroup1');
SELECT emaj.emaj_assign_table('emaj_myschema1', 'mytbl1_log', 'myGroup1');

-- Ok.
--   Various way to specify the triggers to ignore at rollback time.
BEGIN;
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl2', 'myGroup1', '{"ignored_triggers":"mytbl2trg1"}'::JSONB);
  SELECT rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl2';
ROLLBACK;
BEGIN;
  SELECT emaj.emaj_assign_table('myschema1', 'mytbl2', 'myGroup1', '{"ignored_triggers_profiles":["mytbl2trg\\d"]}'::JSONB);
  SELECT rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl2';
ROLLBACK;
BEGIN;
  SELECT emaj.emaj_assign_tables('myschema1', '.*', NULL, 'myGroup1', '{"ignored_triggers_profiles":["trg1$", "trg2$"]}'::JSONB);
  SELECT rel_tblseq, rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_schema = 'myschema1';
ROLLBACK;
BEGIN;
  SELECT emaj.emaj_assign_tables('myschema1', '{"mytbl2"}', 'myGroup1',
                                 '{"ignored_triggers":"mytbl2trg1", "ignored_triggers_profiles":["trg2$"]}'::JSONB);
  SELECT rel_ignored_triggers FROM emaj.emaj_relation WHERE rel_schema = 'myschema1' AND rel_tblseq = 'mytbl2';
ROLLBACK;

SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1',
                              '{"priority":20, "ignored_triggers":null}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl2', 'myGroup1',
                              '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1",
                                "ignored_triggers":["mytbl2trg1", "mytbl2trg2"]}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl2b', 'myGroup1',
                              '{"log_data_tablespace":"tsp log''2", "log_index_tablespace":"tsp log''2"}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'myTbl3', 'myGroup1',
                              '{"priority":10, "log_data_tablespace":"tsplog1", "log_index_tablespace":null}'::JSONB);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl4', 'myGroup1',
                              '{"priority":20, "log_data_tablespace":"tsplog1", "log_index_tablespace":"tsp log''2"}'::JSONB);

-- Already assigned table in the same group.
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');

-----------------------------------
-- emaj_assign_tables with array.
-----------------------------------
-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_tables('dummySchema', ARRAY['dummyTable'], 'dummyGroup');
-- Bad tables.
SELECT emaj.emaj_assign_tables('myschema1', ARRAY['dummytbl1', 'dummytbl2'], 'phil''s group#3",');
-- Empty tables array.
SELECT emaj.emaj_assign_tables('myschema1', ARRAY[]::TEXT[], 'phil''s group#3",');
SELECT emaj.emaj_assign_tables('myschema1', NULL, 'phil''s group#3",');
SELECT emaj.emaj_assign_tables('myschema1', ARRAY[''], 'phil''s group#3",');

-- Ok (with a duplicate table name).
SELECT emaj.emaj_assign_tables('phil''s schema"3', ARRAY['phil''s tbl1', E'myTbl2\\', 'phil''s tbl1'], 'phil''s group#3",');

-----------------------------------
-- emaj_assign_tables with filters.
-----------------------------------
-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_tables('dummySchema', 'dummyIncludeFilter', 'dummyExcludeFilter', 'dummyGroup');
-- Bad schema.
SELECT emaj.emaj_assign_tables('dummySchema', 'dummyIncludeFilter', 'dummyExcludeFilter', 'myGroup2');
SELECT emaj.emaj_assign_tables('emaj', 'dummyIncludeFilter', 'dummyExcludeFilter', 'myGroup2');
-- Empty tables array.
SELECT emaj.emaj_assign_tables('myschema2', NULL, NULL, 'myGroup2');
SELECT emaj.emaj_assign_tables('myschema2', '', '', 'myGroup2');
SELECT emaj.emaj_assign_tables('myschema2', 'mytbl1', 'mytbl1', 'myGroup2');

-- Excluded tables.
-- Bad types.
SELECT emaj.emaj_assign_tables('myschema4', 'mytblp$', '', 'myGroup2');
SELECT emaj.emaj_assign_tables('myschema5', 'unlogged', '', 'myGroup2');
-- Temp table.
BEGIN;
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  SELECT emaj.emaj_assign_tables(nspname, 'mytemptbl', '', 'myGroup2') FROM pg_class, pg_namespace
    WHERE relnamespace = pg_namespace.oid AND relname = 'mytemptbl';
ROLLBACK;
-- Table without pkey for a rollbackable group.
SELECT emaj.emaj_assign_tables('phil''s schema"3', 'myTbl2\\', '', 'myGroup2');

-- OK.
SELECT emaj.emaj_assign_tables('myschema2', 'mytbl.*', 'mytbl(5|6)$', 'myGroup2');
SELECT emaj.emaj_assign_tables('myschema2', 'myTbl.*', '', 'myGroup2');
SELECT emaj.emaj_assign_tables('myschema2', 'mytbl(5|6)$', '', 'myGroup2');

-- Already assigned.
SELECT emaj.emaj_assign_tables('myschema2', 'mytbl(5|6)$', '', 'myGroup2');

-- Assign partitions.
SELECT emaj.emaj_assign_tables('myschema4', '.*', 'mytbl(p|r.)$', 'myGroup4');

-- Assign the unlogged table in an audit_only group.
SELECT emaj.emaj_assign_tables('myschema5', '.*', NULL, 'myGroup5');

-- Assign tables with very long names.
SELECT emaj.emaj_assign_tables('myschema6', 'table_with_\d\d_characters_long_name.*', NULL, 'myGroup6');

-----------------------
-- emaj_assign_sequence.
-----------------------

-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq1', 'dummyGroup');
-- Bad schema.
SELECT emaj.emaj_assign_sequence('dummySchema', 'myseq1', 'myGroup2');
SELECT emaj.emaj_assign_sequence('emaj', 'myseq1', 'myGroup2');
-- Bad sequence.
SELECT emaj.emaj_assign_sequence('myschema2', 'dummySequence', 'myGroup2');

-- Bad mark.
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq1', 'myGroup2', 'EMAJ_LAST_MARK');

-- Ok.
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq1', 'myGroup2');
-- Already assigned sequence.
SELECT emaj.emaj_assign_sequence('myschema2', 'myseq1', 'myGroup2');

-----------------------------------
-- emaj_assign_sequences with array.
-----------------------------------
-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_sequences('dummySchema', ARRAY['dummySequence'], 'dummyGroup');
-- Bad sequences.
SELECT emaj.emaj_assign_sequences('myschema2', ARRAY['dummyseq1', 'dummyseq2'], 'myGroup2');
-- Empty sequences array.
SELECT emaj.emaj_assign_sequences('myschema2', ARRAY[]::TEXT[], 'myGroup2');
SELECT emaj.emaj_assign_sequences('myschema2', NULL, 'myGroup2');
SELECT emaj.emaj_assign_sequences('myschema2', ARRAY[''], 'myGroup2');

-- Ok (with a duplicate sequence name).
SELECT emaj.emaj_assign_sequences('myschema2', ARRAY['myTbl3_col31_seq', 'myTbl3_col31_seq'], 'myGroup2');

-- Assign sequences with double_quoted names.
SELECT emaj.emaj_assign_sequences('phil''s schema"3', ARRAY[E'myTbl2\\_col21_seq', E'phil''s"seq\\1'], 'phil''s group#3",');

-----------------------------------
-- emaj_assign_sequences with filters.
-----------------------------------
-- Error cases.
-- Bad group name.
SELECT emaj.emaj_assign_sequences('dummySchema', 'dummyIncludeFilter', 'dummyExcludeFilter', 'dummyGroup');
-- Bad schema.
SELECT emaj.emaj_assign_sequences('dummySchema', 'dummyIncludeFilter', 'dummyExcludeFilter', 'myGroup1');
SELECT emaj.emaj_assign_sequences('emaj', 'dummyIncludeFilter', 'dummyExcludeFilter', 'myGroup1');
-- Empty sequences array.
SELECT emaj.emaj_assign_sequences('myschema1', NULL, NULL, 'myGroup1');
SELECT emaj.emaj_assign_sequences('myschema1', '', '', 'myGroup1');
SELECT emaj.emaj_assign_sequences('myschema1', 'myTbl3_col31_seq', 'myTbl3_col31_seq', 'myGroup1');

-- Already assigned.
SELECT emaj.emaj_assign_sequences('myschema2', 'myseq1.*', '', 'myGroup2');

-- OK.
SELECT emaj.emaj_assign_sequences('myschema1', 'my.*_seq', 'myseq1$', 'myGroup1');

-----------------------------------
-- emaj_get_assigned_group_table() and emaj_get_assigned_group_sequence().
-----------------------------------

-- Error cases.
-- Bad schema.
SELECT emaj.emaj_get_assigned_group_table('dummySchema', 'mytbl1');
SELECT emaj.emaj_get_assigned_group_sequence('dummySchema', 'myseq1');
-- Bad table and sequence.
SELECT emaj.emaj_get_assigned_group_table('myschema2', 'dummyTable');
SELECT emaj.emaj_get_assigned_group_sequence('myschema2', 'dummySequence');

-- OK.
-- Assigned table and sequence.
SELECT emaj.emaj_get_assigned_group_table('myschema2', 'mytbl1');
SELECT emaj.emaj_get_assigned_group_sequence('myschema2', 'myseq1');

-- Not yet assigned table and sequence.
BEGIN;
  CREATE table myschema1.another_table (c1 TEXT);
  CREATE sequence myschema1.another_sequence;
  SELECT emaj.emaj_get_assigned_group_table('myschema1', 'another_table');
  SELECT emaj.emaj_get_assigned_group_sequence('myschema1', 'another_sequence');
ROLLBACK;

-----------------------------------
-- Check populated groups.
-----------------------------------
SELECT group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging,
       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  FROM emaj.emaj_group ORDER BY group_name;
SELECT * FROM emaj.emaj_group_hist ORDER BY grph_group, grph_time_range;
SELECT * FROM emaj.emaj_relation ORDER BY rel_group, rel_priority, rel_schema, rel_tblseq, rel_time_range;
SELECT schemaname, tablename, tableowner, tablespace FROM pg_tables WHERE schemaname LIKE 'emaj\_%' ORDER BY schemaname, tablename;
SELECT nspname, relname, rolname FROM pg_class, pg_namespace, pg_authid
  WHERE relnamespace = pg_namespace.oid AND relowner = pg_authid.oid AND relkind = 'S' AND nspname LIKE 'emaj\_%'
  ORDER BY nspname, relname;
SELECT nspname, proname, rolname FROM pg_proc, pg_namespace, pg_authid
  WHERE pronamespace = pg_namespace.oid AND proowner = pg_authid.oid AND nspname LIKE 'emaj\_%'
  ORDER BY nspname, proname;

-----------------------------
-- emaj_export_groups_configuration() and emaj_import_groups_configuration() tests.
-----------------------------
--
-- Direct export.
--
--   Bad selected groups array.
SELECT emaj.emaj_export_groups_configuration(ARRAY['myGroup1', 'unknown1', 'unknown2']);

-- Ok.
SELECT json_array_length(emaj.emaj_export_groups_configuration()->'tables_groups');
SELECT json_array_length(emaj.emaj_export_groups_configuration(ARRAY['myGroup1', 'myGroup2'])->'tables_groups');

--
-- Export to a file.
--
--   Error.
SELECT emaj.emaj_export_groups_configuration('/tmp/dummy/location/file');

--   Ok.
SELECT emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json');
SELECT emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_partial.json', ARRAY['myGroup1', 'myGroup2']);
\! wc -l $EMAJTESTTMPDIR/*.json
\! grep -v ', at ' $EMAJTESTTMPDIR/orig_groups_config_all.json

--
-- Direct import.
--
--   Bad content.
SELECT emaj.emaj_import_groups_configuration('{ "dummy_json": null }'::JSON);
--   Missing "group" attribute.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "name": "grp1" } ]}'::JSON);
--   Unknown group level attributes.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "unknown_attr1": null, "unknown_attr2": null } ]}'::JSON);
--   Is_rollbackable not boolean.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "is_rollbackable": "absolutely true"} ]}'::JSON);
--   Missing "schema" attribute in tables array.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { }] } ]}'::JSON);
--   Missing "table" attribute in tables array.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1" }] } ]}'::JSON);
--   Unknown table level attributes.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "unknown_attr": null }] } ]}'::JSON);
--   Priority not numeric.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "a_schema", "table": "a_table", "priority": "high" }] } ]}'::JSON);
--   Ignored_triggers not an array.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "ignored_triggers": "dummy" }] } ]}'::JSON);
--     Not strings.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "ignored_triggers": [ "trg1", 1234, null] }] } ]}'::JSON);
--   Missing "schema" attribute in sequences array.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { }] } ]}'::JSON);
--   Missing "sequence" attribute in sequences array.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1" }] } ]}'::JSON);
--   Unknown sequence level attributes.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1", "sequence": "s1",  "unknown_attr": null }] } ]}'::JSON);
--   Duplicate group in json.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1" }, { "group": "grp1" } ]}'::JSON);

--   Unknown group in array.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', ARRAY['myGroup1', 'myGroup2', 'unknownGroup']);
--   Group already created.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1" }, { "group": "myGroup2" } ] }'::JSON, NULL, FALSE);
--   Bad type for existing groups.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "is_rollbackable": false }, { "group": "myGroup2", "is_rollbackable": false } ] }'::JSON, NULL, TRUE);

--   Table / sequence referenced for several different groups.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "tables": [ { "schema": "public", "table": "t1" } ] },
                                                                   { "group": "myGroup2", "tables": [ { "schema": "public", "table": "t1" } ] } ]}'::JSON,
                                             '{"myGroup1", "myGroup2"}', TRUE);
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "sequences": [ { "schema": "public", "sequence": "s1" } ] },
                                                                   { "group": "myGroup2", "sequences": [ { "schema": "public", "sequence": "s1" } ] } ]}'::JSON,
                                             '{"myGroup1", "myGroup2"}', TRUE);

--   Unknown table, bad data and index log tablespaces and missing ignored triggers (that shouldn't generate a warning).
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "log_data_tablespace": "dummytsp", "log_index_tablespace": "dummytsp", "ignored_triggers": ["emaj_trunc_trg", "dummy"]}] } ]}'::JSON);

--   Unknown sequence.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1", "sequence": "sq1" }] } ]}'::JSON);

--   Table or sequence referenced twice in the group.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "tables": [ { "schema": "myschema1", "table": "mytbl1" },
                                                                                                      { "schema": "myschema1", "table": "mytbl1" }] } ]}'::JSON,
                                             NULL, TRUE);
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "sequences": [ { "schema": "myschema2", "sequence": "myseq1" },
                                                                                                         { "schema": "myschema2", "sequence": "myseq1" }] } ]}'::JSON,
                                             NULL, TRUE);

--   Partitionned table.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "myschema4", "table": "mytblp" }] } ]}'::JSON);

--   emaj schema.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "emaj", "table": "emaj_hist" }] } ]}'::JSON);
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "emaj", "sequence": "emaj_global_seq" }] } ]}'::JSON);

--   Temporary table.
--     This test is commented as the warning message is not stable (the schema name is varying).
--CREATE TEMPORARY TABLE myTempTbl (col1 INT NOT NULL, PRIMARY KEY (col1));
--SELECT emaj.emaj_import_groups_configuration(('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "' || nspname || '", "table": "mytemptbl" }] } ]}')::JSON).
--  FROM pg_class, pg_namespace WHERE relnamespace = pg_namespace.oid AND relname = 'mytemptbl';
--DROP TABLE myTempTbl;

--   Bad data or index log tablespace and missing ignored triggers.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "myschema1", "table": "mytbl1", "log_data_tablespace":"dummydatatsp", "log_index_tablespace":"dummyidxtsp", "ignored_triggers": ["emaj_trunc_trg", "dummy"] }] } ]}'::JSON);

--   Unlogged table in a rollbackable group.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "tables": [ { "schema": "myschema5", "table": "myunloggedtbl" }] } ]}'::JSON, NULL, TRUE);

--   Missing PK on a table in a rollbackable group.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "tables": [ { "schema": "phil''s schema\"3", "table": "myTbl2\\" }] } ]}'::JSON, NULL, TRUE);

--   Ok.
-- A new group with a comment, then changed and finaly deleted.
SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [
   { "group": "new_grp", "comment": "a nice comment for new_grp" }
  ]}'::JSON);
SELECT group_name, group_is_rollbackable, group_is_logging, group_comment
  FROM emaj.emaj_group WHERE group_name = 'new_grp';

SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [
   { "group": "new_grp", "comment": "changed comment for new_grp" }
  ]}'::JSON, NULL, TRUE);
SELECT group_comment FROM emaj.emaj_group WHERE group_name = 'new_grp';

SELECT emaj.emaj_import_groups_configuration('{ "tables_groups": [
   { "group": "new_grp" }
  ]}'::JSON, NULL, TRUE);
SELECT coalesce(group_comment, 'NULL') FROM emaj.emaj_group WHERE group_name = 'new_grp';

SELECT emaj.emaj_drop_group('new_grp');

--
-- Import from a file.
--
--   Error.
SELECT emaj.emaj_import_groups_configuration('/tmp/dummy/location/file');
\! echo 'not a json content' >/tmp/bad_groups_config.json
SELECT emaj.emaj_import_groups_configuration('/tmp/bad_groups_config.json');
\! rm /tmp/bad_groups_config.json

--   Ok.
-- Only 2 from the original groups.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', ARRAY['emptyGroup', 'myGroup5'], TRUE);
-- Change the attributes for a table.
\! sed -e 's/"mypartp1a"/"mypartp1a", "priority": 20, "log_data_tablespace": "tsplog1", "log_index_tablespace": "tsplog1"/' $EMAJTESTTMPDIR/orig_groups_config_all.json >$EMAJTESTTMPDIR/modified_groups_config_1.json
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_1.json', ARRAY['myGroup4'], TRUE);

-- Move a table and a sequence to another group.
-- The table myschema2.mytbl5 and the sequence myschema2.myseq1 are moved from myGroup2 to myGroup4.
\! sed -n -e '1,76p' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Remove the table and the sequence from myGroup2.
\! sed -n -e '81,97p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
\! sed -n -e '102,108p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Copy the moved table.
\! sed -n -e '77,80p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Copy the other tables.
\! sed -n -e '109,132p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Copy the sequences keyword.
\! sed -n -e '93,94p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Copy the moved sequence.
\! sed -n -e '99,102p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     Copy the remaining json structure.
\! sed -n -e '133,$p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_2.json', ARRAY['myGroup2', 'myGroup4'], TRUE);

-- Remove a table and a sequence from a group.
-- The table myschema2.mytbl5 and the sequence myschema2.myseq1 are removed from myGroup4.
\! sed -n -e '1,100p' $EMAJTESTTMPDIR/modified_groups_config_2.json >$EMAJTESTTMPDIR/modified_groups_config_3.json
\! sed -n -e '105,128p' $EMAJTESTTMPDIR/modified_groups_config_2.json >>$EMAJTESTTMPDIR/modified_groups_config_3.json
\! sed -n -e '135,$p' $EMAJTESTTMPDIR/modified_groups_config_2.json >>$EMAJTESTTMPDIR/modified_groups_config_3.json
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_3.json', ARRAY['myGroup4'], TRUE);

-- Register an unknown trigger and an emaj trigger.
\! sed -e 's/"mytbl2trg1","mytbl2trg2"/"unknowntrigger","emaj_trunc_trg"/' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_2.json
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_2.json', NULL, TRUE);

-- Suppress 1 trigger.
\! sed -e 's/"mytbl2trg1",//' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_3.json
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_3.json', NULL, TRUE);
SELECT rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers FROM emaj.emaj_relation
  WHERE rel_ignored_triggers IS NOT NULL ORDER BY 1, 2, 3;

-- Erroneously existing log schema.
SELECT emaj.emaj_drop_group('myGroup1');
BEGIN;
  CREATE SCHEMA emaj_myschema1;
  SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', NULL, TRUE);
ROLLBACK;

-- Import a groups subset and drop the others, one being in logging state.
SELECT emaj.emaj_start_group('myGroup5', 'Start_before_import');
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', ARRAY['myGroup1', 'myGroup2', 'myGroup4'], TRUE, 'IMPORT_%', TRUE);

-- Rebuild all original groups.
-- This will recreate missing groups and assign the removed table and sequence.
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', NULL, TRUE);

-- Test a table repair with the groups configuration import, with a table having triggers to ignored_triggers.
SELECT emaj.emaj_disable_protection_by_event_triggers();
DROP TRIGGER emaj_log_trg ON myschema1.mytbl2;
SELECT emaj.emaj_enable_protection_by_event_triggers();
SELECT emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', NULL, TRUE);
SELECT rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers FROM emaj.emaj_relation
  WHERE rel_ignored_triggers is NOT NULL ORDER BY 1, 2, 3;

-- Keep the current tables groups definition as reference for further tests, once all ignored_triggers configurations reset.
SELECT emaj.emaj_modify_table('myschema1', 'mytbl2', '{"ignored_triggers":null}'::JSONB);
SELECT emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json');

-----------------------------
-- emaj_drop_group() tests.
-----------------------------

-- Unknown group.
SELECT emaj.emaj_drop_group(NULL);
SELECT emaj.emaj_drop_group('unknownGroup');
-- Group in logging state.
SELECT emaj.emaj_start_group('myGroup1', '');
SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_stop_group('myGroup1');
-- Log schema with an object blocking the schema drop.
BEGIN;
  CREATE TABLE emaj_myschema2.dummy_log (col1 int);
  SELECT emaj.emaj_drop_group('myGroup2');
ROLLBACK;
-- Should be OK.
SELECT emaj.emaj_drop_group('myGroup1');
SELECT emaj.emaj_drop_group('myGroup2');
SELECT emaj.emaj_drop_group('emptyGroup');
SELECT emaj.emaj_drop_group('myGroup5');
SELECT emaj.emaj_drop_group('myGroup6');

-- Already dropped.
SELECT emaj.emaj_drop_group('myGroup2');

-----------------------------
-- emaj_force_drop_group() tests.
-----------------------------

-- Unknown group.
SELECT emaj.emaj_force_drop_group(NULL);
SELECT emaj.emaj_force_drop_group('unknownGroup');
-- Already dropped.
SELECT emaj.emaj_force_drop_group('myGroup2');
-- Should be OK.
SELECT emaj.emaj_create_group('myGroup1', FALSE);
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');
SELECT emaj.emaj_start_group('myGroup1', '');
SELECT emaj.emaj_force_drop_group('myGroup1');

SELECT emaj.emaj_create_group('myGroup1');
SELECT emaj.emaj_assign_table('myschema1', 'mytbl1', 'myGroup1');
SELECT emaj.emaj_start_group('myGroup1', '');
SELECT emaj.emaj_force_drop_group('myGroup1');

-----------------------------
-- emaj_forget_group() tests.
-----------------------------
-- Not dropped group.
SELECT emaj.emaj_forget_group('myGroup4');
-- Unknown group.
SELECT emaj.emaj_forget_group('unknownGroup');
-- Should be OK.
SELECT emaj.emaj_forget_group('myGroup1');

-----------------------------
-- Test end: global check.
-----------------------------
SELECT nspname FROM pg_namespace WHERE nspname LIKE 'emaj%' ORDER BY nspname;
SELECT * FROM emaj.emaj_schema ORDER BY 1;
SELECT * FROM emaj.emaj_rel_hist ORDER BY 1, 2, 3;
SELECT * FROM emaj.emaj_relation_change ORDER BY 1, 2, 3, 4;
SELECT hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording, E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d', '%', 'g'), E'\\[.+\\]', '(timestamp)', 'g'),
       hist_user
  FROM emaj.emaj_hist ORDER BY hist_id;
SELECT time_id, time_last_emaj_gid, time_event FROM emaj.emaj_time_stamp ORDER BY time_id;

-- Remove the temp directory.
\! rm -R $EMAJTESTTMPDIR
