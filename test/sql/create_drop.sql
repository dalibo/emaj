-- create_drop.sql : test emaj_create_group(), emaj_comment_group(),
-- emaj_assign_table(), emaj_assign_tables(), emaj_assign_sequence(), emaj_assign_sequences(),
-- emaj_export_groups_configuration(), emaj_import_groups_configuration(),
-- emaj_drop_group() and emaj_force_drop_group() functions
--

-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/create_drop'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

-- set sequence restart value
select public.handle_emaj_sequences(1000);

-----------------------------
-- emaj_create_group() tests
-----------------------------

-- invalid group names
select emaj.emaj_create_group(NULL);
select emaj.emaj_create_group('',false);

-- should be OK
select emaj.emaj_create_group('emptyGroup');
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup2', true, 'comment set at group''s creation');
select emaj.emaj_create_group('phil''s group#3",',false);
select emaj.emaj_create_group('myGroup4');
select emaj.emaj_create_group('myGroup5',false);
select emaj.emaj_create_group('myGroup6');

-- already created
select emaj.emaj_create_group('myGroup1');

-----------------------------
-- emaj_comment_group() tests
-----------------------------

-- unknown group
select emaj.emaj_comment_group(NULL,NULL);
select emaj.emaj_comment_group('unknownGroup',NULL);

-- should be OK
select emaj.emaj_comment_group('myGroup1','a first comment for group #1');
select emaj.emaj_comment_group('myGroup1','a better comment for group #1');
select emaj.emaj_comment_group('emptyGroup','an empty group');

select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_comment_group('myGroup1',NULL);
select group_name, group_comment from emaj.emaj_group where group_name = 'myGroup1';

-----------------------------------
-- emaj_assign_table
-----------------------------------

-- error cases
-- bad group name
select emaj.emaj_assign_table('myschema1','mytbl1','dummyGroup');
-- bad schema
select emaj.emaj_assign_table('dummySchema','mytbl1','myGroup1');
select emaj.emaj_assign_table('emaj','mytbl1','myGroup1');
-- bad table
select emaj.emaj_assign_table('myschema1','dummyTable','myGroup1');
-- partitionned table
select emaj.emaj_assign_table('myschema4','mytblp','myGroup1');
-- temp table
DO LANGUAGE plpgsql
$$
begin
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  begin
    select emaj.emaj_assign_table(nspname,'mytemptbl','myGroup1') from pg_class, pg_namespace
      where relnamespace = pg_namespace.oid and relname = 'mytemptbl';
    return;
  exception when raise_exception then
    raise exception 'Error trapped on emaj_assign_table() call';
  end;
end;
$$;

-- UNLOGGED table on a rollbackable group
select emaj.emaj_assign_table('myschema5','myunloggedtbl','myGroup1');

-- table without PKEY on a rollbackable group
select emaj.emaj_assign_table('phil''s schema"3','myTbl2\','myGroup1');

-- invalid priority
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"priority":"not_numeric"}'::jsonb);

-- invalid tablespace
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"log_data_tablespace":1}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"log_data_tablespace":"dummytsp"}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"log_index_tablespace":1}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"log_index_tablespace":"dummytsp"}'::jsonb);

-- invalid ignored_triggers
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"ignored_triggers":1}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"ignored_triggers":"emaj_log_trg"}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"ignored_triggers":["emaj_trunc_trg"]}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"ignored_triggers":["dummy"]}'::jsonb);

-- invalid ignored_triggers_profiles
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"ignored_triggers_profiles":1}'::jsonb);

-- unknown property
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"unknown_property":null}'::jsonb);

-- bad mark
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1',null,'EMAJ_LAST_MARK');

-- erroneously existing log schema
begin;
  create schema emaj_myschema1;
  select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');
rollback;

-- bad alter_log_table parameter
begin;
  insert into emaj.emaj_param (param_key, param_value_text) values ('alter_log_table','dummmy content');
  select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');
rollback;

-- attempting to assign emaj objects: referencing an emaj schema
select emaj.emaj_assign_table('emaj','emaj_param','myGroup1');
select emaj.emaj_assign_table('emaj_myschema1','mytbl1_log','myGroup1');

-- ok
--   various way to specify the triggers to ignore at rollback time
begin;
  select emaj.emaj_assign_table('myschema1','mytbl2','myGroup1','{"ignored_triggers":"mytbl2trg1"}'::jsonb);
  select rel_ignored_triggers from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2';
rollback;
begin;
  select emaj.emaj_assign_table('myschema1','mytbl2','myGroup1','{"ignored_triggers_profiles":["mytbl2trg\\d"]}'::jsonb);
  select rel_ignored_triggers from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2';
rollback;
begin;
  select emaj.emaj_assign_tables('myschema1','.*', null, 'myGroup1','{"ignored_triggers_profiles":["trg1$", "trg2$"]}'::jsonb);
  select rel_tblseq, rel_ignored_triggers from emaj.emaj_relation where rel_schema = 'myschema1';
rollback;
begin;
  select emaj.emaj_assign_tables('myschema1','{"mytbl2"}','myGroup1','{"ignored_triggers":"mytbl2trg1", "ignored_triggers_profiles":["trg2$"]}'::jsonb);
  select rel_ignored_triggers from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'mytbl2';
rollback;

select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"priority":20, "ignored_triggers":null}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl2','myGroup1','{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1", "ignored_triggers":["mytbl2trg1", "mytbl2trg2"]}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl2b','myGroup1','{"log_data_tablespace":"tsp log''2", "log_index_tablespace":"tsp log''2"}'::jsonb);
select emaj.emaj_assign_table('myschema1','myTbl3','myGroup1','{"priority":10, "log_data_tablespace":"tsplog1", "log_index_tablespace":null}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl4','myGroup1','{"priority":20, "log_data_tablespace":"tsplog1", "log_index_tablespace":"tsp log''2"}'::jsonb);

-- already assigned table in the same group
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');

-----------------------------------
-- emaj_assign_tables with array
-----------------------------------
-- error cases
-- bad group name
select emaj.emaj_assign_tables('dummySchema',array['dummyTable'],'dummyGroup');
-- bad tables
select emaj.emaj_assign_tables('myschema1',array['dummytbl1','dummytbl2'],'phil''s group#3",');
-- empty tables array
select emaj.emaj_assign_tables('myschema1',array[]::text[],'phil''s group#3",');
select emaj.emaj_assign_tables('myschema1',null,'phil''s group#3",');
select emaj.emaj_assign_tables('myschema1',array[''],'phil''s group#3",');

-- ok (with a duplicate table name)
select emaj.emaj_assign_tables('phil''s schema"3',array['phil''s tbl1',E'myTbl2\\','phil''s tbl1'],'phil''s group#3",');

-----------------------------------
-- emaj_assign_tables with filters
-----------------------------------
-- error cases
-- bad group name
select emaj.emaj_assign_tables('dummySchema','dummyIncludeFilter','dummyExcludeFilter','dummyGroup');
-- bad schema
select emaj.emaj_assign_tables('dummySchema','dummyIncludeFilter','dummyExcludeFilter','myGroup2');
select emaj.emaj_assign_tables('emaj','dummyIncludeFilter','dummyExcludeFilter','myGroup2');
-- empty tables array
select emaj.emaj_assign_tables('myschema2',null,null,'myGroup2');
select emaj.emaj_assign_tables('myschema2','','','myGroup2');
select emaj.emaj_assign_tables('myschema2','mytbl1','mytbl1','myGroup2');

-- excluded tables
-- bad types
select emaj.emaj_assign_tables('myschema4','mytblp$','','myGroup2');
select emaj.emaj_assign_tables('myschema5','unlogged','','myGroup2');
-- temp table
begin;
  CREATE TEMPORARY TABLE myTempTbl (
    col1       INT     NOT NULL,
    PRIMARY KEY (col1)
  );
  select emaj.emaj_assign_tables(nspname,'mytemptbl','','myGroup2') from pg_class, pg_namespace
    where relnamespace = pg_namespace.oid and relname = 'mytemptbl';
rollback;
-- table without pkey for a rollbackable group
select emaj.emaj_assign_tables('phil''s schema"3','myTbl2\\','','myGroup2');

-- OK
select emaj.emaj_assign_tables('myschema2','mytbl.*','mytbl(5|6)$','myGroup2');
select emaj.emaj_assign_tables('myschema2','myTbl.*','','myGroup2');
select emaj.emaj_assign_tables('myschema2','mytbl(5|6)$','','myGroup2');

-- already assigned
select emaj.emaj_assign_tables('myschema2','mytbl(5|6)$','','myGroup2');

-- assign partitions
select emaj.emaj_assign_tables('myschema4','.*','mytbl(p|r.)$','myGroup4');

-- assign the unlogged table in an audit_only group
select emaj.emaj_assign_tables('myschema5','.*',null,'myGroup5');

-- assign tables with very long names
select emaj.emaj_assign_tables('myschema6','table_with_\d\d_characters_long_name.*',null,'myGroup6');

-----------------------
-- emaj_assign_sequence
-----------------------

-- error cases
-- bad group name
select emaj.emaj_assign_sequence('myschema2','myseq1','dummyGroup');
-- bad schema
select emaj.emaj_assign_sequence('dummySchema','myseq1','myGroup2');
select emaj.emaj_assign_sequence('emaj','myseq1','myGroup2');
-- bad sequence
select emaj.emaj_assign_sequence('myschema2','dummySequence','myGroup2');

-- bad mark
select emaj.emaj_assign_sequence('myschema2','myseq1','myGroup2','EMAJ_LAST_MARK');

-- ok
select emaj.emaj_assign_sequence('myschema2','myseq1','myGroup2');
-- already assigned sequence
select emaj.emaj_assign_sequence('myschema2','myseq1','myGroup2');

-----------------------------------
-- emaj_assign_sequences with array
-----------------------------------
-- error cases
-- bad group name
select emaj.emaj_assign_sequences('dummySchema',array['dummySequence'],'dummyGroup');
-- bad sequences
select emaj.emaj_assign_sequences('myschema2',array['dummyseq1','dummyseq2'],'myGroup2');
-- empty sequences array
select emaj.emaj_assign_sequences('myschema2',array[]::text[],'myGroup2');
select emaj.emaj_assign_sequences('myschema2',null,'myGroup2');
select emaj.emaj_assign_sequences('myschema2',array[''],'myGroup2');

-- ok (with a duplicate sequence name)
select emaj.emaj_assign_sequences('myschema2',array['myTbl3_col31_seq','myTbl3_col31_seq'],'myGroup2');

-- assign sequences with double_quoted names
select emaj.emaj_assign_sequences('phil''s schema"3',array[E'myTbl2\\_col21_seq',E'phil''s"seq\\1'],'phil''s group#3",');

-----------------------------------
-- emaj_assign_sequences with filters
-----------------------------------
-- error cases
-- bad group name
select emaj.emaj_assign_sequences('dummySchema','dummyIncludeFilter','dummyExcludeFilter','dummyGroup');
-- bad schema
select emaj.emaj_assign_sequences('dummySchema','dummyIncludeFilter','dummyExcludeFilter','myGroup1');
select emaj.emaj_assign_sequences('emaj','dummyIncludeFilter','dummyExcludeFilter','myGroup1');
-- empty sequences array
select emaj.emaj_assign_sequences('myschema1',null,null,'myGroup1');
select emaj.emaj_assign_sequences('myschema1','','','myGroup1');
select emaj.emaj_assign_sequences('myschema1','myTbl3_col31_seq','myTbl3_col31_seq','myGroup1');

-- already assigned
select emaj.emaj_assign_sequences('myschema2','myseq1.*','','myGroup2');

-- OK
select emaj.emaj_assign_sequences('myschema1','my.*_seq','myseq1$','myGroup1');

-----------------------------------
-- check populated groups
-----------------------------------
select group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging,
       group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  from emaj.emaj_group order by group_name;
select * from emaj.emaj_group_hist order by grph_group, grph_time_range;
select * from emaj.emaj_relation order by rel_group, rel_priority, rel_schema, rel_tblseq, rel_time_range;
select schemaname, tablename, tableowner, tablespace from pg_tables where schemaname like 'emaj\_%' order by schemaname, tablename;
select nspname, relname, rolname from pg_class, pg_namespace, pg_authid 
  where relnamespace = pg_namespace.oid and relowner = pg_authid.oid and relkind = 'S' and nspname like 'emaj\_%' order by nspname, relname;
select nspname, proname, rolname from pg_proc, pg_namespace, pg_authid
  where pronamespace = pg_namespace.oid and proowner = pg_authid.oid and nspname like 'emaj\_%' order by nspname, proname;

-----------------------------
-- emaj_export_groups_configuration() and emaj_import_groups_configuration() tests
-----------------------------

-- direct export
--   bad selected groups array
select emaj.emaj_export_groups_configuration(array['myGroup1','unknown1','unknown2']);

-- ok
select json_array_length(emaj.emaj_export_groups_configuration()->'tables_groups');
select json_array_length(emaj.emaj_export_groups_configuration(array['myGroup1','myGroup2'])->'tables_groups');

-- export in file
--   error
select emaj.emaj_export_groups_configuration('/tmp/dummy/location/file');

--   ok
select emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json');
select emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_partial.json', array['myGroup1','myGroup2']);
\! wc -l $EMAJTESTTMPDIR/*.json
\! grep -v ', at ' $EMAJTESTTMPDIR/orig_groups_config_all.json

-- direct import
--   bad content
select * from emaj.emaj_import_groups_configuration('{ "dummy_json": null }'::json);
--   missing "group" attribute
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "name": "grp1" } ]}'::json);
--   unknown group level attributes
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "unknown_attr1": null, "unknown_attr2": null } ]}'::json);
--   is_rollbackable not boolean
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "is_rollbackable": "absolutely true"} ]}'::json);
--   missing "schema" attribute in tables array
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { }] } ]}'::json);
--   missing "table" attribute in tables array
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1" }] } ]}'::json);
--   unknown table level attributes
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "unknown_attr": null }] } ]}'::json);
--   priority not numeric
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "a_schema", "table": "a_table", "priority": "high" }] } ]}'::json);
--   ignored_triggers not an array
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "ignored_triggers": "dummy" }] } ]}'::json);
--     not strings
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1", "ignored_triggers": [ "trg1", 1234, null] }] } ]}'::json);
--   missing "schema" attribute in sequences array
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { }] } ]}'::json);
--   missing "sequence" attribute in sequences array
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1" }] } ]}'::json);
--   unknown sequence level attributes
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1", "sequence": "s1",  "unknown_attr": null }] } ]}'::json);
--   duplicate group in json
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1" }, { "group": "grp1" } ]}'::json);

--   unknown group in array
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', array['myGroup1','myGroup2','unknownGroup']);
--   group already created
select emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1" }, { "group": "myGroup2" } ] }'::json, null, false);
--   bad type for existing groups
select emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "myGroup1", "is_rollbackable": false }, { "group": "myGroup2", "is_rollbackable": false } ] }'::json, null, true);

--   unknown table and sequence
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "tables": [ { "schema": "s1", "table": "t1" }] } ]}'::json);
select * from emaj.emaj_import_groups_configuration('{ "tables_groups": [ { "group": "grp1", "sequences": [ { "schema": "s1", "sequence": "sq1" }] } ]}'::json);

--   ok
-- a new group with a comment, then changed and finaly deleted
select emaj.emaj_import_groups_configuration('{ "tables_groups": [ 
   { "group": "new_grp", "comment": "a nice comment for new_grp" }
  ]}'::json);
select group_name, group_is_rollbackable, group_is_logging, group_comment
  from emaj.emaj_group where group_name = 'new_grp';

select emaj.emaj_import_groups_configuration('{ "tables_groups": [ 
   { "group": "new_grp", "comment": "changed comment for new_grp" }
  ]}'::json, null, true);
select group_comment from emaj.emaj_group where group_name = 'new_grp';

select emaj.emaj_import_groups_configuration('{ "tables_groups": [ 
   { "group": "new_grp" }
  ]}'::json, null, true);
select coalesce(group_comment,'NULL') from emaj.emaj_group where group_name = 'new_grp';

select emaj.emaj_drop_group('new_grp');

-- import from file
--   error
select emaj.emaj_import_groups_configuration('/tmp/dummy/location/file');
\! echo 'not a json content' >/tmp/bad_groups_config.json
select emaj.emaj_import_groups_configuration('/tmp/bad_groups_config.json');
\! rm /tmp/bad_groups_config.json

--   ok
-- only 2 from the original groups
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', array['emptyGroup','myGroup5'], true);
-- change the attributes for a table
\! sed -e 's/"mypartp1a"/"mypartp1a", "priority": 20, "log_data_tablespace": "tsplog1", "log_index_tablespace": "tsplog1"/' $EMAJTESTTMPDIR/orig_groups_config_all.json >$EMAJTESTTMPDIR/modified_groups_config_1.json
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_1.json', array['myGroup4'], true);

-- move a table and a sequence to another group
-- the table myschema2.mytbl5 and the sequence myschema2.myseq1 are moved from myGroup2 to myGroup4
\! sed -n -e '1,76p' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_2.json
--     remove the table and the sequence from myGroup2
\! sed -n -e '81,97p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
\! sed -n -e '102,108p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     copy the moved table
\! sed -n -e '77,80p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     copy the other tables
\! sed -n -e '109,132p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     copy the sequences keyword
\! sed -n -e '93,94p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     copy the moved sequence
\! sed -n -e '99,102p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
--     copy the remaining json structure
\! sed -n -e '133,$p' $EMAJTESTTMPDIR/modified_groups_config_1.json >>$EMAJTESTTMPDIR/modified_groups_config_2.json
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_2.json', array['myGroup2','myGroup4'], true);

-- remove a table and a sequence from a group
-- the table myschema2.mytbl5 and the sequence myschema2.myseq1 are removed from myGroup4
\! sed -n -e '1,100p' $EMAJTESTTMPDIR/modified_groups_config_2.json >$EMAJTESTTMPDIR/modified_groups_config_3.json
\! sed -n -e '105,128p' $EMAJTESTTMPDIR/modified_groups_config_2.json >>$EMAJTESTTMPDIR/modified_groups_config_3.json
\! sed -n -e '135,$p' $EMAJTESTTMPDIR/modified_groups_config_2.json >>$EMAJTESTTMPDIR/modified_groups_config_3.json
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_3.json', array['myGroup4'], true);

-- register an unknown trigger and an emaj trigger
\! sed -e 's/"mytbl2trg1","mytbl2trg2"/"unknowntrigger","emaj_trunc_trg"/' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_2.json
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_2.json', null, true);

-- suppress 1 trigger
\! sed -e 's/"mytbl2trg1",//' $EMAJTESTTMPDIR/modified_groups_config_1.json >$EMAJTESTTMPDIR/modified_groups_config_3.json
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/modified_groups_config_3.json', null, true);
select rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers from emaj.emaj_relation where rel_ignored_triggers is not null order by 1,2,3;

-- erroneously existing log schema
select emaj.emaj_drop_group('myGroup1');
begin;
  create schema emaj_myschema1;
  select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', null, true);
rollback;

-- rebuild all original groups
-- this will assign the just removed table and sequence
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', null, true);

-- test a table repair with the groups configuration import, with a table having triggers to ignored_triggers
select emaj.emaj_disable_protection_by_event_triggers();
drop trigger emaj_log_trg on myschema1.mytbl2;
select emaj.emaj_enable_protection_by_event_triggers();
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/orig_groups_config_all.json', null, true);
select rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers from emaj.emaj_relation where rel_ignored_triggers is not null order by 1,2,3;

-- keep the current tables groups definition as reference for further tests, once all ignored_triggers configurations reset
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers":null}'::jsonb);
select emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json');

-----------------------------
-- emaj_drop_group() tests
-----------------------------

-- unknown group
select emaj.emaj_drop_group(NULL);
select emaj.emaj_drop_group('unknownGroup');
-- group in logging state
select emaj.emaj_start_group('myGroup1','');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_stop_group('myGroup1');
-- log schema with an object blocking the schema drop
begin;
  create table emaj_myschema2.dummy_log (col1 int);
  select emaj.emaj_drop_group('myGroup2');
rollback;
-- should be OK
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_drop_group('myGroup2');
select emaj.emaj_drop_group('emptyGroup');
select emaj.emaj_drop_group('myGroup5');
select emaj.emaj_drop_group('myGroup6');

-- already dropped
select emaj.emaj_drop_group('myGroup2');

-----------------------------
-- emaj_force_drop_group() tests
-----------------------------

-- unknown group
select emaj.emaj_force_drop_group(NULL);
select emaj.emaj_force_drop_group('unknownGroup');
-- already dropped
select emaj.emaj_force_drop_group('myGroup2');
-- should be OK
select emaj.emaj_create_group('myGroup1',false);
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');
select emaj.emaj_start_group('myGroup1','');
select emaj.emaj_force_drop_group('myGroup1');

select emaj.emaj_create_group('myGroup1');
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');
select emaj.emaj_start_group('myGroup1','');
select emaj.emaj_force_drop_group('myGroup1');

-----------------------------
-- emaj_forget_group() tests
-----------------------------
-- not dropped group
select emaj.emaj_forget_group('myGroup4');
-- unknown group
select emaj.emaj_forget_group('unknownGroup');
-- should be OK
select emaj.emaj_forget_group('myGroup1');

-----------------------------
-- test end: global check
-----------------------------
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select * from emaj.emaj_schema order by 1;
select * from emaj.emaj_rel_hist order by 1,2,3;
select * from emaj.emaj_relation_change order by 1,2,3,4;
select hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist order by hist_id;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp order by time_id;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
