-- start_stop.sql : test emaj_start_group(), emaj_start_groups(), 
--                      emaj_stop_group(), emaj_stop_groups(), emaj_force_stop_group(),
--                      emaj_protect_group() and emaj_unprotect_group() functions
--

-- set sequence restart value
-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/create_drop'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

select public.handle_emaj_sequences(2000);

-- build original groups
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/../all_groups_config.json', array['myGroup1','myGroup2','emptyGroup'], true);

-- disable event triggers 
-- this is done to allow tests with missing or renamed or altered components
select emaj.emaj_disable_protection_by_event_triggers();

-----------------------------
-- emaj_start_group() tests
-----------------------------
-- group is unknown in emaj_group
select emaj.emaj_start_group(NULL,NULL);
select emaj.emaj_start_group('unknownGroup',NULL,NULL);
select emaj.emaj_start_groups(array['unknownGroup1','unknownGroup2'],NULL,NULL);
-- reserved mark name
select emaj.emaj_start_group('myGroup1','EMAJ_LAST_MARK');

-- detection of a missing application schema
SET client_min_messages TO WARNING;
begin;
  drop schema myschema1 cascade;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
RESET client_min_messages;
-- detection of a missing application relation
begin;
  drop table myschema1.mytbl4;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of relation type change (a table is now a sequence!)
begin;
  update emaj.emaj_relation set rel_kind = 'S' where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a missing E-Maj log schema
SET client_min_messages TO WARNING;
begin;
  drop schema emaj_myschema1 cascade;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
RESET client_min_messages;
-- detection of a missing log trigger
begin;
  drop trigger emaj_log_trg on myschema1.mytbl1;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a missing log function
begin;
  drop function emaj_myschema1.mytbl1_log_fnct() cascade;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a missing truncate trigger
begin;
  drop trigger emaj_trunc_trg on myschema1.mytbl1;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a missing log table
begin;
  drop table emaj_myschema1.mytbl1_log;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a change in the application table structure (new column)
begin;
  alter table myschema1.mytbl1 add column newcol int;
  alter table myschema1.mytbl1 add column othernewcol text;
  alter table myschema1.mytbl2 add column newcol int;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a change in the application table structure (column type change)
begin;
  alter table myschema1.mytbl4 drop column col42;
  alter table myschema1.mytbl4 alter column col45 type varchar(15);
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a missing primary key
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a table altered as UNLOGGED
begin;
  alter table myschema1."myTbl3" set unlogged;                        -- needs 9.5+
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a primary key structure change
begin;
  alter table myschema1.mytbl4 drop constraint mytbl4_pkey;
  alter table myschema1.mytbl4 add primary key (col41, col42);
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of STORED generated column whose expression has been dropped
begin;
  alter table myschema1.mytbl2b alter column col22 drop expression;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a column transformed into generated column
begin;
  alter table myschema1.mytbl2b drop column col24, add column col24 BOOLEAN GENERATED ALWAYS AS (col21 > 3) STORED;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a bad trigger in a "triggers to ignore at rollback time" array
begin;
  update emaj.emaj_relation set rel_ignored_triggers = '{"dummy1","dummy2"}'
    where rel_schema = 'myschema1' and rel_tblseq = 'mytbl1' and upper_inf(rel_time_range);
  select emaj.emaj_start_group('myGroup1','M1');
rollback;
-- detection of a log table missing a technical column
begin;
  alter table emaj_myschema1.mytbl1_log drop column emaj_verb;
  select emaj.emaj_start_group('myGroup1','M1');
rollback;

-- should be OK
-- use the first correct emaj_start_group() function call to test the emaj_hist purge
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','0.1 second'::interval);
select pg_sleep(0.2);

select emaj.emaj_start_group('myGroup1','Mark1');
-- check old events are deleted
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
       hist_user
  from emaj.emaj_hist order by hist_id;

-- test the smallest history_retention value that means infinity
update emaj.emaj_param set param_value_interval = '100 years'::interval where param_key = 'history_retention';
select emaj.emaj_start_group('myGroup2','Mark2',true);

delete from emaj.emaj_param where param_key = 'history_retention';
select emaj.emaj_start_group('phil''s group#3",','Mark3',false);
select emaj.emaj_start_group('emptyGroup','Mark1');

select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_stop_group('myGroup2');

-- Warnings on FK

-- warning on fkey between tables from different groups
begin;
  alter table myschema2.myTbl4 drop constraint mytbl4_col44_fkey, add constraint mytbl4_col44_fkey 
    foreign key (col44,col45) references myschema1.myTbl1 (col11,col12) on delete cascade on update set null;
  select emaj.emaj_start_group('myGroup2','Mark2');
rollback;

-- Warning on non deferrable fkey set on partitionned table
begin;
  alter table myschema4.myTblP drop constraint mytblp_col1_fkey, add foreign key (col1) references myschema4.mytblr1(col1);
  select emaj.emaj_start_group('myGroup4','Mark1');
rollback;

-- Warning on fkey set on partitionned table with ON UPDATE|DELETE clause
begin;
  alter table myschema4.myTblP drop constraint mytblp_col1_fkey, add foreign key (col1) references myschema4.mytblr1(col1)
    on update set null;
  select emaj.emaj_start_group('myGroup4','Mark1');
rollback;

-- start with generated mark name
select emaj.emaj_start_group('myGroup1','%abc%',true);
select emaj.emaj_start_group('myGroup2','',false);

-- group already started
select emaj.emaj_start_group('myGroup2','Mark3');

-- use of % in start mark name
select emaj.emaj_start_group('myGroup1','Foo%Bar');
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;

-- multiple emaj_start_group() using the same generated start mark name => fails
-- this test is commented because the generated error message differs from one run to another
--begin;
--  select emaj.emaj_start_group('myGroup4');
--  select emaj.emaj_stop_group('myGroup4');
--  select emaj.emaj_start_group('myGroup4',NULL,false);
--rollback;

-- check for emaj_start_group()
select group_name, group_is_logging, group_is_rlbk_protected from emaj.emaj_group order by group_name;
select * from emaj.emaj_log_session order by lses_group, lses_time_range;
select * from emaj.emaj_group_hist order by grph_group, grph_time_range;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark where mark_time_id >= 2000 order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2000 order by time_id;
select hist_id, hist_function, hist_event, hist_object, 
  regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
  hist_user from emaj.emaj_hist where hist_id >= 2000 order by hist_id;

select public.handle_emaj_sequences(2200);


-----------------------------
-- emaj_stop_group() tests
-----------------------------
-- unknown group
select emaj.emaj_stop_group(NULL);
select emaj.emaj_stop_group('unknownGroup');
select emaj.emaj_stop_group(NULL,NULL);
select emaj.emaj_stop_group('unknownGroup',NULL);
-- invalid mark
select emaj.emaj_stop_group('myGroup1','EMAJ_LAST_MARK');
-- already existing mark
select emaj.emaj_stop_group('phil''s group#3",','Mark3');
-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_stop_group('myGroup2');
rollback;

-- should be OK
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_stop_group('emptyGroup');

-- should be OK with a stop mark
select emaj.emaj_stop_group('myGroup2','Stop mark');

-- warning, already stopped
select emaj.emaj_stop_group('myGroup2');
select emaj.emaj_stop_group('myGroup2','Stop mark 2');

-- start with auto-mark in a single transaction
begin transaction;
  select emaj.emaj_start_group('myGroup1');
  select emaj.emaj_start_group('myGroup2','');
commit;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;

begin transaction;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_stop_group('myGroup2','');
commit;

-- multiple emaj_stop_group() using the same generated start mark name => fails
-- this test is commented because the generated error message differs from one run to another
--begin;
--  select emaj.emaj_start_group('myGroup4','a_first_start_mark');
--  select emaj.emaj_stop_group('myGroup4','%');
--  select emaj.emaj_start_group('myGroup4','another_start_mark',false);
--  select emaj.emaj_stop_group('myGroup4','%');
--rollback;

-- check for emaj_stop_group()
select group_name, group_is_logging, group_is_rlbk_protected from emaj.emaj_group order by group_name;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark where mark_time_id >= 2200 order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2200 order by time_id;
select hist_id, hist_function, hist_event, hist_object, 
  regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
  hist_user from emaj.emaj_hist where hist_id >= 2200 order by hist_id;

select public.handle_emaj_sequences(2400);

-----------------------------
-- emaj_start_groups() tests
-----------------------------
select emaj.emaj_stop_group('myGroup1',NULL);
-- NULL group names array
select emaj.emaj_start_groups(NULL,NULL,NULL);

-- at least one group is unknown
select emaj.emaj_start_groups('{""}',NULL);
select emaj.emaj_start_groups('{"unknownGroup",""}',NULL,true);
select emaj.emaj_start_groups('{"myGroup1","unknownGroup"}',NULL,false);

-- reserved mark name
select emaj.emaj_start_groups('{"myGroup1"}','EMAJ_LAST_MARK');

-- 2 groups already started
select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','Mark1',true);
select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','Mark1',false);
select emaj.emaj_stop_groups('{"myGroup1","myGroup2"}');

-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Mark1',true);
rollback;

-- should be OK, with a warning on fkey between tables from different groups and warning on group names array content
begin;
  alter table myschema2.myTbl4 drop constraint mytbl4_col44_fkey;
  alter table myschema2.myTbl4 add constraint mytbl4_col44_fkey 
    FOREIGN KEY (col44,col45) REFERENCES myschema1.myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL;
  select emaj.emaj_start_groups(array['myGroup1',NULL,'myGroup2','','myGroup2','myGroup2','myGroup1'],'Mark1');
rollback;

-- check for emaj_start_groups()
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Mark1',true);
select group_name, group_is_logging, group_is_rlbk_protected from emaj.emaj_group order by group_name;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark where mark_time_id >= 2400 order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2400 order by time_id;
select hist_id, hist_function, hist_event, hist_object, 
  regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
  hist_user from emaj.emaj_hist where hist_id >= 2400 order by hist_id;

select public.handle_emaj_sequences(2500);

-----------------------------
-- emaj_stop_groups() tests
-----------------------------
-- NULL group names array
select emaj.emaj_stop_groups(NULL);

-- at least one group is unknown
select emaj.emaj_stop_groups('{""}');
select emaj.emaj_stop_groups('{"unknownGroup",""}');
select emaj.emaj_stop_groups('{"myGroup1","unknownGroup"}');

-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_stop_groups(array['myGroup1','myGroup2']);
rollback;

-- should be OK
select emaj.emaj_stop_groups(array['myGroup1','myGroup2'],'Global Stop at %');

select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;

-- with warning about group names array content
select emaj.emaj_stop_groups(array['myGroup1',NULL,'myGroup2','','myGroup2','myGroup2','myGroup1']);

-----------------------------
-- emaj_force_stop_group() tests
-----------------------------
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Mark1',true);
-- unknown group
select emaj.emaj_force_stop_group(NULL);
select emaj.emaj_force_stop_group('unknownGroup');

-- should be OK
-- missing application schema
SET client_min_messages TO WARNING;
begin;
  drop schema mySchema2 cascade;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
RESET client_min_messages;
-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- missing log trigger
begin;
  drop trigger emaj_log_trg on myschema2.mytbl4;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- missing truncate trigger
begin;
  drop trigger emaj_trunc_trg on myschema2.mytbl4;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- sane group
select emaj.emaj_force_stop_group('myGroup2');
select emaj.emaj_force_stop_group('myGroup1');

-- warning, already stopped
select emaj.emaj_force_stop_group('myGroup2');

-- check for emaj_stop_groups() and emaj_force_stop_group()
-- impact of stopped groups
select group_name, group_is_logging, group_is_rlbk_protected from emaj.emaj_group order by group_name;
select * from emaj.emaj_log_session order by lses_group, lses_time_range;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark where mark_time_id >= 2500 order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2500 order by time_id;
select hist_id, hist_function, hist_event, hist_object, 
  regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
  hist_user from emaj.emaj_hist where hist_id >= 2500 order by hist_id;

select public.handle_emaj_sequences(2600);

-----------------------------
-- emaj_protect_group() tests
-----------------------------
-- group is unknown
select emaj.emaj_protect_group(NULL);
select emaj.emaj_protect_group('unknownGroup');
-- group is not rollbackable
select emaj.emaj_protect_group('phil''s group#3",');
-- group is not in logging state
select emaj.emaj_protect_group('myGroup1');
-- should be ok
select emaj.emaj_start_group('myGroup1','M1');
select emaj.emaj_protect_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';
-- protect an already protected group
select emaj.emaj_protect_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';
-- stop should reset the protection
select emaj.emaj_stop_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';

-----------------------------
-- emaj_unprotect_group() tests
-----------------------------
-- group is unknown
select emaj.emaj_unprotect_group(NULL);
select emaj.emaj_unprotect_group('unknownGroup');
-- group is not rollbackable
select emaj.emaj_unprotect_group('phil''s group#3",');
-- group is not in logging state
select emaj.emaj_unprotect_group('myGroup1');
-- should be ok
select emaj.emaj_start_group('myGroup1','M1');
select emaj.emaj_protect_group('myGroup1');
select emaj.emaj_unprotect_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';
-- unprotect an already unprotected group
select emaj.emaj_unprotect_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_stop_group('myGroup1');
select group_is_logging, group_is_rlbk_protected from emaj.emaj_group where group_name = 'myGroup1';

select emaj.emaj_enable_protection_by_event_triggers();
-- check for emaj_protect_group() and emaj_unprotect_group()
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2600 order by time_id;
select hist_id, hist_function, hist_event, hist_object, 
  regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'),
  hist_user from emaj.emaj_hist where hist_id >= 2600 order by hist_id;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
