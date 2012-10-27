-- startStop.sql : test emaj_start_group(), emaj_start_groups(), 
--                      emaj_stop_group(), emaj_stop_groups() and emaj_force_stop_group() functions
--
SET client_min_messages TO WARNING;
-- prepare groups
select emaj.emaj_create_group('myGroup1');
select emaj.emaj_create_group('myGroup2');

INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','1 second'::interval);
select pg_sleep(1);

-----------------------------
-- emaj_start_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_start_group(NULL,NULL);
select emaj.emaj_start_group('unknownGroup',NULL,NULL);
-- reserved mark name
select emaj.emaj_start_group('myGroup1','EMAJ_LAST_MARK');
-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_start_group('myGroup2','M1');
rollback;

-- should be OK
select emaj.emaj_start_group('myGroup1','Mark1');
select emaj.emaj_start_group('myGroup2','Mark2',true);
select emaj.emaj_start_group('phil''s group#3",','Mark3',false);

select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_stop_group('myGroup2');

-- should be OK, with a warning on fkey between tables from different groups
begin;
  alter table myschema2.myTbl4 drop constraint mytbl4_col44_fkey;
  alter table myschema2.myTbl4 add constraint mytbl4_col44_fkey 
    FOREIGN KEY (col44,col45) REFERENCES myschema1.myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL;
  select emaj.emaj_start_group('myGroup2','Mark2');
rollback;

-- start with generated mark name
select emaj.emaj_start_group('myGroup1','%abc%',true);
select emaj.emaj_start_group('myGroup2','',false);

-- group already started
select emaj.emaj_start_group('myGroup2','Mark3');

-- check how truncate reacts  - tables are empty anyway
-- ... for a rollbackable group (must be blocked in pg 8.4+)
SET client_min_messages TO NOTICE;

truncate myschema1.mytbl1 cascade;
-- ... for an audit_only group (must be logged in pg 8.4+)
truncate "phil's schema3"."phil's tbl1" cascade;
select "phil's col11", "phil's col12", "phil\s col13", 
       emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip 
  from "emaj #'3"."phil's schema3_phil's tbl1_log";

SET client_min_messages TO WARNING;

-- impact of started group
select group_name, group_state, group_nb_table, group_nb_sequence, group_comment 
  from emaj.emaj_group order by group_name, group_state;
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-- check old events are deleted
select hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from emaj.emaj_hist order by hist_id;
delete from emaj.emaj_param where param_key = 'history_retention';

-----------------------------
-- emaj_stop_group() tests
-----------------------------
-- unknown group
select emaj.emaj_stop_group(NULL);
select emaj.emaj_stop_group('unkownGroup');
select emaj.emaj_stop_group(NULL,NULL);
select emaj.emaj_stop_group('unkownGroup',NULL);
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

-- impact of stopped group
select group_name, group_state, group_nb_table, group_nb_sequence, group_comment 
  from emaj.emaj_group order by group_name, group_state;
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-- should be OK
select emaj.emaj_stop_group('myGroup2','Stop mark');

-- warning, already stopped
select emaj.emaj_stop_group('myGroup2');
select emaj.emaj_stop_group('myGroup2','Stop mark 2');

-- start with auto-mark in a single transaction
begin transaction;
  select emaj.emaj_start_group('myGroup1',NULL);
  select emaj.emaj_start_group('myGroup2','');
commit;
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

begin transaction;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_stop_group('myGroup2');
commit;

-- use of % in start mark name
select emaj.emaj_start_group('myGroup1','Foo%Bar');
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-----------------------------
-- emaj_start_groups() tests
-----------------------------
select emaj.emaj_stop_group('myGroup1');
-- NULL group names array
select emaj.emaj_start_groups(NULL,NULL,NULL);

-- at least one group is unknown in emaj_group_def
select emaj.emaj_start_groups('{""}',NULL);
select emaj.emaj_start_groups('{"unknownGroup",""}',NULL,true);
select emaj.emaj_start_groups('{"myGroup1","unknownGroup"}',NULL,false);

-- reserved mark name
select emaj.emaj_start_groups('{"myGroup1"}','EMAJ_LAST_MARK');

-- second group is already started
select emaj.emaj_start_group('myGroup2','Mark1',true);
select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','Mark1',false);
select emaj.emaj_stop_group('myGroup2');

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

-- impact of started group
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Mark1',true);
select group_name, group_state, group_nb_table, group_nb_sequence, group_comment 
  from emaj.emaj_group order by group_name, group_state;
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-----------------------------
-- emaj_stop_groups() tests
-----------------------------
-- NULL group names array
select emaj.emaj_stop_groups(NULL);

-- at least one group is unknown in emaj_group_def
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
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-- with warning about group names array content
select emaj.emaj_stop_groups(array['myGroup1',NULL,'myGroup2','','myGroup2','myGroup2','myGroup1']);

-----------------------------
-- emaj_force_stop_group() tests
-----------------------------
select emaj.emaj_start_groups(array['myGroup1','myGroup2'],'Mark1',true);
-- unknown group
select emaj.emaj_force_stop_group(NULL);
select emaj.emaj_force_stop_group('unkownGroup');

-- should be OK
-- missing application schema
begin;
  drop schema mySchema2 cascade;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- missing application table
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- missing log trigger
begin;
  drop trigger myschema2_mytbl4_emaj_log_trg on myschema2.mytbl4;
  select emaj.emaj_force_stop_group('myGroup2');
rollback;
-- sane group
select emaj.emaj_force_stop_group('myGroup2');
select emaj.emaj_force_stop_group('myGroup1');

-- warning, already stopped
select emaj.emaj_force_stop_group('myGroup2');

-- impact of stopped group
select group_name, group_state, group_nb_table, group_nb_sequence, group_comment 
  from emaj.emaj_group order by group_name, group_state;
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;

-- test end: (groups are stopped) reset history and force sequences id
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 3000;
alter sequence emaj.emaj_mark_mark_id_seq restart 300;
alter sequence emaj.emaj_sequence_sequ_id_seq restart 3000;
alter sequence emaj.emaj_seq_hole_sqhl_id_seq restart 300;

