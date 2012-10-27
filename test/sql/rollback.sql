-- rollback.sql : test updates log, emaj_rollback_group(), emaj_logged_rollback_group(),
--                emaj_rollback_groups(), and emaj_logged_rollback_groups() functions
--
-----------------------------
-- rollback nothing tests
-----------------------------

-- group or groups is NULL
select emaj.emaj_rollback_group(NULL,NULL);
select emaj.emaj_logged_rollback_group(NULL,NULL);

select emaj.emaj_rollback_groups(NULL,NULL);
select emaj.emaj_logged_rollback_groups(NULL,NULL);

-- group is unknown in emaj_group_def
select emaj.emaj_rollback_group('unknownGroup',NULL);
select emaj.emaj_logged_rollback_group('unknownGroup',NULL);

select emaj.emaj_rollback_groups('{"unknownGroup"}',NULL);
select emaj.emaj_logged_rollback_groups('{"unknownGroup","myGroup1"}',NULL);
begin;
  select emaj.emaj_start_group('myGroup1','');
  select emaj.emaj_rollback_groups('{"myGroup1","unknownGroup"}','EMAJ_LAST_MARK');
rollback;

-- group not in logging state
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_rollback_group('myGroup2','EMAJ_LAST_MARK');

select emaj.emaj_logged_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_group('myGroup2','EMAJ_LAST_MARK');

select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}',NULL);
begin;
  select emaj.emaj_start_group('myGroup1','');
  select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');
rollback;

-- start groups and set some marks
select emaj.emaj_start_group('myGroup1','Mark11');
select emaj.emaj_start_group('myGroup2','Mark21');

select emaj.emaj_set_mark_group('myGroup1','Different_Mark');
select emaj.emaj_set_mark_group('myGroup2','Different_Mark');

-- log tables are empty
select count(*) from emaj.myschema1_mytbl1_log;
select count(*) from emaj.myschema1_mytbl2_log;
select count(*) from emajb.myschema1_mytbl2b_log;
select count(*) from "emajC"."myschema1_myTbl3_log";
select count(*) from emaj.myschema1_mytbl4_log;
select count(*) from emaj.myschema2_mytbl1_log;
select count(*) from emaj.myschema2_mytbl2_log;
select count(*) from "emajC"."myschema2_myTbl3_log";
select count(*) from emaj.myschema2_mytbl4_log;

-- unknown mark name
select emaj.emaj_rollback_group('myGroup1',NULL);
select emaj.emaj_rollback_group('myGroup1','DummyMark');

select emaj.emaj_logged_rollback_group('myGroup1',NULL);
select emaj.emaj_logged_rollback_group('myGroup1','DummyMark');

select emaj.emaj_rollback_groups('{"myGroup1","myGroup2",""}',NULL);
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2",NULL}',NULL);
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','DummyMark');

select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2","myGroup2"}',NULL);
select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mark11');

-- mark name referencing different points in time
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Different_Mark');

-- attemp to rollback an 'audit_only' group
select emaj.emaj_rollback_group('phil''s group#3",','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_group('phil''s group#3",','M1_audit_only');

-- attemp to rollback to a stop mark
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_start_group('myGroup1','StartMark',false);
  select emaj.emaj_rename_mark_group('myGroup1',(select emaj.emaj_get_previous_mark_group('myGroup1','StartMark')), 'GeneratedStopMark');
  select emaj.emaj_rollback_group('myGroup1','GeneratedStopMark');
rollback;

-- missing application table and mono-group rollback
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_rollback_group('myGroup2','Mark21');
rollback;
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_logged_rollback_group('myGroup2','Mark21');
rollback;

-- should be OK
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_rollback_group('myGroup2','Mark21');

select emaj.emaj_logged_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_group('myGroup2','Mark21');

select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mark1B');

select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mark1B');

select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mark1B');

-- missing application table and multi-groups rollback
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mark1B');
rollback;
begin;
  drop table mySchema2."myTbl3" cascade;
  select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mark1B');
rollback;

-- restart groups
select emaj.emaj_stop_groups('{"myGroup1","myGroup2"}');
select emaj.emaj_start_group('myGroup1','Mark11');
select emaj.emaj_start_group('myGroup2','Mark21');

-----------------------------
-- log phase #1 with 2 unlogged rollbacks
-----------------------------
-- Populate application tables
set search_path=myschema1;
-- inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger)
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
begin transaction;
  update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
  insert into myTbl2 values (1,'ABC',current_date);
commit;
delete from myTbl1 where col11 > 10;
insert into myTbl2 values (2,'DEF',NULL);
insert into myTbl2 values (3,'GHI',NULL);
update myTbl2 set col22 = NULL WHERE col23 IS NULL;
delete from myTbl2 where col21 = 3 and col22 is NULL;
select count(*) from mytbl1;
select count(*) from mytbl2;
select count(*) from mytbl2b;
select count(*) from "myTbl3";
select count(*) from myTbl4;

-- set a mark
select emaj.emaj_set_mark_group('myGroup1','Mark12');
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;

-- inserts/updates/deletes in myTbl3 and myTbl4
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl4 values (1,'FK...',1,1,'ABC');
update myTbl4 set col43 = NULL where col41 = 1;
select count(*) from "myTbl3";
select count(*) from myTbl4;

-- set a mark
select emaj.emaj_set_mark_group('myGroup1','Mark13');

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_mytbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_mytbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.myschema1_mytbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl4_log order by emaj_gid, emaj_tuple desc;

-- rollback #1
alter table mySchema1.myTbl2 disable trigger myTbl2trg;
select emaj.emaj_rollback_group('myGroup1','Mark12');
alter table mySchema1.myTbl2 enable trigger myTbl2trg;
-- check impact
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select * from emaj.emaj_fk;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
select col31, col33 from myschema1."myTbl3" order by col31;
select col41, col42, col43, col44, col45 from myschema1.myTbl4 order by col41;

-- rollback #2 (and stop)
alter table mySchema1.myTbl2 disable trigger myTbl2trg;
select emaj.emaj_rollback_group('myGroup1','Mark11');
alter table mySchema1.myTbl2 enable trigger myTbl2trg;
select emaj.emaj_stop_group('myGroup1');
-- check impact
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select * from emaj.emaj_fk;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.myschema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-- restart group
select emaj.emaj_start_group('myGroup1','Mark11');

-- check the logs are empty again
select count(*) from emaj.myschema1_mytbl1_log;
select count(*) from emaj.myschema1_mytbl2_log;
select count(*) from emajb.myschema1_mytbl2b_log;
select count(*) from "emajC"."myschema1_myTbl3_log";
select count(*) from emaj.myschema1_mytbl4_log;


-----------------------------
-- log phase #2 with 2 unlogged rollbacks
-----------------------------
-- Populate application tables
set search_path=myschema1;
-- inserts/updates/deletes in myTbl1, myTbl2 and myTbl2b (via trigger)
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
begin transaction;
  update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
  insert into myTbl2 values (1,'ABC',current_date);
commit;
delete from myTbl1 where col11 > 10;
insert into myTbl2 values (2,'DEF',NULL);
select count(*) from mytbl1;
select count(*) from mytbl2;
select count(*) from mytbl2b;

-- set a mark
select emaj.emaj_set_mark_group('myGroup1','Mark12');

-- inserts/updates/deletes in myTbl3 and myTbl4
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl4 values (1,'FK...',1,1,'ABC');
select count(*) from "myTbl3";
select count(*) from myTbl4;

-- set a mark
select emaj.emaj_set_mark_group('myGroup1','Mark13');

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_mytbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_mytbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.myschema1_mytbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl4_log order by emaj_gid, emaj_tuple desc;

-- logged rollback #1
alter table mySchema1.myTbl2 disable trigger myTbl2trg;
select emaj.emaj_logged_rollback_group('myGroup1','Mark12');
alter table mySchema1.myTbl2 enable trigger myTbl2trg;
-- check impact
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select * from emaj.emaj_fk;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema1_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl4_log order by emaj_gid, emaj_tuple desc;
select col31, col33 from myschema1."myTbl3" order by col31;
select col41, col42, col43, col44, col45 from myschema1.myTbl4 order by col41;

-- logged rollback #2
alter table mySchema1.myTbl2 disable trigger myTbl2trg;
select emaj.emaj_logged_rollback_group('myGroup1','Mark11');
alter table mySchema1.myTbl2 enable trigger myTbl2trg;
-- check impact
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select * from emaj.emaj_fk;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.myschema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-----------------------------
-- unlogged rollback of logged rollbacks #3
-----------------------------
alter table mySchema1.myTbl2 disable trigger myTbl2trg;
select emaj.emaj_rollback_group('myGroup1','Mark13');
alter table mySchema1.myTbl2 enable trigger myTbl2trg;
-- check impact
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_global_seq, mark_state, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
select sqhl_id, sqhl_schema, sqhl_table, sqhl_hole_size from emaj.emaj_seq_hole order by sqhl_id;
select * from emaj.emaj_fk;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema1_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, emaj_verb, emaj_tuple, emaj_gid from emajb.myschema1_myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-- check content of emaj_rlbk_stat table
select rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_nb_rows from
(select * from emaj.emaj_rlbk_stat order by rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime) as t;

-----------------------------
-- test use of partitionned tables
-----------------------------
select emaj.emaj_start_group('myGroup4','myGroup4_start');
insert into myschema4.myTblM values ('2001-09-11',0,'abc'),('2011-09-11',10,'def'),('2021-09-11',20,'ghi');

select emaj.emaj_set_mark_group('myGroup4','mark1');
delete from myschema4.myTblM;

select emaj.emaj_logged_rollback_group('myGroup4','mark1');

select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mytblm_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mytblc1_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj.myschema4_mytblc2_log;

select emaj.emaj_rollback_group('myGroup4','myGroup4_start');

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 5000;
alter sequence emaj.emaj_mark_mark_id_seq restart 500;
alter sequence emaj.emaj_sequence_sequ_id_seq restart 5000;
alter sequence emaj.emaj_seq_hole_sqhl_id_seq restart 500;

