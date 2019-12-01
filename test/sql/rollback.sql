-- rollback.sql : test updates log, emaj_rollback_group(), emaj_logged_rollback_group(),
--                emaj_rollback_groups(), emaj_logged_rollback_groups(),
--                emaj_cleanup_rollback_state(), emaj_rollback_activity(),
--                emaj_consolidate_rollback_group() and emaj_get_consolidable_rollbacks() functions.
--

-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 3000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 3000;

-- set the triggers state
select emaj.emaj_ignore_app_trigger('REMOVE','myschema1','mytbl2','mytbl2%');
ALTER TABLE mySchema1.myTbl2 ENABLE TRIGGER myTbl2trg2;
select emaj.emaj_ignore_app_trigger('ADD','myschema1','mytbl2','mytbl2trg2');

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
select count(*) from emaj_myschema1.mytbl1_log;
select count(*) from emaj_myschema1.mytbl2_log;
select count(*) from emaj_myschema1.mytbl2b_log;
select count(*) from emaj_myschema1."myTbl3_log";
select count(*) from emaj_myschema1.mytbl4_log;
select count(*) from emaj_myschema2.mytbl1_log;
select count(*) from emaj_myschema2.mytbl2_log;
select count(*) from emaj_myschema2."myTbl3_log";
select count(*) from emaj_myschema1.myTbl4_log;

-- protected group
select emaj.emaj_protect_group('myGroup1');
select emaj.emaj_protect_group('myGroup2');
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_groups('{"myGroup2","myGroup1"}',NULL);
select emaj.emaj_unprotect_group('myGroup1');
select emaj.emaj_unprotect_group('myGroup2');
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');

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
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');

-- attempt to rollback 'audit_only' groups
select emaj.emaj_create_group('auditOnlyEmptyGroup',false,true);
select emaj.emaj_rollback_group('phil''s group#3",','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_groups(array['phil''s group#3",','auditOnlyEmptyGroup'],'M1_audit_only');
select emaj.emaj_drop_group('auditOnlyEmptyGroup');

-- attempt to rollback to a stop mark
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_start_group('myGroup1','StartMark',false);
  select emaj.emaj_rename_mark_group('myGroup1',(select emaj.emaj_get_previous_mark_group('myGroup1','StartMark')), 'GeneratedStopMark');
  select emaj.emaj_rollback_group('myGroup1','GeneratedStopMark');
rollback;

-- attempt to rollback over protected marks
begin;
  select emaj.emaj_set_mark_group('myGroup1','Protected Mark 1');
  select emaj.emaj_protect_mark_group('myGroup1','Protected Mark 1');
  select emaj.emaj_set_mark_group('myGroup1','Protected Mark 2');
  select emaj.emaj_protect_mark_group('myGroup1','Protected Mark 2');
  select emaj.emaj_rollback_group('myGroup1','Mark11');
rollback;

-- attempt to rollback several groups over one single-mark protected marks
begin;
  select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Common Mark 1');
  select emaj.emaj_set_mark_group('myGroup1','Protected Mark 1');
  select emaj.emaj_protect_mark_group('myGroup1','Protected Mark 1');
  select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Common Mark 1');
rollback;

-- attempt to logged-rollback over a protected mark
begin;
  select emaj.emaj_protect_mark_group('myGroup1','EMAJ_LAST_MARK');
  select emaj.emaj_logged_rollback_group('myGroup1','Mark11');
rollback;

-- rollback to a protected mark
begin;
  select emaj.emaj_protect_mark_group('myGroup1','EMAJ_LAST_MARK');
  select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');
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

-- internal error when the row corresponding to an application sequence to rollback is missing in emaj_sequence
begin;
  delete from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq1';
  select emaj.emaj_rollback_group('myGroup2','Mark21');
rollback;

-- should be OK, with different cases of dblink status
-- hide dblink_connect functions
alter function public.dblink_connect_u(text,text) rename to renamed_dblink_connect_u;
alter function public.dblink_connect_u(text) rename to renamed_dblink_connect_u;
select emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_rollback_group('myGroup2','Mark21');
alter function public.renamed_dblink_connect_u(text,text) rename to dblink_connect_u;
alter function public.renamed_dblink_connect_u(text) rename to dblink_connect_u;

select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mark1B');

-- no user/password defined in emaj_param
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');
-- bad user/password defined in emaj_param
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=<user> password=<password>');
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mark1B');

-- transaction not in READ COMMITTED isolation level => the dblink connection is not possible
begin;
  set transaction isolation level repeatable read;
  select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','EMAJ_LAST_MARK');
commit;

-- dblink connection should now be ok (missing right on dblink functions is tested in adm1.sql)
update emaj.emaj_param set param_value_text = 'user=postgres password=postgres' 
  where param_key = 'dblink_user_password';
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
set search_path=public,myschema1;
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
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;

-- inserts/updates/deletes in myTbl3 and myTbl4
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl4 values (1,'FK...',1,1,'ABC');
update myTbl4 set col43 = NULL where col41 = 1;
select count(*) from "myTbl3";
select count(*) from myTbl4;

-- set a mark
select emaj.emaj_set_mark_group('myGroup1','Mark13');
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- rollback #1
select emaj.emaj_rollback_group('myGroup1','Mark12');
-- check impact
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl4_log order by emaj_gid, emaj_tuple desc;
select col31, col33 from myschema1."myTbl3" order by col31;
select col41, col42, col43, col44, col45 from myschema1.myTbl4 order by col41;

-- rollback #2 (and stop)
select emaj.emaj_rollback_group('myGroup1','Mark11');
select emaj.emaj_stop_group('myGroup1');
-- check impact
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-- restart group
select emaj.emaj_start_group('myGroup1','Mark11');

-- check the logs are empty again
select count(*) from emaj_myschema1.mytbl1_log;
select count(*) from emaj_myschema1.mytbl2_log;
select count(*) from emaj_myschema1.mytbl2b_log;
select count(*) from emaj_myschema1."myTbl3_log";
select count(*) from emaj_myschema1.mytbl4_log;

-----------------------------
-- log phase #2 with 2 logged rollbacks
-----------------------------
-- Populate application tables
set search_path=public,myschema1;
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
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.mytbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- logged rollback #1
select emaj.emaj_logged_rollback_group('myGroup1','Mark12');
-- check impact
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl4_log order by emaj_gid, emaj_tuple desc;
select col31, col33 from myschema1."myTbl3" order by col31;
select col41, col42, col43, col44, col45 from myschema1.myTbl4 order by col41;

-- logged rollback #2
select emaj.emaj_logged_rollback_group('myGroup1','Mark11');
-- check impact
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-----------------------------
-- unlogged rollback of logged rollbacks #3
-----------------------------
alter table mySchema1.myTbl2 disable trigger myTbl2trg1;
select emaj.emaj_rollback_group('myGroup1','Mark13');
alter table mySchema1.myTbl2 enable trigger myTbl2trg1;
-- check impact
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col11, col12, col13 from myschema1.myTbl1 order by col11, col12;
select col21, col22 from myschema1.myTbl2 order by col21;
select col20, col21 from myschema1.myTbl2b order by col20;

-----------------------------
-- rollback an empty group
-----------------------------
select emaj.emaj_rollback_group('emptyGroup','EGM4');
select emaj.emaj_logged_rollback_group('emptyGroup','EGM3');

-----------------------------
-- test use of partitionned tables
-----------------------------
select emaj.emaj_start_group('myGroup4','myGroup4_start');
insert into myschema4.myTblM values ('2001-09-11',0,'abc'),('2011-09-11',10,'def'),('2021-09-11',20,'ghi');
insert into myschema4.myTblP values (-1,'abc'),(0,'def'),(1,'ghi');

select emaj.emaj_set_mark_group('myGroup4','mark1');
delete from myschema4.myTblM;
update myschema4.myTblP set col2 = 'DEF' where col1 = 0;

select emaj.emaj_logged_rollback_group('myGroup4','mark1');

select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mytblm_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mytblc1_log;
select col1, col2, col3, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mytblc2_log;
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartp1_log;          -- empty in pg 9.6-
select col1, col2, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema4.mypartp2_log;          -- empty in pg 9.6-

-- use the functions dedicated to emaj_web
-- for an equivalent of "select * from emaj.emaj_rollback_group('myGroup4','myGroup4_start',true);"
select * from emaj._rlbk_async(emaj._rlbk_init(array['myGroup4'], 'myGroup4_start', false, 1, false, true), false);
-- and check the result
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, 
       rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id,
       rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_messages
 from emaj.emaj_rlbk order by rlbk_id desc limit 1;

-----------------------------
-- test emaj_rollback_activity()
-----------------------------
-- insert necessary rows into rollback tables in order to test various cases of emaj_rollback_activity() reports.
-- these tests ares performed inside transactions that are then rolled back.
begin;
-- 1 rollback operation in EXECUTING state, but no rollback steps have started yet
  insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-1, now()-'0.1 seconds'::interval);
  insert into emaj.emaj_time_stamp (time_id, time_clock_timestamp) values (-2, '2000-01-01 01:00:00');
  insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, 
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
    values (1232,array['group1232'],'mark1232',-2,-1,true,false,
             1,5,4,3,'EXECUTING');
  insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_estimated_quantity, rlbp_start_datetime, rlbp_duration)
    values (1232, 'RLBK_TABLE','schema','t1','','50 seconds'::interval,null,null,null),
           (1232, 'RLBK_TABLE','schema','t2','','30 seconds'::interval,null,null,null),
           (1232, 'RLBK_TABLE','schema','t3','','20 seconds'::interval,null,null,null),
           (1232, 'CTRL+DBLINK','',     '',  '','0.3 second'::interval,3,   null,null);
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
-- the first RLBK_TABLE has started, but the elapse of the step is not yet > estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '11 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '11 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't1';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - ((20.0 + 30.0)+(50.0 - 11.0) + 2*0.3/3) / (11.1 + (20.0 + 30.0)+(50.0 - 11.0) + 2*0.3/3); -- the completion % should be 11%
-- the first RLBK_TABLE is completed, and the step duration < the estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '45.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '45 seconds'::interval,
                                 rlbp_duration = '45 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't1';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - (20.0 + 30.0 + 2*0.3/3) / (45.1 + (20.0 + 30.0) + 2*0.3/3);  -- the completion % should be 47%
-- the second RLBK_TABLE has started, but the elapse of the step is not yet > estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '73.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '28 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't2';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - (20.0 + (30.0 - 28.0) + 1*0.3/3) / (73.1 + (20.0 + (30.0 - 28.0) + 1*0.3/3));  -- the completion % should be 77%
-- the second RLBK_TABLE has started, but the elapse of the step is already > estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '85.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '40 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't2';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - (20.0 + 1*0.3/3) / (85.1 + (20.0 + 1*0.3/3));  -- the completion % should be 81%
-- the second RLBK_TABLE has started, but the elapse of the step is already > estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '105.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '60 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't2';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - (20.0 + 1*0.3/3) / (105.1 + (20.0 + 1*0.3/3 ));  -- the completion % should be 84%
-- the second RLBK_TABLE is completed, and the step duration > the estimated duration 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '110.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '65 seconds'::interval,
                                 rlbp_duration = '65 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't2';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - (20.0 + 1*0.3/3) / (110.1 + (20.0 + 1*0.3/3));  -- the completion % should be 85%
-- the third RLBK_TABLE has started, and is almost completed 
  update emaj.emaj_time_stamp set time_tx_timestamp = now() - '124.1 seconds'::interval where time_id = -1;
  update emaj.emaj_rlbk_plan set rlbp_start_datetime = now() - '19 seconds'::interval
    where rlbp_rlbk_id = 1232 and rlbp_table = 't3';
  select rlbk_id, rlbk_status, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
  select 1.0 - ((20.0 - 19.0)) / (124.1 + (20.0 - 19.0));  -- the completion % should be 99%
rollback;
begin;
-- 1 rollback operation in LOCKING state and without step other than LOCK_TABLE
  insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-1, now()-'2 minutes'::interval);
  insert into emaj.emaj_time_stamp (time_id, time_clock_timestamp) values (-2, '2000-01-01 01:00:00');
  insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, 
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
    values (1233,array['group1233'],'mark1233',-2,-1,true,false,
             1,5,4,3,'LOCKING');
  insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
    values (1233, 'LOCK_TABLE','schema','t1','',null,null,null),
           (1233, 'LOCK_TABLE','schema','t2','',null,null,null),
           (1233, 'LOCK_TABLE','schema','t3','',null,null,null);
  select rlbk_id, rlbk_status, rlbk_elapse, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
-- the rollback operation in LOCKING state now has RLBK_TABLE steps
  insert into emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
             rlbp_estimated_duration, rlbp_start_datetime, rlbp_duration)
    values (1233, 'RLBK_TABLE','schema','t1','','0:20:00'::interval,null,null),
           (1233, 'RLBK_TABLE','schema','t2','','0:02:00'::interval,null,null),
           (1233, 'RLBK_TABLE','schema','t3','','0:00:20'::interval,null,null);
  select rlbk_id, rlbk_status, rlbk_elapse, rlbk_remaining, rlbk_completion_pct from emaj._rollback_activity();
-- +1 rollback operation in PLANNING state
  insert into emaj.emaj_time_stamp (time_id, time_tx_timestamp) values (-3, now()-'1 minute'::interval);
  insert into emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, 
             rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status)
    values (1234,array['group1234'],'mark1234',-2,-3,true,false,
             1,5,4,3,'PLANNING');
  select rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session, rlbk_nb_table,
         rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_elapse, rlbk_remaining, rlbk_completion_pct 
    from emaj._rollback_activity();
rollback;

-----------------------------
-- test emaj_consolidate_rollback_group() and emaj_get_consolidable_rollbacks()
-----------------------------
-- group is NULL or unknown
select emaj.emaj_consolidate_rollback_group(NULL,NULL);
select emaj.emaj_consolidate_rollback_group('unknownGroup',NULL);

-- mark is unknown
select emaj.emaj_consolidate_rollback_group('myGroup1',NULL);
select emaj.emaj_consolidate_rollback_group('myGroup1','unknown mark');

-- mark is known but is not an end rollback mark
select emaj.emaj_consolidate_rollback_group('myGroup1','Mark12');

-- mark is an end rollback mark but the rollback target mark is invalid
begin;
  update emaj.emaj_mark set mark_logged_rlbk_target_mark = 'dummy' where mark_name = 'Mark12';
  select emaj.emaj_consolidate_rollback_group('myGroup1','Mark12');
rollback;

-- should be ok
set search_path=public,myschema1;
select emaj.emaj_ignore_app_trigger('ADD', 'myschema1', 'mytbl2', 'mytbl2trg%');

-- rollback without log rows to delete
select emaj.emaj_set_mark_group('myGroup1','Conso_M1');
select emaj.emaj_logged_rollback_group('myGroup1','Conso_M1');
select emaj.emaj_consolidate_rollback_group('myGroup1','EMAJ_LAST_MARK');

-- mark Conso_M1, updates, mark Conso_M2, updates, logged rlbk back to Conso_M1, updates, rename both marks, consolidate, check and cancel
select emaj.emaj_set_mark_group('myGroup1','Conso_M2');
insert into myTbl1 select i, 'Test', 'Conso' from generate_series (1000,1011) as i;
insert into myTbl2 values (1000,'TC1',NULL);
delete from myTbl1 where col11 > 1010;

select emaj.emaj_set_mark_group('myGroup1','Conso_M3');
update myTbl2 set col22 = 'TC2' WHERE col22 ='TC1';

select emaj.emaj_logged_rollback_group('myGroup1','Conso_M2');
insert into myTbl2 values (1000,'TC3',NULL);

select emaj.emaj_rename_mark_group('myGroup1','Conso_M2','Renamed_conso_M2');
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','Renamed_last_mark');
select cons_group, cons_target_rlbk_mark_name, cons_target_rlbk_mark_time_id, 
       regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), cons_end_rlbk_mark_time_id, cons_rows, cons_marks 
  from emaj.emaj_get_consolidable_rollbacks();
  
select emaj.emaj_consolidate_rollback_group('myGroup1','Renamed_last_mark');
select cons_group, cons_target_rlbk_mark_name, cons_target_rlbk_mark_time_id, 
       regexp_replace(cons_end_rlbk_mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), cons_end_rlbk_mark_time_id, cons_rows, cons_marks
  from emaj.emaj_get_consolidable_rollbacks();

-- consolidate a rollback already consolidated
select emaj.emaj_consolidate_rollback_group('myGroup1','Renamed_last_mark');

select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence where sequ_schema = 'emaj_myschema1' order by sequ_time_id, sequ_schema, sequ_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole where sqhl_schema = 'myschema1' order by 1,2,3;

select emaj.emaj_rollback_group('myGroup1','Conso_M1');
select emaj.emaj_ignore_app_trigger('REMOVE', 'myschema1', 'mytbl2', '%');

-- consolidate a stopped (and empty) group
begin;
  select emaj.emaj_rename_mark_group('emptyGroup','EMAJ_LAST_MARK','end_rlbk_mark');
  select emaj.emaj_stop_group('emptyGroup','stop');
  select * from emaj.emaj_get_consolidable_rollbacks();
  select emaj.emaj_consolidate_rollback_group('emptyGroup','end_rlbk_mark');
  select * from emaj.emaj_mark where mark_group = 'emptyGroup' order by mark_time_id, mark_group;
rollback;


-- check that dropping the group deletes rows from emaj_sequence and emaj_seq_hole
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_drop_group('myGroup1');
  select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence where sequ_name like 'myschema1%' order by sequ_time_id, sequ_schema, sequ_name;
  select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole where sqhl_schema = 'myschema1' order by 1,2,3;
rollback;

-----------------------------
-- test emaj_cleanup_rollback_state()
-----------------------------
-- rollback a transaction with an E-Maj rollback to generate an ABORTED rollback event
begin;
  select emaj.emaj_rollback_group('myGroup4','myGroup4_start');
rollback;
select rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session from emaj.emaj_rlbk
  where rlbk_status in ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED') order by rlbk_id;
select emaj.emaj_cleanup_rollback_state();

-----------------------------
-- check rollback tables
-----------------------------
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_session, rlbk_nb_table,
       rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_begin_hist_id, rlbk_dblink_schema, rlbk_is_dblink_used,
       case when rlbk_end_datetime is null then 'null' else '[ts]' end as "end_datetime", rlbk_messages
  from emaj.emaj_rlbk order by rlbk_id;
select rlbs_rlbk_id, rlbs_session, 
       case when rlbs_end_datetime is null then 'null' else '[ts]' end as "end_datetime"
  from emaj.emaj_rlbk_session order by rlbs_rlbk_id, rlbs_session;
select rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id, rlbp_batch_number, rlbp_session,
       rlbp_object_def, rlbp_estimated_quantity, rlbp_estimate_method, rlbp_quantity
  from emaj.emaj_rlbk_plan order by rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object;
select rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id, rlbt_quantity from emaj.emaj_rlbk_stat
  order by rlbt_rlbk_id, rlbt_step, rlbt_schema, rlbt_table, rlbt_object;

-----------------------------
-- test end: reset history and force sequences id
-----------------------------
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 3000 order by time_id;
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist where hist_id >= 3000 order by hist_id) as t;

