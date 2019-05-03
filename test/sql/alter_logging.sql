-- alterLogging.sql : test emaj_alter_group() and emaj_alter_groups() functions with groups in LOGGING state
--
-- It follows alter.sql that tests the same functions but with groups in IDLE state.
-- It uses the same tables and groups, and the same sequences range
-- It includes the final checks for both alter.sql and alterLogging.sql scenarios

select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','Mk1');

-----------------------------
-- change attributes in emaj_group_def
-----------------------------

-- change the priority
update emaj.emaj_group_def set grpdef_priority = 30 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  -- also test the internal _adjust_group_properties() function, by simulating wrong values for group_has_waiting_changes
update emaj.emaj_group set group_has_waiting_changes = true where group_name = 'emptyGroup';
update emaj.emaj_group set group_has_waiting_changes = false where group_name = 'myGroup1';
select emaj._adjust_group_properties();
select group_name, group_has_waiting_changes from emaj.emaj_group where group_name = 'myGroup1' or group_name = 'emptyGroup' order by 1;

select emaj.emaj_alter_group('myGroup1','Priority Changed');

-- change the log data tablespace and the log index tablespace for different tables
update emaj.emaj_group_def set grpdef_log_dat_tsp = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = 'tsplog1' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
set default_tablespace = tspemaj_renamed;
update emaj.emaj_group set group_has_waiting_changes = false where group_name = 'myGroup1';
select emaj._adjust_group_properties();
select group_has_waiting_changes from emaj.emaj_group where group_name = 'myGroup1';

select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','Attributes_changed');
reset default_tablespace;

-- perform some operations: set an intermediate mark, update myTbl3 and rollback
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk2');
insert into myschema1."myTbl3" values (11, now(), 11.0);
update myschema2."myTbl3" set col33 = 11.0 where col31 = 10;
select emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk2');

-- change the priority back
update emaj.emaj_group_def set grpdef_priority = 20 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_groups(array['myGroup1','myGroup2']);

-- change the other attributes back
update emaj.emaj_group_def set grpdef_log_dat_tsp = 'tsp log''2' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = NULL where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');

-----------------------------
-- add sequences
-----------------------------

-- add myschema2.mySeq2 to the myGroup2 group
-- first perform changes and set marks
select emaj.emaj_set_mark_group('myGroup2','ADD_SEQ test');
select nextval('myschema2.myseq2');
select emaj.emaj_set_mark_group('myGroup2','Before ADD_SEQ');
select nextval('myschema2.myseq2');

-- then add the sequence to the group
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq2');

update emaj.emaj_group set group_has_waiting_changes = false where group_name = 'myGroup2';
select emaj._adjust_group_properties();

select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup2';
select emaj.emaj_alter_group('myGroup2', 'Alter to add myseq2');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup2';
select * from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' order by rel_time_range;
select * from emaj.emaj_sequence where sequ_name = 'myseq2';

-- perform some other updates
select nextval('myschema2.myseq2');
select emaj.emaj_set_mark_group('myGroup2','After ADD_SEQ');
select nextval('myschema2.myseq2');
select nextval('myschema2.myseq2');

--testing snap and sql generation
\! mkdir -p /tmp/emaj_test/alter
\! rm -Rf /tmp/emaj_test/alter/*
  select emaj.emaj_snap_group('myGroup2','/tmp/emaj_test/alter','');
\! cat /tmp/emaj_test/alter/myschema2_myseq2.snap
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_snap_log_group('myGroup2',NULL,'After ADD_SEQ','/tmp/emaj_test/alter',NULL);
\! grep myseq2 /tmp/emaj_test/alter/myGroup2_sequences_at_Mk1
\! grep myseq2 /tmp/emaj_test/alter/myGroup2_sequences_at_After_ADD_SEQ

\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, '/tmp/emaj_test/alter/myFile');
\! grep myseq2 /tmp/emaj_test/alter/myFile
  select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, '/tmp/emaj_test/alter/myFile',array['myschema2.myseq2']);
\! rm -R /tmp/emaj_test/alter/*

-- testing mark deletions
begin;
  -- testing delete a single mark set after the sequence addition
  select emaj.emaj_delete_mark_group('myGroup2','After ADD_SEQ');
  select 'Should not exist' from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'After ADD_SEQ';
  -- testing delete all marks up to a mark set after the alter_group
  select emaj.emaj_delete_before_mark_group('myGroup2','EMAJ_LAST_MARK');
  select * from emaj.emaj_mark where mark_group = 'myGroup2';
  select * from emaj.emaj_sequence where sequ_name = 'myseq2' order by sequ_time_id;
rollback;

begin;
  -- testing the alter_group mark deletion
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add myseq2';
  select emaj.emaj_delete_mark_group('myGroup2','Alter to add myseq2');
  select * from emaj.emaj_sequence where sequ_name = 'myseq2' order by sequ_time_id limit 1;
rollback;

-- test sequences at bounds following a rollback targeting a mark set before an ADD_SEQ
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add myseq2';
-- beware, the sequence changes generated by the E-Maj rollback will not be rollback by the transaction rollback
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_rollback_group('myGroup2','Before ADD_SEQ',true);
  select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2';
rollback;

-- estimate and then perform a logged rollback to reach a mark prior the ADD_SEQ operation and check
select * from emaj.emaj_estimate_rollback_group('myGroup2','Before ADD_SEQ',true);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup2','Before ADD_SEQ',true);
select * from myschema2.myseq2;
select * from emaj.emaj_sequence where sequ_name = 'myseq2' order by sequ_time_id;

-- consolidate this logged rollback
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','Logged_Rlbk_End');
select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add myseq2';
select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup2'; -- should report 1 row to consolidate
select * from emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2' order by sequ_schema, sequ_name, sequ_time_id;

-- remove mySeq2 from the group, then re-add it
delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myseq2';
select emaj.emaj_alter_group('myGroup2', 'Alter to remove myseq2');

insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq2');
select emaj.emaj_alter_group('myGroup2', 'Alter to re-add myseq2');
select * from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' order by rel_time_range;

-- finaly remove mySeq2 from the group
delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myseq2';
select emaj.emaj_alter_group('myGroup2', 'Alter to really remove myseq2');

-- and perform a full rollback (that do not process mySeq2)
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup2', 'ADD_SEQ test',true);
select * from myschema2.myseq2;

-- verify that all time range bounds of emaj_relation has its corresponding row into emaj_sequence
select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2' and sequ_time_id in
  (select lower(rel_time_range) from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' union 
   select upper(rel_time_range) from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2')
  order by sequ_time_id;

-----------------------------
-- remove sequences
-----------------------------

select emaj.emaj_set_mark_group('myGroup1','Mk2b');
select emaj.emaj_set_mark_group('myGroup1','Before_remove');

update emaj.emaj_group_def set grpdef_group = 'temporarily_removed' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_alter_group('myGroup1','Sequence_removed');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select * from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'myTbl3_col31_seq' order by rel_time_range;
select * from emaj.emaj_verify_all();

--testing snap and sql generation
\! rm -Rf /tmp/emaj_test/alter/*
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/alter','');
\! ls /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter/*
select emaj.emaj_snap_log_group('myGroup1','Mk1',NULL,'/tmp/emaj_test/alter',NULL);

-- sequences at begin bound
\! cat /tmp/emaj_test/alter/myGroup1_sequences_at_Mk1
-- sequences at end bound
\! find /tmp/emaj_test/alter/ -regex '.*/myGroup1_sequences_at_[0123456789].*' | xargs cat
\! rm -R /tmp/emaj_test/alter/*

select emaj.emaj_set_mark_group('myGroup1','After_remove');

-- testing sql script generation
select emaj.emaj_gen_sql_group('myGroup1', NULL, 'EMAJ_LAST_MARK', '/tmp/emaj_test/alter/myFile',array['myschema1.myTbl3_col31_seq']);
\! grep -v 'generated by E-Maj at' /tmp/emaj_test/alter/myFile
\! rm -R /tmp/emaj_test/alter/*

begin;
  -- testing the alter_group mark deletion
  select emaj.emaj_delete_mark_group('myGroup1','Sequence_removed');
  select * from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'myTbl3_col31_seq' order by rel_time_range;
  select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id,
    mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
    from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id desc limit 2;
rollback;

begin;
  -- testing delete a single mark set before the sequence removal
  select emaj.emaj_delete_mark_group('myGroup1','Before_remove');
  select * from emaj.emaj_sequence where sequ_schema = 'myschema1'
    and sequ_time_id not in (select distinct mark_time_id from emaj.emaj_mark where mark_group = 'myGroup1');
  -- testing rollback
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_logged_rollback_group('myGroup1','Mk2b',true) order by 1,2;
  select * from emaj.emaj_rollback_group('myGroup1','Mk2b',true) order by 1,2;
  savepoint svp1;
  -- testing group's reset
    select emaj.emaj_stop_group('myGroup1');
    select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
    select emaj.emaj_reset_group('myGroup1');
    select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  rollback to svp1;
  -- testing marks deletion
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  -- testing the sequence drop
  drop sequence mySchema1."myTbl3_col31_seq" cascade;
rollback;

-- re-add the removed sequence
update emaj.emaj_group_def set grpdef_group = 'myGroup1' where grpdef_group = 'temporarily_removed';
select emaj.emaj_alter_group('myGroup1', 'sequence re-added to myGroup1');

select emaj.emaj_cleanup_rollback_state();

-----------------------------
-- add tables
-----------------------------

-- add myschema2.mytbl7 to the myGroup2 group
-- first perform updates and set marks
select emaj.emaj_set_mark_group('myGroup2','ADD_TBL test');
insert into myschema2.mytbl7 values (1),(2),(3);
select emaj.emaj_set_mark_group('myGroup2','Before ADD_TBL');
delete from myschema2.mytbl7 where col71 = 3;

-- then add the table to the group
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl7');
select emaj.emaj_alter_group('myGroup2', 'Alter to add mytbl7');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup2';
select * from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'mytbl7' order by rel_time_range;
-- perform some other updates
insert into myschema2.mytbl7 values (4),(5);

-- remove the alter group mark just after the alter operation and then get statistics and set a mark
begin;
  select emaj.emaj_delete_mark_group('myGroup2','Alter to add mytbl7');
  select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows 
    from emaj.emaj_log_stat_group('myGroup2','Before ADD_TBL',NULL);
  select emaj.emaj_set_mark_group('myGroup2','Just after ADD_TBL');
  select mark_log_rows_before_next from emaj.emaj_mark where mark_name = 'Before ADD_TBL';
rollback;

-- set marks and look at statistics
select emaj.emaj_set_mark_group('myGroup2','Just after ADD_TBL');
update myschema2.mytbl7 set col71 = 6 where col71 = 2;
select col71, emaj_verb, emaj_tuple from emaj_myschema2.mytbl7_log order by emaj_gid;
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_group('myGroup2','ADD_TBL test',NULL);
select emaj.emaj_set_mark_group('myGroup2','After ADD_TBL');
select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_role, stat_verb, stat_rows
  from emaj.emaj_detailed_log_stat_group('myGroup2','ADD_TBL test','After ADD_TBL');

--testing snap and sql generation
\! rm -Rf /tmp/emaj_test/alter/*
  select emaj.emaj_snap_group('myGroup2','/tmp/emaj_test/alter','');
\! ls /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_snap_log_group('myGroup2',NULL,'After ADD_TBL','/tmp/emaj_test/alter',NULL);
\! ls /tmp/emaj_test/alter/myschema2*
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_TBL', NULL, '/tmp/emaj_test/alter/myFile',array['myschema2.mytbl7']);

-- testing mark deletions
begin;
  -- testing delete a single mark set after the table addition
  select emaj.emaj_delete_mark_group('myGroup2','After ADD_TBL');
  select 'Should not exist' from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'After ADD_TBL';
  -- testing delete all marks up to a mark set after the alter_group
  select emaj.emaj_delete_before_mark_group('myGroup2','EMAJ_LAST_MARK');
  select * from emaj.emaj_mark where mark_group = 'myGroup2';
  select * from emaj.emaj_sequence where sequ_name = 'mytbl7_log_seq' order by sequ_time_id;
rollback;

begin;
  -- testing the alter_group mark deletion
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add mytbl7';
  select emaj.emaj_delete_mark_group('myGroup2','Alter to add mytbl7');
  select * from emaj.emaj_sequence where sequ_name = 'mytbl7_log_seq' order by sequ_time_id limit 1;
rollback;

-- test sequences and holes at bounds following a rollback targeting a mark set before an ADD_TBL
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add mytbl7';
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_rollback_group('myGroup2','Before ADD_TBL',true);
  select * from emaj.emaj_sequence where sequ_schema = 'emaj_myschema2' and sequ_name = 'mytbl7_log_seq' order by sequ_time_id;
  select * from emaj.emaj_seq_hole where sqhl_schema = 'myschema2' and sqhl_table = 'mytbl7';
  select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_rows 
    from emaj._log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);
rollback;

-- estimate and then perform a logged rollback to reach a mark prior the ADD_TBL operation and check
select * from emaj.emaj_estimate_rollback_group('myGroup2','Before ADD_TBL',true);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup2','Before ADD_TBL',true);
select col71, emaj_verb, emaj_tuple from emaj_myschema2.mytbl7_log order by emaj_gid;
select * from myschema2.mytbl7 order by col71;

-- consolidate this logged rollback
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','Logged_Rlbk_End');
select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Alter to add mytbl7';
select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup2'; -- should report 1 row to consolidate
select * from emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
select * from emaj.emaj_sequence where sequ_schema = 'emaj_myschema2' and sequ_name = 'mytbl7_log_seq' order by sequ_time_id;

-- perform some other updates
insert into myschema2.myTbl7 values (7),(8),(9);

-- verify the group and detect the lack of the new log table
begin;
  select emaj.emaj_disable_protection_by_event_triggers();
  drop table emaj_myschema2.mytbl7_log;
  select * from emaj.emaj_verify_all();
  select emaj.emaj_enable_protection_by_event_triggers();
rollback;

-- remove myTbl7 from the group, then re-add it
delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl7';
select emaj.emaj_alter_group('myGroup2', 'Alter to remove mytbl7');

insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl7');
select emaj.emaj_alter_group('myGroup2', 'Alter to re-add mytbl7');
select * from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'mytbl7' order by rel_time_range;
delete from myschema2.myTbl7 where col71 = 9;

-- get statistics, using directly internal functions as done by web clients
select stat_group, stat_schema, stat_table, stat_log_schema, stat_log_table,
       stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_rows 
  from emaj._log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);
select stat_group, stat_schema, stat_table, stat_log_schema, stat_log_table,
       stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_role, stat_verb, stat_rows 
  from emaj._detailed_log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);

-- snap the logs
select emaj.emaj_snap_log_group('myGroup2',NULL,NULL,'/tmp/emaj_test/alter',NULL);
\! ls /tmp/emaj_test/alter/myschema2_mytbl7_log*
\! rm -R /tmp/emaj_test/alter/*

-- and finaly remove myTbl7 from the group
delete from emaj.emaj_group_def where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl7';
select emaj.emaj_alter_group('myGroup2', 'Alter to really remove mytbl7');

-- and perform a full rollback (that do not process myTbl7)
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup2', 'ADD_TBL test',true);
select * from myschema2.mytbl7 order by col71;


-----------------------------
-- remove tables
-----------------------------
insert into myschema1."myTbl3" (col33) values (1.);
select emaj.emaj_set_mark_group('myGroup1','Mk2c');
insert into myschema1."myTbl3" (col33) values (1.);

update emaj.emaj_group_def set grpdef_group = 'temporarily_removed'
  where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and (grpdef_tblseq = 'myTbl3' or grpdef_tblseq = 'mytbl2b');

update emaj.emaj_group set group_has_waiting_changes = false where group_name = 'myGroup1';
select emaj._adjust_group_properties();
select group_has_waiting_changes from emaj.emaj_group where group_name = 'myGroup1';

select emaj.emaj_alter_group('myGroup1', '2 tables removed from myGroup1');
select group_name, group_last_alter_time_id, group_has_waiting_changes, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select * from emaj.emaj_relation where rel_schema = 'myschema1' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'mytbl2b') order by 1,2,3;

delete from myschema1."myTbl3" where col33 = 1.;
select count(*) from emaj_myschema1."myTbl3_log_1";
select * from emaj.emaj_verify_all();

-- test statistics following a rollback targeting a mark set before a REMOVE_TBL
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_rollback_group('myGroup1','Mk2c',true);
  select stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
    from emaj.emaj_log_stat_group('myGroup1','Mk2b',NULL) order by 1,2,3,4;
rollback;

-- get statistics, using directly internal functions as done by web clients
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_rows 
  from emaj._log_stat_groups('{"myGroup1"}',false,NULL,NULL);
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_role, stat_verb, stat_rows 
  from emaj._detailed_log_stat_groups('{"myGroup1"}',false,NULL,NULL);

--testing snap and sql generation
\! rm -Rf /tmp/emaj_test/alter/*
select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/alter','');
\! ls /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter/*
select emaj.emaj_snap_log_group('myGroup1',NULL,NULL,'/tmp/emaj_test/alter',NULL);
\! ls /tmp/emaj_test/alter/myschema1*
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/emaj_test/alter/myFile',array['myschema1.myTbl3']);
\! rm -R /tmp/emaj_test/alter/*

-- test marks deletions
begin;
-- testing delete a single mark set before the table removal
  select emaj.emaj_delete_mark_group('myGroup1','Mk2c');
  select * from emaj.emaj_mark where mark_group = 'myGroup1' and mark_name = 'Mk2b';
-- testing marks deletion (delete all marks before the alter_group)
  select emaj.emaj_delete_before_mark_group('myGroup1','EMAJ_LAST_MARK');
  select 'should not exist' from pg_class, pg_namespace 
    where relnamespace = pg_namespace.oid and nspname = 'emaj_myschema1' and relname in ('myTbl3_log','mytbl2b_log');
  select * from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq in ('myTbl3','mytbl2b') order by rel_time_range;
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq in ('myTbl3','mytbl2b') order by 1,2,3;
rollback;

begin;
-- testing the alter_group mark deletion
  select mark_group, mark_name, mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next,
         mark_logged_rlbk_target_mark
    from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id desc limit 2;
    
  select emaj.emaj_delete_mark_group('myGroup1','2 tables removed from myGroup1');
  select * from emaj.emaj_relation where rel_schema = 'myschema1' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'mytbl2b') order by rel_tblseq;
  select * from emaj.emaj_sequence where 
    sequ_name in ('mytbl2b_log_seq', 'myTbl3_log_seq') order by sequ_time_id desc, sequ_name limit 2;
  select * from emaj.emaj_mark where mark_group = 'myGroup1' and mark_name = '2 tables removed from myGroup1';
rollback;

begin;
-- testing marks deletion (other cases)
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq = 'mytbl2b' order by 1,2,3;
  select 'found' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log_1' and nspname = 'emaj_myschema1';
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log' and nspname = 'emaj_myschema1';
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq = 'mytbl2b' order by 1,2,3;
rollback;

-- testing rollback and consolidation
insert into myschema1.mytbl1 values (100, 'Alter_logg', E'\\000'::bytea);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup1','Mk2b',true) order by 1,2;
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','Logged_Rlbk_End');
update emaj.emaj_group_def set grpdef_group = 'temporarily_removed'
  where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_group('myGroup1','2nd remove_tbl');
select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup1';
select * from emaj.emaj_consolidate_rollback_group('myGroup1', 'Logged_Rlbk_End');
select count(*) from emaj_myschema1.mytbl1_log_1;   -- the log table should be empty
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup1','Mk2b',true) order by 1,2;

-- testing group's reset
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_reset_group('myGroup1');
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and upper(relh_time_range) > 6050 order by 1,2,3;
rollback;

-- testing group's stop and start
begin;
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_start_group('myGroup1');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log' and nspname = 'emaj_myschema1';
rollback;

-- testing the table drop (remove first the sequence linked to the table, otherwise an event triger fires)
begin;
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_alter_group('myGroup1','alter_before_drop_mytbl3');
  drop table mySchema1."myTbl3";
rollback;

-- re-add all removed tables
update emaj.emaj_group_def set grpdef_group = 'myGroup1' where grpdef_group = 'temporarily_removed';
select emaj.emaj_alter_group('myGroup1', 'tables re-added to myGroup1');

select emaj.emaj_cleanup_rollback_state();

-- set an intermediate mark
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk3');
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark order by mark_time_id, mark_group;

-- estimate a rollback crossing alter group operations
delete from emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false);

-- execute a rollback not crossing any alter group operation
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk3',false) order by 1,2;

-- execute rollbacks crossing alter group operations
select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2');
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',true) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',true) order by 1,2;

-- execute additional rollback not crossing alter operations anymore
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;

-- empty logging groups
begin;
  delete from emaj.emaj_group_def where grpdef_group IN ('myGroup1','myGroup2');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','empty groups');
  select * from emaj.emaj_relation where rel_group IN ('myGroup1','myGroup2') order by rel_schema, rel_tblseq, rel_time_range;
-- add one table and sequence to the empty groups
  insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
-- TODO: uncomment the next line once it will be possible to add a sequence to a logging group
--  insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','add a single table');
  select * from emaj.emaj_relation
    where (rel_schema = 'myschema1' and rel_tblseq = 'mytbl1') or (rel_schema = 'myschema2' and rel_tblseq = 'myseq1')
    order by rel_schema, rel_tblseq, rel_time_range;
rollback;

-----------------------------
-- change the group ownership of a table and a sequence (and some other attributes)
-----------------------------

update emaj.emaj_group_def set grpdef_group = 'myGroup1'
  where grpdef_group = 'myGroup2' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3_col31_seq';
update emaj.emaj_group_def set grpdef_group = 'myGroup1', grpdef_log_dat_tsp = 'tsplog1', grpdef_log_idx_tsp = 'tsplog1'
  where grpdef_group = 'myGroup2' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';

-- case when the source group is idle and the destination group is logging
begin;
  select emaj.emaj_stop_group('myGroup2');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}', 'move to myGroup1 while logging');
  select * from emaj.emaj_alter_plan where altr_time_id = (select max(altr_time_id) from emaj.emaj_alter_plan) order by 1,2,3,4,5;
  select hist_function, hist_event, hist_object, hist_wording from emaj.emaj_hist 
    where hist_function = 'ALTER_GROUPS' order by hist_id desc limit 8;
rollback;

-- case when both groups are logging
-- should not work, because myGroup2 is missing in the groups list and both groups are in LOGGING state
select emaj.emaj_alter_group('myGroup1', 'move to myGroup1');

-- should be OK
-- generates updates before the group change
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','before move');
update myschema2."myTbl3" set col32 = col32 + '1 day' where col31 >= 8;

select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}', 'move to myGroup1');
select * from emaj.emaj_relation where rel_schema = 'myschema2' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'myTbl3_col31_seq') order by 1,2,3,4;

-- perform various tasks on the groups
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','after move');
update myschema2."myTbl3" set col33 = 12.0 where col31 >= 9;
select  stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_rows
  from emaj.emaj_log_stat_groups('{"myGroup1","myGroup2"}', 'before move', NULL)
  where stat_schema = 'myschema2' and stat_table = 'myTbl3';
select  stat_group, stat_schema, stat_table, stat_first_mark, stat_last_mark, stat_verb, stat_role, stat_rows
  from emaj.emaj_detailed_log_stat_groups('{"myGroup1","myGroup2"}', 'before move', NULL) where stat_schema = 'myschema2' and stat_table = 'myTbl3';

-- generate a sql script crossing the move (before rollback)
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'before move', NULL, '/tmp/emaj_test/alter/myFile',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);
\! rm /tmp/emaj_test/alter/*

-- rollback to a mark set after the group change
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','after move');

-- rollback to a mark set before the group change
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','before move',true);

-- generate a sql script crossing the move (after rollback)
select emaj.emaj_gen_sql_group('myGroup1', 'before move', NULL, '/tmp/emaj_test/alter/myFile',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'before move', NULL, '/tmp/emaj_test/alter/myFile',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);
\! rm -R /tmp/emaj_test

-- revert the emaj_group_def change and apply with a destination group in idle state
update emaj.emaj_group_def set grpdef_group = 'myGroup2'
  where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3_col31_seq';
update emaj.emaj_group_def set grpdef_group = 'myGroup2', grpdef_log_dat_tsp = NULL, grpdef_log_idx_tsp = NULL
  where grpdef_group = 'myGroup1' and grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
select emaj.emaj_stop_group('myGroup2');
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}', 'back to idle myGroup2');

-----------------------------
-- test end: check
-----------------------------

select emaj.emaj_force_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup2');
select emaj.emaj_force_drop_group('myGroup4');
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select sch_name from emaj.emaj_schema order by 1;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id > 6000 order by hist_id;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id > 6000 order by time_id;
select * from emaj.emaj_alter_plan order by 1,2,3,4,5;

truncate emaj.emaj_hist;
