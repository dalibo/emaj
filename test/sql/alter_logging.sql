-- alterLogging.sql : test groups structure changes with groups in LOGGING state
--
-- It follows alter.sql that tests the same functions but with groups in IDLE state.
-- It uses the same tables and groups, and the same sequences range
-- It includes the final checks for both alter.sql and alterLogging.sql scenarios

-- set sequence restart value
select public.handle_emaj_sequences(9000);

-- define and create the temp file directory to be used by the script
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`'/alter_logging'
\set EMAJTESTTMPDIR `echo $EMAJTESTTMPDIR`
\! mkdir -p $EMAJTESTTMPDIR

select emaj.emaj_remove_tables('myschema2', array['mytbl7','mytbl8']);
select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','Mk1');

-- Save the groups configuration
select emaj.emaj_export_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json');

-----------------------------
-- change attributes
-----------------------------

-- bad mark (duplicate name)
select emaj.emaj_modify_table('myschema1','mytbl1','{"priority":30}'::jsonb,emaj.emaj_get_previous_mark_group('myGroup1',now()));

-- change the priority
select emaj.emaj_modify_table('myschema1','mytbl1','{"priority":30}'::jsonb,'Priority Changed');

-- change the log data tablespace and the log index tablespace for different tables
set default_tablespace = tspemaj_renamed;
select emaj.emaj_modify_table('myschema1','mytbl2b','{"log_data_tablespace":null}'::jsonb,'Attributes_changed_1');
select emaj.emaj_modify_table('myschema2','mytbl6','{"log_index_tablespace":"tsplog1"}'::jsonb,'Attributes_changed_2');
reset default_tablespace;

-- change the triggers to ignore at rollback time
select emaj.emaj_modify_table('myschema1','mytbl2','{"ignored_triggers_profiles":["trg1", "trg2"]}');
select rel_schema, rel_tblseq, rel_time_range, rel_ignored_triggers from emaj.emaj_relation where rel_ignored_triggers is not null order by 1,2,3;

-- perform some operations: set an intermediate mark, update myTbl3 and rollback
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk2');
insert into myschema1."myTbl3" values (11, now(), 11.0);
update myschema2."myTbl3" set col33 = 11.0 where col31 = 10;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk2');

-- revert the properties changes
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1","myGroup2"}', true);

-- checks for attributes changes
select * from emaj.emaj_relation_change where rlchg_time_id >= 9000 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 9000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 9000 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(9100);

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
select emaj.emaj_assign_sequence('myschema2', 'myseq2', 'myGroup2', 'Add myseq2');

select group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup2';
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' order by rel_time_range;
select * from emaj.emaj_sequence where sequ_name = 'myseq2';

-- perform some other updates
select nextval('myschema2.myseq2');
select emaj.emaj_set_mark_group('myGroup2','After ADD_SEQ');
select nextval('myschema2.myseq2');
select nextval('myschema2.myseq2');

--testing snap and sql generation
\! mkdir $EMAJTESTTMPDIR/snap
select emaj.emaj_snap_group('myGroup2',:'EMAJTESTTMPDIR' || '/snap','');
\! cat $EMAJTESTTMPDIR/snap/myschema2_myseq2.snap
\! rm $EMAJTESTTMPDIR/snap/*

select emaj.emaj_snap_log_group('myGroup2','Mk1','After ADD_SEQ',:'EMAJTESTTMPDIR' || '/snap',NULL);
\! grep myseq2 $EMAJTESTTMPDIR/snap/myGroup2_sequences_at_Mk1
\! grep myseq2 $EMAJTESTTMPDIR/snap/myGroup2_sequences_at_After_ADD_SEQ
\! rm $EMAJTESTTMPDIR/snap/*

select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, :'EMAJTESTTMPDIR' || '/myFile');
\! grep myseq2 $EMAJTESTTMPDIR/myFile
select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_SEQ', NULL, :'EMAJTESTTMPDIR' || '/myFile',array['myschema2.myseq2']);
\! rm $EMAJTESTTMPDIR/myFile

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
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add myseq2';
  select emaj.emaj_delete_mark_group('myGroup2','Add myseq2');
  select * from emaj.emaj_sequence where sequ_name = 'myseq2' order by sequ_time_id limit 1;
rollback;

-- test sequences at bounds following a rollback targeting a mark set before an ADD_SEQ
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add myseq2';
-- beware, the sequence changes generated by the E-Maj rollback will not be rollback by the transaction rollback
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_rollback_group('myGroup2','Before ADD_SEQ',true);
  select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2';
rollback;

-- estimate and then perform a logged rollback to reach a mark prior the ADD_SEQ operation and check
delete from emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
select * from emaj.emaj_estimate_rollback_group('myGroup2','Before ADD_SEQ',true);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup2','Before ADD_SEQ',true);
select * from myschema2.myseq2;
select * from emaj.emaj_sequence where sequ_name = 'myseq2' order by sequ_time_id;

-- consolidate this logged rollback
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','Logged_Rlbk_End');
select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add myseq2';
select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup2'; -- should report 1 row to consolidate
select * from emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2' order by sequ_schema, sequ_name, sequ_time_id;

-- remove mySeq2 from the group, then re-add it
select emaj.emaj_remove_sequence('myschema2', 'myseq2', 'Remove myseq2');
select emaj.emaj_assign_sequence('myschema2', 'myseq2', 'myGroup2', 'Re-add myseq2');

select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' order by rel_time_range;

-- finaly remove mySeq2 from the group
select emaj.emaj_remove_sequence('myschema2', 'myseq2', 'Really remove myseq2');

-- and perform a full rollback (that do not process mySeq2)
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup2', 'ADD_SEQ test',true);
select * from myschema2.myseq2;

-- verify that all time range bounds of emaj_relation has its corresponding row into emaj_sequence
select * from emaj.emaj_sequence where sequ_schema = 'myschema2' and sequ_name = 'myseq2' and sequ_time_id in
  (select lower(rel_time_range) from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2' union 
   select upper(rel_time_range) from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'myseq2')
  order by sequ_time_id;

-- checks for add sequences test
select * from emaj.emaj_relation_change where rlchg_time_id >= 9100 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 9100 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 9100 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(9300);

-----------------------------
-- remove sequences
-----------------------------

select emaj.emaj_set_mark_group('myGroup1','Mk2b');
select emaj.emaj_set_mark_group('myGroup1','Before_remove');

select group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select emaj.emaj_remove_sequence('myschema1', 'myTbl3_col31_seq', 'Sequence_removed');
select group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
  from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'myTbl3_col31_seq' order by rel_time_range;
select * from emaj.emaj_verify_all();

--testing snap and sql generation
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR' || '/snap','');
\! ls $EMAJTESTTMPDIR/snap
\! rm -R $EMAJTESTTMPDIR/snap/*

select emaj.emaj_snap_log_group('myGroup1','Mk1',NULL,:'EMAJTESTTMPDIR' || '/snap',NULL);
-- sequences at begin bound
\! cat $EMAJTESTTMPDIR/snap/myGroup1_sequences_at_Mk1
-- sequences at end bound
\! find $EMAJTESTTMPDIR/snap -regex '.*/myGroup1_sequences_at_[0123456789].*' | xargs cat
\! rm $EMAJTESTTMPDIR/snap/*

select emaj.emaj_set_mark_group('myGroup1','After_remove');

-- testing sql script generation
select emaj.emaj_gen_sql_group('myGroup1', 'Mk1', 'EMAJ_LAST_MARK', :'EMAJTESTTMPDIR' || '/script.sql',array['myschema1.myTbl3_col31_seq']);
\! grep -v 'generated by E-Maj at' $EMAJTESTTMPDIR/script.sql
\! rm $EMAJTESTTMPDIR/script.sql

begin;
  -- testing the alter_group mark deletion
  select emaj.emaj_delete_mark_group('myGroup1','Sequence_removed');
  select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
    from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'myTbl3_col31_seq' order by rel_time_range;
  select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id,
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
    select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
    select emaj.emaj_reset_group('myGroup1');
    select count(*) from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range);
  rollback to svp1;
  -- testing marks deletion
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
    from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select count(*) from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range);
  -- testing the sequence drop
  drop sequence mySchema1."myTbl3_col31_seq" cascade;
rollback;

-- revert the changes
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1"}', true, 'Revert_sequences_removal');

select emaj.emaj_cleanup_rollback_state();

-- checks for remove sequences test
select * from emaj.emaj_relation_change where rlchg_time_id >= 9300 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 9300 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 9300 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(9500);

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
select emaj.emaj_assign_table('myschema2', 'mytbl7', 'myGroup2', null, 'Add mytbl7');

select group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup2';
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'mytbl7' order by rel_time_range;
-- perform some other updates
insert into myschema2.mytbl7 values (4),(5);

-- remove the alter group mark just after the alter operation and then get statistics and set a mark
begin;
  select emaj.emaj_delete_mark_group('myGroup2','Add mytbl7');
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
select emaj.emaj_snap_group('myGroup2',:'EMAJTESTTMPDIR' || '/snap','');
\! ls $EMAJTESTTMPDIR/snap
\! rm $EMAJTESTTMPDIR/snap/*
select emaj.emaj_snap_log_group('myGroup2','Mk1','After ADD_TBL',:'EMAJTESTTMPDIR' || '/snap',NULL);
\! ls $EMAJTESTTMPDIR/snap/myschema2*

select emaj.emaj_gen_sql_group('myGroup2', 'Before ADD_TBL', NULL, :'EMAJTESTTMPDIR' || '/script.sql',array['myschema2.mytbl7']);
\! rm $EMAJTESTTMPDIR/script.sql

-- testing mark deletions
begin;
  -- testing delete a single mark set after the table addition
  select emaj.emaj_delete_mark_group('myGroup2','After ADD_TBL');
  select 'Should not exist' from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'After ADD_TBL';
  -- testing delete all marks up to a mark set after the alter_group
  select emaj.emaj_delete_before_mark_group('myGroup2','EMAJ_LAST_MARK');
  select * from emaj.emaj_mark where mark_group = 'myGroup2';
  select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
    where tbl_name = 'mytbl7' order by tbl_time_id;
rollback;

begin;
  -- testing the add table mark deletion
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add mytbl7';
  select emaj.emaj_delete_mark_group('myGroup2','Add mytbl7');
  select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
    where tbl_name = 'mytbl7' order by tbl_time_id limit 1;
rollback;

-- test sequences and holes at bounds following a rollback targeting a mark set before an ADD_TBL
begin;
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add mytbl7';
  select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
    from emaj.emaj_rollback_group('myGroup2','Before ADD_TBL',true);
  select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
    where tbl_schema = 'myschema2' and tbl_name = 'mytbl7' order by tbl_time_id;
  select * from emaj.emaj_seq_hole where sqhl_schema = 'myschema2' and sqhl_table = 'mytbl7';
  select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_rows 
    from emaj._log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);
rollback;

-- estimate and then perform a logged rollback to reach a mark prior the ADD_TBL operation and check
delete from emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
select * from emaj.emaj_estimate_rollback_group('myGroup2','Before ADD_TBL',true);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup2','Before ADD_TBL',true);
select col71, emaj_verb, emaj_tuple from emaj_myschema2.mytbl7_log order by emaj_gid;
select * from myschema2.mytbl7 order by col71;

-- consolidate this logged rollback
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','Logged_Rlbk_End');
select mark_time_id from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'Add mytbl7';
select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup2'; -- should report 1 row to consolidate
select * from emaj.emaj_consolidate_rollback_group('myGroup2', 'Logged_Rlbk_End');
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
  where tbl_schema = 'myschema2' and tbl_name = 'mytbl7' order by tbl_time_id;

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
select emaj.emaj_remove_table('myschema2', 'mytbl7', 'Remove mytbl7');
select emaj.emaj_assign_table('myschema2', 'mytbl7', 'myGroup2', null, 'Re-add mytbl7');

select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq = 'mytbl7' order by rel_time_range;
delete from myschema2.myTbl7 where col71 = 9;

-- get statistics, using directly internal functions as done by web clients
select stat_group, stat_schema, stat_table, stat_log_schema, stat_log_table,
       stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_rows 
  from emaj._log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);
select stat_group, stat_schema, stat_table, stat_log_schema, stat_log_table,
       stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_role, stat_verb, stat_rows 
  from emaj._detailed_log_stat_groups('{"myGroup2"}',false,'ADD_TBL test',NULL);

-- snap the logs
select emaj.emaj_snap_log_group('myGroup2','Mk1',NULL,:'EMAJTESTTMPDIR' || '/snap',NULL);
\! ls $EMAJTESTTMPDIR/snap/myschema2_mytbl7_log*
\! rm $EMAJTESTTMPDIR/snap/*

-- and finaly revert the group configuration changes
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup2"}', true);

-- and perform a full rollback (that do not process myTbl7)
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_group('myGroup2', 'ADD_TBL test',true);
select * from myschema2.mytbl7 order by col71;

-- checks for add tables tests
select * from emaj.emaj_relation_change where rlchg_time_id >= 9500 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 9500 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 9500 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(9700);

-----------------------------
-- remove tables
-----------------------------
insert into myschema1."myTbl3" (col33) values (1.);
select emaj.emaj_set_mark_group('myGroup1','Mk2c');
insert into myschema1."myTbl3" (col33) values (1.);

select emaj.emaj_remove_tables('myschema1', '^(myTbl3|mytbl2b)$', null, '2 tables removed from myGroup1');

select group_name, group_last_alter_time_id, group_nb_table, group_nb_sequence
  from emaj.emaj_group where group_name = 'myGroup1';
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  from emaj.emaj_relation where rel_schema = 'myschema1' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'mytbl2b') order by 1,2,3;

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
  from emaj._log_stat_groups('{"myGroup1"}',false,'Mk1',NULL);
select stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_gid, stat_last_mark, stat_last_mark_gid, stat_role, stat_verb, stat_rows 
  from emaj._detailed_log_stat_groups('{"myGroup1"}',false,'Mk1',NULL);

--testing snap and sql generation
select emaj.emaj_snap_group('myGroup1',:'EMAJTESTTMPDIR' || '/snap','');
\! ls $EMAJTESTTMPDIR/snap
\! rm $EMAJTESTTMPDIR/snap/*

select emaj.emaj_snap_log_group('myGroup1','Mk1',NULL,:'EMAJTESTTMPDIR' || '/snap',NULL);
\! ls $EMAJTESTTMPDIR/snap/myschema1*

select emaj.emaj_gen_sql_group('myGroup1', 'Mk1', NULL, :'EMAJTESTTMPDIR' || '/script.sql',array['myschema1.myTbl3']);
\! rm $EMAJTESTTMPDIR/script.sql

-- test marks deletions
begin;
-- testing delete a single mark set before the table removal
  select emaj.emaj_delete_mark_group('myGroup1','Mk2c');
  select * from emaj.emaj_mark where mark_group = 'myGroup1' and mark_name = 'Mk2b';
-- testing marks deletion (delete all marks before the alter_group)
  select emaj.emaj_delete_before_mark_group('myGroup1','EMAJ_LAST_MARK');
  select 'should not exist' from pg_class, pg_namespace 
    where relnamespace = pg_namespace.oid and nspname = 'emaj_myschema1' and relname in ('myTbl3_log','mytbl2b_log');
  select count(*) from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq in ('myTbl3','mytbl2b');
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq in ('myTbl3','mytbl2b') order by 1,2,3;
rollback;

begin;
-- testing the alter group mark deletion
  select emaj.emaj_delete_mark_group('myGroup1','2 tables removed from myGroup1');
  select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    from emaj.emaj_relation where rel_schema = 'myschema1' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'mytbl2b') order by rel_tblseq;
  select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
    where tbl_name in ('mytbl2b', 'myTbl3') order by tbl_time_id desc, tbl_name limit 2;
  select * from emaj.emaj_mark where mark_group = 'myGroup1' and mark_name = '2 tables removed from myGroup1';
rollback;

begin;
-- testing marks deletion (other cases)
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq = 'mytbl2b' order by 1,2,3;
  select 'found' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log_1' and nspname = 'emaj_myschema1';
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log' and nspname = 'emaj_myschema1';
  select count(*) from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range);
  select * from emaj.emaj_rel_hist where relh_schema = 'myschema1' and relh_tblseq = 'mytbl2b' order by 1,2,3;
rollback;

-- testing rollback and consolidation
insert into myschema1.mytbl1 values (100, 'Alter_logg', E'\\000'::bytea);
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_group('myGroup1','Mk2b',true) order by 1,2;
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','Logged_Rlbk_End');
select emaj.emaj_remove_table('myschema1', 'mytbl1','2nd remove_tbl');

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
  select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
    from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2,3;
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'mytbl2b_log' and nspname = 'emaj_myschema1';
rollback;

-- testing the table drop (remove first the sequence linked to the table, otherwise an event triger fires)
begin;
  select emaj.emaj_remove_sequence('myschema1', 'myTbl3_col31_seq','remove_seq_before_drop_mytbl3');
  drop table mySchema1."myTbl3";
rollback;

-- testing an ALTER TABLE leading to a table rewrite
begin;
  alter table mySchema1."myTbl3" alter column col33 type decimal (10,1);
rollback;

-- re-add all removed tables
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', array['myGroup1'], true, 'Revert_tables_removal');

select emaj.emaj_cleanup_rollback_state();

-- set an intermediate mark
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk3');
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted,
       mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark
  from emaj.emaj_mark order by mark_time_id, mark_group;

-- estimate a rollback crossing alter group operations
delete from emaj.emaj_rlbk_stat;    -- to avoid unstable results in estimates
select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false);

-- execute a rollback not crossing any alter group operation
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk3',false) order by 1,2;

-- execute rollbacks crossing alter group operations
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2');
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',true) order by 1,2;

select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',true,'Rollback 2 groups crossing an alter group') order by 1,2;

-- execute additional rollback not crossing alter operations anymore
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;

-- checks for remove tables tests
select * from emaj.emaj_relation_change where rlchg_time_id >= 9700 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 9700 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 9700 order by hist_id;

-- set sequence restart value
select public.handle_emaj_sequences(10000);

-----------------------------
-- change the group ownership of a table and a sequence (and some other attributes)
-----------------------------

-- case when the source group is idle and the destination group is logging
begin;
  select emaj.emaj_stop_group('myGroup2');
  select emaj.emaj_move_table('myschema2', 'myTbl3', 'myGroup1', 'move table to myGroup1 while logging');
  select emaj.emaj_move_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup1', 'move seq to myGroup1 while logging');
  select emaj.emaj_modify_table('myschema2', 'myTbl3', '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1"}'::jsonb, 'modify table to myGroup1 while logging');
  select hist_function, hist_event, hist_object, hist_wording from emaj.emaj_hist 
    where hist_function like 'MO%' order by hist_id desc limit 10;
rollback;

-- case when both groups are logging
-- generates updates before the group change
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','before move');
update myschema2."myTbl3" set col32 = col32 + '1 day' where col31 >= 8;

select emaj.emaj_move_table('myschema2', 'myTbl3', 'myGroup1', 'move table to myGroup1');
select emaj.emaj_move_sequence('myschema2', 'myTbl3_col31_seq', 'myGroup1', 'move seq to myGroup1');
select emaj.emaj_modify_table('myschema2', 'myTbl3', '{"log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1", "priority":100}'::jsonb, 'modify table');
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
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script1.sql',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);

-- rollback to a mark set after the group change
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','after move',false,'Logged rollback on 2 groups');

-- rollback to a mark set before the group change
select rlbk_severity, regexp_replace(rlbk_message,E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g')
  from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','before move',true);

-- generate a sql script crossing the move (after rollback)
select emaj.emaj_gen_sql_group('myGroup1', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script2.sql',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);
select emaj.emaj_gen_sql_groups('{"myGroup1","myGroup2"}', 'before move', NULL, :'EMAJTESTTMPDIR' || '/script3.sql',
  array['myschema2.myTbl3','myschema2.myTbl3_col31_seq']);

\! rm $EMAJTESTTMPDIR/script*

-- revert the changes and apply with a destination group in idle state

select emaj.emaj_stop_group('myGroup2');
-- This first import try fails because a myGroup1 is missing
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', array['myGroup2'], true);
select emaj.emaj_import_groups_configuration(:'EMAJTESTTMPDIR' || '/all_groups_conf.json', '{"myGroup1","myGroup2"}', true);

-----------------------------
-- some extra tests with tables and sequences removal and addition
-----------------------------
select emaj.emaj_start_group('myGroup2','Start');
insert into myschema2.mytbl1 values (100, 'Started', E'\\000'::bytea);
select nextval('myschema2.myseq1');

-- remove and assign somes tables and sequences in a single transaction
begin;
  select emaj.emaj_remove_tables('myschema2','{"mytbl1","mytbl2","myTbl3"}'::text[],'REMOVE_3_TABLES');
  select emaj.emaj_remove_sequence('myschema2','myseq1','REMOVE_1_SEQUENCES');

  select emaj.emaj_assign_tables('myschema2','{"mytbl1","mytbl2","myTbl3"}'::text[],'myGroup2',null,'ASSIGN_3_TABLES');
  select emaj.emaj_assign_sequences('myschema2','{"myseq1","myseq2"}'::text[],'myGroup2','ASSIGN_2_SEQUENCES');
  insert into myschema2.mytbl1 values (110, 'Assigned', E'\\000'::bytea);
  select nextval('myschema2.myseq1');

  -- what if some (all but the emaj sequence) emaj components are missing at remove_table time ?
  select emaj.emaj_disable_protection_by_event_triggers();
  drop table emaj_myschema2.mytbl1_log;
  drop function emaj_myschema2.mytbl1_log_fnct() cascade;
  select emaj.emaj_enable_protection_by_event_triggers();
  select emaj.emaj_remove_tables('myschema2','{"mytbl1","mytbl2","myTbl3"}');
  select emaj.emaj_remove_sequences('myschema2','{"myseq1","myseq2"}');
  select emaj.emaj_assign_tables('myschema2','{"mytbl1","mytbl2","myTbl3"}','myGroup2',
                                 '{"priority":1, "log_data_tablespace":"tsplog1", "log_index_tablespace":"tsplog1"}'::jsonb);
  select emaj.emaj_assign_sequences('myschema2','{"myseq1","myseq2"}'::text[],'myGroup2','Mark_assign_sequences');
commit;

-- remove tables and sequences with bad marks
select emaj.emaj_remove_table('myschema2','mytbl1','Mark_assign_sequences');
select emaj.emaj_remove_sequence('myschema2','myseq1','Mark_assign_sequences');

-- alter a table in a logging group
select emaj.emaj_remove_table('myschema1','mytbl4');
alter table myschema1.mytbl4 alter column col45 type varchar(15);
select emaj.emaj_assign_table('myschema1','mytbl4','myGroup1');

begin;
  select emaj.emaj_remove_table('myschema1','mytbl4');
  alter table myschema1.mytbl4 alter column col45 type varchar(10);
  select emaj.emaj_assign_table('myschema1','mytbl4','myGroup1',NULL,'Assign_mytbl4');
commit;

-- checks
select count(*) from emaj.emaj_mark where mark_group = 'myGroup2' and (mark_name like 'REMOVE%' or mark_name like 'ASSIGN%');
select rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_log_schema, rel_log_table, rel_log_seq_last_value
  from emaj.emaj_relation where rel_schema = 'myschema2' and rel_tblseq in ('mytbl1','mytbl2','myTbl3','myseq1','myseq2')
  order by rel_schema, rel_tblseq, rel_time_range;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence
  where sequ_schema = 'myschema2' and sequ_name like 'myseq%' order by sequ_name, sequ_time_id;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table
  where tbl_schema = 'myschema2' and tbl_name = 'mytbl1' order by tbl_time_id;

-- referential integrity violation attempt while rollbacking to a mark set before an ADD_TBL with a FK involved in the operation.
insert into myschema1.mytbl2 values (1000,'a',current_date);
insert into myschema1.mytbl4 values (100,'b',1000,2,'ABC');
begin;
  select emaj.emaj_remove_table('myschema1','mytbl4','Remove_mytbl4');
  select emaj.emaj_assign_table('myschema1','mytbl4','myGroup1',NULL,'Reassign_mytbl4');
-- deactivate the dblink usage to avoid deadlock, the assign_table and the rollback being executed in the same transaction,
--   they conflict on the emaj_myschema1.mytbl4_log_seq log sequence
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
-- the rollback should fail at FK check
  select * from emaj.emaj_rollback_group('myGroup1',emaj.emaj_get_previous_mark_group('myGroup1','Remove_mytbl4'),true);
rollback;
select * from emaj.emaj_rollback_group('myGroup1','EMAJ_LAST_MARK');

-----------------------------
-- some extra tests with group ownership change for some tables and sequences
-----------------------------
-- from an idle to a logging group
select emaj.emaj_move_tables('myschema4','.*','','myGroup1','MOVE_TBL_idle_to_logging');
select emaj.emaj_move_sequences('myschema2','.*','','myGroup1','MOVE_SEQ_idle_to_logging');

-- bad mark
select emaj.emaj_move_tables('myschema4','.*','','myGroup2',emaj.emaj_get_previous_mark_group('myGroup1',now()));
select emaj.emaj_move_sequences('myschema2','.*','','myGroup2',emaj.emaj_get_previous_mark_group('myGroup1',now()));

-- from a logging group to another logging group
select emaj.emaj_move_tables('myschema4','.*','','myGroup2','MOVE_TBL_logging_to_logging');
select emaj.emaj_move_sequences('myschema2','.*','','myGroup2','MOVE_SEQ_logging_to_logging');

-- reset an idle group to check the emaj_sequence cleaning
begin;
  select emaj.emaj_reset_group('myGroup4');
  select rel_group, count(*) from emaj.emaj_sequence, emaj.emaj_relation
    where sequ_schema = rel_schema and sequ_name = rel_tblseq and sequ_time_id <@ rel_time_range
      and sequ_schema = 'myschema2' and sequ_name = 'myseq1'
    group by 1;
rollback;

-- and back to the idle group
select emaj.emaj_move_tables('myschema4','.*','','myGroup4','MOVE_TBL_logging_to_idle');
select emaj.emaj_move_sequences('myschema2','.*','','myGroup4','MOVE_SEQ_logging_to_idle');

-- move a table with fkey and verify all
select emaj.emaj_move_table('myschema2','mytbl2','myGroup1','Move a table with fkey');
select * from emaj.emaj_verify_all();
select emaj.emaj_move_table('myschema2','mytbl2','myGroup2','Move a table with fkey back');

-- rename a table and/or change its schema
alter table myschema1.mytbl1 rename to mytbl1_new_name;
select emaj.emaj_remove_table('myschema1','mytbl1');
select emaj.emaj_assign_table('myschema1','mytbl1_new_name','myGroup1');

alter table myschema1.mytbl1_new_name set schema public;
select emaj.emaj_remove_table('myschema1','mytbl1_new_name');
select emaj.emaj_assign_table('public','mytbl1_new_name','myGroup1');

alter table public.mytbl1_new_name rename to mytbl1;
alter table public.mytbl1 set schema myschema1;
select emaj.emaj_remove_table('public','mytbl1_new_name');
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1');

-----------------------------
-- test end: check
-----------------------------
select emaj.emaj_stop_group('myGroup1');
select emaj.emaj_drop_group('myGroup1');

select emaj.emaj_stop_group('myGroup2');
select emaj.emaj_drop_group('myGroup2');

select emaj.emaj_drop_group('myGroup4');
select emaj.emaj_drop_group('myGroup5');

-- checks for group ownership change tests
select * from emaj.emaj_relation_change where rlchg_time_id >= 10000 order by 1,2,3,4;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 10000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id >= 10000 order by hist_id;

select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select sch_name from emaj.emaj_schema order by 1;
select * from emaj.emaj_rel_hist order by 1,2,3;

select rlbk_id, rlbk_status, rlbk_comment from emaj.emaj_rlbk where rlbk_comment is not null order by 1;

truncate emaj.emaj_hist;

-- remove the temp directory
\! rm -R $EMAJTESTTMPDIR
