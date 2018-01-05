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
select emaj.emaj_alter_group('myGroup1','Priority Changed');

-- change the emaj names prefix, the log schema, the log data tablespace and the log index tablespace for different tables
update emaj.emaj_group_def set grpdef_log_schema_suffix = NULL where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_emaj_names_prefix = 's1t3' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_log_dat_tsp = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = 'tsplog1' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
set default_tablespace = tspemaj_renamed;
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}','Attributes_changed');
reset default_tablespace;

-- set an intermediate mark
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk2');

-- change the priority back
update emaj.emaj_group_def set grpdef_priority = 20 where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
select emaj.emaj_alter_groups(array['myGroup1','myGroup2']);

-- change the other attributes back
update emaj.emaj_group_def set grpdef_log_schema_suffix = 'C' where grpdef_schema = 'myschema2' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_emaj_names_prefix = NULL where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3';
update emaj.emaj_group_def set grpdef_log_dat_tsp = 'tsp log''2' where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl2b';
update emaj.emaj_group_def set grpdef_log_idx_tsp = NULL where grpdef_schema = 'myschema2' and grpdef_tblseq = 'mytbl6';
select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');

-----------------------------
-- remove sequences
-----------------------------

--TODO: remove the transaction when adding a sequence will be possible and move the rollbacks later
select emaj.emaj_set_mark_group('myGroup1','Mk2b');
begin;
  select emaj.emaj_set_mark_group('myGroup1','Mk2c');
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_alter_group('myGroup1');
  select group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';
  select * from emaj.emaj_relation where rel_schema = 'myschema1' and rel_tblseq = 'myTbl3_col31_seq';
  select * from emaj.emaj_verify_all();
  --testing snap and sql generation
\! mkdir -p /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter
\! mkdir /tmp/emaj_test/alter
  select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/alter','');
\! ls /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_snap_log_group('myGroup1','Mk1',NULL,'/tmp/emaj_test/alter',NULL);
\! cat /tmp/emaj_test/alter/myGroup1_sequences_at_Mk1
\! rm -R /tmp/emaj_test/alter/*
  savepoint svp1;
    select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/emaj_test/alter/myFile',array['myschema1.myTbl3_col31_seq']);
  rollback to svp1;
\! rm -R /tmp/emaj_test
  -- testing delete a single mark set before the sequence removal
  select emaj.emaj_delete_mark_group('myGroup1','Mk2c');
  select * from emaj.emaj_sequence where sequ_time_id not in (select distinct mark_time_id from emaj.emaj_mark where mark_group = 'myGroup1');
  -- testing rollback
--select * from emaj.emaj_alter_plan where altr_time_id = (select max(altr_time_id) from emaj.emaj_alter_plan);
  select * from emaj.emaj_logged_rollback_group('myGroup1','Mk2b',true) order by 1,2;
  select * from emaj.emaj_rollback_group('myGroup1','Mk2b',true) order by 1,2;
--select * from emaj.emaj_alter_plan where altr_time_id = (select max(altr_time_id) from emaj.emaj_alter_plan);
  savepoint svp2;
  -- testing group's reset
  select emaj.emaj_stop_group('myGroup1');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select emaj.emaj_reset_group('myGroup1');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  rollback to svp2;
  -- testing marks deletion
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  -- testing the sequence drop
  drop sequence mySchema1."myTbl3_col31_seq" cascade;
--select * from emaj.emaj_hist order by hist_id desc limit 50;
rollback;

-----------------------------
-- remove tables
-----------------------------

--TODO: remove the transaction when adding a sequence will be possible and move the rollbacks later
begin;
  insert into myschema1."myTbl3" (col33) values (1.);
  select emaj.emaj_set_mark_group('myGroup1','Mk2c');
--select group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and (grpdef_tblseq = 'myTbl3' or grpdef_tblseq = 'mytbl2b');
  select emaj.emaj_alter_group('myGroup1');
  select group_nb_table, group_nb_sequence from emaj.emaj_group where group_name = 'myGroup1';
  select * from emaj.emaj_relation where rel_schema = 'myschema1' and (rel_tblseq = 'myTbl3' or rel_tblseq = 'mytbl2b') order by 1,2;
  delete from myschema1."myTbl3" where col33 = 1.;
  select count(*) from "emajC"."myschema1_myTbl3_log_1";
  select * from emaj.emaj_verify_all();
  -- testing log stat
  select * from emaj.emaj_log_stat_group('myGroup1',NULL,NULL);
  select * from emaj.emaj_detailed_log_stat_group('myGroup1',NULL,NULL);
  --testing snap and sql generation
\! mkdir -p /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter
\! mkdir /tmp/emaj_test/alter
  select emaj.emaj_snap_group('myGroup1','/tmp/emaj_test/alter','');
\! ls /tmp/emaj_test/alter
\! rm -R /tmp/emaj_test/alter/*
  select emaj.emaj_snap_log_group('myGroup1',NULL,NULL,'/tmp/emaj_test/alter',NULL);
\! ls /tmp/emaj_test/alter/myschema1*
\! rm -R /tmp/emaj_test/alter/*
  savepoint svp1;
    select emaj.emaj_gen_sql_group('myGroup1', NULL, NULL, '/tmp/emaj_test/alter/myFile',array['myschema1.myTbl3']);
  rollback to svp1;
\! rm -R /tmp/emaj_test
  savepoint svp2;
  -- testing delete a single mark set before the sequence removal
  select emaj.emaj_delete_mark_group('myGroup1','Mk2c');
  select * from emaj.emaj_sequence where sequ_time_id not in (select distinct mark_time_id from emaj.emaj_mark where mark_group = 'myGroup1');
  -- testing marks deletion (delete all marks before the alter_group)
  select emaj.emaj_delete_before_mark_group('myGroup1','EMAJ_LAST_MARK');
  select 'should not exist' from pg_namespace where nspname = 'emajb';
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'myschema1_mytbl2b_log' and nspname = 'emajb';
  rollback to svp2;
  -- testing marks deletion (other cases)
  select emaj.emaj_set_mark_group('myGroup1','Mk2d');
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2b');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select 'found' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'myschema1_mytbl2b_log_1' and nspname = 'emajb';
  select emaj.emaj_delete_before_mark_group('myGroup1','Mk2d');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'myschema1_mytbl2b_log' and nspname = 'emajb';
  rollback to svp2;
  -- testing rollback and consolidation
  delete from emaj.emaj_param where param_key = 'dblink_user_password';
  insert into myschema1.mytbl1 values (100, 'Alter_logg', E'\\000'::bytea);
  select * from emaj.emaj_logged_rollback_group('myGroup1','Mk2b',true) order by 1,2;
  select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','Logged_Rlbk_End');
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'mytbl1';
  select emaj.emaj_alter_group('myGroup1','2nd remove_tbl');
  select * from emaj.emaj_get_consolidable_rollbacks() where cons_group = 'myGroup1'; -- should report 2 rows to consolidate
  select * from emaj.emaj_consolidate_rollback_group('myGroup1', 'Logged_Rlbk_End');
  select count(*) from emaj.myschema1_mytbl1_log_1;   -- the log table should be empty
  select * from emaj.emaj_rollback_group('myGroup1','Mk2b',true) order by 1,2;
  savepoint svp3;
  -- testing group's reset
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_reset_group('myGroup1');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select 'should not exist' from pg_namespace where nspname = 'emajb';
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'myschema1_mytbl2b_log' and nspname = 'emajb';
  rollback to svp3;
  -- testing group's stop and start
  select emaj.emaj_stop_group('myGroup1');
  select emaj.emaj_start_group('myGroup1');
  select * from emaj.emaj_relation where rel_group = 'myGroup1' and not upper_inf(rel_time_range) order by 1,2;
  select 'should not exist' from pg_namespace where nspname = 'emajb';
  select 'should not exist' from pg_class, pg_namespace where relnamespace = pg_namespace.oid and relname = 'myschema1_mytbl2b_log' and nspname = 'emajb';
  rollback to svp1;
  -- testing the table drop (remove first the sequence linked to the table, otherwise an event triger fires)
  delete from emaj.emaj_group_def where grpdef_schema = 'myschema1' and grpdef_tblseq = 'myTbl3_col31_seq';
  select emaj.emaj_alter_group('myGroup1','alter_before_drop_mytbl3');
  drop table mySchema1."myTbl3";
--select * from emaj.emaj_hist order by hist_id desc limit 50;
rollback;
select emaj.emaj_cleanup_rollback_state();

-- set an intermediate mark
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Mk3');

select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_id > 6000 order by mark_id;

-- estimate a rollback crossing alter group operations
select emaj.emaj_estimate_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false);

-- execute a rollback not crossing any alter group operation
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk3',false) order by 1,2;

-- execute rollbacks crossing alter group operations
select emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2');
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk2',true) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk2',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',true) order by 1,2;

-- execute additional rollback not crossing alter operations anymore
select * from emaj.emaj_logged_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;
select * from emaj.emaj_rollback_groups('{"myGroup1","myGroup2"}','Mk1',false) order by 1,2;

-- empty logging groups
begin;
  delete from emaj.emaj_group_def where grpdef_group IN ('myGroup1','myGroup2');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
-- add one table or sequence to the empty groups
-- TODO: remove the stop_groups() call once it will be possible to add a table/sequence to a logging group
  select emaj.emaj_stop_groups('{"myGroup1","myGroup2"}');
  insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
  insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
  select emaj.emaj_alter_groups('{"myGroup1","myGroup2"}');
rollback;

-----------------------------
-- test end: check
-----------------------------

select emaj.emaj_force_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup2');
select emaj.emaj_force_drop_group('myGroup4');
select nspname from pg_namespace where nspname like 'emaj%' order by nspname;
select sch_name from emaj.emaj_schema order by 1;
select hist_function, hist_event, hist_object, 
       regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), 
       hist_user 
  from emaj.emaj_hist where hist_id > 6000 order by hist_id;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id > 6000 order by time_id;
select * from emaj.emaj_alter_plan order by 1,2,3,4,5;

truncate emaj.emaj_hist;
