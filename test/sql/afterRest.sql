-- afterRest.sql: Test emaj environment restored from a dump taken at the end of schedule reg tests
--                All operations are executed by super-user
--

-----------------------------
-- Checking restore
-----------------------------
select relname,relkind from pg_class where relname like 'emaj_%';
select * from emaj.emaj_global_seq;

-----------------------------
-- Step 1 : for myGroup2, update tables and set a mark 
-----------------------------
set search_path=myschema2;
insert into myTbl1 select 100+i, 'KLM', E'\\000\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\000\\034'::bytea where col11 >105;
insert into myTbl2 values (100,'KLM','2012-12-31');
delete from myTbl1 where col11 > 110;
select nextval('myschema2.myseq1');
--
select emaj.emaj_set_mark_group('myGroup2','After restore mark');
--
-----------------------------
-- Checking step 1
-----------------------------
-- emaj tables
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
-- user tables
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;
--
-----------------------------
-- Step 2 : for myGroup2, rollback to mark Multi-1 (set before dump/restore) 
-----------------------------
select * from emaj.emaj_log_stat_group('myGroup2','Multi-1',NULL);
select emaj.emaj_rollback_group('myGroup2','Multi-1');
--
-----------------------------
-- Checking step 2
-----------------------------
-- emaj tables
select mark_id, mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_last_seq_hole_id, mark_last_sequence_id, mark_log_rows_before_next from emaj.emaj_mark order by mark_id;
select sequ_id,sequ_schema, sequ_name, regexp_replace(sequ_mark,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'), sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_id;
-- user tables
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from "emajC"."myschema2_myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj.mySchema2_myTbl4_log order by emaj_gid, emaj_tuple desc;
--
-----------------------------
-- Step 3 : stop myGroup2
-----------------------------
select emaj.emaj_stop_group('myGroup2');
--
-----------------------------
-- test end: check and reset history
-----------------------------
select hist_id, hist_function, hist_event, hist_object, regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;
--
truncate emaj.emaj_hist;
alter sequence emaj.emaj_hist_hist_id_seq restart 10000;

