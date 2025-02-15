-- adm1.sql : complex scenarios executed by an emaj_adm role
--
SET datestyle TO ymd;

-- set sequence restart value
select public.handle_emaj_sequences(12000);

truncate emaj.emaj_hist;

-----------------------------
-- grant emaj_adm role 
-----------------------------
grant emaj_adm to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
--
set role emaj_regression_tests_adm_user1;

-----------------------------
-- authorized table accesses
-----------------------------
select 'select ok' as result from (select count(*) from emaj.emaj_version_hist) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_param) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_visible_param) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_time_stamp) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_hist) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_group) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_schema) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_relation) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rel_hist) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_log_session) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_mark) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_sequence) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_table) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_seq_hole) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_session) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_plan) as t;
select 'select ok' as result from (select count(*) from emaj.emaj_rlbk_stat) as t;
select 'select ok' as result from (select count(*) from emaj_mySchema1.myTbl1_log) as t;

-----------------------------
-- stop, reset and drop existing groups
-----------------------------
select emaj.emaj_stop_group('myGroup1','Simple stop mark');
select emaj.emaj_reset_group('myGroup1');
select emaj.emaj_drop_group('myGroup1');
select emaj.emaj_force_drop_group('myGroup2');
select emaj.emaj_force_stop_group('emptyGroup');
select emaj.emaj_drop_group('emptyGroup');

-- emaj tables
select * from emaj.emaj_group;
select sch_name from emaj.emaj_schema;
select * from emaj.emaj_relation;
select * from emaj.emaj_mark;
select * from emaj.emaj_sequence;
select * from emaj.emaj_table;
select * from emaj.emaj_seq_hole;
select count(*) from emaj.emaj_rlbk;
select count(*) from emaj.emaj_rlbk_session;
select count(*) from emaj.emaj_rlbk_plan;
select count(*) from emaj.emaj_rlbk_stat;

-----------------------------
-- cleanup application tables
-----------------------------
reset role;
truncate mySchema1.myTbl1, mySchema1.myTbl2, mySchema1."myTbl3", mySchema1.myTbl4, mySchema1.myTbl2b; 
truncate mySchema2.myTbl1, mySchema2.myTbl2, mySchema2."myTbl3", mySchema2.myTbl4, mySchema2.myTbl5, mySchema2.myTbl6, mySchema2.myTbl7, mySchema2.myTbl8;
insert into myschema2.myTbl7 select i from generate_series(0,100,1) i;
insert into myschema2.myTbl6 (col61) values (0);
insert into myschema2.myTbl8 (col81) values (0);
alter sequence mySchema2.mySeq1 restart 1000;
truncate mySchema4.myTblM, mySchema4.myTblC1, mySchema4.myTblC2;
truncate mySchema4.myTblP, mySchema4.myPartP1a, mySchema4.myPartP1b, mySchema4.myPartP2, mySchema4.myTblR1, mySchema4.myTblR2 CASCADE;
-- analyze to get some statistics
analyze;

set role emaj_regression_tests_adm_user2;

-----------------------------
-- explicitely purge the histories
-----------------------------
select emaj.emaj_purge_histories('0 SECOND');
select hist_function, hist_event, hist_wording
  from emaj.emaj_hist order by hist_id;

-----------------------------
-- recreate and start groups
-----------------------------
-- set the parameter to drop the emaj_user_port column and add an 'extra_col_appname' column
insert into emaj.emaj_param (param_key, param_value_text) values ('alter_log_table',
  'ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr(), ADD COLUMN extra_col_appname TEXT DEFAULT current_setting(''application_name'')');

select emaj.emaj_create_group('myGroup1');
select emaj.emaj_comment_group('myGroup1','This is group #1');
select emaj.emaj_assign_table('myschema1','mytbl1','myGroup1','{"priority":20}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl2','myGroup1','{"log_data_tablespace":"tsplog1","log_index_tablespace":"tsplog1"}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl2b','myGroup1','{"log_data_tablespace":"tsp log''2","log_index_tablespace":"tsp log''2"}'::jsonb);
select emaj.emaj_assign_table('myschema1','myTbl3','myGroup1','{"priority":10,"log_data_tablespace":"tsplog1"}'::jsonb);
select emaj.emaj_assign_table('myschema1','mytbl4','myGroup1','{"priority":20,"log_data_tablespace":"tsplog1","log_index_tablespace":"tsp log''2"}'::jsonb);
select emaj.emaj_assign_sequences('myschema1','.*',null,'myGroup1');

select emaj.emaj_create_group('myGroup2',true,'This is group #2');
select emaj.emaj_assign_tables('myschema2','.*','mytbl[7,8]','myGroup2');
select emaj.emaj_assign_sequences('myschema2','.*','myseq2','myGroup2');

select emaj.emaj_create_group('emptyGroup');

-- try to rename the last mark for a group that has no mark
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','new_mark_name');

-- force a purge of the history, the alter and the rollback tables
INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','0.1 second'::interval);
select pg_sleep(0.2);
select emaj.emaj_start_group('myGroup1','M1');
delete from emaj.emaj_param where param_key = 'history_retention';

select emaj.emaj_start_group('myGroup2','M1');

-- set sequence restart value
select public.handle_emaj_sequences(12100);

-----------------------------
-- Step 1 : for myGroup1, update tables and set 2 marks
-----------------------------
--
reset role;
set search_path=public,myschema1;
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
insert into myTbl2 values (1,'ABC','2010-12-31');
delete from myTbl1 where col11 > 10;
insert into myTbl2 values (2,'DEF',NULL);
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
--
set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','M2');
--
reset role;
set search_path=public,myschema1;
insert into myTbl4 values (1,'FK...',1,1,'ABC');
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2;
insert into myTbl4 values (3,'FK...',1,10,'ABC');
-- the 2 next statements activate fkey on delete and on update clauses 
delete from myTbl1 where col11 = 10;
update myTbl1 set col12='DEF' where col11 <= 2;
--
set role emaj_regression_tests_adm_user2;
select emaj.emaj_set_mark_group('myGroup1','M3');
select emaj.emaj_comment_mark_group('myGroup1','M3','Third mark set');

-----------------------------
-- Checking step 1
-----------------------------
-- emaj tables
select group_name, group_is_rollbackable, group_last_alter_time_id, group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment
  from emaj.emaj_group order by group_name;
select * from emaj.emaj_group_hist order by grph_group, grph_time_range;
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12000 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12000 order by hist_id;

-- user tables
reset role;
select * from mySchema1.myTbl1 order by col11,col12;
select * from mySchema1.myTbl2 order by col21;
select * from mySchema1.myTbl2b order by col20;
select col31,col33 from mySchema1."myTbl3" order by col31;
select * from mySchema1.myTbl4 order by col41;
-- log tables
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid, emaj_user, emaj_user_ip, extra_col_appname
    from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12200);

-----------------------------
-- Step 2 : for myGroup2, start, update tables and set 2 marks 
-----------------------------
set search_path=public,myschema2;
insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,11) as i;
update myTbl1 set col13=E'\\034'::bytea where col11 <= 3;
insert into myTbl2 values (1,'ABC','2010-01-01');
delete from myTbl1 where col11 > 10;
select nextval('myschema2.myseq1');
insert into myTbl2 values (2,'DEF',NULL);
insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100;
insert into myTbl5 values (1,'{"abc","def","ghi"}','{1,2,3}',NULL,'{}',NULL,'{"id":1000}','[2020-01-01, 2021-01-01)',NULL);
insert into myTbl5 values (2,array['abc','def','ghi'],array[3,4,5],array['2000/02/01'::date,'2000/02/28'::date],'{"id":1001, "c1":"abc"}',NULL,'{"id":1001}',NULL,XMLPARSE (CONTENT '<foo>bar</foo>'));
update myTbl5 set col54 = '{"2010/11/28","2010/12/03"}', col55 = '{"id":1001, "c2":"def"}', col57 = '{"id":1001, "c3":"ghi"}' where col54 is null;
insert into myTbl6 select i, point(i,1.3), '((0,0),(2,2))', circle(point(5,5),i),'((-2,-2),(3,0),(1,4))','10.20.30.40/27','EXECUTING',(i,point(i,1.3))::mycomposite from generate_series (1,8) as i;
update myTbl6 set col64 = '<(5,6),3.5>', col65 = null, col67 = 'COMPLETED' where col61 between 1 and 3;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('myGroup2','M2');
--
reset role;
set search_path=public,myschema2;
select nextval('myschema2.myseq1');
select nextval('myschema2.myseq1');
select nextval('myschema2.myseq1');
--
alter sequence mySeq1 NO MAXVALUE NO CYCLE;
--
insert into myTbl4 values (1,'FK...',1,1,'ABC');
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2;
delete from mytbl5 where 4 = any(col53);
delete from myTbl6 where col65 is null and col61 <> 0;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('myGroup2','M3');
-----------------------------
-- Checking step 2
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12200 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12200 order by hist_id;

-- user tables
reset role;
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
select * from mySchema2.myTbl5 order by col51;
select * from mySchema2.myTbl6 order by col61;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl6_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12300);

-----------------------------
-- Step 3 : for myGroup2, double logged rollback
-----------------------------
reset role;
analyze mytbl4;
-- rollback with dblink_connect_u not granted

revoke execute on function dblink_connect_u(text,text) from emaj_adm;
set role emaj_regression_tests_adm_user2;
select * from emaj.emaj_logged_rollback_group('myGroup2','M2',false) order by 1,2;
select * from emaj.emaj_logged_rollback_group('myGroup2','M3',false) order by 1,2;
reset role;
grant execute on function dblink_connect_u(text,text) to emaj_adm;
set role emaj_regression_tests_adm_user1;

-----------------------------
-- Checking step 3
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12300 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12300 order by hist_id;

-- user tables
reset role;
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
select * from mySchema2.myTbl5 order by col51;
select * from mySchema2.myTbl6 order by col61;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl6_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12400);

-----------------------------
-- Step 4 : for myGroup1, rollback then update tables then set 3 marks
-----------------------------
select * from emaj.emaj_rollback_group('myGroup1','M2',false) order by 1,2;
--
reset role;
set search_path=public,myschema1;
insert into myTbl1 select i, 'DEF', E'\\000'::bytea from generate_series (100,110) as i;
insert into myTbl2 values (3,'GHI','2010-01-02');
delete from myTbl1 where col11 = 1;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('myGroup1','M4');
--
reset role;
update "myTbl3" set col33 = col33 / 2;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('myGroup1','M5');
--
reset role;
update myTbl1 set col11 = 99 where col11 = 1;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_set_mark_group('myGroup1','M6');
-----------------------------
-- Checking step 4
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12400 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12400 order by hist_id;

-- user tables
reset role;
select * from mySchema1.myTbl1 order by col11,col12;
select * from mySchema1.myTbl2 order by col21;
select * from mySchema1.myTbl2b order by col20;
select col31,col33 from mySchema1."myTbl3" order by col31;
select * from mySchema1.myTbl4 order by col41;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12500);

-----------------------------
-- Step 5 : for myGroup2, logged rollback again then unlogged rollback 
-----------------------------
select * from emaj.emaj_logged_rollback_group('myGroup2','M2',false) order by 1,2;
--
select * from emaj.emaj_rollback_group('myGroup2','M3',false) order by 1,2;
-----------------------------
-- Checking step 5
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12500 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12500 order by hist_id;

-- user tables
reset role;
select * from mySchema2.myTbl1 order by col11,col12;
select * from mySchema2.myTbl2 order by col21;
select col31,col33 from mySchema2."myTbl3" order by col31;
select * from mySchema2.myTbl4 order by col41;
select * from mySchema2.myTbl5 order by col51;
select * from mySchema2.myTbl6 order by col61;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl4_log order by emaj_gid, emaj_tuple desc;
select col51, col52, col53, col54, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema2.mytbl5_log order by emaj_gid, emaj_tuple desc;
select col61, col62, col63, col64, col65, col66, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema2.myTbl6_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12600);

-----------------------------
-- Step 6 : for myGroup1, update tables, rollback, other updates, then logged rollback
-----------------------------
reset role;
set search_path=public,myschema1;
--
insert into myTbl1 values (1, 'Step 6', E'\\000'::bytea);
insert into myTbl4 values (11,'FK...',1,1,'Step 6');
insert into myTbl4 values (12,'FK...',1,1,'Step 6');
--
set role emaj_regression_tests_adm_user1;
select * from emaj.emaj_rollback_group('myGroup1','M5',false) order by 1,2;
--
reset role;
insert into myTbl1 values (1, 'Step 6', E'\\001'::bytea);
insert into myTbl4 values (11,'',1,1,'Step 6');
insert into myTbl4 values (12,'',1,1,'Step 6');
--
-- for an equivalent of "select * from emaj.emaj_logged_rollback_group('myGroup1','M4',true,'my comment');"
set role emaj_regression_tests_adm_user1;
select * from emaj._rlbk_async(emaj._rlbk_init(array['myGroup1'], 'M4', true, 1, false, true, 'my comment'), false);

select emaj.emaj_comment_rollback(12601,'Updated comment');

-- and check the rollback result
select rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_comment,
       rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id,
       rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_messages
 from emaj.emaj_rlbk order by rlbk_id desc limit 1;

-----------------------------
-- Checking step 6
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
-- check that mark_log_stat_before_next column is always equal to either NULL or the emaj_log_stat_rows() function's result
-- this should always return 0 row
select * from 
  (select mark_time_id, mark_group, mark_name, mark_log_rows_before_next - 
    (select sum(stat_rows) from emaj.emaj_log_stat_group(mark_group, mark_name, 
      (select mark_name from emaj.emaj_mark m2 where m2.mark_group = m1.mark_group and m2.mark_time_id > m1.mark_time_id order by mark_time_id limit 1))
    ) as checked_stat_rows from emaj.emaj_mark m1 where mark_log_rows_before_next is not null
  ) as t 
  where checked_stat_rows <> 0;  
--
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12600 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12600 order by hist_id;

-- user tables
reset role;
select * from mySchema1.myTbl1 order by col11,col12;
select * from mySchema1.myTbl2 order by col21;
select * from mySchema1.myTbl2b order by col20;
select col31,col33 from mySchema1."myTbl3" order by col31;
select * from mySchema1.myTbl4 order by col41;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

-- set sequence restart value
select public.handle_emaj_sequences(12700);

-----------------------------
-- Step 7 : for myGroup1, update tables, rename a mark, then delete 2 marks then delete all before a mark 
-----------------------------
set search_path=public,myschema1;
--
reset role;
delete from "myTbl3" where col31 = 14;
delete from "myTbl3" where col31 = 15;
delete from "myTbl3" where col31 = 16;
delete from "myTbl3" where col31 = 17;
delete from "myTbl3" where col31 = 18;
--
set role emaj_regression_tests_adm_user1;
select emaj.emaj_rename_mark_group('myGroup1',mark_name,'Before logged rollback to M4') from emaj.emaj_mark where mark_comment like '%to mark M4 start';
-- 
select emaj.emaj_delete_mark_group('myGroup1',mark_name) from emaj.emaj_mark where mark_comment like '%to mark M4 end';
select emaj.emaj_delete_mark_group('myGroup1','M1');
--
select emaj.emaj_delete_before_mark_group('myGroup1','M4');
-----------------------------
-- Checking step 7
-----------------------------
-- emaj tables
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;
select tbl_schema, tbl_name, tbl_time_id, tbl_log_seq_last_val from emaj.emaj_table order by tbl_time_id, tbl_schema, tbl_name;
select sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size from emaj.emaj_seq_hole order by 1,2,3;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 12700 order by time_id;
select hist_function, hist_event, hist_object,
       regexp_replace(regexp_replace(regexp_replace(hist_wording,
            E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),
            E'\\d\\d\\d\\d/\\d\\d\\/\\d\\d\\ \\d\\d\\:\\d\\d:\\d\\d .*?\\)','<timestamp>)','g'),
            E'\\[.+\\]','(timestamp)','g'), 
       hist_user
  from emaj.emaj_hist where hist_id >= 12700 order by hist_id;

-- user tables
reset role;
select * from mySchema1.myTbl1 order by col11,col12;
select * from mySchema1.myTbl2 order by col21;
select * from mySchema1.myTbl2b order by col20;
select col31,col33 from mySchema1."myTbl3" order by col31;
select * from mySchema1.myTbl4 order by col41;
-- log tables
set role emaj_regression_tests_adm_user1;
select col11, col12, col13, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl1_log order by emaj_gid, emaj_tuple desc;
select col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2_log order by emaj_gid, emaj_tuple desc;
select col20, col21, col22, col23, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl2b_log order by emaj_gid, emaj_tuple desc;
select col31, col33, emaj_verb, emaj_tuple, emaj_gid from emaj_myschema1."myTbl3_log" order by emaj_gid, emaj_tuple desc;
select col41, col42, col43, col44, col45, emaj_verb, emaj_tuple, emaj_gid from emaj_mySchema1.myTbl4_log order by emaj_gid, emaj_tuple desc;

--
--		grp1									grp2
--1	M1 up M2 up M3 
--2											M1 up M2 up M3
--3											LR-M2 (->M2%S+M2%E) LR-M3 (->M3%S+M3%E)
--4	R-M2 up M4 up M5 up M6
--5											LR-M2 (->M2%S+M2%E) R-M3
--6	up R-M5 up LR-M4(->M4S+M4E)
--7	up M7 REN-M4S DEL-M4E DEL-M1 DELBEF-M4
--
reset role;
