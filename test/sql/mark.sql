-- mark.sql : test emaj_set_mark_group(), emaj_set_mark_groups(), emaj_comment_mark_group(),
-- emaj_rename_mark_group(), emaj_get_previous_mark_group(), emaj_delete_mark_group(),
-- emaj_protect_mark_group() and emaj_unprotect_mark_group() functions
--

-- set sequence restart value
alter sequence emaj.emaj_hist_hist_id_seq restart 2000;
alter sequence emaj.emaj_time_stamp_time_id_seq restart 2000;
alter sequence emaj.emaj_global_seq restart 20000;

select emaj.emaj_start_group('myGroup1','Mark1');
select emaj.emaj_start_group('myGroup2','Mark2');
select emaj.emaj_start_group('emptyGroup','MarkInit');
-----------------------------
-- emaj_set_mark_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_set_mark_group(NULL,NULL);
select emaj.emaj_set_mark_group('unknownGroup',NULL);

-- reserved mark name
select emaj.emaj_set_mark_group('myGroup1','EMAJ_LAST_MARK');

-- should be OK
select emaj.emaj_set_mark_group('myGroup1','SM1');
select emaj.emaj_set_mark_group('myGroup2','SM1');
select emaj.emaj_set_mark_group('myGroup2','phil''s mark #1');
select emaj.emaj_set_mark_group('emptyGroup','SM1');

-- duplicate mark name
select emaj.emaj_set_mark_group('myGroup1','SM1');

-- mark with generated name and in a single transaction
begin transaction;
  select emaj.emaj_set_mark_group('myGroup1',NULL);
  select emaj.emaj_set_mark_group('myGroup2','');
commit;

-- default value for mark name
select pg_sleep(0.001);
select emaj.emaj_set_mark_group('myGroup2');

-- use of % in mark name
select emaj.emaj_set_mark_group('myGroup1','Foo%Bar');

-- multiple emaj_set_mark_group() using the same generated start mark name => fails
-- this test is commented because the generated error message differs from one run to another
--begin;
--  select emaj.emaj_start_group('myGroup4');
--  select emaj.emaj_set_mark_group('myGroup4');
--  select emaj.emaj_set_mark_group('myGroup4');
--rollback;

-----------------------------
-- emaj_set_mark_groups() tests
-----------------------------
-- NULL group names array
select emaj.emaj_set_mark_groups(NULL,NULL);

-- groups array is unknown in emaj_group_def
select emaj.emaj_set_mark_groups('{"unknownGroup",""}',NULL);

-- reserved mark name
select emaj.emaj_set_mark_groups('{"myGroup1"}','EMAJ_LAST_MARK');

-- should be OK
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','SM3');

-- duplicate mark name and warning on group names array content
select emaj.emaj_set_mark_groups(array['myGroup1',NULL,'myGroup2','','myGroup2','emptyGroup','myGroup2','myGroup1'],'SM3');

-- generated mark name
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','');
select pg_sleep(0.001);
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}',NULL);
select pg_sleep(0.001);
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}');

-- use of % in mark name
select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','Bar%Foo');

-- impact of mark set
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2000 order by time_id;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;

-----------------------------
-- emaj_comment_mark_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_comment_mark_group(NULL,NULL,NULL);
select emaj.emaj_comment_mark_group('unknownGroup',NULL,NULL);

-- mark is unknown
select emaj.emaj_comment_mark_group('myGroup1','unknownMark',NULL);

-- should be OK
select emaj.emaj_comment_mark_group('myGroup1','SM1','a first comment for group #1');
select emaj.emaj_comment_mark_group('myGroup1','SM1','a better comment for group #1');
select emaj.emaj_comment_mark_group('myGroup2','SM1','a first comment for group #2');
select emaj.emaj_comment_mark_group('myGroup2','SM1',NULL);
select emaj.emaj_comment_mark_group('myGroup2','EMAJ_LAST_MARK','a comment for group #2');
select emaj.emaj_comment_mark_group('myGroup2','phil''s mark #1','a good phil''s comment!');
select emaj.emaj_comment_mark_group('emptyGroup','SM1','a comment on a mark for an empty group');

-----------------------------
-- emaj_get_previous_mark_group()
-----------------------------
-- group is unknown in both emaj_group_def version
select emaj.emaj_get_previous_mark_group(NULL,NULL::timestamptz);
select emaj.emaj_get_previous_mark_group('unknownGroup',NULL::timestamptz);
select emaj.emaj_get_previous_mark_group(NULL,NULL::text);
select emaj.emaj_get_previous_mark_group('unknownGroup',NULL::text);

-- mark is unknown in emaj_mark
select emaj.emaj_get_previous_mark_group('myGroup2','unknownMark');

-- should be OK
select emaj.emaj_get_previous_mark_group('myGroup2',(select time_clock_timestamp from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup2' and mark_name = 'SM1'));
select emaj.emaj_get_previous_mark_group('myGroup2',(select time_clock_timestamp from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup2' and mark_name = 'SM1')+'0.000001 SECOND'::interval);
select emaj.emaj_get_previous_mark_group('myGroup1',(select min(time_clock_timestamp) from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup1'));

select emaj.emaj_get_previous_mark_group('myGroup2','SM1');
select emaj.emaj_get_previous_mark_group('emptyGroup','SM1');
select coalesce(emaj.emaj_get_previous_mark_group('myGroup2','Mark2'),'No previous mark');
select emaj.emaj_get_previous_mark_group('myGroup2',(select emaj.emaj_get_previous_mark_group('myGroup2',(select emaj.emaj_get_previous_mark_group('myGroup2',(select emaj.emaj_get_previous_mark_group('myGroup2','EMAJ_LAST_MARK')))))));

-----------------------------
-- emaj_rename_mark_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_rename_mark_group(NULL,NULL,NULL);
select emaj.emaj_rename_mark_group('unknownGroup',NULL,NULL);

-- unknown mark name
select emaj.emaj_rename_mark_group('myGroup1','DummyMark','new mark');

-- invalid new mark name
select emaj.emaj_rename_mark_group('myGroup1','Mark1','EMAJ_LAST_MARK');

-- new mark name already exists
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','SM1');

-- should be OK
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK',NULL);
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','SM2');
select emaj.emaj_rename_mark_group('myGroup2','EMAJ_LAST_MARK','SM2');
select emaj.emaj_rename_mark_group('myGroup2','phil''s mark #1','john''s mark #1');
select emaj.emaj_rename_mark_group('emptyGroup','EMAJ_LAST_MARK','SM2');

-- simulate SM2 is the end mark of a logged rollback operations on both myGroup1 and myGroup2 groups
update emaj.emaj_mark set mark_logged_rlbk_target_mark = 'Mark1' where mark_name = 'SM2';
select emaj.emaj_rename_mark_group('myGroup1','Mark1','First Mark');

-- impact of mark rename
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;

-----------------------------
-- emaj_delete_mark_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_delete_mark_group(NULL,NULL);
select emaj.emaj_delete_mark_group('unknownGroup',NULL);

-- unknown mark name
select emaj.emaj_delete_mark_group('myGroup2',NULL);
select emaj.emaj_delete_mark_group('myGroup2','DummyMark');

-- next attempts should be OK
select emaj.emaj_delete_mark_group('myGroup1','EMAJ_LAST_MARK');

-- simulate SM3 is an end mark of a logged rollback operations on myGroup1 group
update emaj.emaj_mark set mark_logged_rlbk_target_mark = 'SM1' where mark_group = 'myGroup1' and mark_name = 'SM3';
select emaj.emaj_delete_mark_group('myGroup1','SM1');
select mark_group, mark_name, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup1' and mark_name = 'SM3';

select emaj.emaj_delete_mark_group('emptyGroup','MarkInit');

select emaj.emaj_delete_mark_group('myGroup1','First Mark');

select sum(emaj.emaj_delete_mark_group('myGroup1',mark_name)) from 
 (select mark_name from emaj.emaj_mark
    where mark_group = 'myGroup1' and (mark_name ~ E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d')
    order by mark_time_id) as t;
    
select emaj.emaj_delete_mark_group('myGroup2','john''s mark #1');

-- error: at least 1 mark should remain
select emaj.emaj_delete_mark_group('myGroup1','SM3');

-----------------------------
-- emaj_delete_before_mark_group() tests
-----------------------------
-- group is unknown in emaj_group_def
select emaj.emaj_delete_before_mark_group(NULL,NULL);
select emaj.emaj_delete_before_mark_group('unknownGroup',NULL);

-- unknown mark name
select emaj.emaj_delete_before_mark_group('myGroup2','DummyMark');

-- NULL input for mark name returns NULL
select emaj.emaj_delete_before_mark_group('myGroup2',NULL);

-- should be OK
select emaj.emaj_delete_before_mark_group('myGroup1','SM3');
select emaj.emaj_delete_before_mark_group('myGroup1','EMAJ_LAST_MARK');
select emaj.emaj_set_mark_group('emptyGroup','EGM3');
select emaj.emaj_set_mark_group('emptyGroup','EGM4');
select emaj.emaj_delete_before_mark_group('emptyGroup','SM2');

-- simulate SM2 is an end mark of a logged rollback operations on myGroup2 group
update emaj.emaj_mark set mark_logged_rlbk_target_mark = 'SM1' where mark_group = 'myGroup2' and mark_name = 'SM2';
-- and delete marks including SM1
select emaj.emaj_delete_before_mark_group('myGroup2',
      (select emaj.emaj_get_previous_mark_group('myGroup2',
             (select time_clock_timestamp from emaj.emaj_mark, emaj.emaj_time_stamp where time_id = mark_time_id and mark_group = 'myGroup2' and mark_group = 'myGroup2' and mark_name = 'SM2')+'0.000001 SECOND'::interval)));
select mark_group, mark_name, mark_logged_rlbk_target_mark from emaj.emaj_mark where mark_group = 'myGroup2' and mark_name = 'SM2';

-- check emaj_delete_before_mark_group() also cleans up the emaj_hist table
insert into emaj.emaj_param (param_key, param_value_interval) values ('history_retention','0 second'::interval);
select emaj.emaj_set_mark_group('phil''s group#3",','Mark4');
select emaj.emaj_delete_before_mark_group('phil''s group#3",','Mark4');
delete from emaj.emaj_param where param_key = 'history_retention';

-----------------------------
-- emaj_protect_mark_group() tests
-----------------------------
-- group is unknown
select emaj.emaj_protect_mark_group(NULL,NULL);
select emaj.emaj_protect_mark_group('unknownGroup',NULL);
-- group is not rollbackable
select emaj.emaj_protect_mark_group('phil''s group#3",',NULL);
-- mark is unknown
select emaj.emaj_protect_mark_group('myGroup1',NULL);
select emaj.emaj_protect_mark_group('myGroup1','unknownMark');
-- should be ok
select emaj.emaj_protect_mark_group('myGroup1','EMAJ_LAST_MARK');
select mark_time_id, mark_name, mark_group, mark_is_deleted, mark_is_rlbk_protected from emaj.emaj_mark where mark_group = 'myGroup1';
select emaj.emaj_protect_mark_group('emptyGroup','EMAJ_LAST_MARK');

-- protect an already protected group
select emaj.emaj_protect_mark_group('myGroup1','SM3');
select mark_time_id, mark_name, mark_group, mark_is_deleted, mark_is_rlbk_protected from emaj.emaj_mark where mark_group = 'myGroup1';

-----------------------------
-- emaj_unprotect_mark_group() tests
-----------------------------
-- group is unknown
select emaj.emaj_unprotect_mark_group(NULL,NULL);
select emaj.emaj_unprotect_mark_group('unknownGroup',NULL);
-- group is not rollbackable
select emaj.emaj_unprotect_mark_group('phil''s group#3",',NULL);
-- mark is unknown
select emaj.emaj_unprotect_mark_group('myGroup1',NULL);
select emaj.emaj_unprotect_mark_group('myGroup1','unknownMark');
-- should be ok
select emaj.emaj_unprotect_mark_group('myGroup1','EMAJ_LAST_MARK');
select mark_time_id, mark_name, mark_group, mark_is_deleted, mark_is_rlbk_protected from emaj.emaj_mark where mark_group = 'myGroup1';

select emaj.emaj_unprotect_mark_group('emptyGroup','EMAJ_LAST_MARK');

-- protect an already protected group
select emaj.emaj_unprotect_mark_group('myGroup1','SM3');
select mark_time_id, mark_name, mark_group, mark_is_deleted, mark_is_rlbk_protected from emaj.emaj_mark where mark_group = 'myGroup1';

-- check mark protections is removed by stop_group functions
select emaj.emaj_protect_mark_group('myGroup1','EMAJ_LAST_MARK');
select regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_is_deleted, mark_is_rlbk_protected 
  from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id, mark_group;
  
select emaj.emaj_stop_group('myGroup1');
select regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_is_deleted, mark_is_rlbk_protected 
  from emaj.emaj_mark where mark_group = 'myGroup1' order by mark_time_id, mark_group;

-----------------------------
-- test functions with group not in logging state 
-----------------------------
-- myGroup1 is already stopped
select emaj.emaj_stop_group('myGroup2');

select emaj.emaj_set_mark_group('myGroup1','SM1');
select emaj.emaj_set_mark_groups(array['myGroup1','myGroup2'],'SM1');
select emaj.emaj_rename_mark_group('myGroup1','EMAJ_LAST_MARK','RENAMED');
select emaj.emaj_protect_mark_group('myGroup1','RENAMED');
select emaj.emaj_unprotect_mark_group('myGroup1','EMAJ_LAST_MARK');

-- check marks state
select mark_group, regexp_replace(mark_name,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_comment, mark_log_rows_before_next, mark_logged_rlbk_target_mark from emaj.emaj_mark order by mark_time_id, mark_group;
select sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_is_called from emaj.emaj_sequence order by sequ_time_id, sequ_schema, sequ_name;

-----------------------------
-- test end: check, reset history and force sequences id
-----------------------------
select time_id, time_last_emaj_gid, time_event from emaj.emaj_time_stamp where time_id >= 2000 order by time_id;
select hist_id, hist_function, hist_event, regexp_replace(hist_object,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'), regexp_replace(regexp_replace(hist_wording,E'\\d\\d\.\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d','%','g'),E'\\[.+\\]','(timestamp)','g'), hist_user from 
  (select * from emaj.emaj_hist order by hist_id) as t;

