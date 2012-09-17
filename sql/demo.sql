-- demo.sql : V 0.12.0
--
-- This test script must be executed with a superuser role.
-- E-Maj extension must have been previously installed.
-- The script creates a dedicated schema with some "application" tables and sequences.
-- At the end of the script's execution, the demo environment is left as is, so that users can examine the results.
-- The script can be run several times in sequence.
-- To remove all traces left by this demo, just 
--     SELECT emaj.emaj_demo_cleanup();

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                          EMAJ Demo start                            ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\set ON_ERROR_STOP
\set ECHO none
SET client_min_messages TO WARNING;

\echo '### Check the E-Maj environment.'
CREATE or REPLACE FUNCTION public.emaj_demo_tmp()
RETURNS TEXT LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_msg          TEXT;
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj demo: the current user (%) is not a superuser.', current_user;
    END IF;
-- check E-Maj is installed, by checking that the emaj schema exists
    PERFORM 1 FROM pg_namespace WHERE nspname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj demo: the schema ''emaj'' doesn''t exist. Is E-Maj really installed ?';
    END IF;
-- get the installed emaj version 
    SELECT 'E-Maj version ' || param_value_text || ' found into this database' INTO v_msg 
      FROM emaj.emaj_param WHERE param_key = 'emaj_version';
-- if the emaj_demo_cleanup function exists (created by a previous run of the demo script), execute it 
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_demo_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_demo_cleanup();
    END IF;
-- the function drops itself before exiting
    DROP FUNCTION public.emaj_demo_tmp();
    RETURN v_msg;
  END;
$tmp$;
SELECT public.emaj_demo_tmp();

\echo '### Create the emaj.emaj_demo_cleanup() function to use later in order to remove objects created by this script.'
CREATE or REPLACE FUNCTION emaj.emaj_demo_cleanup()
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_demo_cleanup$
  DECLARE
  BEGIN
-- stop if needed and drop the demo groups
    PERFORM emaj.emaj_force_stop_group(group_name) FROM emaj.emaj_group
      WHERE group_name IN ('emaj demo group 1','emaj demo group 2') AND group_state = 'LOGGING';
    PERFORM emaj.emaj_drop_group(group_name) FROM emaj.emaj_group
      WHERE group_name IN ('emaj demo group 1','emaj demo group 2');
-- remove demo groups definition from the emaj_group_def table
    DELETE FROM emaj.emaj_group_def WHERE grpdef_group IN ('emaj demo group 1','emaj demo group 2');
-- drop the demo app schema and its content
    DROP SCHEMA IF EXISTS emaj_demo_app_schema CASCADE;
-- the function drops itself before exiting
    DROP FUNCTION emaj.emaj_demo_cleanup();
    RETURN 'The E-Maj demo environment has been deleted';
  END;
$emaj_demo_cleanup$;

\set ECHO queries

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '### Let us create a schema with some application tables and sequences.  ###'
\echo '### Note that myTbl3 table name is case sensitive.                      ###' 
\echo '###                                                                     ###'
\echo '###########################################################################'

DROP SCHEMA IF EXISTS emaj_demo_app_schema CASCADE;
CREATE SCHEMA emaj_demo_app_schema;

SET search_path=emaj_demo_app_schema;

CREATE TABLE emaj_demo_app_schema.myTbl1 (
  col11       DECIMAL(7)       NOT NULL,
  col12       CHAR(10)         NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);
CREATE TABLE emaj_demo_app_schema.myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);
CREATE TABLE emaj_demo_app_schema."myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32,col33);
CREATE TABLE emaj_demo_app_schema.myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE
);
CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Now let us define and create 2 rollbackable groups                 ###'
\echo '###  "maj demo group 1" and "maj demo group 2".                         ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Populate the emaj_group_def table.'
delete from emaj.emaj_group_def where grpdef_group in ('emaj demo group 1','emaj demo group 2');
insert into emaj.emaj_group_def values 
    ('emaj demo group 1','emaj_demo_app_schema','mytbl1',null,'_demo'),
    ('emaj demo group 1','emaj_demo_app_schema','mytbl2',null,'_demo'),
    ('emaj demo group 2','emaj_demo_app_schema','myTbl3_col31_seq',null,null),
    ('emaj demo group 2','emaj_demo_app_schema','myTbl3',null,'_demo'),
    ('emaj demo group 1','emaj_demo_app_schema','mytbl4',null,'_demo'),
    ('emaj demo group 2','emaj_demo_app_schema','myseq1',null,null);

\echo '### Create both table groups (the function returns the number of objects belonging to the group).'
select emaj.emaj_create_group('emaj demo group 1',true);
select emaj.emaj_create_group('emaj demo group 2',true);

\echo '### Set a comment for the first table group.'
select emaj.emaj_comment_group('emaj demo group 1','This group has no sequence');

\echo '### Look at our groups in the internal emaj_group table (They should be IDLE state).'
select * from emaj.emaj_group where group_name in ('emaj demo group 1','emaj demo group 2');

\echo '### Look at the content of the emaj_demo schema containing our log tables.'
select relname from pg_class, pg_namespace 
  where relnamespace = pg_namespace.oid and nspname = 'emaj_demo' and relkind = 'r';

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Application tables can be populated, but without logging ... for   ###'
\echo '###  the moment.                                                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Insert few rows into myTbl1, myTbl2 and myTbl4 tables.'
insert into myTbl1 
  select i, 'ABC', E'\\000\\001\\002'::bytea from generate_series (1,10) as i;
insert into myTbl2 
  select i, 'ABC', current_date + to_char(i,'99 DAYS')::interval from generate_series (1,10) as i;
insert into myTbl4 values (1,'FK...',1,1,'ABC');

\echo '###  As log triggers are not yet enabled, log tables remain empty.'
select * from emaj_demo.emaj_demo_app_schema_mytbl1_log;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us start the first group and perform some table updates.       ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Start the first group to activate its log triggers.'
select emaj.emaj_start_group('emaj demo group 1','MARK1');

\echo '### Perform 1 INSERT, 1 UPDATE and 1 DELETE on myTbl4.'
insert into myTbl4 values (2,'FK...',1,1,'ABC');
update myTbl4 set col43 = 2 where col41 = 2;
delete from myTbl4 where col41 = 1;

\echo '### Look at the myTbl4 log table.'
select * from emaj_demo.emaj_demo_app_schema_mytbl4_log;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us set a second mark on "emaj demo group 1" tables group and   ###'
\echo '###  perform some new tables updates.                                   ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Set MARK2.'
select emaj.emaj_set_mark_group('emaj demo group 1','MARK2');

\echo '### Perform some updates on myTbl1 and myTbl2.'
insert into myTbl1 
  select i, 'DEF', E'\\003\\004\\005'::bytea from generate_series (11,20) as i;
delete from myTbl2 where col21 > 8;

\echo '### What does the statistic function report about our latest updates?'
select * from emaj.emaj_log_stat_group('emaj demo group 1','EMAJ_LAST_MARK',NULL);

\echo '### And in a more detailed way?'
select * from emaj.emaj_detailed_log_stat_group('emaj demo group 1','EMAJ_LAST_MARK',NULL);

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us set a third mark on "emaj demo group 1" tables group and    ###'
\echo '###  perform some new tables updates.                                   ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Set MARK3.'
select emaj.emaj_set_mark_group('emaj demo group 1','MARK3');

\echo '### Perform some updates on myTbl1 and myTbl2.'
update myTbl4 set col43 = NULL where col41 between 5 and 10;
update myTbl2 set col23 = col23 + '1 YEAR'::interval;

\echo '### Look at the known marks for our group 1 (there should be 3 active marks).'
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';

\echo '### Count the total number of logged updates.'
select sum(stat_rows) from emaj.emaj_log_stat_group('emaj demo group 1',NULL,NULL);

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us rollback "emaj demo group 1" tables group to MARK2.         ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### (unlogged) rollback to MARK2.'
select emaj.emaj_rollback_group('emaj demo group 1','MARK2');

\echo '### Look at the known marks for our group 1. MARK 3 has disappeared.'
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';

\echo '### Count the total number of logged updates. 20 have been rollbacked'
select sum(stat_rows) from emaj.emaj_log_stat_group('emaj demo group 1',NULL,NULL);

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us chain again some updates and set mark.                      ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Perform some updates on myTbl1 and myTbl2.'
insert into myTbl1 
  select i, 'GHI', E'\\006\\007\\010'::bytea from generate_series (21,25) as i;
delete from myTbl2 where col21 > 8;

\echo '### Set MARK4.'
select emaj.emaj_set_mark_group('emaj demo group 1','MARK4');

\echo '### Perform some updates on myTbl1 and myTbl2.'
update myTbl2 set col23 = col23 - '1 YEAR'::interval;
update myTbl4 set col43 = NULL where col41 = 2;

\echo '### Set MARK5.'
select emaj.emaj_set_mark_group('emaj demo group 1','MARK5');

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us perform a logged rollback of "emaj demo group 1" to MARK4.  ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### First verify tables content.'
select * from myTbl2 where col23 < current_date;
select * from myTbl4 where col43 is NULL;

\echo '### (unlogged) rollback to MARK2.'
select emaj.emaj_logged_rollback_group('emaj demo group 1','MARK4');

\echo '### Check the resulting tables content.'
select * from myTbl2 where col23 < current_date;
select * from myTbl4 where col43 is NULL;

\echo '### Latests updates have been canceled. But old MARK4 is still. 2 new marks frame the rollback operation.'
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';
\echo '### And the rollback operation has been logged.'
select * from emaj_demo.emaj_demo_app_schema_mytbl4_log;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us improve our marks.                                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Remove the mark corresponding to the logged rollback start (RLBK_MARK4_<time>_START).'
select emaj.emaj_delete_mark_group('emaj demo group 1',emaj.emaj_get_previous_mark_group('emaj demo group 1','EMAJ_LAST_MARK'));

\echo '### Rename and comment the mark corresponding to the logged rollback stop (RLBK_MARK4_<time>_STOP).'
select emaj.emaj_rename_mark_group('emaj demo group 1','EMAJ_LAST_MARK','End of rollback');
select emaj.emaj_comment_mark_group('emaj demo group 1','End of rollback','This mark correspond to the end of the logged rollback');

\echo '### Look at the result in the emaj_group table.'
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';

\echo '### Find the name of the mark set 1 micro-second ago.'
select emaj.emaj_get_previous_mark_group('emaj demo group 1',current_timestamp - '1 microsecond'::interval);

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Now, we can rollback ... our last rollback.                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### (unlogged) rollback to MARK5.'
select emaj.emaj_rollback_group('emaj demo group 1','MARK5');

\echo '### ... and look at the result in the emaj_group table.'
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Estimate how long a roolback would take.                           ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Estimate the duration of a rollback to the first mark.'
\echo '###  (See the documentation to see how this estimate is computed)'
select emaj.emaj_estimate_rollback_duration('emaj demo group 1','MARK1');

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us start the second group and perform some table updates.      ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Start the second group to activate its log triggers.'
select emaj.emaj_start_group('emaj demo group 2','MARK1');

\echo '### perform some table updates and sequence changes.'
insert into "myTbl3" (col33) select generate_series (1,10)*random();
alter sequence emaj_demo_app_schema.mySeq1 increment 3 maxvalue 3000;
select nextval('emaj_demo_app_schema.mySeq1');

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us set a common mark on both groups and perform some updates.  ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Set a mark for both group in a single operation.'
select emaj.emaj_set_mark_groups(array['emaj demo group 1','emaj demo group 2'],'COMMON_MARK2');

\echo '### perform some table updates and sequence changes.'
update "myTbl3" set col32 = col32 + '1 DAY'::interval where col31 <= 3;
select nextval('emaj_demo_app_schema.mySeq1');
insert into myTbl4 values (3,'XYZ',2,2,'ABC');
select nextval('emaj_demo_app_schema.mySeq1');

\echo '### look at the mySeq1 sequence table.'
select * from emaj_demo_app_schema.mySeq1;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Let us rollback both groups together to COMMON_MARK2.              ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Look at the emaj_group table.'
select * from emaj.emaj_mark where mark_group in ('emaj demo group 1','emaj demo group 2');

\unset ON_ERROR_STOP

\echo '### Try first to rollback to MARK1 (each group has one MARK1 mark, but they do not represent the same point in time).'
begin;
  select emaj.emaj_rollback_groups(array['emaj demo group 1','emaj demo group 2'],'MARK1');
rollback;

\set ON_ERROR_STOP

\echo '### So now let us use the real common mark.'
select emaj.emaj_rollback_groups(array['emaj demo group 1','emaj demo group 2'],'COMMON_MARK2');

\echo '### Look at the mySeq1 sequence table to see the result of the rollback.'
select * from emaj_demo_app_schema.mySeq1;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Now let us consider we can forget all what has been logged         ###'
\echo '###  before MARK5 for the first group.                                  ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Look at the log table linked to myTbl4.'
select * from emaj_demo.emaj_demo_app_schema_mytbl4_log;

\echo '### Purge the obsolete log.'
select emaj.emaj_delete_before_mark_group('emaj demo group 1','MARK5');

\echo '### Look at the result in the log table and in the emaj_group table.'
select * from emaj_demo.emaj_demo_app_schema_mytbl4_log;
select * from emaj.emaj_mark where mark_group = 'emaj demo group 1';

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Our logging period is completed. Let us stop the groups.           ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Stop the first group.'
select emaj.emaj_stop_group('emaj demo group 1');

\echo '### Stop the second group and set a specific stop mark name.'
select emaj.emaj_stop_group('emaj demo group 2','Our own stop mark for the second group');

\echo '### The marks are still there but are logically deleted.'
select * from emaj.emaj_mark where mark_group in ('emaj demo group 1','emaj demo group 2');

\echo '### The log rows are still there too, but are not usable for any rollback.'
select * from emaj_demo."emaj_demo_app_schema_myTbl3_log";

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Sometimes, tables structure or tables groups content change...     ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### While the first group is IDLE, alter the myTbl4 table.'
alter table emaj_demo_app_schema.myTbl4 add column col46 bigint;

\echo '### Move mySeq1 sequence into the first group.'
update emaj.emaj_group_def set grpdef_group = 'emaj demo group 1' where grpdef_tblseq = 'myseq1' and grpdef_group = 'emaj demo group 2';

\echo '### Ask E-Maj to take into account these changes (starting with the second group to detach the sequence from it).'
select emaj.emaj_alter_group('emaj demo group 2');
select emaj.emaj_alter_group('emaj demo group 1');

\echo '### Both groups are now ready to be started.'

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  The emaj_hist table records all E-Maj events. It can be examined   ###'
\echo '###  by the administrator at any time if needed.                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\echo '### Here is the part of the history that reflects the execution of this script.'
select * from emaj.emaj_hist where hist_id >= (select hist_id from emaj.emaj_hist where hist_function = 'CREATE_GROUP' and hist_event = 'BEGIN' and hist_object = 'emaj demo group 1' order by hist_id desc limit 1);

\echo '### The demo environment is left as is to let you play with it.'
\echo '### To remove it, just execute:'
\echo '###     SELECT emaj.emaj_demo_cleanup();'
\echo '### This demo script can be rerun as many times as you wish.'
\echo '###'
\echo '### This ends the E-Maj demo. Thank You for using E-Maj and have fun!'

