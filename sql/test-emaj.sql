-- test_emaj.sql : V 0.9.1
--
-- This test script must be executed with a superuser role.  
-- It will also use a "normal" role for application operations. 
-- This role myUser must have been created before, for instance with sql verbs like:
--    CREATE ROLE myUser LOGIN PASSWORD '';
--    GRANT ALL ON DATABASE <this database> TO myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                          EMAJ test start                            ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\set ON_ERROR_STOP
\set ECHO all

-- give myUser the emaj_viewer rights
GRANT emaj_viewer TO myUser;

SET ROLE myUser;

-- create a schema and application table for the test
\echo '--- Create an application schema and tables for the test ---'

DROP SCHEMA IF EXISTS mySchema CASCADE;
CREATE SCHEMA mySchema;
ALTER ROLE myUser SET search_path=mySchema;
SET search_path=mySchema;

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myschema.myTbl1 (
  col11       DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);
DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myschema.myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);
DROP TABLE IF EXISTS "myTbl3" ;
CREATE TABLE myschema."myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32,col33);
DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myschema.myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21),
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE
);

-- table myTbl2b logs myTbl2 changes via a trigger
DROP TABLE IF EXISTS myTbl2b ;
CREATE TABLE myschema.myTbl2b (
  col20       SERIAL           NOT NULL,
  col21       INT              NOT NULL,
  PRIMARY KEY (col20)
);

CREATE or REPLACE FUNCTION myschema.myTbl2trgfct () RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO myTbl2b (col21) SELECT OLD.col21;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE TRIGGER myTbl2trg
  AFTER INSERT OR UPDATE OR DELETE ON myschema.myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct ();

RESET ROLE;

-- populate group table
\echo '--- Now Superuser populates the emaj_group_def table ---'
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl1');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl2');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl2b');
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl4');

-- create EMAJ objects
\echo '--- Create EMAJ objects ---'
select emaj.emaj_create_group('myAppl1');
--select emaj.emaj_set_mark_group('myAppl1','BATCH2');
--select emaj.emaj_stop_group('myAppl1');
--select emaj.emaj_rollback_group('myAppl1','BATCH2');

\echo '--- look at the emaj_group and emaj_relation tables'
select * from emaj.emaj_group;
select * from emaj.emaj_relation;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        S T A R T _ G R O U P                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- activate log trigger to simulate the begining of the batch window
\echo '--- Log triggers activation and setting of a BATCH1 mark ---'
select emaj.emaj_start_group('myAppl1','BATCH1');
--select emaj.emaj_reset_group('myAppl1');
--select emaj.emaj_drop_group('myAppl1');
select * from emaj.emaj_sequence;

-- try a truncate a table to see how it reacts
\echo 'PostgreSQL version'
SELECT version();

\unset ON_ERROR_STOP
BEGIN;
TRUNCATE myschema.myTbl4 ;
ROLLBACK;
\set ON_ERROR_STOP

-- simulate an application processing
SET ROLE myUser;
\echo '--- Simulate the update activity of a first batch program ---'
insert into myTbl1 values (1,'ABC',E'\014'::bytea);
insert into myTbl1 values (1,'DEF',E'\014'::bytea);
insert into myTbl2 values (1,'ABC',current_date);
insert into myTbl2 values (2,'DEF',NULL);
insert into myTbl4 values (1,'FK...',1,1,'ABC');
update myTbl1 set col13=E'\034'::bytea where col12='ABC';
update myTbl1 set col13=NULL where col12='DEF';
insert into "myTbl3" (col33) select generate_series (1,10)*random();

RESET ROLE;
\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   1                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback the activity
\echo '--- rollback to the begining ---'
select emaj.emaj_rollback_group('myAppl1','BATCH1');
\echo '--- sequence holes after rollback to mark BATCH1 ---'
select * from emaj.emaj_seq_hole;

select * from emaj.emaj_log_stat_group('myAppl1',NULL,NULL);

-- rerun the application processing simulation
SET ROLE myUser;
\echo '--- Simulate the update activity of a first batch program ---'
insert into myTbl1 values (1,'ABC',E'\014'::bytea);
insert into myTbl1 values (1,'DEF',E'\014'::bytea);
insert into myTbl2 values (1,'ABC',current_date);
insert into myTbl2 values (2,'DEF',NULL);
insert into myTbl4 values (1,'FK...',1,1,'ABC');
update myTbl1 set col13=E'\034'::bytea where col12='ABC';
update myTbl1 set col13=NULL where col12='DEF';
insert into "myTbl3" (col33) select generate_series (1,10)*random();

\echo '--- Show the myTbl3_col31_seq sequence'
select * from "myTbl3_col31_seq";
select nextval('myschema."myTbl3_col31_seq"');

RESET ROLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                    S E T _ M A R K _ G R O U P   BATCH2             ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- Set a second mark. Then simulate another application processing

\echo '--- Set a BATCH2 mark ---'
select emaj.emaj_set_mark_group('myAppl1','BATCH2');
--select emaj.emaj_set_mark_group('myAppl1','BATCH2');
select * from emaj.emaj_mark;

\echo '---    and look at logs ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;

SET ROLE myUser;
--drop table myschema.mytbl1 cascade;

\echo '--- Simulate the update activity of a second batch program, including a sequence change ---'
-- sequence change
alter sequence myschema."myTbl3_col31_seq" increment 3 maxvalue 10000000;
-- test table structure change
--alter table myTbl1 alter column col12 type char(20);

-- application tables updates
\echo '--- perform 100 inserts and rollback them immediately'
begin transaction;
insert into "myTbl3" (col33) select 1000+generate_series (1,100)*random();
rollback;

\echo '--- Other regular tables writes'
insert into myTbl1 values (1,'GHI',E'\014'::bytea);
insert into myTbl1 values (1,'JKL',E'\014'::bytea);
delete from myTbl1 where col12 = 'DEF';
insert into myTbl2 values (3,'GHI','01/01/2009');
update "myTbl3" set col33 = 0 where col31 = 1;
insert into "myTbl3" (col33) values (3);

\echo '--- Look at the myTbl3_col31_seq sequence'
select * from "myTbl3_col31_seq";

RESET ROLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                    S E T _ M A R K _ G R O U P    BATCH3            ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- Set a third mark. Then simulate another application processing

\echo '--- Set a BATCH3 mark and a mark with generated name ---'
select emaj.emaj_set_mark_group('myAppl1','BATCH3');
--select emaj.emaj_set_mark_group('myAppl1','BATCH3');
select emaj.emaj_set_mark_group('myAppl1',NULL);
select * from emaj.emaj_mark;

\echo '---    and look at logs ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;

\echo '--- take a snap of tables content for the group --- '
select emaj.emaj_snap_group('myAppl1','/tmp');

SET ROLE myUser;
\echo '--- Simulate the update activity of a third batch program ---'

-- application tables updates
insert into myTbl1 values (1,'MNO',E'\014'::bytea);
insert into myTbl1 values (1,'PQR',E'\014'::bytea);
insert into "myTbl3" (col33) values (4);
delete from "myTbl3";

RESET ROLE;

\echo '--- Log tables at the end of the second batch processing ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;

\echo '--- statistics on log tables on various marks ranges ---'
select * from emaj.emaj_detailed_log_stat_group('myAppl1','BATCH1','BATCH2');
select * from emaj.emaj_log_stat_group('myAppl1','BATCH1','BATCH2');
select * from emaj.emaj_detailed_log_stat_group('myAppl1','BATCH2','BATCH3');
select * from emaj.emaj_log_stat_group('myAppl1','BATCH2','BATCH3');
select * from emaj.emaj_detailed_log_stat_group('myAppl1',NULL,'BATCH2');
select * from emaj.emaj_log_stat_group('myAppl1',NULL,'BATCH2');
select * from emaj.emaj_detailed_log_stat_group('myAppl1','BATCH3',NULL);
select * from emaj.emaj_log_stat_group('myAppl1','BATCH3',NULL);
select * from emaj.emaj_detailed_log_stat_group('myAppl1',NULL,NULL);
select * from emaj.emaj_log_stat_group('myAppl1',NULL,NULL);

\echo '--- delete the BATCH2 mark and see the consequences on emaj tables ---'
select emaj.emaj_delete_mark_group('myAppl1','BATCH2');
--select emaj.emaj_delete_mark_group('myAppl1','BATCH1');
select * from emaj.emaj_mark;
select * from emaj.emaj_sequence;

-- get the rollback duration estimate with no statistics
select * from emaj.emaj_rlbk_stat;
select emaj.emaj_estimate_rollback_duration('myAppl1','BATCH1');
-- analyze to get proper statistics (in particular the pg_class.reltuples) 
analyze;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   2                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback to an intermediate mark
\echo '--- rollback to mark BATCH3 ---'
select emaj.emaj_rollback_group('myAppl1','EMAJ_LAST_MARK');
--select emaj.emaj_rollback_group('myAppl1','BATCH3');
select * from emaj.emaj_mark;
\echo '--- sequence holes after rollback to mark BATCH3 ---'
select * from emaj.emaj_seq_hole;
\echo '--- log tables after rollback to mark BATCH3 ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;
select nextval('myschema."myTbl3_col31_seq"');

SET ROLE myUser;
\echo '--- Simulate the update activity of a fourth batch program started after first rollback ---'

-- application tables updates
insert into myTbl1 values (4,'MNO',E'\014'::bytea);
insert into myTbl1 values (4,'PQR',E'\014'::bytea);
insert into "myTbl3" (col33) values (4);

RESET ROLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   3                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback again to the intermediate mark already used for a previous rollback
\echo '--- rollback again to mark BATCH3 ---'
select emaj.emaj_rollback_group('myAppl1','BATCH3');
select * from emaj.emaj_mark;

\echo '--- sequence holes after rollback to mark BATCH3 ---'
select * from emaj.emaj_seq_hole;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   4   (logged)               ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- logged rollback to the initial mark
\echo '--- rollback to mark BATCH1 ---'
select emaj.emaj_logged_rollback_group('myAppl1','EMAJ_LAST_MARK');
select emaj.emaj_logged_rollback_group('myAppl1','BATCH1');

select * from emaj.emaj_mark;
select * from emaj.emaj_sequence;
select * from emaj.emaj_seq_hole;
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   5                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- get the rollback duration estimate
select * from emaj.emaj_rlbk_stat;
select emaj.emaj_estimate_rollback_duration('myAppl1','BATCH1');

--select pg_sleep(5);

-- Rename the initial mark
\echo '--- rename mark BATCH1 into FIRST-BATCH ---'
select emaj.emaj_rename_mark_group('myAppl1','BATCH1','FIRST-BATCH');

select * from emaj.emaj_mark;
select * from emaj.emaj_sequence;
select * from emaj.emaj_seq_hole;
-- Rollback to the initial mark
\echo '--- rollback to mark BATCH1, now renamed, and stop the group ---'
select emaj.emaj_rollback_and_stop_group('myAppl1','FIRST-BATCH');
--select emaj.emaj_rollback_group('myAppl1','FIRST-BATCH');

\echo '--- sequence holes after rollback to mark FIRST-BATCH ---'
select * from emaj.emaj_seq_hole;

-- stop again the group (issues a warning)
select emaj.emaj_stop_group('myAppl1');

select * from emaj.emaj_mark;

\echo '--- log tables after rollback to mark BATCH1 ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;

-- deactivate log triggers (end of batch window)
--\echo '--- Deactivate log triggers ---'
--select emaj.emaj_stop_group('myAppl1');
--select emaj.emaj_start_group('myAppl1','BATCH1');

-- reset log tables
\echo '--- Reset log tables ---'
select emaj.emaj_reset_group('myAppl1');

\echo '--- Log tables after reset ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl2b_log;
select * from emaj.emaj_sequence;

\echo '---    and myTbl3_col31_seq sequence'
select * from "myTbl3_col31_seq";

select * from emaj.emaj_mark;

-- deletion of log objects
\echo '--- Deletion of log objects ---'
--select emaj.emaj_drop_group('myAppl1');
select group_name, emaj.emaj_drop_group(group_name) from emaj.emaj_group;

-- show the whole history of operations
\echo '--- History table ---'
select * from emaj.emaj_hist ORDER BY hist_datetime;

\echo '--- test-emaj script successfully completed ---'

\unset ECHO

