-- demo.sql : V 0.11.0
--
-- This test script must be executed with a superuser role.
-- E-Maj extension must have been previously installed.
-- It will also use a "normal" role for application operations. 
-- This role myUser must have been created before, for instance with sql verbs like:
--    CREATE ROLE myUser LOGIN PASSWORD '';
--    GRANT ALL ON DATABASE <this database> TO myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                          EMAJ Demo start                            ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\set ON_ERROR_STOP
\set ECHO queries

-- give myUser the emaj_viewer rights
GRANT emaj_viewer TO myUser;

SET ROLE myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '### Role myUser creates its schema, tables, index, sequences, triggers  ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- create a schema and application tables for the test

DROP SCHEMA IF EXISTS mySchema CASCADE;
CREATE SCHEMA mySchema;
ALTER ROLE myUser SET search_path=mySchema;
SET search_path=mySchema;

DROP TABLE IF EXISTS myTbl1;
CREATE TABLE myschema.myTbl1 (
  col11       DECIMAL(7)       NOT NULL,
  col12       CHAR(10)         NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);
DROP TABLE IF EXISTS myTbl2;
CREATE TABLE myschema.myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);
DROP TABLE IF EXISTS "myTbl3";
CREATE TABLE myschema."myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32,col33)
;
DROP TABLE IF EXISTS myTbl4;
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

-- table myTbl5 logs myTbl2 changes via a trigger
DROP TABLE IF EXISTS myTbl5;
CREATE TABLE myschema.myTbl5 (
  col50       SERIAL           NOT NULL,
  col51       INT              NOT NULL,
  PRIMARY KEY (col50)
);

CREATE or REPLACE FUNCTION myschema.myTbl2trgfct () RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO myTbl5 (col51) SELECT OLD.col21;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO myTbl5 (col51) SELECT NEW.col21;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO myTbl5 (col51) SELECT NEW.col21;
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE TRIGGER myTbl2trg
  AFTER INSERT OR UPDATE OR DELETE ON myschema.myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct ();

RESET ROLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###  Now the emaj administrator defines the group\'s definition and     ###'
\echo '###  creates the group                                                  ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- populate group table
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl1',null);
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl2',null);
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3_col31_seq',null);
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3',null);
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl4',null);
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl5',null);

-- create EMAJ objects
\echo '--- Create EMAJ objects ---'
select emaj.emaj_create_group('myAppl1');

\echo '--- look at the emaj_group and emaj_relation tables'
select * from emaj.emaj_group;
select * from emaj.emaj_relation;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        S T A R T _ G R O U P                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- activate log trigger to simulate the begining of the batch window
\echo '--- Log triggers activation and setting of a MARK1 mark ---'
select emaj.emaj_start_group('myAppl1','MARK1');
select * from emaj.emaj_sequence;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###             Role myUser performs a 1st set of updates               ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- simulation of an application processing
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
\echo '###                    S E T _ M A R K _ G R O U P   MARK2              ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- Set a second mark. Then simulate another application processing

\echo '--- Set a MARK2 mark ---'
select emaj.emaj_set_mark_group('myAppl1','MARK2');
select * from emaj.emaj_mark;

\echo '---    and look at logs ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;

SET ROLE myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###             Role myUser performs a 2nd set of updates               ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- sequence change
alter sequence myschema."myTbl3_col31_seq" increment 3 maxvalue 10000000;

-- application tables updates
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
\echo '###                    S E T _ M A R K _ G R O U P    MARK3             ###'
\echo '###                                                                     ###'
\echo '###########################################################################'
-- Set a third mark. Then simulate another application processing

\echo '--- Set a MARK3 mark ---'
select emaj.emaj_set_mark_group('myAppl1','MARK3');
select * from emaj.emaj_mark;

\echo '---    and look at logs ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;

SET ROLE myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###           Role myUser performs a 3dr set of updates                 ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

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
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;

\echo '--- statistics on log tables on various marks ranges and with various detail levels ---'
select * from emaj.emaj_log_stat_group('myAppl1','MARK1','MARK2');
select * from emaj.emaj_log_stat_group('myAppl1','MARK2','MARK3');
select * from emaj.emaj_log_stat_group('myAppl1',NULL,'MARK2');
select * from emaj.emaj_log_stat_group('myAppl1','MARK3',NULL);
select * from emaj.emaj_detailed_log_stat_group('myAppl1',NULL,NULL);

\echo '--- rename MARK1 mark, delete MARK2 mark and see the consequences on emaj tables ---'
select emaj.emaj_delete_mark_group('myAppl1','MARK2');
select emaj.emaj_rename_mark_group('myAppl1','MARK1','First Mark for Appli1');

select * from emaj.emaj_mark;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   1                          ###'
\echo '###           L O G G E D _ R O L L B A C K   T O   MARK3               ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback to an intermediate mark
\echo '--- rollback to MARK3 ---'
select emaj.emaj_logged_rollback_group('myAppl1','EMAJ_LAST_MARK');
select * from emaj.emaj_mark;

\echo '--- log tables after rollback to mark MARK3 ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;
select nextval('myschema."myTbl3_col31_seq"');

SET ROLE myUser;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###           Role myUser performs a 4th set of updates                 ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- application tables updates
insert into myTbl1 values (4,'MNO',E'\014'::bytea);
insert into myTbl1 values (4,'PQR',E'\014'::bytea);
insert into "myTbl3" (col33) values (4);

RESET ROLE;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   2                          ###'
\echo '###          R E G U L A R _ R O L L B A C K   T O   MARK3              ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback again to the intermediate mark already used for a previous rollback
\echo '--- rollback again to mark MARK3, but without logging ---'
select emaj.emaj_rollback_group('myAppl1','MARK3');
select * from emaj.emaj_mark;

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        R O L L B A C K   3                          ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- Rollback to the initial mark
\echo '--- rollback the renamed initial mark and stop the group (deactivate the triggers) ---'
select emaj.emaj_rollback_and_stop_group('myAppl1','First Mark for Appli1');

select * from emaj.emaj_mark;

\echo '--- log tables after rollback to mark MARK1 ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;

-- reset log tables
\echo '--- Reset log tables ---'
select emaj.emaj_reset_group('myAppl1');

\echo '--- Log tables after reset ---'
select * from emaj.myschema_myTbl1_log;
select * from emaj.myschema_myTbl2_log;
select * from emaj."myschema_myTbl3_log";
select * from emaj.myschema_myTbl5_log;
select * from emaj.emaj_sequence;

\echo '---    and myTbl3_col31_seq sequence'
select * from "myTbl3_col31_seq";

select * from emaj.emaj_mark;

-- deletion of log objects
\echo '--- Deletion of log objects ---'
select emaj.emaj_drop_group('myAppl1');

-- show the whole history of operations
\echo '--- History table ---'
select * from emaj.emaj_hist ORDER BY hist_datetime;

\echo '--- End of E-Maj demo ---'

\unset ECHO

