-- prep-pr.sql : Version <NEXT_VERSION>
--
-- This script prepares an application and E-Maj context to test parallel rollback 
-- using emajParallelRollback PHP module. It must be executed with a superuser role.  
-- It will also use a "normal" role for application operations. 
-- This role myUser must have been created before, for instance with sql verbs like:
--    CREATE ROLE myUser LOGIN PASSWORD '';
--    GRANT ALL ON DATABASE <this database> TO myUser;

-- Once this file is executed, you can type 
-- ./php/emajParallelRollback.php -g myAppl1 -m BATCH1 -s 3 -v <and connection parameters> 

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###     Settint up an environment to test E-Maj parallel rollback       ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

\set ON_ERROR_STOP
\set ECHO queries

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
-- update emaj.emaj_param set param_value_boolean = true where param_key = 'log_only';

-- populate group table
\echo '--- Now Superuser populates the emaj_group_def table ---'
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl1');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl2');
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myAppl1','myschema','myTbl3');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl2b');
insert into emaj.emaj_group_def values ('myAppl1','myschema','mytbl4');

-- create EMAJ objects
\echo '--- Create EMAJ objects ---'
select emaj.emaj_create_group('myAppl1');

\echo '###########################################################################'
\echo '###                                                                     ###'
\echo '###                        S T A R T _ G R O U P                        ###'
\echo '###                                                                     ###'
\echo '###########################################################################'

-- activate log trigger to simulate the begining of the batch window
\echo '--- Log triggers activation and setting of a BATCH1 mark ---'
select emaj.emaj_start_group('myAppl1','BATCH1');

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

RESET ROLE;

\echo '--- Set a BATCH2 mark ---'
select emaj.emaj_set_mark_group('myAppl1','BATCH2');

SET ROLE myUser;
\echo '--- Simulate the update activity of a second batch program, including a sequence change ---'
-- sequence change
alter sequence myschema."myTbl3_col31_seq" increment 3 maxvalue 10000000;

-- application tables updates
insert into myTbl1 values (1,'GHI',E'\014'::bytea);
insert into myTbl1 values (1,'JKL',E'\014'::bytea);
delete from myTbl1 where col12 = 'DEF';
insert into myTbl2 values (3,'GHI','01/01/2009');
update "myTbl3" set col33 = 0 where col31 = 1;
insert into "myTbl3" (col33) values (3);

RESET ROLE;

\echo '--- Set a BATCH3 mark ---'
select emaj.emaj_set_mark_group('myAppl1','BATCH3');

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

\echo '--- deactivate trigger on mytbl2 ---'
alter table mytbl2 disable trigger mytbl2trg;
--select emaj.emaj_rollback_group('myAppl1','BATCH1');

\echo '--- test-emaj-2 script successfuly completed. ---'
\echo '--- A parallel rollback can be performed, using a command like:'
\echo '    ./emajParallelRollback.php -g myAppl1 -m BATCH3 -s 3 -v <and any needed connection parameters among -dhpUW>'

\unset ECHO

