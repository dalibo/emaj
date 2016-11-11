-- ppa.sql: Create and setup all application objects that will be needed for the ppa plugin tests
--
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP

------------------------------------------------------------
-- drop any remaining tables groups
------------------------------------------------------------
SELECT emaj.emaj_force_drop_group(group_name) FROM emaj.emaj_group;

------------------------------------------------------------
-- setup emaj parameters
------------------------------------------------------------
DELETE FROM emaj.emaj_param WHERE param_key = 'dblink_user_password';
INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES 
  ('dblink_user_password','user=postgres password=postgres');

------------------------------------------------------------
-- create 3 application schemas with tables, sequences, triggers
------------------------------------------------------------
--
-- First schema
--
DROP SCHEMA IF EXISTS mySchema1 CASCADE;
CREATE SCHEMA mySchema1;

SET search_path=mySchema1;

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myTbl1 (
  badName     DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (badName,col12)
);
ALTER TABLE myTbl1 RENAME badName TO col11;
CREATE INDEX myTbl1_idx on myTbl1 (col13);

DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);

DROP TABLE IF EXISTS "myTbl3" ;
CREATE TABLE "myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32,col33);

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21),
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL
);

DROP TABLE IF EXISTS myTbl2b ;
CREATE TABLE myTbl2b (
  col20       SERIAL           NOT NULL,
  col21       INT              NOT NULL,
  PRIMARY KEY (col20)
);

CREATE or REPLACE FUNCTION myTbl2trgfct () RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT OLD.col21;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO mySchema1.myTbl2b (col21) SELECT NEW.col21;
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE TRIGGER myTbl2trg
  AFTER INSERT OR UPDATE OR DELETE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct ();

-- Second schema

DROP SCHEMA IF EXISTS mySchema2 CASCADE;
CREATE SCHEMA mySchema2;

SET search_path=mySchema2;

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myTbl1 (
  col11       DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);

DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE             ,
  PRIMARY KEY (col21)
);

DROP TABLE IF EXISTS "myTbl3" ;
CREATE TABLE "myTbl3" (
  col31       SERIAL           NOT NULL,
  col32       TIMESTAMP        DEFAULT now(),
  col33       DECIMAL (12,2)   ,
  PRIMARY KEY (col31)
);
CREATE INDEX myIdx3 ON "myTbl3" (col32,col33);

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21),
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL
);

CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

-- Third schema (for a audit_only group)

DROP SCHEMA IF EXISTS "phil's schema3" CASCADE;
CREATE SCHEMA "phil's schema3";

SET search_path="phil's schema3";

DROP TABLE IF EXISTS myTbl1 ;
CREATE TABLE myTbl1 (
  col11       DECIMAL (7)      NOT NULL,
  col12       CHAR (10)        NOT NULL,
  col13       BYTEA            ,
  PRIMARY KEY (col11,col12)
);

DROP TABLE IF EXISTS myTbl2 ;
CREATE TABLE myTbl2 (
  col21       INT              NOT NULL,
  col22       TEXT             ,
  col23       DATE
);

CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

-----------------------------
-- create roles and give rigths on application objects
-----------------------------
\unset ON_ERROR_STOP
create role emaj_regression_tests_adm_user login password '';
create role emaj_regression_tests_viewer_user login password '';
create role emaj_regression_tests_anonym_user login password '';
\set ON_ERROR_STOP
--
grant all on schema mySchema1, mySchema2, "phil's schema3" to emaj_regression_tests_adm_user, emaj_regression_tests_viewer_user;
--
grant select on mySchema1.myTbl1, mySchema1.myTbl2, mySchema1."myTbl3", mySchema1.myTbl4, mySchema1.myTbl2b to emaj_regression_tests_viewer_user;
grant select on mySchema2.myTbl1, mySchema2.myTbl2, mySchema2."myTbl3", mySchema2.myTbl4 to emaj_regression_tests_viewer_user;
grant select on "phil's schema3".myTbl1, "phil's schema3".myTbl2 to emaj_regression_tests_viewer_user;
grant select on sequence mySchema1."myTbl3_col31_seq" to emaj_regression_tests_viewer_user;
grant select on sequence mySchema2."myTbl3_col31_seq" to emaj_regression_tests_viewer_user;
--
grant all on mySchema1.myTbl1, mySchema1.myTbl2, mySchema1."myTbl3", mySchema1.myTbl4, mySchema1.myTbl2b to emaj_regression_tests_adm_user;
grant all on mySchema2.myTbl1, mySchema2.myTbl2, mySchema2."myTbl3", mySchema2.myTbl4 to emaj_regression_tests_adm_user;
grant all on "phil's schema3".myTbl1, "phil's schema3".myTbl2 to emaj_regression_tests_adm_user;
grant all on sequence mySchema1."myTbl3_col31_seq" to emaj_regression_tests_adm_user;
grant all on sequence mySchema2."myTbl3_col31_seq" to emaj_regression_tests_adm_user;
grant all on sequence mySchema2.mySeq1 to emaj_regression_tests_adm_user;
grant all on sequence "phil's schema3".mySeq1 to emaj_regression_tests_adm_user;

-----------------------------
-- prepare and create groups
-----------------------------
delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl1',20);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2',NULL);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl2b',NULL);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3_col31_seq',1);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','myTbl3',10);
insert into emaj.emaj_group_def values ('myGroup1','myschema1','mytbl4',20);
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl1');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl2');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3_col31_seq');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myTbl3');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','mytbl4');
insert into emaj.emaj_group_def values ('myGroup2','myschema2','myseq1');
-- The third group name contains space, comma # and '
insert into emaj.emaj_group_def values ('phil''s group#3','phil''s schema3','mytbl1');
insert into emaj.emaj_group_def values ('phil''s group#3','phil''s schema3','mytbl2');
insert into emaj.emaj_group_def values ('phil''s group#3','phil''s schema3','myseq1');
insert into emaj.emaj_group_def values ('dummyGrp1','dummySchema','mytbl4');
insert into emaj.emaj_group_def values ('dummyGrp2','myschema1','dummyTable');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema1','mytbl1');
insert into emaj.emaj_group_def values ('dummyGrp3','myschema2','mytbl2');


select emaj.emaj_create_group('myGroup1',true);
select emaj.emaj_create_group('myGroup2',true);
select emaj.emaj_create_group('phil''s group#3',false);
select emaj.emaj_comment_group('myGroup1','Useless comment!');

-----------------------------
-- start 2 groups with a mark MARK1
-----------------------------

select emaj.emaj_start_groups('{"myGroup1","myGroup2"}','MARK1');

-----------------------------
-- simulation of a first application processing only updating mySchema1
-----------------------------

SET search_path=mySchema1;
insert into myTbl1 values (1,'ABC',E'\014'::bytea);
insert into myTbl1 values (1,'DEF',E'\014'::bytea);
insert into myTbl2 values (1,'ABC',current_date);
insert into myTbl2 values (2,'DEF',NULL);
insert into myTbl4 values (1,'FK...',1,1,'ABC');
update myTbl1 set col13=E'\034'::bytea where col12='ABC';
update myTbl1 set col13=NULL where col12='DEF';
insert into "myTbl3" (col33) select generate_series (1,10)*random();

-----------------------------
-- set a second MARK2 mark on myGroup1 only, with a comment
-----------------------------

select emaj.emaj_set_mark_group('myGroup1','MARK2');
select emaj.emaj_comment_mark_group('myGroup1','MARK2','End of 1st program');

-----------------------------
-- simulation of a second application processing only updating mySchema1
-----------------------------

SET search_path=mySchema1;
-- sequence change
alter sequence mySchema1."myTbl3_col31_seq" increment 3 maxvalue 10000000;
-- application tables updates
insert into myTbl1 values (1,'GHI',E'\014'::bytea);
insert into myTbl1 values (1,'JKL',E'\014'::bytea);
delete from myTbl1 where col12 = 'DEF';
insert into myTbl2 values (3,'GHI','01/01/2009');
update "myTbl3" set col33 = 0 where col31 = 1;
insert into "myTbl3" (col33) values (3);

-----------------------------
-- set a MARK3 mark on myGroup1 and myGroup2
-----------------------------

select emaj.emaj_set_mark_groups('{"myGroup1","myGroup2"}','MARK3');

-----------------------------
-- simulation of a third application processing only updating mySchema2
-----------------------------
SET search_path=mySchema2;
-- application tables updates
insert into myTbl1 values (1,'MNO',E'\014'::bytea);
insert into myTbl1 values (1,'PQR',E'\014'::bytea);
insert into "myTbl3" (col33) values (4);
delete from "myTbl3";

-----------------------------
-- logged rollback myGroup2 to 'MARK3'
-----------------------------

select emaj.emaj_set_mark_group('myGroup2','tmp_mark');
select emaj.emaj_logged_rollback_group('myGroup2','MARK1');
select emaj.emaj_rollback_group('myGroup2','tmp_mark');
select emaj.emaj_delete_mark_group('myGroup2','tmp_mark');

select emaj.emaj_logged_rollback_group('myGroup2','MARK3');

RESET search_path;

