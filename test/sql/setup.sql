-- setup.sql: Create and setup all application objects that will be needed for regression tests
--            Also perform some checks about emaj functions rights and commments
--
SET client_min_messages TO WARNING;

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
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED
);

DO $$
BEGIN
  DROP TABLE IF EXISTS myTbl2b ;
  IF emaj._pg_version_num() < 100000 THEN
    EXECUTE 'CREATE TABLE myTbl2b (
      col20       SERIAL           NOT NULL,
      col21       INT              NOT NULL,
      PRIMARY KEY (col20)
    );';
  ELSE
    EXECUTE 'CREATE TABLE myTbl2b (
      col20       INT              NOT NULL GENERATED ALWAYS AS IDENTITY,
      col21       INT              NOT NULL,
      PRIMARY KEY (col20)
    );';
  END IF;
END;
$$;

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
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL
);

DROP TABLE IF EXISTS myTbl5 ;
CREATE TABLE myTbl5 (
  col51       INT              NOT NULL,
  col52       TEXT[]           ,
  col53       INT[]            ,
  col54       DATE[]           ,
  col55       JSON             ,
  PRIMARY KEY (col51)
);

DROP TABLE IF EXISTS myTbl6 ;
CREATE TABLE myTbl6 (
  col61       INT4             NOT NULL,
  col62       POINT            ,
  col63       BOX              ,
  col64       CIRCLE           ,
  col65       PATH             ,
  col66       inet             ,
  PRIMARY KEY (col61)
);

-- This table will remain outside table groups and a foreign key will be created later in createDrop.sql
DROP TABLE IF EXISTS myTbl7 ;
CREATE TABLE myTbl7 (
  col71       INT              NOT NULL,
  PRIMARY KEY (col71)
);

-- This table will remain outside table groups and a foreign key will be created later in createDrop.sql
DROP TABLE IF EXISTS myTbl8 ;
CREATE TABLE myTbl8 (
  col81       INT              NOT NULL,
  PRIMARY KEY (col81)
);

CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

-- This sequence will remain outside any groups until the addition into a group in logging state
CREATE SEQUENCE mySeq2;

-- Third schema (for an audit_only group)

DROP SCHEMA IF EXISTS "phil's schema3" CASCADE;
CREATE SCHEMA "phil's schema3";

SET search_path="phil's schema3";

DROP TABLE IF EXISTS "phil's tbl1" ;
CREATE TABLE "phil's tbl1" (
  "phil's col11" DECIMAL (7)      NOT NULL,
  "phil's col12" CHAR (10)        NOT NULL,
  "phil\s col13" BYTEA            ,
  PRIMARY KEY ("phil's col11","phil's col12")
);

DROP TABLE IF EXISTS "myTbl2\" ;
CREATE TABLE "myTbl2\" (
  col21       SERIAL           NOT NULL,
  col22       TEXT             ,
  col23       DATE
);

DROP TABLE IF EXISTS myTbl4 ;
CREATE TABLE myTbl4 (
  col41       INT              NOT NULL,
  col42       TEXT             ,
  col43       INT              ,
  col44       DECIMAL(7)       ,
  col45       CHAR(10)         ,
  PRIMARY KEY (col41),
  FOREIGN KEY (col44,col45) REFERENCES "phil's tbl1" ("phil's col11","phil's col12") ON DELETE CASCADE ON UPDATE SET NULL
);
ALTER TABLE "myTbl2\" ADD CONSTRAINT mytbl2_col21_fkey FOREIGN KEY (col21) REFERENCES myTbl4 (col41);

CREATE SEQUENCE "phil's seq\1" MINVALUE 1000 MAXVALUE 2000 CYCLE;

-- Fourth schema (for partitioning)

DROP SCHEMA IF EXISTS mySchema4 CASCADE;
CREATE SCHEMA mySchema4;

SET search_path=mySchema4;

-- Old partitionning style

DROP TABLE IF EXISTS myTblM ;
CREATE TABLE myTblM (
  col1       DATE             NOT NULL,
  col2       INT              ,
  col3       VARCHAR          ,
  PRIMARY KEY (col1)
);

DROP TABLE IF EXISTS myTblC1 ;
CREATE TABLE myTblC1 (CHECK (col1 BETWEEN '2000-01-01' AND '2009-12-31'), PRIMARY KEY (col1, col2)) INHERITS (myTblM);

DROP TABLE IF EXISTS myTblC2 ;
CREATE TABLE myTblC2 (CHECK (col1 BETWEEN '2010-01-01' AND '2019-12-31'), PRIMARY KEY (col1, col2)) INHERITS (myTblM);

DROP TRIGGER IF EXISTS myTblM_insert_trigger ON myTblM; 
CREATE OR REPLACE FUNCTION myTblM_insert_trigger() RETURNS TRIGGER AS $trigger$ 
BEGIN 
  IF NEW.col1 BETWEEN '2000-01-01' AND '2009-12-31' THEN
    INSERT INTO myschema4.myTblC1 VALUES (NEW.*);
    RETURN NULL;
  ELSEIF NEW.col1 BETWEEN '2010-01-01' AND '2019-12-31' THEN
    INSERT INTO myschema4.myTblC2 VALUES (NEW.*);
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$trigger$ LANGUAGE PLPGSQL;
CREATE TRIGGER myTblM_insert_trigger BEFORE INSERT ON myTblM FOR EACH ROW EXECUTE PROCEDURE mySchema4.myTblM_insert_trigger();

-- New partitionning style(PG 10+)

DROP TABLE IF EXISTS myTblP;
CREATE TABLE myTblP (
  col1       INT              NOT NULL,
  col2       TEXT,
  col3       SERIAL
) PARTITION BY RANGE (col1);
-- create the table with PG 9.6- so that next scripts work
CREATE TABLE IF NOT EXISTS myTblP (
  col1       INT              NOT NULL,
  col2       TEXT,
  col3       SERIAL
);

DROP TABLE IF EXISTS myPartP1 ;
CREATE TABLE myPartP1 PARTITION OF myTblP (PRIMARY KEY (col1)) FOR VALUES FROM (MINVALUE) TO (0);
-- create the table with PG 9.6- so that next scripts do not abort
CREATE TABLE IF NOT EXISTS myPartP1 (PRIMARY KEY (col1)) INHERITS (myTblP);

DROP TABLE IF EXISTS myPartP2 ;
CREATE TABLE myPartP2 PARTITION OF myTblP (PRIMARY KEY (col1)) FOR VALUES FROM (0) TO (9);
-- create the table with PG 9.6- so that next scripts do not abort
CREATE TABLE IF NOT EXISTS myPartP2 (PRIMARY KEY (col1)) INHERITS (myTblP);

-- fifth schema (for unsupported tables)

DROP SCHEMA IF EXISTS mySchema5 CASCADE;
CREATE SCHEMA mySchema5;

SET search_path=mySchema5;

-- myTempTbl will be created in the test script that needs is

DROP TABLE IF EXISTS myUnloggedTbl;
CREATE UNLOGGED TABLE myUnloggedTbl (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);

DROP TABLE IF EXISTS myOidsTbl;
CREATE TABLE myOidsTbl (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
) WITH OIDS;

-----------------------------
-- create roles and give rigths on application objects
-----------------------------
create role emaj_regression_tests_adm_user login password 'adm';
create role emaj_regression_tests_viewer_user login password 'viewer';
create role emaj_regression_tests_anonym_user login password 'anonym';
--
grant all on schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 to emaj_regression_tests_adm_user, emaj_regression_tests_viewer_user;
--
grant select on all tables in schema mySchema1, mySchema2, "phil's schema3", mySchema4 to emaj_regression_tests_viewer_user;
grant select on all sequences in schema mySchema1, mySchema2, mySchema4 to emaj_regression_tests_viewer_user;
--
grant all on all tables in schema mySchema1, mySchema2, "phil's schema3", mySchema4 to emaj_regression_tests_adm_user;
grant all on all sequences in schema mySchema1, mySchema2, "phil's schema3", mySchema4 to emaj_regression_tests_adm_user;
