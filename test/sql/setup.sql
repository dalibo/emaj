-- setup.sql: Create and setup all application objects that will be needed for regression tests
--            Also perform some checks about emaj functions rights and commments
--
SET client_min_messages TO WARNING;

------------------------------------------------------------
-- create several application schemas with tables, sequences, triggers
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
  PRIMARY KEY (col41,col43),
  FOREIGN KEY (col43) REFERENCES myTbl2 (col21) DEFERRABLE INITIALLY IMMEDIATE,
  CONSTRAINT mytbl4_col44_fkey FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED
);

DO $$
BEGIN
  DROP TABLE IF EXISTS myTbl2b ;
  IF emaj._pg_version_num() < 180000 THEN
    EXECUTE 'CREATE TABLE myTbl2b (
      col20       INT              NOT NULL GENERATED ALWAYS AS IDENTITY,
      col21       INT              NOT NULL,
      col22       FLOAT            GENERATED ALWAYS AS (1::float / col21) STORED,
      col23       FLOAT            GENERATED ALWAYS AS (cos(col21 * pi() / 4)) STORED,
      col24       BOOLEAN          DEFAULT TRUE,
      PRIMARY KEY (col20)
    );';
  ELSE
    EXECUTE 'CREATE TABLE myTbl2b (
      col20       INT              NOT NULL GENERATED ALWAYS AS IDENTITY,
      col21       INT              NOT NULL,
      col22       FLOAT            GENERATED ALWAYS AS (1::float / col21) STORED,
      col23       FLOAT            GENERATED ALWAYS AS (cos(col21 * pi() / 4)),
      col24       BOOLEAN          DEFAULT TRUE,
      PRIMARY KEY (col20)
    );';
  END IF;
END;
$$;

CREATE or REPLACE FUNCTION myTbl2trgfct1 () RETURNS trigger AS $$
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
CREATE TRIGGER myTbl2trg1
  AFTER INSERT OR UPDATE OR DELETE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct1();

CREATE or REPLACE FUNCTION myTbl2trgfct2 () RETURNS trigger AS $$
BEGIN
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER myTbl2trg2
  BEFORE INSERT OR UPDATE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct2();

ALTER TABLE mySchema1.myTbl2 DISABLE TRIGGER myTbl2trg2;

--
-- Second schema
--

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
  CONSTRAINT mytbl4_col44_fkey FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE ON UPDATE SET NULL
);

DROP DOMAIN IF EXISTS idjson;
CREATE DOMAIN idjson AS JSON CHECK (json_typeof(VALUE -> 'id') = 'number' AND json_typeof(VALUE -> 'id') IS NOT NULL);

DROP TABLE IF EXISTS myTbl5 ;
CREATE TABLE myTbl5 (
  col51       INT              NOT NULL,
  col52       TEXT[]           ,
  col53       INT[]            ,
  col54       DATE[]           ,
  col55       JSON             ,
  col56       JSONB            ,
  col57       idjson           ,
  col58       DATERANGE        ,
  col59       XML              ,
  PRIMARY KEY (col51)
);

DROP TYPE IF EXISTS myEnum;
CREATE TYPE myEnum AS ENUM ('EXECUTING', 'COMPLETED');

DROP TYPE IF EXISTS myComposite;
CREATE TYPE myComposite AS (compo_id INT, compo_text POINT);

DROP TABLE IF EXISTS myTbl6 ;
CREATE TABLE myTbl6 (
  col61       INT4             NOT NULL,
  col62       POINT            ,
  col63       BOX              ,
  col64       CIRCLE           ,
  col65       PATH             ,
  col66       INET             ,
  col67       myEnum           ,
  col68       myComposite      ,
--col69       POINT            GENERATED ALWAYS AS (point(col63)) STORED,     -- fails in PG11-
  PRIMARY KEY (col61),
  EXCLUDE USING gist (col63 WITH &&)
);

DO $$
BEGIN
  IF emaj._pg_version_num() >= 120000 THEN
    EXECUTE 'ALTER TABLE myTbl6 ADD COLUMN col69 POINT GENERATED ALWAYS AS (point(col63)) STORED';
  ELSE
    EXECUTE 'ALTER TABLE myTbl6 ADD COLUMN col69 POINT';
  END IF;
END;
$$;

-- This table will remain outside table groups
DROP TABLE IF EXISTS myTbl7 ;
CREATE TABLE myTbl7 (
  col71       INT              NOT NULL,
  PRIMARY KEY (col71)
);

-- This table will remain outside table groups
DROP TABLE IF EXISTS myTbl8 ;
CREATE TABLE myTbl8 (
  col81       INT              NOT NULL,
  PRIMARY KEY (col81)
);

ALTER TABLE myschema2.myTbl6 ADD FOREIGN KEY (col61) REFERENCES myschema2.myTbl7 (col71) DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE myschema2.myTbl8 ADD FOREIGN KEY (col81) REFERENCES myschema2.myTbl6 (col61) DEFERRABLE;

CREATE SEQUENCE mySeq1 MINVALUE 1000 MAXVALUE 2000 CYCLE;

-- This sequence will remain outside any groups until the addition into a group in logging state
CREATE SEQUENCE mySeq2;

--
-- Third schema (for an audit_only group)
--

DROP SCHEMA IF EXISTS "phil's schema""3" CASCADE;
CREATE SCHEMA "phil's schema""3";

SET search_path="phil's schema""3";

DROP TABLE IF EXISTS "phil's tbl1" ;
CREATE TABLE "phil's tbl1" (
  "phil's col11"   DECIMAL (7)      NOT NULL,
  "phil's col12"   CHAR (10)        NOT NULL,
  "phil\s""col13"  BYTEA            ,
  PRIMARY KEY ("phil's col11","phil's col12")
);

DROP TABLE IF EXISTS "myTbl2\" ;
CREATE TABLE "myTbl2\" (
  col21            SERIAL           NOT NULL,
  col22            TEXT             ,
  col23            DATE
);

DROP TABLE IF EXISTS "my""tbl4" ;
CREATE TABLE "my""tbl4" (
  col41            INT              NOT NULL,
  col42            TEXT             ,
  col43            INT              ,
  col44            DECIMAL(7)       ,
  col45            CHAR(10)         ,
  PRIMARY KEY (col41),
  CONSTRAINT "my""tbl4_col44_fkey" FOREIGN KEY (col44,col45) REFERENCES "phil's tbl1" ("phil's col11","phil's col12") ON DELETE CASCADE ON UPDATE SET NULL
);
ALTER TABLE "myTbl2\" ADD CONSTRAINT mytbl2_col21_fkey FOREIGN KEY (col21) REFERENCES "my""tbl4" (col41);

CREATE SEQUENCE "phil's""seq\1" MINVALUE 1000 MAXVALUE 2000 CYCLE;

--
-- Fourth schema (for partitioning)
--

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

-- Declarative partitionning (with subpartitions, FK and triggers on partitionned table)

DROP TABLE IF EXISTS myTblP;
CREATE TABLE myTblP (
  col1       INT              NOT NULL,
  col2       CHAR             NOT NULL,
  col3       TEXT             NOT NULL,
  col4       SERIAL,
  PRIMARY KEY (col1, col2)
) PARTITION BY RANGE (col1);

DROP TABLE IF EXISTS myPartP1;
CREATE TABLE myPartP1 PARTITION OF myTblP FOR VALUES FROM (MINVALUE) TO (0) PARTITION BY RANGE (col2);

DROP TABLE IF EXISTS myPartP1A;
CREATE TABLE myPartP1A PARTITION OF myPartP1 FOR VALUES FROM (MINVALUE) TO ('L');

DROP TABLE IF EXISTS myPartP1B;
CREATE TABLE myPartP1B PARTITION OF myPartP1 FOR VALUES FROM ('L') TO (MAXVALUE);

DROP TABLE IF EXISTS myPartP2;
CREATE TABLE myPartP2 PARTITION OF myTblP FOR VALUES FROM (0) TO (9);

DROP TABLE IF EXISTS myTblR1 ;
CREATE TABLE myTblR1 (
  col1       INT              NOT NULL PRIMARY KEY
);
-- Add a global FK.
ALTER TABLE myTblP ADD FOREIGN KEY (col1) REFERENCES myTblR1(col1)
  ON DELETE CASCADE
;

DROP TABLE IF EXISTS myTblR2;
CREATE TABLE myTblR2 (
  col1       SERIAL           NOT NULL PRIMARY KEY,
  col2       INT,
  col3       CHAR,
  col4       TEXT
);

-- Add a global FK.
ALTER TABLE myTblR2 ADD FOREIGN KEY (col2, col3) REFERENCES myTblP(col1, col2)
  DEFERRABLE INITIALLY DEFERRED
;
-- Add a global trigger.
CREATE TRIGGER z_min_update
  BEFORE UPDATE ON myTblP
  FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();

--
-- fifth schema (for tables unsupported in rollbackable tables groups)
--

DROP SCHEMA IF EXISTS mySchema5 CASCADE;
CREATE SCHEMA mySchema5;

SET search_path=mySchema5;

-- myTempTbl will be created in the test script that needs is

DROP TABLE IF EXISTS myUnloggedTbl;
CREATE UNLOGGED TABLE myUnloggedTbl (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);

--
-- sixth schema (for tables with very long names)
--

DROP SCHEMA IF EXISTS mySchema6 CASCADE;
CREATE SCHEMA mySchema6;

SET search_path=mySchema6;

DROP TABLE IF EXISTS table_with_50_characters_long_name_____0_________0;
CREATE TABLE table_with_50_characters_long_name_____0_________0 (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);

DROP TABLE IF EXISTS table_with_51_characters_long_name_____0_________0a;
CREATE TABLE table_with_51_characters_long_name_____0_________0a (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);

DROP TABLE IF EXISTS table_with_55_characters_long_name_____0_________0abcde;
CREATE TABLE table_with_55_characters_long_name_____0_________0abcde (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);
DROP TABLE IF EXISTS table_with_55_characters_long_name_____0_________0fghij;
CREATE TABLE table_with_55_characters_long_name_____0_________0fghij (
  col1       INT     NOT NULL,
  PRIMARY KEY (col1)
);

-----------------------------
-- create roles and give rights
-----------------------------
create role emaj_regression_tests_adm_user1 login password 'adm';
create role emaj_regression_tests_adm_user2 login password 'adm';
create role emaj_regression_tests_viewer_user login password 'viewer';
create role emaj_regression_tests_anonym_user login password 'anonym';
--
grant create on tablespace tsplog1, "tsp log'2"
  to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
-- give the CREATE right to emaj_adm on either trpemaj or tspemaj_renamed tablespace
DO LANGUAGE plpgsql
$$
  BEGIN
    PERFORM 0 FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
    IF FOUND THEN
      grant create on tablespace tspemaj to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
    ELSE
      grant create on tablespace tspemaj_renamed to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
    END IF;
  END;
$$;
--
grant all on database regression to emaj_regression_tests_anonym_user;

-----------------------------
-- create the function that will check and set the last_value of emaj technical sequences
-----------------------------
CREATE OR REPLACE FUNCTION public.handle_emaj_sequences(v_restart INT) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$handle_emaj_sequences$
DECLARE
  v_lastval     INT;
BEGIN
-- checks current last_value to be sure there will not be any sequence values overlay
--   emaj_hist_hist_id_seq
  SELECT last_value INTO v_lastval
    FROM emaj.emaj_hist_hist_id_seq;
--  RAISE WARNING 'handle_emaj_sequences: the current emaj_hist_hist_id_seq last_value = %',v_lastval;
  IF v_lastval > v_restart THEN
    RAISE EXCEPTION 'handle_emaj_sequences: the current emaj_hist_hist_id_seq last_value (%) is too high to be set to %',
      v_lastval, v_restart;
  END IF;
--   emaj_time_stamp_time_id_seq
  SELECT last_value INTO v_lastval
    FROM emaj.emaj_time_stamp_time_id_seq;
--  RAISE WARNING 'handle_emaj_sequences: the current emaj_time_stamp_time_id_seq last_value = %',v_lastval;
  IF v_lastval > v_restart THEN
    RAISE EXCEPTION 'handle_emaj_sequences: the current emaj_time_stamp_time_id_seq last_value (%) is too high to be set to %',
      v_lastval, v_restart;
  END IF;
--   emaj_rlbk_rlbk_id_seq
  SELECT last_value INTO v_lastval
    FROM emaj.emaj_rlbk_rlbk_id_seq;
--  RAISE WARNING 'handle_emaj_sequences: the current emaj_rlbk_rlbk_id_seq last_value = %',v_lastval;
  IF v_lastval > v_restart THEN
    RAISE EXCEPTION 'handle_emaj_sequences: the current emaj_rlbk_rlbk_id_seq last_value (%) is too high to be set to %',
      v_lastval, v_restart;
  END IF;
--   emaj_global_seq
  SELECT last_value INTO v_lastval
    FROM emaj.emaj_global_seq;
--  RAISE WARNING 'handle_emaj_sequences: the current emaj_global_seq last_value = %',v_lastval;
  IF v_lastval > v_restart * 1000 THEN
    RAISE EXCEPTION 'handle_emaj_sequences: the current emaj_global_seq last_value (%) is too high to be set to %',
      v_lastval, v_restart * 1000;
  END IF;
-- OK, let's set the sequences values
  PERFORM setval('emaj.emaj_hist_hist_id_seq', v_restart - 1, true);
  PERFORM setval('emaj.emaj_time_stamp_time_id_seq', v_restart - 1, true);
  PERFORM setval('emaj.emaj_rlbk_rlbk_id_seq', v_restart - 1, true);
  PERFORM setval('emaj.emaj_global_seq', v_restart * 1000, true);
END;
$handle_emaj_sequences$;
