-- emaj_prepare_parallel_rollback_test.sql : Version <devel>
--
-- This script prepares an application and an E-Maj context to test parallel rollback 
-- using the emajParallelRollback client. 
--
-- It must be executed by a role member of emaj_adm or by a superuser.
--
-- The user and password to be used for the dblink connection must have been defined,
-- with a statement like:
--    INSERT INTO emaj.emaj_param (param_key, param_value_text)
--      VALUES ('dblink_user_password','user=<user> password=<password>'
--
-- Once this script is executed, you can type:
-- ./client/emajParallelRollback.php -g 'emaj parallel rollback test group' -m BEFORE_PROC_2 -s 3 -v <and connection parameters>
--
-- The role configured for the dblink connection and the role used to perform the parallel 
-- rollback operation must also be at least member of emaj_adm.
--
-- The script can be run several times in sequence.
-- To remove all traces left by this test, just execute:
--     SELECT emaj.emaj_parallel_rollback_test_cleanup();
--
\echo '---------------------------------------------------------------------------'
\echo '---                                                                     ---'
\echo '---     Setting up an environment to test E-Maj parallel rollbacks      ---'
\echo '---                                                                     ---'
\echo '---------------------------------------------------------------------------'

\set ON_ERROR_STOP
\set ECHO none
SET client_min_messages TO WARNING;

\echo '---'
\echo '--- Check the E-Maj environment.'
\echo '---'
DO LANGUAGE plpgsql
$do$
  DECLARE
    v_msg          TEXT;
  BEGIN
-- check E-Maj is installed as an extension
    PERFORM 0 FROM pg_catalog.pg_extension WHERE extname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj parallel rollback test setup: E-Maj is not installed, or is not installed as an EXTENSION';
    END IF;
-- check the current role is member of emaj_adm (or is a superuser)
    IF NOT pg_catalog.pg_has_role('emaj_adm','USAGE') THEN
      RAISE EXCEPTION 'E-Maj parallel rollback test setup: the current user (%) is not member of emaj_adm and is not a superuser.', current_user;
    END IF;
-- check that the dblink connection parameter is defined
    PERFORM 0 FROM emaj.emaj_param WHERE param_key = 'dblink_user_password';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj parallel rollback test setup: the "dblink_user_password" parameter is unknown in emaj_param. Please create it with a statement like INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES (''dblink_user_password'',''user=<user> password=<password>'');.';
    END IF;
-- check the max_prepared_transactions GUC value
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE EXCEPTION 'E-Maj parallel rollback test setup: the "max_prepared_transactions" parameter value (%) on this cluster is too low to let parallel rollbacks work.',current_setting('max_prepared_transactions');
    END IF;
-- if the emaj_parallel_rollback_test_cleanup function exists (created by a previous run of this script), execute it
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_parallel_rollback_test_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_parallel_rollback_test_cleanup();
    END IF;
  END;
$do$;

\echo '---'
\echo '--- Create the emaj.emaj_parallel_rollback_test_cleanup() function to use later in order to remove objects created by this script.'
\echo '---'
CREATE or REPLACE FUNCTION emaj.emaj_parallel_rollback_test_cleanup()
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_parallel_rollback_test_cleanup$
  DECLARE
  BEGIN
-- stop if needed and drop the test group
    PERFORM emaj.emaj_force_stop_group(group_name) FROM emaj.emaj_group
      WHERE group_name = 'emaj parallel rollback test group' AND group_is_logging;
    PERFORM emaj.emaj_drop_group(group_name) FROM emaj.emaj_group
      WHERE group_name = 'emaj parallel rollback test group';
-- remove test group definition from the emaj_group_def table
    DELETE FROM emaj.emaj_group_def WHERE grpdef_group = 'emaj parallel rollback test group';
-- drop the demo app schema and its content
    DROP SCHEMA IF EXISTS emaj_parallel_rollback_test_app_schema CASCADE;
-- the function drops itself before exiting
    DROP FUNCTION emaj.emaj_parallel_rollback_test_cleanup();
    RETURN 'The E-Maj parallel rollback test environment has been deleted';
  END;
$emaj_parallel_rollback_test_cleanup$;

\set ECHO queries

-- create a schema and application table for the test
\echo '---'
\echo '--- Create an application schema and tables for the test'
\echo '---'

DROP SCHEMA IF EXISTS emaj_parallel_rollback_test_app_schema CASCADE;
CREATE SCHEMA emaj_parallel_rollback_test_app_schema;

SET search_path=emaj_parallel_rollback_test_app_schema;

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
  FOREIGN KEY (col44,col45) REFERENCES myTbl1 (col11,col12) ON DELETE CASCADE
);

-- table myTbl2b logs myTbl2 changes via a trigger
DROP TABLE IF EXISTS myTbl2b ;
CREATE TABLE myTbl2b (
  col20       SERIAL           NOT NULL,
  col21       INT              NOT NULL,
  PRIMARY KEY (col20)
);

CREATE or REPLACE FUNCTION myTbl2trgfct () RETURNS trigger AS $$
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
  AFTER INSERT OR UPDATE OR DELETE ON myTbl2
  FOR EACH ROW EXECUTE PROCEDURE myTbl2trgfct ();

-- populate group table
\echo '---'
\echo '--- Now the E-Maj administrator populates the emaj_group_def table'
\echo '---'
DELETE FROM emaj.emaj_group_def WHERE grpdef_group = 'emaj parallel rollback test group';
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','mytbl1');
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','mytbl2');
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','myTbl3_col31_seq');
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','myTbl3');
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','mytbl2b');
INSERT INTO emaj.emaj_group_def VALUES ('emaj parallel rollback test group','emaj_parallel_rollback_test_app_schema','mytbl4');

-- create E-Maj objects
\echo '---'
\echo '--- Create E-Maj objects'
\echo '---'
SELECT emaj.emaj_create_group('emaj parallel rollback test group');

-- activate log trigger to simulate the begining of the application processing
\echo '---'
\echo '--- Log triggers activation and setting of a BEFORE_PROC_1 mark'
\echo '---'
SELECT emaj.emaj_start_group('emaj parallel rollback test group','BEFORE_PROC_1');

-- simulation of an application processing
\echo '---'
\echo '--- Simulate the update activity of a first processing'
\echo '---'
INSERT INTO myTbl1 VALUES (1,'ABC',E'\014'::bytea);
INSERT INTO myTbl1 VALUES (1,'DEF',E'\014'::bytea);
INSERT INTO myTbl2 VALUES (1,'ABC',current_date);
INSERT INTO myTbl2 VALUES (2,'DEF',NULL);
INSERT INTO myTbl4 VALUES (1,'FK...',1,1,'ABC');
UPDATE myTbl1 SET col13=E'\034'::bytea WHERE col12='ABC';
UPDATE myTbl1 SET col13=NULL WHERE col12='DEF';
INSERT INTO "myTbl3" (col33) SELECT generate_series (1,10)*random();

\echo '---'
\echo '--- Application tables at the end of this first processing'
\echo '---'
SELECT * FROM myTbl1;
SELECT * FROM myTbl2;
SELECT * FROM "myTbl3";
SELECT * FROM myTbl2b;
SELECT * FROM "myTbl3_col31_seq";

\echo '---'
\echo '--- Set a BEFORE_PROC_2 mark'
\echo '---'
SELECT emaj.emaj_set_mark_group('emaj parallel rollback test group','BEFORE_PROC_2');

\echo '---'
\echo '--- Simulate the update activity of a second processing, including a sequence change'
\echo '---'
-- sequence change
ALTER SEQUENCE "myTbl3_col31_seq" INCREMENT 3 MAXVALUE 10000000;

-- application tables updates
INSERT INTO myTbl1 VALUES (1,'GHI',E'\014'::bytea);
INSERT INTO myTbl1 VALUES (1,'JKL',E'\014'::bytea);
DELETE FROM myTbl1 WHERE col12 = 'DEF';
INSERT INTO myTbl2 VALUES (3,'GHI','01/01/2009');
UPDATE "myTbl3" SET col33 = 0 WHERE col31 = 1;
INSERT INTO "myTbl3" (col33) VALUES (3);

\echo '---'
\echo '--- Set a BEFORE_PROC_3 mark'
\echo '---'
SELECT emaj.emaj_set_mark_group('emaj parallel rollback test group','BEFORE_PROC_3');

\echo '---'
\echo '--- Simulate the update activity of a third processing'
\echo '---'

-- application tables updates
INSERT INTO myTbl1 VALUES (1,'MNO',E'\014'::bytea);
INSERT INTO myTbl1 VALUES (1,'PQR',E'\014'::bytea);
INSERT INTO "myTbl3" (col33) VALUES (4);
DELETE FROM "myTbl3";

\echo '---'
\echo '--- deactivate trigger on mytbl2'
\echo '---'
ALTER TABLE mytbl2 DISABLE TRIGGER mytbl2trg;

RESET search_path;

\echo '---'
\echo '--- emaj_prepare_parallel_rollback_test.sql script successfuly completed.'
\echo '--- A parallel rollback can be performed, from the E-Maj main directory, using a command like:'
\echo '    ./client/emajParallelRollback.php -g ''emaj parallel rollback test group'' -m BEFORE_PROC_2 -s 3 -v <and any needed connection parameters among -dhpUW>'
\echo '--- "./client/emajParallelRollback.php --help" provides information about available parameters.'
\echo '---'

\unset ECHO
