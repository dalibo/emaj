-- non_superuser_install.sql
--     single script to test emaj installed with psql by a non superuser role
--
SET client_min_messages TO WARNING;

------------------------------------------------------------
-- Create roles, give grants and create needed extensions
------------------------------------------------------------
-- _regress_emaj_install owns the emaj environment
create role _regress_emaj_install login password 'install';

-- _regress_emaj_app owns the application objects
create role _regress_emaj_app login password 'app';

grant all on database regression to _regress_emaj_install, _regress_emaj_app;
grant create on schema public to _regress_emaj_install;

create extension dblink;

------------------------------------------------------------
-- Create the application objects and give grants to _regress_emaj_install
------------------------------------------------------------
set role _regress_emaj_app;

DROP SCHEMA IF EXISTS mySchema7 CASCADE;
CREATE SCHEMA mySchema7;

SET search_path=mySchema7;

CREATE TABLE myTbl1 (
  col11       SERIAL           NOT NULL,
  col12       TEXT             ,
  PRIMARY KEY (col11)
);

CREATE SEQUENCE mySeq1;

reset search_path;

------------------------------------------------------------
-- install the extension using the psql script
------------------------------------------------------------
set role _regress_emaj_install;

\set ECHO errors

\i sql/emaj-devel.sql

\set ECHO all

\dx


------------------------------------------------------------
-- drop the extension
------------------------------------------------------------
set role _regress_emaj_install;

select emaj.emaj_drop_extension();

-- Drop the extra extensions to get a stable re-install test
drop extension btree_gist;

reset role;
drop extension dblink;

-- Check that the emaj schema is not known anymore
\dn emaj


------------------------------------------------------------
-- Drop the application objects
------------------------------------------------------------
set role _regress_emaj_app;

DROP SCHEMA IF EXISTS mySchema7 CASCADE;

------------------------------------------------------------
-- Drop roles
------------------------------------------------------------
reset role;

revoke all on database regression from _regress_emaj_install, _regress_emaj_app;
revoke all on schema public from _regress_emaj_install;

drop role _regress_emaj_install, _regress_emaj_app;
