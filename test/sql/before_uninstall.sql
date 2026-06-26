-- Before_uninstall.sql : prepare the uninstall test.
--

-- Record the dblink_user_password parameter.
SELECT emaj.emaj_set_param('dblink_user_password', 'user=postgres password=postgres');

-- Grant emaj_adm so that the cleanup.sql report be stable.
GRANT emaj_adm TO _regress_emaj_adm1, _regress_emaj_adm2;

-- Execute the demo scripts.
\o /dev/null
\i sql/emaj_demo.sql
\o

-- Execute the psql script that prepares parallel_rollback demo.
\o /dev/null
\i sql/emaj_prepare_parallel_rollback_test.sql
\o

\set ON_ERROR_STOP OFF
\set ECHO all

-- Direct emaj extension drop attempt.
DROP extension emaj CASCADE;

-- Direct emaj schema drop attempt.
DROP SCHEMA emaj CASCADE;

-- Look at the table groups.
SELECT group_name FROM emaj.emaj_group ORDER BY group_name;

-- Look at the public and cleanup functions.
\df public._emaj*
\df emaj.emaj_*_cleanup*
