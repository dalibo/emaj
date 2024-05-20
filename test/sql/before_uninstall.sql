-- before_uninstall.sql : prepare the uninstall test
--

-- Record the dblink_user_password parameter
insert into emaj.emaj_param (param_key, param_value_text) 
  values ('dblink_user_password','user=postgres password=postgres');

-- Grant emaj_adm so that the cleanup.sql report be stable
grant emaj_adm to emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;

-- Execute the demo scripts
\o /dev/null
\i sql/emaj_demo.sql
\o

-- Execute the psql script that prepares parallel_rollback demo
\o /dev/null
\i sql/emaj_prepare_parallel_rollback_test.sql
\o

\set ON_ERROR_STOP OFF
\set ECHO all

-- Direct emaj extension drop attempt
drop extension emaj cascade;

-- Direct emaj schema drop attempt
drop schema emaj cascade;

-- Look at the tables groups
select group_name from emaj.emaj_group order by group_name;

-- Look at the public and cleanup functions
\df public._emaj*
\df emaj.emaj_*_cleanup*
