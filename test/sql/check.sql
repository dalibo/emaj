-- check.sql: Perform various checks on the installed E-Maj components.
--            Also appreciate the regression test coverage.
--

-----------------------------
-- count all functions in emaj schema and functions callable by users (emaj_xxx)
-----------------------------
select count(*) from pg_proc, pg_namespace 
  where pg_namespace.oid=pronamespace and nspname = 'emaj' and (proname like E'emaj\\_%' or proname like E'\\_%');

select count(*) from pg_proc, pg_namespace 
  where pg_namespace.oid=pronamespace and nspname = 'emaj' and proname like E'emaj\\_%';

-----------------------------
-- check that no function has kept its default rights to public
-----------------------------
-- should return no row
select proname, proacl from pg_proc, pg_namespace 
  where pg_namespace.oid=pronamespace
    and nspname = 'emaj' and proname not like '%_log_fnct'
    and proacl is null;

-----------------------------
-- check that no user function has the default comment
-----------------------------
-- should return no row
select pg_proc.proname
  from pg_proc
    join pg_namespace on (pronamespace=pg_namespace.oid)
    left outer join pg_description on (pg_description.objoid = pg_proc.oid 
                     and classoid = (select oid from pg_class where relname = 'pg_proc')
                     and objsubid=0)
  where nspname = 'emaj' and proname like E'emaj\\_%' and 
        pg_description.description = 'E-Maj internal function';

-----------------------------
-- perform various consistency checks on technical tables
-----------------------------
-- no row in emaj_schema not linked to a relation assigned to a group (to complement the fkey between emaj_relation and emaj_schema)
select sch_name from emaj.emaj_schema where sch_name not in (select distinct rel_log_schema from emaj.emaj_relation);

-----------------------------
-- get test coverage data just before cleanup
-----------------------------
-- wait to let the statistics collector aggregate the latest stats
select pg_sleep(1.5);

-- display the functions that are not called by any regression test script
--   (_build_path_name() is executed but is inlined in calling statements, and so it is not counted in statistics
--    emaj_drop_extension() is not called by the standart test scenarios, but a dedicated scenario tests it)
select nspname, proname from pg_proc, pg_namespace
  where pronamespace = pg_namespace.oid
    and nspname = 'emaj' and (proname like E'emaj\\_%' or proname like E'\\_%')
    and proname not in ('_build_path_name', 'emaj_drop_extension')
except
select schemaname, funcname from pg_stat_user_functions
  where schemaname = 'emaj' and (funcname like E'emaj\\_%' or funcname like E'\\_%')
order by 1,2;

-- display the number of calls for each emaj function
--   (_verify_groups(), _log_stat_tbl() and _get_log_sequence_last_value() functions are excluded as their number of calls is not stable.
--      _log_stat_tbl() and _get_log_sequence_last_value() sometimes differ in verify.sql or adm3.sql, always at the same place:
--      a call to emaj.emaj_verify_all() (verify.sql line 313) and to _remove_tables() (adm3.sql line 409). But these functions do not
--      call _log_stat_tbl(), directly or indirectly. This happens mostly with PG 11.)
select funcname, calls from pg_stat_user_functions
  where schemaname = 'emaj' and (funcname like E'emaj\\_%' or funcname like E'\\_%')
    and funcname not in (
      '_log_stat_tbl', '_get_log_sequence_last_value',
      '_verify_groups'
                        )
  order by funcname, funcid;

-- count the total number of user-callable function calls (those who failed are not counted)
select sum(calls) from pg_stat_user_functions where funcname like E'emaj\\_%';

-----------------------------
-- execute the perl script that checks the code
-----------------------------

\! perl ${EMAJ_DIR}/tools/check_code.pl | grep -P '^WARNING:|^ERROR:'
