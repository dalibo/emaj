-- Check.sql: Perform various checks on the installed E-Maj components.
--            Also appreciate the regression test coverage.
--

-----------------------------
-- Count all functions in emaj schema and functions callable by users (emaj_xxx).
-----------------------------
SELECT count(*) FROM pg_proc, pg_namespace
  WHERE pg_namespace.oid=pronamespace AND nspname = 'emaj' AND (proname LIKE E'emaj\\_%' OR proname LIKE E'\\_%');

SELECT count(*) FROM pg_proc, pg_namespace
  WHERE pg_namespace.oid=pronamespace AND nspname = 'emaj' AND proname LIKE E'emaj\\_%';

-----------------------------
-- Check that no function has kept its default rights to public.
-----------------------------
-- Should return no row.
SELECT proname, proacl FROM pg_proc, pg_namespace
  WHERE pg_namespace.oid=pronamespace
    AND nspname = 'emaj' AND proname NOT LIKE '%_log_fnct'
    AND proacl IS NULL;

-----------------------------
-- Check that no user function has the default comment.
-----------------------------
-- Should return no row.
SELECT pg_proc.proname
  FROM pg_proc
    JOIN pg_namespace on (pronamespace=pg_namespace.oid)
    LEFT OUTER JOIN pg_description on (pg_description.objoid = pg_proc.oid
                     AND classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_proc')
                     AND objsubid=0)
  WHERE nspname = 'emaj' AND proname LIKE E'emaj\\_%' AND
        pg_description.description = 'E-Maj internal function';

-----------------------------
-- Perform various consistency checks on technical tables.
-----------------------------
-- No row in emaj_schema not linked to a relation assigned to a group (to complement the fkey between emaj_relation and emaj_schema).
SELECT sch_name FROM emaj.emaj_schema WHERE sch_name NOT IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation);

-----------------------------
-- Get test coverage data just before cleanup.
-----------------------------

-- Display the functions that are not called by any regression test script.
-- Some functions are excluded:
--   _emaj_param_before_stmt_fnct and _emaj_default_param_before_stmt_fnct are called by triggers but always raise an exception.
--       (and thus are not listed in pg_stat_user_functions for PG14- versions),
--   _build_path_name() is executed but is inlined in calling statements, and so it is not counted in statistics,
--   emaj_drop_extension() is not called by the standart test scenarios, but a dedicated scenario tests it.
SELECT proname FROM pg_proc, pg_namespace
  WHERE pronamespace = pg_namespace.oid
    AND nspname = 'emaj' AND (proname LIKE E'emaj\\_%' OR proname LIKE E'\\_%')
    AND proname NOT IN ('_emaj_default_param_before_stmt_fnct', '_emaj_param_before_stmt_fnct', '_build_path_name', 'emaj_drop_extension')
EXCEPT
SELECT funcname FROM pg_stat_user_functions
  WHERE schemaname = 'emaj' AND (funcname LIKE E'emaj\\_%' OR funcname LIKE E'\\_%')
ORDER BY 1;

-- Display the number of calls for each emaj function.
--   (_verify_groups(), _log_stat_tbl() and _get_log_sequence_last_value() functions are excluded as their number of calls is not stable.
--      _log_stat_tbl() and _get_log_sequence_last_value() sometimes differ in verify.sql or adm3.sql, always at the same place:
--      A call to emaj.emaj_verify_all() (verify.sql line 313) and to _remove_tables() (adm3.sql line 409). But these functions do not.
--      Call _log_stat_tbl(), directly or indirectly. This happens mostly with PG 11.).
SELECT funcname, calls FROM pg_stat_user_functions
  WHERE schemaname = 'emaj' AND (funcname LIKE E'emaj\\_%' OR funcname LIKE E'\\_%')
    AND funcname NOT IN (
      '_log_stat_tbl', '_get_log_sequence_last_value',
      '_verify_groups'
                        )
  ORDER BY funcname, funcid;

-- Count the total number of user-callable function calls (those who failed are not counted).
SELECT sum(calls) FROM pg_stat_user_functions WHERE funcname LIKE E'emaj\\_%';

-----------------------------
-- Execute the perl script that checks the code.
-----------------------------

\! perl ${EMAJ_DIR}/tools/check_code.pl | grep -P '^WARNING:|^ERROR:'
