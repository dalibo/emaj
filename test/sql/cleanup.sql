-- cleanup.sql: Clean up the regression test environment, in particular roles 
--              (all components inside the regression database will be deleted with the regression database)

-----------------------------
-- drop emaj_regression_tests_adm_user role
-----------------------------
revoke emaj_adm from emaj_regression_tests_adm_user;
revoke all on schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_adm_user;
revoke all on all tables in schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_adm_user;
revoke all on all sequences in schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_adm_user;
drop role emaj_regression_tests_adm_user;

-----------------------------
-- drop emaj_regression_tests_viewer_user role
-----------------------------
revoke all on schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_viewer_user;
revoke all on all tables in schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_viewer_user;
revoke all on all sequences in schema mySchema1, mySchema2, "phil's schema3", mySchema4, mySchema5 from emaj_regression_tests_viewer_user;
drop role emaj_regression_tests_viewer_user;

-----------------------------
-- drop emaj_regression_tests_anonym_user role
-----------------------------
revoke all on database regression from emaj_regression_tests_anonym_user;
drop role emaj_regression_tests_anonym_user;

-----------------------------
-- rename the tspemaj tablespace if it exists
-----------------------------
DO LANGUAGE plpgsql
$$
  BEGIN
    PERFORM 0 FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
    IF FOUND THEN
      ALTER TABLESPACE tspemaj RENAME TO tspemaj_renamed;
    END IF;
  END;
$$;

--------------------------------------------
-- Dump the regression database (once the test roles have been dropped)
--------------------------------------------
\! ${EMAJ_DIR}/test/${PGVER}/bin/pg_dump regression >${EMAJ_DIR}/test/${PGVER}/results/regression.dump

