-- cleanup.sql: Clean up the regression test environment, in particular roles 
--              (all components inside the regression database will be deleted with the regression database)

-----------------------------
-- cleanup the temporary files structure
-----------------------------
\setenv EMAJTESTTMPDIR '/tmp/emaj_'`echo $PGVER`
\! rm -Rf $EMAJTESTTMPDIR

-----------------------------
-- drop the function that checks and sets the last_value of emaj technical sequences
-----------------------------
DROP FUNCTION public.handle_emaj_sequences(INT);

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

-----------------------------
-- drop emaj_adm roles
-----------------------------
revoke emaj_adm from emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
revoke all on schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  from emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
revoke all on all tables in schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  from emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
revoke all on all sequences in schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6
  from emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;
revoke all on tablespace tsplog1, "tsp log'2", tspemaj_renamed
  from emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;

drop role emaj_regression_tests_adm_user1, emaj_regression_tests_adm_user2;

-----------------------------
-- drop emaj_regression_tests_viewer_user role
-----------------------------
revoke all on schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6 from emaj_regression_tests_viewer_user;
revoke all on all tables in schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6 from emaj_regression_tests_viewer_user;
revoke all on all sequences in schema mySchema1, mySchema2, "phil's schema""3", mySchema4, mySchema5, mySchema6 from emaj_regression_tests_viewer_user;
drop role emaj_regression_tests_viewer_user;

-----------------------------
-- drop emaj_regression_tests_anonym_user role
-----------------------------
revoke all on database regression from emaj_regression_tests_anonym_user;
drop role emaj_regression_tests_anonym_user;

--------------------------------------------
-- Dump the regression database (once the test roles have been dropped)
--------------------------------------------
\! ${EMAJ_DIR}/test/${PGVER}/bin/pg_dump regression >${EMAJ_DIR}/test/${PGVER}/results/regression.dump
