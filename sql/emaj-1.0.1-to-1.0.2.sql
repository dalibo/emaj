--
-- E-Maj: migration from 1.0.1 to 1.0.2
--
-- This software is distributed under the GNU General Public License.
--
-- This script migrates an existing installation of E-Maj extension.
-- If version 1.0.1 has not been yet installed, simply use emaj.sql script. 
--

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO WARNING;
--SET client_min_messages TO NOTICE;
\echo 'E-maj upgrade from version 1.0.1 to version 1.0.2'
\echo 'Checking...'
------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Creation of a specific function to check the migration conditions are met.
-- The function generates an exception if at least one condition is not met.
CREATE or REPLACE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_emajVersion        TEXT;
  BEGIN
-- the emaj version registered in emaj_param must be '1.0.1'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '1.0.1' THEN
      RAISE EXCEPTION 'The current E-Maj version (%) is not 1.0.1',v_emajVersion;
    END IF;
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj installation: the current user (%) is not a superuser.', current_user;
    END IF;
--
    RETURN;
  END;
$tmp$;
SELECT emaj.tmp();
DROP FUNCTION emaj.tmp();

-- OK, upgrade...
\echo '... OK, Migration start...'

BEGIN TRANSACTION;

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

\echo 'Updating E-Maj internal objects ...'

------------------------------------
--                                --
-- emaj functions                 --
--                                --
------------------------------------

CREATE OR REPLACE FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_groups_step3$
-- This is the third step of a rollback group processing.
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
  DECLARE
    v_realMark              TEXT;
    v_markName              TEXT;
    v_markDatetime          TIMESTAMPTZ;
    v_markLastSeqHoleId     BIGINT;
  BEGIN
-- Get the real mark name (using first supplied group name, and check that all groups
--   can use the same name has already been done at step 1)
    SELECT emaj._get_mark_name(v_groupNames[1],v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION '_rlbk_groups_step3: Internal error - mark % not found for group %.', v_mark, v_groupNames[1];
    END IF;
-- Get other mark characteristics
    SELECT mark_datetime, mark_last_seq_hole_id INTO v_markDatetime, v_markLastSeqHoleId FROM emaj.emaj_mark 
      WHERE mark_group = v_groupNames[1] AND mark_name = v_realMark;
-- check that no updates have been recorded between planning time (_rlbk_groups_step1) and lock time
-- (_rlbk_groups_step2) for tables that did not need to be rolled back at planning time.
-- This may occur and cannot be avoided because tables cannot be locked before processing the first rollback step. 
-- (Sessions must lock the tables they will rollback and the planning processing distribute those tables to sessions.)
    PERFORM 1 FROM (SELECT rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
                      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' 
                        AND rel_rows = 0
                    ) AS t
      WHERE emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_markDatetime, NULL, v_markLastSeqHoleId, NULL) > 0;
    IF FOUND THEN
      RAISE EXCEPTION '_rlbk_groups_step3: The rollback operation has been cancelled due to concurrent activity on tables to process. Please retry.';
    END IF;
    IF NOT v_unloggedRlbk THEN
-- If rollback is "logged" rollback, build a mark name with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the current time
--   compute the generated mark name
      v_markName = 'RLBK_' || v_realMark || '_' || to_char(current_timestamp, 'HH24.MI.SS.MS') || '_START';
-- ...  and set it
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true);
    END IF;
    RETURN;
  END;
$_rlbk_groups_step3$;

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------
-- revoke grants on all functions from PUBLIC
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN) FROM PUBLIC; 

-- and give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN) TO emaj_adm; 

-- and give appropriate rights on functions to emaj_viewer role
--GRANT EXECUTE ON FUNCTION emaj. TO emaj_viewer;

------------------------------------
--                                --
-- commit migration               --
--                                --
------------------------------------

UPDATE emaj.emaj_param SET param_value_text = '1.0.2' WHERE param_key = 'emaj_version';

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 1.0.2', 'Migration from 1.0.1 completed');

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-Maj successfully migrated to 1.0.2'

