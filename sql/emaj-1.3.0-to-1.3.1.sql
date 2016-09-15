--
-- E-Maj: migration from 1.3.0 to 1.3.1
--
-- This software is distributed under the GNU General Public License.
--
-- This script migrates an existing installation of E-Maj extension.
-- If E-Maj is not yet installed, use the emaj.sql script. 
--

\set ON_ERROR_STOP ON
\set QUIET ON
SET client_min_messages TO WARNING;
--SET client_min_messages TO NOTICE;
\echo 'E-maj upgrade from version 1.3.0 to version 1.3.1'
\echo 'Checking...'
------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Creation of a specific function to check the migration conditions are met.
-- The function generates an exception if at least one condition is not met.
DROP FUNCTION IF EXISTS emaj.tmp();
CREATE FUNCTION emaj.tmp() 
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_emajVersion            TEXT;
    v_pgVersion              INTEGER = emaj._pg_version_num();
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj installation: the current user (%) is not a superuser.', current_user;
    END IF;
-- the emaj version registered in emaj_param must be '1.3.0'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '1.3.0' THEN
      RAISE EXCEPTION 'The current E-Maj version (%) is not 1.3.0',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 8.3
    IF v_pgVersion < 803 THEN
      RAISE EXCEPTION 'The current PostgreSQL version (%) is not compatible with E-Maj 1.3.1 (8.3 minimum)',v_pgVersion;
    END IF;
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
-- emaj functions: recreate all   --
--                                --
------------------------------------

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_lastGlobalSeq BIGINT, v_nbSession INT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence
-- The function is called by emaj._rlbk_session_exec()
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence value limit for rollback
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_nbPk                   BIGINT;
    v_cols                   TEXT[] = '{}';
    v_colsPk                 TEXT[] = '{}';
    v_condsPk                TEXT[] = '{}';
    v_colList                TEXT;
    v_pkColList              TEXT;
    v_pkCondList             TEXT;
    v_maxGlobalSeq           BIGINT;
    r_col                    RECORD;
  BEGIN
    v_fullTableName  = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName   = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'All log rows with emaj_gid > ' || v_lastGlobalSeq);
-- set the v_maxGlobalSeq variable using the emaj_global_seq last value
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_maxGlobalSeq FROM emaj.emaj_global_seq;
-- Build some pieces of SQL statements
--   build the tables's columns list
    FOR r_col IN
      SELECT 'tbl.' || quote_ident(attname) AS col_name FROM pg_catalog.pg_attribute
        WHERE attrelid = v_fullTableName::regclass
          AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
      LOOP
      v_cols = array_append(v_cols, r_col.col_name);
    END LOOP;
    v_colList = array_to_string(v_cols,',');
-- TODO when pg 8.3 will not be supported any more :
--   SELECT array_to_string(array_agg('tbl.' || quote_ident(attname)),', ') INTO v_colList
--     FROM pg_catalog.pg_attribute
--     WHERE attrelid = 'myschema1.mytbl1'::regclass AND attnum > 0 AND NOT attisdropped;
--   build the pkey columns list and the "equality on the primary key" conditions
    FOR r_col IN
      SELECT quote_ident(attname) AS col_pk_name,
             'tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname) AS col_pk_cond
        FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName::regclass AND indisprimary
          AND attnum > 0 AND attisdropped = false
        ORDER BY attnum
      LOOP
      v_colsPk = array_append(v_colsPk, r_col.col_pk_name);
      v_condsPk = array_append(v_condsPk, r_col.col_pk_cond);
    END LOOP;
    v_pkColList = array_to_string(v_colsPk,',');
    v_pkCondList = array_to_string(v_condsPk, ' AND ');
-- create the temporary table containing all primary key values with their earliest emaj_gid
    IF v_nbSession = 1 THEN
      v_tableType = 'TEMP';
      v_tmpTable = 'emaj_tmp_' || pg_backend_pid();
    ELSE
--   with multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
--   but it may be an UNLOGGED table with pg 9.1+
      IF v_pgVersion >= 901 THEN
        v_tableType = 'UNLOGGED';
      ELSE
        v_tableType = '';
      END IF;
      v_tmpTable = 'emaj.emaj_tmp_' || pg_backend_pid();
    END IF;
    EXECUTE 'CREATE ' || v_tableType || ' TABLE ' || v_tmpTable || ' AS '
         || '  SELECT ' || v_pkColList || ', min(emaj_gid) as emaj_gid'
         || '    FROM ' || v_logTableName
         || '    WHERE emaj_gid > ' || v_lastGlobalSeq
         || '    GROUP BY ' || v_pkColList;
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- delete all rows from the application table corresponding to each touched primary key
--   this deletes rows inserted or updated during the rolled back period
    EXECUTE 'DELETE FROM ONLY ' || v_fullTableName || ' tbl USING ' || v_tmpTable || ' keys '
         || '  WHERE ' || v_pkCondList;
-- if the number of pkey to process is greater than 1.000, analyze the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement
    IF v_nbPk > 1000 THEN
      EXECUTE 'ANALYZE ' || v_logTableName;
    END IF; 
-- insert into the application table rows that were deleted or updated during the rolled back period
    EXECUTE 'INSERT INTO ' || v_fullTableName
         || '  SELECT ' || v_colList
         || '    FROM ' || v_logTableName || ' tbl, ' || v_tmpTable || ' keys '
         || '    WHERE ' || v_pkCondList || ' AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
         || '      AND tbl.emaj_gid > ' || v_lastGlobalSeq || 'AND tbl.emaj_gid <= ' || v_maxGlobalSeq;
-- drop the now useless temporary table
    EXECUTE 'DROP TABLE ' || v_tmpTable;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT)
RETURNS void LANGUAGE plpgsql AS
$_rlbk_session_lock$
-- It creates the session row in the emaj_rlbk_session table and then locks all the application tables for the session.
  DECLARE
    v_pgVersion              INT = emaj._pg_version_num();
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_nbRetry                SMALLINT = 0;
    v_lockmode               TEXT;
    v_ok                     BOOLEAN = false;
    v_nbTbl                  INT;
    r_tbl                    RECORD;
  BEGIN
-- try to open a dblink connection for #session > 1 (the attempt for session 1 has already been done)
    IF v_session > 1 THEN
      PERFORM emaj._dblink_open_cnx('rlbk#'||v_session);
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups INTO v_groupNames FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- create the session row the emaj_rlbk_session table.
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
             'VALUES (' || v_rlbkId || ',' || v_session || ',' || txid_current() || ',' ||
              quote_literal(clock_timestamp()) || ') RETURNING 1';
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
--    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      v_isDblinkUsable = true;
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- insert lock begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'BEGIN', array_to_string(v_groupNames,','), 'Rollback session #' || v_session);
--
-- acquire locks on tables
--
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- scan all tables of the session, in priority ascending order (priority being defined in emaj_group_def and stored in emaj_relation)
        FOR r_tbl IN
          SELECT quote_ident(rlbp_schema) || '.' || quote_ident(rlbp_table) AS fullName,
                 EXISTS (SELECT 1 FROM emaj.emaj_rlbk_plan rlbp2
                         WHERE rlbp2.rlbp_rlbk_id = v_rlbkId AND rlbp2.rlbp_session = v_session AND
                               rlbp2.rlbp_schema = rlbp1.rlbp_schema AND rlbp2.rlbp_table = rlbp1.rlbp_table AND
                               rlbp2.rlbp_step = 'DIS_LOG_TRG') AS disLogTrg
            FROM emaj.emaj_rlbk_plan rlbp1, emaj.emaj_relation
            WHERE rel_schema = rlbp_schema AND rel_tblseq = rlbp_table
            AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_session = v_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
--   lock each table
--     The locking level is EXCLUSIVE mode, except for tables whose log trigger needs to be disabled/enables when postgres version is prior 9.5.
--     In this later case, the ACCESS EXCLUSIVE mode is used, blocking all concurrent accesses to these tables.
--     In the former case, the EXCLUSIVE mode blocks all concurrent update capabilities of all tables of the groups (including tables with no logged
--     update to rollback), in order to ensure a stable state of the group at the end of the rollback operation). But these tables can be accessed
--     by SELECT statements during the E-Maj rollback.
          IF v_pgVersion < 905 AND r_tbl.disLogTrg THEN
            v_lockmode = 'ACCESS EXCLUSIVE';
          ELSE
            v_lockmode = 'EXCLUSIVE';
          END IF;
          EXECUTE 'LOCK TABLE ' || r_tbl.fullName || ' IN ' || v_lockmode || ' MODE';
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_session_lock: a deadlock has been trapped while locking tables for groups %.', array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      PERFORM emaj._rlbk_error(v_rlbkId, '_rlbk_session_lock: too many (5) deadlocks encountered while locking tables', 'rlbk#'||v_session);
      RAISE EXCEPTION '_rlbk_session_lock: too many (5) deadlocks encountered while locking tables for groups %.',array_to_string(v_groupNames,',');
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'END', array_to_string(v_groupNames,','), 'Rollback session #' || v_session || ': ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
-- TODO: trap and record exception during the rollback operation when pg 8.3 will not be supported any more
--  EXCEPTION
--    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
--      RAISE;
--    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
--      PERFORM emaj._rlbk_error(v_rlbkId, SQLERRM, 'rlbk#'||v_session);
--      RAISE;
  END;
$_rlbk_session_lock$;

-- Set comments for all internal functions,
-- by directly inserting a row in the pg_description table for all emaj functions that do not have yet a recorded comment
INSERT INTO pg_catalog.pg_description (objoid, classoid, objsubid, description)
  SELECT pg_proc.oid, pg_class.oid, 0 , 'E-Maj internal function'
    FROM pg_catalog.pg_proc, pg_catalog.pg_class
    WHERE pg_class.relname = 'pg_proc'
      AND pg_proc.oid IN               -- list all emaj functions that do not have yet a comment in pg_description
       (SELECT pg_proc.oid
          FROM pg_catalog.pg_proc
               JOIN pg_catalog.pg_namespace ON (pronamespace=pg_namespace.oid)
               LEFT OUTER JOIN pg_catalog.pg_description ON (pg_description.objoid = pg_proc.oid
                                     AND classoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_proc')
                                     AND objsubid = 0)
          WHERE nspname = 'emaj' AND (proname LIKE E'emaj\\_%' OR proname LIKE E'\\_%')
            AND pg_description.description IS NULL
       );

------------------------------------
--                                --
-- commit migration               --
--                                --
------------------------------------

UPDATE emaj.emaj_param SET param_value_text = '1.3.1' WHERE param_key = 'emaj_version';

-- insert the migration record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 1.3.1', 'Migration from 1.3.0 completed');

-- post installation checks
CREATE OR REPLACE FUNCTION emaj._tmp_post_install()
RETURNS VOID LANGUAGE plpgsql AS
$tmp$
  DECLARE
    v_mpt                    INTEGER;
    v_schemaList             TEXT;
    r_sch                    RECORD;
  BEGIN
-- check the max_prepared_transactions GUC value
    SELECT setting INTO v_mpt FROM pg_catalog.pg_settings WHERE name = 'max_prepared_transactions';
    IF v_mpt <= 1 THEN
      RAISE WARNING 'E-Maj migration: as the max_prepared_transactions parameter is set to % on this cluster, no parallel rollback is possible.',v_mpt;
    END IF;
-- check the dblink contrib or extension is already installed and give EXECUTE right to emaj_adm on dblink_connect_u functions
--   Scan all schemas where dblink is installed
--   Note dblink may have been installed in several schemas and we don't know at installation time 
--   what will be the search_path of future users.
--   Grants on this particular dblink_connect_u function are revoked by default to public, 
--   but emaj administrators need it to monitor the rollback operations progress.
    v_schemaList = '';
    FOR r_sch IN 
      SELECT DISTINCT nspname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
          WHERE pg_proc.pronamespace = pg_namespace.oid
            AND proname = 'dblink'
      LOOP
      IF v_schemaList = '' THEN
        v_schemaList = r_sch.nspname;
      ELSE
        v_schemaList = v_schemaList || ',' || r_sch.nspname;
      END IF;
      EXECUTE 'GRANT EXECUTE ON FUNCTION ' || 
              quote_ident(r_sch.nspname) || '.dblink_connect_u(text,text) TO emaj_adm';
    END LOOP;
-- return the output message depending whether dblink is installed or not 
    IF v_schemaList = '' THEN
      RAISE WARNING 'E-Maj migration: the dblink contrib/extension is not installed. It is advisable to have the dblink contrib/extension installed in order to take benefit of the E-Maj rollback monitoring and E-Maj parallel rollback capabilities. (You will need to "GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO emaj_adm" once dblink is installed)';
    ELSE
      RAISE WARNING 'E-Maj migration: the dblink functions exist in schema(s): %. Be sure these functions will be accessible by emaj_adm roles through their schemas search_path.',v_schemaList;
    END IF;
    RETURN;
  END;
$tmp$;
SELECT emaj._tmp_post_install();
DROP FUNCTION emaj._tmp_post_install();

COMMIT;

SET client_min_messages TO default;
\echo '>>> E-Maj successfully migrated to 1.3.1'

