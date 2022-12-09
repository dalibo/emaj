--
-- E-Maj: migration from 4.1.0 to <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- Complain if this script is executed in psql, rather than via an ALTER EXTENSION statement.
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

------------------------------------
--                                --
-- Checks                         --
--                                --
------------------------------------
-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_nbNoError              INT;
    v_nbWarning              INT;
    v_groupList              TEXT;
  BEGIN
-- The emaj version registered in emaj_param must be '4.1.0'.
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '4.1.0' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.1.0',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 9.5.
    IF current_setting('server_version_num')::int < 90500 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.5.', current_setting('server_version');
    END IF;
-- Check E-Maj environment state.
    SELECT count(msg) FILTER (WHERE msg = 'No error detected'),
           count(msg) FILTER (WHERE msg LIKE 'Warning:%')
      INTO v_nbNoError, v_nbWarning
      FROM emaj.emaj_verify_all() AS t(msg);
    IF v_nbNoError = 0 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details.';
    END IF;
    IF v_nbWarning > 0 THEN
      RAISE WARNING 'E-Maj upgrade: the E-Maj environment health check reports warning. You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details.';
    END IF;
-- No existing group must have been created with a postgres version prior 8.4.
    SELECT string_agg(group_name, ', ') INTO v_groupList FROM emaj.emaj_group
      WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                 to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804;
    IF v_groupList IS NOT NULL THEN
      RAISE EXCEPTION 'E-Maj upgrade: groups "%" have been created with a too old postgres version (< 8.4). Drop these groups before upgrading. ',v_groupList;
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.1.0 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------


--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

------------------------------------
--                                --
-- emaj functions                 --
--                                --
------------------------------------
-- Recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script.


--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._rlbk_init(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN, p_nbSession INT, p_multiGroup BOOLEAN,
                                           p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps (or NULL if there are some NULL input).
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_markName               TEXT;
    v_markTimeId             BIGINT;
    v_markTimestamp          TIMESTAMPTZ;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsed           BOOLEAN;
    v_dbLinkSchema           TEXT;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_stmt                   TEXT;
    v_rlbkId                 INT;
  BEGIN
-- Check supplied group names and mark parameters.
    SELECT emaj._rlbk_check(p_groupNames, p_mark, p_isAlterGroupAllowed, FALSE) INTO v_markName;
    IF v_markName IS NOT NULL THEN
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Get the time stamp id and its clock timestamp for the first group (as we know this time stamp is the same for all groups of the array).
      SELECT time_id, time_clock_timestamp INTO v_markTimeId, v_markTimestamp
        FROM emaj.emaj_mark
             JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
        WHERE mark_group = p_groupNames[1]
          AND mark_name = v_markName;
-- Insert a BEGIN event into the history.
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
                array_to_string(p_groupNames,','),
                CASE WHEN p_isLoggedRlbk THEN 'Logged' ELSE 'Unlogged' END || ' rollback to mark ' || v_markName
                || ' [' || v_markTimestamp || ']'
               )
        RETURNING hist_id INTO v_histId;
-- Get the total number of tables and sequences for these groups.
      SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
        FROM emaj.emaj_group
        WHERE group_name = ANY (p_groupNames) ;
-- First try to open a dblink connection.
      SELECT p_status, (p_status >= 0), CASE WHEN p_status >= 0 THEN p_schema ELSE NULL END
        INTO v_dbLinkCnxStatus, v_isDblinkUsed, v_dbLinkSchema
        FROM emaj._dblink_open_cnx('rlbk#1');
-- For parallel rollback (i.e. when nb sessions > 1), the dblink connection must be ok.
      IF p_nbSession > 1 AND NOT v_isDblinkUsed THEN
        RAISE EXCEPTION '_rlbk_init: Cannot use several sessions without dblink connection capability. (Status of the dblink'
                        ' connection attempt = % - see E-Maj documentation)',
          v_dbLinkCnxStatus;
      END IF;
-- Create the row representing the rollback event in the emaj_rlbk table and get the rollback id back.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, ' ||
               'rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
               'rlbk_dblink_schema, rlbk_is_dblink_used) ' ||
               'VALUES (' || quote_literal(p_groupNames) || ',' || quote_literal(v_markName) || ',' ||
               v_markTimeId || ',' || p_isLoggedRlbk || ',' || quote_nullable(p_isAlterGroupAllowed) || ',' ||
               p_nbSession || ',' || v_nbTblInGroups || ',' || v_nbSeqInGroups || ', ''PLANNING'',' || v_histId || ',' ||
               quote_nullable(v_dbLinkSchema) || ',' || v_isDblinkUsed || ') RETURNING rlbk_id';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_rlbkId;
-- Create the session row the emaj_rlbk_session table.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
               'VALUES (' || v_rlbkId || ', 1, ' || txid_current() || ',' ||
                quote_literal(clock_timestamp()) || ') RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Call the rollback planning function to define all the elementary steps to perform, compute their estimated duration
-- and spread the elementary steps among sessions.
      v_stmt = 'SELECT emaj._rlbk_planning(' || v_rlbkId || ')';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_effNbTable;
-- Update the emaj_rlbk table to set the real number of tables to process and adjust the rollback status.
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_table = ' || v_effNbTable ||
               ', rlbk_status = ''LOCKING'' ' || ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
    END IF;
--
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(p_rlbkId INT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called to perform a rollback operation. It is also called to simulate a rollback operation and get its duration estime.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viewer role can write into rollback tables, when estimating the rollback
--   duration, without having specific privileges on them to do it.
  DECLARE
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_markTimeId             BIGINT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_effNbTable             INT;
    v_isEmajExtension        BOOLEAN;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- Get the rollback characteristics for the emaj_rlbk event.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_ctrlStepName
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get some mark attributes from emaj_mark.
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Get all duration parameters that will be needed later from the emaj_param table, or get default values for rows
-- that are not present in emaj_param table.
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_fkey_check_duration'),'5 microsecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_step_rollback_duration'),'2.5 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::INTERVAL)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk;
-- Insert into emaj_rlbk_plan a row per table currently belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_is_repl_role_replica)
      SELECT p_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, '', FALSE
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = ANY(v_groupNames)
          AND rel_kind = 'r';
-- Insert into emaj_rlbk_plan a row per table to effectively rollback.
-- The numbers of log rows is computed using the _log_stat_tbl() function.
-- A final check will be performed after tables will be locked to be sure no new table will have been updated.
     INSERT INTO emaj.emaj_rlbk_plan
            (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_is_repl_role_replica, rlbp_target_time_id,
             rlbp_estimated_quantity)
      SELECT p_rlbkId, 'RLBK_TABLE', rel_schema, rel_tblseq, '', FALSE, greatest(v_markTimeId, lower(rel_time_range)),
             emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL)
        FROM
          (SELECT *
             FROM emaj.emaj_relation
             WHERE upper_inf(rel_time_range)
               AND rel_group = ANY (v_groupNames)
               AND rel_kind = 'r'
          ) AS t
        WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0;
      GET DIAGNOSTICS v_effNbTable = ROW_COUNT;
-- For tables having all foreign keys linking tables in the rolled back groups, set the rlbp_is_repl_role_replica flag to TRUE.
-- This only concerns emaj installed as an extension because one needs to be sure that the the _rlbk_tbl() function is executed with a
-- superuser role (this is needed to set the session_replication_role to 'replica').
      v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
      IF v_isEmajExtension AND v_effNbTable > 0 THEN
        WITH fkeys AS (
            -- the foreign keys belonging to tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, nf.nspname, tf.relname, rel_group,
                   rel_group = ANY (v_groupNames) AS are_both_tables_in_groups
              FROM emaj.emaj_rlbk_plan,
                   pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation ON (rel_schema = nf.nspname AND rel_tblseq = tf.relname
                                                          AND upper_inf(rel_time_range))
              WHERE rlbp_rlbk_id = p_rlbkId                               -- The RLBK_TABLE steps for this rollback operation
                AND rlbp_step = 'RLBK_TABLE'
                AND contype = 'f'                                         -- FK constraints
                AND tf.relkind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                          --   partitionned tables
                AND t.relname = rlbp_table
                AND n.nspname = rlbp_schema
          UNION
            -- the foreign keys referencing tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, n.nspname, t.relname, rel_group,
                   rel_group = ANY (v_groupNames) AS are_both_tables_in_groups
              FROM emaj.emaj_rlbk_plan,
                   pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation ON (rel_schema = n.nspname AND rel_tblseq = t.relname
                                                          AND upper_inf(rel_time_range))
              WHERE rlbp_rlbk_id = p_rlbkId                               -- The RLBK_TABLE steps for this rollback operation
                AND rlbp_step = 'RLBK_TABLE'
                AND contype = 'f'                                         -- FK constraints
                AND t.relkind = 'r'                                       -- only constraints referenced by true tables, ie. excluding
                                                                          --   partitionned tables
                AND tf.relname = rlbp_table
                AND nf.nspname = rlbp_schema
        ), fkeys_agg AS (
          -- aggregated foreign keys by tables to rollback
          SELECT rlbp_schema, rlbp_table,
                 count(*) AS nb_all_fk, count(*) FILTER (WHERE are_both_tables_in_groups) AS nb_fk_groups_ok
            FROM fkeys
            GROUP BY 1,2
        )
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_is_repl_role_replica = TRUE
          FROM fkeys_agg
          WHERE rlbp_rlbk_id = p_rlbkId                                    -- The RLBK_TABLE steps for this rollback operation
            AND rlbp_step IN ('RLBK_TABLE', 'LOCK_TABLE')
            AND emaj_rlbk_plan.rlbp_table = fkeys_agg.rlbp_table
            AND emaj_rlbk_plan.rlbp_schema = fkeys_agg.rlbp_schema
            AND nb_all_fk = nb_fk_groups_ok                                -- if all fkeys are linking tables in the rolled back groups
        ;
      END IF;
--
-- Group tables into batchs to process all tables linked by foreign keys as a batch.
--
    v_batchNumber = 1;
-- Allocate tables with rows to rollback to batch number starting with the heaviest to rollback tables as reported by the
-- emaj_log_stat_group() function.
    FOR r_tbl IN
      SELECT rlbp_schema, rlbp_table, rlbp_is_repl_role_replica
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'RLBK_TABLE'
        ORDER BY rlbp_estimated_quantity DESC, rlbp_schema, rlbp_table
    LOOP
-- If the table is not already allocated to a batch number (it may have been already allocated because of a fkey link).
      IF EXISTS
          (SELECT 0
             FROM emaj.emaj_rlbk_plan
             WHERE rlbp_rlbk_id = p_rlbkId
               AND rlbp_step = 'RLBK_TABLE'
               AND rlbp_schema = r_tbl.rlbp_schema
               AND rlbp_table = r_tbl.rlbp_table
               AND rlbp_batch_number IS NULL
          ) THEN
-- Allocate the table to the batch number, with all other tables linked by foreign key constraints.
        PERFORM emaj._rlbk_set_batch_number(p_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table, r_tbl.rlbp_is_repl_role_replica);
        v_batchNumber = v_batchNumber + 1;
      END IF;
    END LOOP;
--
-- If unlogged rollback, register into emaj_rlbk_plan "disable log triggers", "deletes from log tables"
-- and "enable log trigger" steps.
--
    IF NOT v_isLoggedRlbk THEN
-- Compute the cost for each DIS_LOG_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('DIS_LOG_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all DIS_LOG_TRG steps.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                       rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'DIS_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
               v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE';
-- Insert all DELETE_LOG steps. But the duration estimates will be computed later.
-- The estimated number of log rows to delete is set to the estimated number of updates. This is underestimated in particular when
-- SQL UPDATES are logged. But the collected statistics used for duration estimates are also based on the estimated number of updates.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_target_time_id,
                                       rlbp_batch_number, rlbp_estimated_quantity)
        SELECT p_rlbkId, 'DELETE_LOG', rlbp_schema, rlbp_table, '', rlbp_target_time_id, rlbp_batch_number, rlbp_estimated_quantity
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE';
-- Compute the cost for each ENA_LOG_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('ENA_LOG_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all ENA_LOG_TRG steps.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                       rlbp_estimated_duration, rlbp_estimate_method)
        SELECT p_rlbkId, 'ENA_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number, v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE';
    END IF;
--
-- Process application triggers to temporarily set as ALWAYS triggers.
-- This concerns triggers that must be kept enabled during the rollback processing but the rollback function for its table is executed
-- with session_replication_role = replica.
--
-- Compute the cost for each SET_ALWAYS_APP_TRG step.
    SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
      FROM emaj._estimate_rlbk_step_duration('SET_ALWAYS_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all SET_ALWAYS_APP_TRG steps.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                     rlbp_estimated_duration, rlbp_estimate_method)
      SELECT p_rlbkId, 'SET_ALWAYS_APP_TRG', rlbp_schema, rlbp_table, tgname, rlbp_batch_number, v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan
             JOIN pg_catalog.pg_class ON (relname = rlbp_table)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
             JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'RLBK_TABLE'                               -- rollback step
          AND rlbp_is_repl_role_replica                              -- ... in session_replication_role = replica
          AND NOT tgisinternal                                       -- application triggers only
          AND tgname NOT IN ('emaj_trunc_trg','emaj_log_trg')
          AND tgenabled = 'O'                                        -- ... enabled in local mode
          AND EXISTS                                                 -- ... and to be kept enabled
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = rlbp_schema
                     AND rel_tblseq = rlbp_table
                     AND upper_inf(rel_time_range)
                     AND tgname = ANY (rel_ignored_triggers)
                );
-- Compute the cost for each SET_LOCAL_APP_TRG step.
    SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
      FROM emaj._estimate_rlbk_step_duration('SET_LOCAL_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all SET_LOCAL_APP_TRG steps
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
                                     rlbp_batch_number, rlbp_estimated_duration, rlbp_estimate_method)
      SELECT p_rlbkId, 'SET_LOCAL_APP_TRG', rlbp_schema, rlbp_table, rlbp_object,
             rlbp_batch_number, v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'SET_ALWAYS_APP_TRG';
--
-- Process application triggers to disable and re-enable.
-- This concerns triggers that must be disabled during the rollback processing and the rollback function for its table is not executed
-- with session_replication_role = replica.
--
-- Compute the cost for each DIS_APP_TRG step.
    SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
      FROM emaj._estimate_rlbk_step_duration('DIS_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all DIS_APP_TRG steps.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
                                     rlbp_estimated_duration, rlbp_estimate_method)
      SELECT p_rlbkId, 'DIS_APP_TRG', rlbp_schema, rlbp_table, tgname, rlbp_batch_number, v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan
             JOIN pg_catalog.pg_class ON (relname = rlbp_table)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
             JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'RLBK_TABLE'                               -- rollback step
          AND NOT tgisinternal                                       -- application triggers only
          AND tgname NOT IN ('emaj_trunc_trg','emaj_log_trg')
          AND (tgenabled IN ('A', 'R')                               -- enabled ALWAYS or REPLICA triggers
              OR (tgenabled = 'O' AND NOT rlbp_is_repl_role_replica) -- or enabled ORIGIN triggers for rollbacks not processed
              )                                                      --   in session_replication_role = replica)
          AND NOT EXISTS                                             -- ... that must be disabled
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = rlbp_schema
                     AND rel_tblseq = rlbp_table
                     AND upper_inf(rel_time_range)
                     AND tgname = ANY (rel_ignored_triggers)
                );
-- Compute the cost for each ENA_APP_TRG step.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('ENA_APP_TRG', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Insert all ENA_APP_TRG steps.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object,
                                     rlbp_app_trg_type,
                                     rlbp_batch_number, rlbp_estimated_duration, rlbp_estimate_method)
      SELECT p_rlbkId, 'ENA_APP_TRG', rlbp_schema, rlbp_table, rlbp_object,
             CASE tgenabled WHEN 'A' THEN 'ALWAYS' WHEN 'R' THEN 'REPLICA' ELSE '' END,
             rlbp_batch_number, v_estimDuration, v_estimMethod
        FROM emaj.emaj_rlbk_plan
             JOIN pg_catalog.pg_class ON (relname = rlbp_table)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rlbp_schema)
             JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid AND tgname = rlbp_object)
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'DIS_APP_TRG';
--
-- Process foreign key to define which action to perform on them
--
-- First compute the fixed duration estimates for each 'DROP_FK' and 'SET_FK_DEF' steps.
    SELECT p_estimateMethod, p_estimatedDuration INTO v_estimDropFkMethod, v_estimDropFkDuration
      FROM emaj._estimate_rlbk_step_duration('DROP_FK', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
    SELECT p_estimateMethod, p_estimatedDuration INTO v_estimSetFkDefMethod, v_estimSetFkDefDuration
      FROM emaj._estimate_rlbk_step_duration('SET_FK_DEF', NULL, NULL, NULL, NULL, v_fixed_step_rlbk, NULL);
-- Select all foreign keys belonging to or referencing the tables to process.
    FOR r_fk IN
        SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable,
               c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
          FROM emaj.emaj_rlbk_plan r
               JOIN pg_catalog.pg_class t ON (t.relname = r.rlbp_table)
               JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rlbp_schema)
               JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid)
          WHERE c.contype = 'f'                                            -- FK constraints only
            AND rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'                                   -- Tables to rollback
            AND NOT rlbp_is_repl_role_replica                              -- ... not in a session_replication_role = replica
      UNION
        SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable,
               c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
          FROM emaj.emaj_rlbk_plan r
               JOIN pg_catalog.pg_class rt ON (rt.relname = r.rlbp_table)
               JOIN pg_catalog.pg_namespace rn ON (rn.oid = rt.relnamespace AND rn.nspname = r.rlbp_schema)
               JOIN pg_catalog.pg_constraint c ON (c.confrelid = rt.oid)
               JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
               JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
          WHERE c.contype = 'f'                                            -- FK constraints only
            AND rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'RLBK_TABLE'                                   -- Tables to rollback
            AND NOT rlbp_is_repl_role_replica                              -- ... not in a session_replication_role = replica
          ORDER BY nspname, relname, conname
    LOOP
-- Depending on the foreign key characteristics, record as 'to be dropped' or 'to be set deferred' or 'to just be reset immediate'.
      IF NOT r_fk.condeferrable OR r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
-- Non deferrable fkeys and deferrable fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped.
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
          ) VALUES (
          p_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
          v_estimDropFkDuration, v_estimDropFkMethod
          );
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_object_def, rlbp_estimated_quantity
          ) VALUES (
          p_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, r_fk.def, r_fk.reltuples
          );
      ELSE
-- Other deferrable but not deferred fkeys need to be set deferred.
        IF NOT r_fk.condeferred THEN
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            p_rlbkId, 'SET_FK_DEF', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimSetFkDefDuration, v_estimSetFkDefMethod
            );
        END IF;
-- Deferrable fkeys are recorded as 'to be set immediate at the end of the rollback operation'.
-- Compute the number of fkey values to check at set immediate time.
        SELECT (coalesce(
-- Get the number of rolled back rows in the referencing table, if any.
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan
              WHERE rlbp_rlbk_id = p_rlbkId
                AND rlbp_step = 'RLBK_TABLE'                                   -- tables of the rollback event
                AND rlbp_schema = r_fk.nspname
                AND rlbp_table = r_fk.relname)                                 -- referencing schema.table
            , 0)) + (coalesce(
-- Get the number of rolled back rows in the referenced table, if any.
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan
                   JOIN pg_catalog.pg_class rt ON (rt.relname = rlbp_table)
                   JOIN pg_catalog.pg_namespace rn ON (rn.oid = rt.relnamespace AND rn.nspname = rlbp_schema)
                   JOIN pg_catalog.pg_constraint c ON (c.confrelid  = rt.oid)
              WHERE rlbp_rlbk_id = p_rlbkId
                AND rlbp_step = 'RLBK_TABLE'                                   -- tables of the rollback event
                AND c.oid = r_fk.conoid                                        -- constraint id
           )
            , 0)) INTO v_checks;
-- And record the SET_FK_IMM step.
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_estimated_quantity
          ) VALUES (
          p_rlbkId, 'SET_FK_IMM', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, v_checks
          );
      END IF;
    END LOOP;
--
-- Now compute the estimation duration for each complex step ('RLBK_TABLE', 'DELETE_LOG', 'ADD_FK', 'SET_FK_IMM').
--
-- Compute the rollback duration estimates for the tables.
    FOR r_tbl IN
      SELECT *
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'RLBK_TABLE'
    LOOP
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('RLBK_TABLE', r_tbl.rlbp_schema, r_tbl.rlbp_table, NULL,
                                               r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_row_rlbk);
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema
          AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the estimated log rows delete duration.
    FOR r_tbl IN
      SELECT *
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'DELETE_LOG'
    LOOP
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('DELETE_LOG', r_tbl.rlbp_schema, r_tbl.rlbp_table, NULL,
                                               r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_row_del_log);
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'DELETE_LOG'
          AND rlbp_schema = r_tbl.rlbp_schema
          AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the fkey recreation duration.
    FOR r_fk IN
      SELECT *
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'ADD_FK'
    LOOP
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('ADD_FK', r_tbl.rlbp_schema, r_tbl.rlbp_table, r_fk.rlbp_object,
                                               r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_fkey_check);
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'ADD_FK'
          AND rlbp_schema = r_fk.rlbp_schema
          AND rlbp_table = r_fk.rlbp_table
          AND rlbp_object = r_fk.rlbp_object;
    END LOOP;
-- Compute the fkey checks duration.
    FOR r_fk IN
      SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'SET_FK_IMM'
    LOOP
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDuration
        FROM emaj._estimate_rlbk_step_duration('SET_FK_IMM', r_tbl.rlbp_schema, r_tbl.rlbp_table, r_fk.rlbp_object,
                                               r_tbl.rlbp_estimated_quantity, v_fixed_step_rlbk, v_avg_fkey_check);
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'SET_FK_IMM'
          AND rlbp_schema = r_fk.rlbp_schema
          AND rlbp_table = r_fk.rlbp_table
          AND rlbp_object = r_fk.rlbp_object;
    END LOOP;
--
-- Allocate batch number to sessions to spread the load on sessions as best as possible.
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys.
--
-- Initialisation.
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = '0 SECONDS'::INTERVAL;
    END LOOP;
-- Allocate tables batch to sessions, starting with the heaviest to rollback batch.
    FOR r_batch IN
        SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_batch_number IS NOT NULL
          GROUP BY rlbp_batch_number
          ORDER BY sum(rlbp_estimated_duration) DESC
    LOOP
-- Compute the least loaded session.
      v_minSession=1; v_minDuration = v_sessionLoad [1];
      FOR v_session IN 2 .. v_nbSession LOOP
        IF v_sessionLoad [v_session] < v_minDuration THEN
          v_minSession = v_session;
          v_minDuration = v_sessionLoad [v_session];
        END IF;
      END LOOP;
-- Allocate the batch to the session.
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_session = v_minSession
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_batch_number = r_batch.rlbp_batch_number;
      v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + r_batch.batch_duration;
    END LOOP;
-- Assign session 1 to all 'LOCK_TABLE' steps not yet affected.
    UPDATE emaj.emaj_rlbk_plan
      SET rlbp_session = 1
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_session IS NULL;
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate.
-- Get the number of recorded steps (except LOCK_TABLE).
    SELECT count(*) INTO v_nbStep
      FROM emaj.emaj_rlbk_plan
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_step <> 'LOCK_TABLE';
    IF v_nbStep > 0 THEN
-- If CTRLxDBLINK statistics are available, compute an average cost.
      SELECT sum(rlbt_duration) * v_nbStep / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = v_ctrlStepName
          AND rlbt_quantity > 0;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
-- Otherwise, use the fixed_step_rollback_duration parameter.
        v_estimDuration = v_fixed_dblink_rlbk * v_nbStep;
        v_estimMethod = 3;
      END IF;
-- Insert the 'CTRLxDBLINK' pseudo step.
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_estimated_quantity,
          rlbp_estimated_duration, rlbp_estimate_method
        ) VALUES (
          p_rlbkId, v_ctrlStepName, '', '', '', v_nbStep, v_estimDuration, v_estimMethod
        );
    END IF;
-- Return the number of tables to effectively rollback.
    RETURN v_effNbTable;
  END;
$_rlbk_planning$;

CREATE OR REPLACE FUNCTION emaj._rlbk_end(p_rlbkId INT, p_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_end$
-- This is the last step of a rollback group processing. It:
--    - deletes the marks that are no longer available,
--    - deletes the recorded sequences values for these deleted marks
--    - copy data into the emaj_rlbk_stat table,
--    - rollbacks all sequences of the groups,
--    - set the end rollback mark if logged rollback,
--    - and finaly set the operation as COMPLETED or COMMITED.
-- It returns the execution report of the rollback operation (a set of rows).
  DECLARE
    v_stmt                   TEXT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_isAlterGroupAllowed    BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_ctrlDuration           INTERVAL;
    v_markTimeId             BIGINT;
    v_nbSeq                  INT;
    v_markName               TEXT;
    v_messages               TEXT[] = ARRAY[]::TEXT[];
    v_msg                    TEXT;
    v_msgList                TEXT;
    r_msg                    RECORD;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table, rlbk_nb_sequence,
           rlbk_dblink_schema, rlbk_is_dblink_used, time_clock_timestamp
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl, v_nbSeq,
           v_dblinkSchema, v_isDblinkUsed, v_rlbkDatetime
      FROM emaj.emaj_rlbk
           JOIN emaj.emaj_time_stamp ON (time_id = rlbk_time_id)
      WHERE rlbk_id = p_rlbkId;
-- Get the mark timestamp for the 1st group (they all share the same timestamp).
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- If "unlogged" rollback, delete all marks later than the now rolled back mark and the associated sequences.
    IF NOT v_isLoggedRlbk THEN
-- Get the highest mark time id of the mark used for rollback, for all groups.
-- Delete the marks that are suppressed by the rollback (the related sequences have been already deleted by rollback functions)
-- with a logging in the history.
      WITH deleted AS
        (DELETE FROM emaj.emaj_mark
           WHERE mark_group = ANY (v_groupNames)
             AND mark_time_id > v_markTimeId
           RETURNING mark_time_id, mark_group, mark_name
        ),
           sorted_deleted AS                                         -- the sort is performed to produce stable results in regression tests
        (SELECT mark_group, mark_name
           FROM deleted
           ORDER BY mark_time_id, mark_group
        )
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END,
               'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted'
        FROM sorted_deleted;
-- And reset the mark_log_rows_before_next column for the new last mark.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND (mark_group, mark_time_id) IN                -- select only the last non deleted mark of each concerned group
              (SELECT mark_group, max(mark_time_id)
                 FROM emaj.emaj_mark
                 WHERE mark_group = ANY (v_groupNames)
                   AND NOT mark_is_deleted
                 GROUP BY mark_group
              );
-- The sequences related to the deleted marks can be also suppressed.
-- Delete first application sequences related data for the groups.
      DELETE FROM emaj.emaj_sequence
        USING emaj.emaj_relation
        WHERE sequ_schema = rel_schema
          AND sequ_name = rel_tblseq
          AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND rel_kind = 'S'
          AND sequ_time_id > v_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
-- Delete then tables related data for the groups.
      DELETE FROM emaj.emaj_table
        USING emaj.emaj_relation
        WHERE tbl_schema = rel_schema
          AND tbl_name = rel_tblseq
          AND upper_inf(rel_time_range)
          AND rel_group = ANY (v_groupNames)
          AND rel_kind = 'r'
          AND tbl_time_id > v_markTimeId
          AND tbl_time_id <@ rel_time_range
          AND tbl_time_id <> lower(rel_time_range);
    END IF;
-- Delete the now useless 'LOCK TABLE' steps from the emaj_rlbk_plan table.
    v_stmt = 'DELETE FROM emaj.emaj_rlbk_plan ' ||
             ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ''LOCK_TABLE'' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Prepare the CTRLxDBLINK pseudo step statistic by computing the global time spent between steps.
    SELECT coalesce(sum(ctrl_duration),'0'::INTERVAL) INTO v_ctrlDuration
      FROM
        (SELECT rlbs_session, rlbs_end_datetime - min(rlbp_start_datetime) - sum(rlbp_duration) AS ctrl_duration
           FROM emaj.emaj_rlbk_session rlbs
                JOIN emaj.emaj_rlbk_plan rlbp ON (rlbp_rlbk_id = rlbs_rlbk_id AND rlbp_session = rlbs_session)
           WHERE rlbs_rlbk_id = p_rlbkId
           GROUP BY rlbs_session, rlbs_end_datetime
        ) AS t;
-- Report duration statistics into the emaj_rlbk_stat table.
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_stat (rlbt_step, rlbt_schema, rlbt_table, rlbt_object,' ||
             '      rlbt_rlbk_id, rlbt_quantity, rlbt_duration)' ||
--   copy elementary steps for RLBK_TABLE, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_rlbk_id,' ||
             '      rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 6 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''DIS_APP_TRG'',''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_APP_TRG'',''ENA_LOG_TRG'') ' ||
             '    GROUP BY 1, 2, 3, 4, 5' ||
             '  UNION ALL ' ||
--   and the final CTRLxDBLINK pseudo step statistic
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, ' ||
             '      rlbp_estimated_quantity, ' || quote_literal(v_ctrlDuration) ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''CTRL+DBLINK'',''CTRL-DBLINK'') ' ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Build the execution report.
-- Start with the NOTICE messages.
    v_messages = array_append(v_messages, 'Notice: ' || format ('%s / %s tables effectively processed.', v_effNbTbl::TEXT, v_nbTbl::TEXT));
    IF v_nbSeq > 0 THEN
      v_messages = array_append(v_messages, 'Notice: ' || format ('%s sequences processed.', v_nbSeq::TEXT));
    END IF;
-- And then the WARNING messages for any elementary action from group structure change that has not been rolled back.
    FOR r_msg IN
-- Steps are splitted into 2 groups to filter them differently.
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               (CASE rlchg_change_kind
                  WHEN 'ADD_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'ADD_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'REMOVE_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_SEQUENCE' THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN 'MOVE_TABLE' THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
          FROM
-- Suppress duplicate ADD_TABLE / MOVE_TABLE / REMOVE_TABLE or ADD_SEQUENCE / MOVE_SEQUENCE / REMOVE_SEQUENCE for same table or sequence,
-- by keeping the most recent changes.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND rlchg_group = ANY (v_groupNames)
                      AND rlchg_tblseq <> ''
                      AND rlchg_rlbk_id IS NULL
                      AND rlchg_change_kind IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                  ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE rlchg_time_id = time_id
      UNION
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               (CASE rlchg_change_kind
                  WHEN 'CHANGE_PRIORITY' THEN
                    'Tables group change not rolled back: E-Maj priority for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_DATA_TABLESPACE' THEN
                    'Tables group change not rolled back: log data tablespace for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_INDEX_TABLESPACE' THEN
                    'Tables group change not rolled back: log index tablespace for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_IGNORED_TRIGGERS' THEN
                    'Tables group change not rolled back: ignored triggers list for '
                    || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  ELSE rlchg_change_kind::TEXT || ' / ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  END)::TEXT AS message
          FROM
-- Suppress duplicates for other change kind for each table or sequence.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND rlchg_group = ANY (v_groupNames)
                      AND rlchg_tblseq <> ''
                      AND rlchg_rlbk_id IS NULL
                      AND rlchg_change_kind NOT IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                 ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2
        ORDER BY rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq
    LOOP
      v_messages = array_append(v_messages, 'Warning: ' || r_msg.message);
    END LOOP;
-- Update the groups structure changes that are been covered by the rollback.
    UPDATE emaj.emaj_relation_change
      SET rlchg_rlbk_id = p_rlbkId
      WHERE rlchg_time_id > v_markTimeId
        AND rlchg_group = ANY (v_groupNames)
        AND rlchg_rlbk_id IS NULL;
-- Rollback the application sequences belonging to the groups.
-- Warning, this operation is not transaction safe (that's why it is placed at the end of the rollback operation)!.
-- If the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time.
    PERFORM emaj._rlbk_seq(t.*, greatest(v_markTimeId, lower(t.rel_time_range)))
      FROM
        (SELECT *
           FROM emaj.emaj_relation
           WHERE upper_inf(rel_time_range)
             AND rel_group = ANY (v_groupNames)
             AND rel_kind = 'S'
           ORDER BY rel_schema, rel_tblseq
        ) as t;
-- If rollback is a "logged" rollback, automatically set a mark representing the tables state just after the rollback.
-- This mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time.
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_DONE';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, p_multiGroup, TRUE, v_mark);
    END IF;
-- Return and trace the execution report
    FOREACH v_msg IN ARRAY v_messages
    LOOP
      SELECT substring(v_msg FROM '^(Notice|Warning): '), substring(v_msg, '^(?:Notice|Warning): (.*)') INTO rlbk_severity, rlbk_message;
      RETURN NEXT;
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, UPPER(rlbk_severity), 'Rollback id ' || p_rlbkId,
                rlbk_message);
    END LOOP;
-- Update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the output messages.
    SELECT string_agg(quote_literal(msg), ',') FROM unnest(v_messages) AS msg INTO v_msgList;
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = '''
          || CASE WHEN v_isDblinkUsed THEN 'COMPLETED' ELSE 'COMMITTED' END
          || ''', rlbk_end_datetime = clock_timestamp(), rlbk_messages = ARRAY[' || v_msgList || ']' ||
               ' WHERE rlbk_id = ' || p_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Close the dblink connection, if any.
    IF v_isDblinkUsed THEN
      PERFORM emaj._dblink_close_cnx('rlbk#1', v_dblinkSchema);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END',
              array_to_string(v_groupNames,','),
              'Rollback_id ' || p_rlbkId || ', ' || v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed'
             );
-- Final return.
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_end(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_end$;

--<end_functions>                                pattern used by the tool that extracts and insert the functions definition
------------------------------------------
--                                      --
-- event triggers and related functions --
--                                      --
------------------------------------------

------------------------------------
--                                --
-- emaj roles and rights          --
--                                --
------------------------------------
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA emaj FROM PUBLIC;

GRANT ALL ON ALL TABLES IN SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL SEQUENCES IN SCHEMA emaj TO emaj_adm;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA emaj TO emaj_adm;

GRANT SELECT ON ALL TABLES IN SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA emaj TO emaj_viewer;
REVOKE SELECT ON TABLE emaj.emaj_param FROM emaj_viewer;


------------------------------------
--                                --
-- Complete the upgrade           --
--                                --
------------------------------------

-- Enable the event triggers.
DO
$tmp$
  DECLARE
    v_event_trigger_array    TEXT[];
  BEGIN
-- Build the event trigger names array from the pg_event_trigger table.
    SELECT coalesce(array_agg(evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
      FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- Call the _enable_event_triggers() function.
    PERFORM emaj._enable_event_triggers(v_event_trigger_array);
  END;
$tmp$;

-- Set comments for all internal functions, by directly inserting a row in the pg_description table for all emaj functions
-- that do not have yet a recorded comment.
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

-- Update the version id in the emaj_param table.
ALTER TABLE emaj.emaj_param DISABLE TRIGGER emaj_param_change_trg;
UPDATE emaj.emaj_param SET param_value_text = '<devel>' WHERE param_key = 'emaj_version';
ALTER TABLE emaj.emaj_param ENABLE TRIGGER emaj_param_change_trg;

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.1.0 completed');

-- Post installation checks.
DO
$tmp$
  DECLARE
  BEGIN
-- Check the max_prepared_transactions GUC value.
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback is possible.', current_setting('max_prepared_transactions');
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
