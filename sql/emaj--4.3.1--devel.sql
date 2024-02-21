--
-- E-Maj: migration from 4.3.1 to <devel>
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
  BEGIN
-- The emaj version registered in emaj_param must be '4.3.1'.
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '4.3.1' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.3.1',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 11.
    IF current_setting('server_version_num')::int < 110000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 11.', current_setting('server_version');
    END IF;
-- Check E-Maj environment state.
    SELECT count(msg) FILTER (WHERE msg = 'No error detected'),
           count(msg) FILTER (WHERE msg LIKE 'Warning:%')
      INTO v_nbNoError, v_nbWarning
      FROM emaj.emaj_verify_all() AS t(msg);
    IF v_nbNoError = 0 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. You may execute '
                      '"SELECT * FROM emaj.emaj_verify_all();" to get more details.';
    END IF;
    IF v_nbWarning > 0 THEN
      RAISE WARNING 'E-Maj upgrade: the E-Maj environment health check reports warning. You may execute "SELECT * FROM '
                    'emaj.emaj_verify_all();" to get more details.';
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.3.1 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------
-- Table containing the history of all installed E-Maj versions.
CREATE TABLE emaj.emaj_version_hist (
  verh_version                 TEXT        NOT NULL,       -- emaj version name
  verh_time_range              TSTZRANGE   NOT NULL,       -- validity time stamps range (with inclusive bounds)
                                                           --   the lower bound corresponds to the installation/upgrade end time or the
                                                           --   latest database logical restore time
  verh_install_duration        INTERVAL,                   -- installation or upgrade duration
  verh_txid                    BIGINT
                               DEFAULT txid_current(),     -- id of the transaction installing/upgrading the version
  PRIMARY KEY (verh_version),
  EXCLUDE USING gist (verh_version WITH =, verh_time_range WITH &&)
  );
COMMENT ON TABLE emaj.emaj_version_hist IS
$$Contains E-Maj versions history.$$;

-- Table containing the groups history, i.e. the history of groups creations and drops.
CREATE TABLE emaj.emaj_group_hist (
  grph_group                   TEXT        NOT NULL,       -- group name
  grph_time_range              INT8RANGE   NOT NULL,       -- time stamps range of the group
  grph_is_rollbackable         BOOLEAN,                    -- false for 'AUDIT_ONLY' and true for 'ROLLBACKABLE' groups
  grph_log_sessions            INT,                        -- number of log sessions during the entire group's life
  PRIMARY KEY (grph_group, grph_time_range),
  EXCLUDE USING gist (grph_group WITH =, grph_time_range WITH &&)
  );
-- Functional index on emaj_group_hist used to speedup the history purge function.
CREATE INDEX emaj_group_hist_idx1 ON emaj.emaj_group_hist ((upper(grph_time_range)));
COMMENT ON TABLE emaj.emaj_group_hist IS
$$Contains E-Maj groups history.$$;

-- Table containing the log sessions, i.e. the history of groups starts and stops.
CREATE TABLE emaj.emaj_log_session (
  lses_group                   TEXT        NOT NULL,       -- group name
  lses_time_range              INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
  lses_marks                   INTEGER,                    -- number of recorded marks during the session, including rolled back marks
  lses_log_rows                BIGINT,                     -- number of changes estimates during the session (updated at each mark set)
  PRIMARY KEY (lses_group, lses_time_range),
  EXCLUDE USING gist (lses_group WITH =, lses_time_range WITH &&)
  );
-- Functional index on emaj_log_session used to speedup the history purge function.
CREATE INDEX emaj_log_session_idx1 ON emaj.emaj_log_session ((upper(lses_time_range)));
COMMENT ON TABLE emaj.emaj_log_session IS
$$Contains the log sessions history of E-Maj groups.$$;

--
-- Rename the emaj_time_stamp.time_tx_id column
--
ALTER TABLE emaj.emaj_time_stamp RENAME time_tx_id TO time_txid;

--
-- Create 3 materialized views and a temporary table to build timed events from the emaj_hist and emaj_time_stamp tables.
-- This will be used to populate the emaj_log_session table, and adjust the emaj_time_stamp table.
-- The first materialized view will also be used to populate the new emaj_version_hist table.
--

CREATE MATERIALIZED VIEW emaj.emaj_tmp_hist AS (
  SELECT *
    FROM emaj.emaj_hist
    WHERE (hist_function IN
             ('CREATE_GROUP', 'DROP_GROUP', 'IMPORT_GROUPS',
              'ASSIGN_TABLE', 'REMOVE_TABLE', 'MOVE_TABLE', 'MODIFY_TABLE',
              'ASSIGN_TABLES', 'REMOVE_TABLES', 'MOVE_TABLES', 'MODIFY_TABLES',
              'ASSIGN_SEQUENCE', 'REMOVE_SEQUENCE', 'MOVE_SEQUENCE',
              'ASSIGN_SEQUENCES', 'REMOVE_SEQUENCES', 'MOVE_SEQUENCES',
              'START_GROUP', 'STOP_GROUP', 'START_GROUPS', 'STOP_GROUPS',
              'SET_MARK_GROUP', 'SET_MARK_GROUPS', 'ROLLBACK_GROUP', 'ROLLBACK_GROUPS')
            AND hist_event IN ('BEGIN', 'END'))
       OR hist_function = 'EMAJ_INSTALL'
);
CREATE INDEX ON emaj.emaj_tmp_hist(hist_txid);

CREATE MATERIALIZED VIEW emaj.emaj_tmp_time_stamp AS (
  SELECT time_id, time_clock_timestamp, time_stmt_timestamp, time_txid, time_event
    FROM emaj.emaj_time_stamp
    WHERE time_clock_timestamp >= (SELECT min(hist_datetime) FROM emaj.emaj_tmp_hist)
      AND time_id > 0
);
CREATE INDEX ON emaj.emaj_tmp_time_stamp(time_txid);

CREATE MATERIALIZED VIEW emaj.emaj_tmp_event AS (
  SELECT b.hist_id as begin_hist_id, b.hist_function, b.hist_txid, b.hist_datetime AS begin_ts, min(e.hist_id) as end_hist_id
    FROM emaj.emaj_tmp_hist b, emaj.emaj_tmp_hist e
    WHERE b.hist_function = e.hist_function
      AND b.hist_event = 'BEGIN' AND e.hist_event = 'END'
      AND b.hist_txid = e.hist_txid
      AND e.hist_id > b.hist_id
      AND b.hist_function <> 'EMAJ_INSTALL'
    GROUP BY 1,2,3,4
    ORDER BY b.hist_id
  );

CREATE TEMPORARY TABLE emaj_tmp_timed_event (
  event_hist_id_begin        BIGINT,
  event_hist_id_end          BIGINT,
  event_time_id              BIGINT,
  event_function             TEXT,
  event_object               TEXT
);

-- Scan the functions begin and end from the emaj_hist table and look at the emaj_time_stamp table to get their related time_id.
DO
$do$
  DECLARE
    v_rlbkId                 INTEGER;
    v_timeId                 BIGINT;
    v_timeEvent              CHAR(1);
    v_timeClockTs            TIMESTAMPTZ;
    v_lastRlbkTimeId         BIGINT;
    v_lastRlbkBeginTs        TIMESTAMPTZ;
    v_lastRlbkEndTs          TIMESTAMPTZ;
    v_lastRlbkObject         TEXT;
    r_fctExec                RECORD;
  BEGIN
-- Scan the functions execution built from emaj_hist.
    FOR r_fctExec IN
      SELECT begin_hist_id, end_hist_id, b.hist_function, b.hist_txid, begin_ts,
             e.hist_datetime AS end_ts, e.hist_object, e.hist_wording
        FROM emaj.emaj_tmp_event b
             JOIN emaj.emaj_tmp_hist e ON (e.hist_id = end_hist_id)
    LOOP
      IF r_fctExec.hist_function IN ('ROLLBACK_GROUP','ROLLBACK_GROUPS') THEN
-- Process the E-Maj rollback function executions in a special way, because when a logged rollback uses dblink connections, the txids
--   in emaj_hist and emaj_time_stamp differ. But the rollback time_id is already known in the emaj_rlbk table. So get it from there.
-- Get the rollback id from the end rollback event.
        v_rlbkId = substr(r_fctExec.hist_wording,13)::INTEGER;
-- Read the emaj_rlbk table to get the rollback time_id.
        SELECT rlbk_time_id
          INTO v_timeId
          FROM emaj.emaj_rlbk
          WHERE rlbk_id = v_rlbkId;
-- Record the event.
        INSERT INTO emaj_tmp_timed_event
          VALUES (r_fctExec.begin_hist_id, r_fctExec.end_hist_id, v_timeId, r_fctExec.hist_function, r_fctExec.hist_object);
-- Keep in memory the characteristics of this rollback to use it if needed later for mark set event executed inside this rollback.
        v_lastRlbkTimeId = v_timeId;
        v_lastRlbkBeginTs = r_fctExec.begin_ts;
        v_lastRlbkEndTs = r_fctExec.end_ts;
        v_lastRlbkObject = r_fctExec.hist_object;
      ELSE
-- Process other function executions.
-- Look at the emaj_time_stamp events to find the time stamp in the execution time frame for the same transaction.
        SELECT time_id, time_event, time_clock_timestamp
          INTO v_timeId, v_timeEvent, v_timeClockTs
          FROM emaj.emaj_tmp_time_stamp
          WHERE r_fctExec.hist_txid = time_txid
            AND time_clock_timestamp BETWEEN r_fctExec.begin_ts AND r_fctExec.end_ts;
        IF FOUND THEN
-- OK, we got it, so record the event.
          INSERT INTO emaj_tmp_timed_event
            VALUES (r_fctExec.begin_hist_id, r_fctExec.end_hist_id, v_timeId, r_fctExec.hist_function, r_fctExec.hist_object);
        ELSE
-- We have not found matching time stamp.
          IF r_fctExec.hist_function IN ('SET_MARK_GROUP','SET_MARK_GROUPS') THEN
-- If it deals with a mark set, look at the previous time stamp for the same transaction.
--   This cover cases with groups structure change while in logging state. (the main function shares the time stamp with the mark set)
            SELECT time_id, time_event, time_clock_timestamp
              INTO v_timeId, v_timeEvent, v_timeClockTs
              FROM emaj.emaj_tmp_time_stamp
              WHERE time_txid = r_fctExec.hist_txid
                AND time_clock_timestamp < r_fctExec.begin_ts
              ORDER BY time_id DESC
              LIMIT 1;
            IF FOUND THEN
-- OK, we got it, so record the event.
              INSERT INTO emaj_tmp_timed_event
                VALUES (r_fctExec.begin_hist_id, r_fctExec.end_hist_id, v_timeId, r_fctExec.hist_function, r_fctExec.hist_object);
            ELSE
              IF r_fctExec.begin_ts BETWEEN v_lastRlbkBeginTs AND v_lastRlbkEndTs AND v_lastRlbkObject = v_lastRlbkObject THEN
-- We have not found it but the mark set is inside the time interval of the last executed rollback operation.
                v_timeId = v_lastRlbkTimeId;
-- Record the event.
                INSERT INTO emaj_tmp_timed_event
                  VALUES (r_fctExec.begin_hist_id, r_fctExec.end_hist_id, v_timeId, r_fctExec.hist_function, r_fctExec.hist_object);
              ELSE
                IF r_fctExec.hist_object IS NOT NULL THEN
-- The time id of the mark set is definitely not found.
                  RAISE WARNING 'Search for time stamp id: for hist_id %, time_id not found (function = % starting at %)',
                                 r_fctExec.begin_hist_id, r_fctExec.hist_function, r_fctExec.begin_ts;
                END IF;
              END IF;
            END IF;
          ELSE
-- The time id of the other function is not found.
            RAISE WARNING 'Search for time stamp id: for hist_id %, time_id not found (function = % starting at %)',
                           r_fctExec.begin_hist_id, r_fctExec.hist_function, r_fctExec.begin_ts;
          END IF;
        END IF;
      END IF;
    END LOOP;
  END;
$do$;

--
-- Once this events history is built, update or populate the stable emaj tables.
--

-- emaj_time_stamp: update the time_event columns for marks generated at start group or stop group events.
--   Set the time_event to 'S' or 'X' instead of 'M' for the start group or stop group operations respectively.
UPDATE emaj.emaj_time_stamp
  SET time_event = CASE WHEN event_function IN ('START_GROUP', 'START_GROUPS') THEN 'S' ELSE 'X' END
  FROM emaj_tmp_timed_event
  WHERE event_function IN ('START_GROUP', 'START_GROUPS', 'STOP_GROUP', 'STOP_GROUPS')
    AND time_id = event_time_id
    AND time_event = 'M';

-- populate emaj_group_hist.
DO
$do$
  DECLARE
    v_isRollbackable         BOOLEAN;
    r_event                  RECORD;
  BEGIN
    RAISE NOTICE 'Starting the rows generation for the new emaj_group_hist table.';
    RAISE NOTICE 'Based on existing history and logs content, #log_sessions statistics may be inaccurate.';
-- Scan and filter CREATE_GROUP and DROP_GROUP timed events.
    FOR r_event IN
      SELECT event_hist_id_begin, event_time_id, event_function, event_object
        FROM emaj_tmp_timed_event
        WHERE event_function IN ('CREATE_GROUP', 'DROP_GROUP')
        ORDER BY event_hist_id_end
    LOOP
      CASE r_event.event_function
        WHEN 'CREATE_GROUP' THEN
-- Get the is_rollbackable state from the BEGIN event in emaj_hist
          SELECT hist_wording = 'rollbackable'
            INTO v_isRollbackable
            FROM emaj.emaj_hist
            WHERE hist_id = r_event.event_hist_id_begin;
-- Create the emaj_group_hist row.
          BEGIN
            INSERT INTO emaj.emaj_group_hist(grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
              VALUES (r_event.event_object, int8range(r_event.event_time_id, NULL, '[]'), v_isRollbackable, 0);
          EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '*** Insert into emaj_group_hist failed. Continue.';
          END;
        WHEN 'DROP_GROUP' THEN
-- Close the latest time range of the group, if any.
          UPDATE emaj.emaj_group_hist
            SET grph_time_range = int8range(lower(grph_time_range), r_event.event_time_id, '[]')
            WHERE grph_group = r_event.event_object
              AND upper_inf(grph_time_range);
          IF NOT FOUND THEN
-- Otherwise, create a new row without defined lower bound.
            INSERT INTO emaj.emaj_group_hist(grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
              VALUES (r_event.event_object, int8range(NULL, r_event.event_time_id, '[]'), NULL, 0);
          END IF;
     END CASE;
    END LOOP;
-- Scan emaj_group to detect group creations that occurred before the current history lower bound.
    INSERT INTO emaj.emaj_group_hist(grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
      SELECT group_name, int8range(group_creation_time_id, NULL, '[]'), group_is_rollbackable, 0
        FROM emaj.emaj_group
        WHERE NOT EXISTS(
                SELECT 0 FROM emaj.emaj_group_hist WHERE grph_group = group_name AND upper_inf(grph_time_range)
                        );
  END;
$do$;

-- Populate emaj_log_session.
DO
$do$
  DECLARE
    v_startTimeId            BIGINT;
    v_1stMarkAfterStopTimeId BIGINT;
    v_nbMark                 INTEGER;
    v_nbChange               BIGINT;
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_groupIsLogging         BOOLEAN;
    v_groupCreationTimeId    BIGINT;
    r_group                  RECORD;
    r_event                  RECORD;
    r_rel                    RECORD;
  BEGIN
    RAISE NOTICE 'Starting the rows generation for the new emaj_log_session table.';
    RAISE NOTICE 'Based on existing history and logs content, #marks and #changes statistics may be inaccurate.';
-- Process each known group in sequence.
    FOR r_group IN
      SELECT DISTINCT grph_group
        FROM emaj.emaj_group_hist
        ORDER BY grph_group
    LOOP
-- Initialize variables.
      v_nbMark = 0;
      v_startTimeId = NULL;
      v_1stMarkAfterStopTimeId = NULL;
-- Look at each create/start/stop/set_mark event for the group.
      FOR r_event IN
        SELECT *, substring(event_function, '#"%#"_GROUP%', '#') AS abbrev_function
          FROM emaj_tmp_timed_event
          WHERE event_function IN ('START_GROUP', 'START_GROUPS', 'STOP_GROUP', 'STOP_GROUPS', 'SET_MARK_GROUP', 'SET_MARK_GROUPS')
            AND event_object ~ ('(^|,)' || r_group.grph_group || '(,|$)')
          ORDER BY event_hist_id_end
      LOOP
        CASE r_event.abbrev_function
          WHEN 'START' THEN
-- A group start => keep the start time id.
            v_nbMark = 1;
            v_startTimeId = r_event.event_time_id;
          WHEN 'STOP' THEN
-- A group stop => generate a log session.
            v_nbChange = 0;
            IF v_nbMark > 0 THEN
-- Get the changes statistics on the time frame, if there is at least 1 mark.
              FOR r_rel IN
                SELECT rel_schema, rel_tblseq, rel_time_range
                  FROM emaj.emaj_relation
                  WHERE rel_group = r_group.grph_group
                    AND rel_kind = 'r'
                    AND rel_time_range && int8range(coalesce(v_startTimeId, v_1stMarkAfterStopTimeId), r_event.event_time_id, '[)')
              LOOP
                SELECT tbl_log_seq_last_val INTO v_beginLastValue
                  FROM emaj.emaj_table
                  WHERE tbl_schema = r_rel.rel_schema
                    AND tbl_name = r_rel.rel_tblseq
                    AND tbl_time_id = greatest(coalesce(v_startTimeId, v_1stMarkAfterStopTimeId), lower(r_rel.rel_time_range));
                IF FOUND THEN
                  SELECT tbl_log_seq_last_val INTO v_endLastValue
                    FROM emaj.emaj_table
                    WHERE tbl_schema = r_rel.rel_schema
                      AND tbl_name = r_rel.rel_tblseq
                      AND tbl_time_id = least(r_event.event_time_id, upper(r_rel.rel_time_range));
                  IF FOUND THEN
                    v_nbChange = v_nbChange + v_endLastValue - v_beginLastValue;
                  END IF;
                END IF;
              END LOOP;
            END IF;
-- If the start time is unknown, use the group creation time as start time
--   (it's better than having null log session lower bound and a finite creation time).
            IF v_startTimeId IS NULL THEN
              SELECT lower(grph_time_range) INTO v_startTimeId
		        FROM emaj.emaj_group_hist
                WHERE grph_group = r_group.grph_group
                  AND grph_time_range >@ r_event.event_time_id;
            END IF;
-- Create the log session.
            INSERT INTO emaj.emaj_log_session VALUES
              (r_group.grph_group, int8range(v_startTimeId, r_event.event_time_id, '[]'), v_nbMark, v_nbChange);
            UPDATE emaj.emaj_group_hist
              SET grph_log_sessions = grph_log_sessions + 1
              WHERE grph_group = r_group.grph_group
                AND grph_time_range @> r_event.event_time_id;
-- Reset variables.
            v_startTimeId = NULL;
            v_1stMarkAfterStopTimeId = NULL;
          WHEN 'SET_MARK' THEN
-- A mark set for the group => increment the marks counter.
            IF v_1stMarkAfterStopTimeId IS NULL THEN
              v_1stMarkAfterStopTimeId = r_event.event_time_id;
            END IF;
            v_nbMark = v_nbMark + 1;
        END CASE;
      END LOOP;
-- If the group currently exists and is in logging state, create the current log session.
      SELECT group_is_logging, group_creation_time_id
        INTO v_groupIsLogging, v_groupCreationTimeId
        FROM emaj_group
        WHERE group_name = r_group.grph_group;
      IF v_groupIsLogging THEN
        v_nbChange = 0;
        IF v_startTimeId IS NOT NULL OR v_1stMarkAfterStopTimeId IS NOT NULL THEN
-- If a start group or a mark set is known, compute changes statistics.
          FOR r_rel IN
            SELECT rel_schema, rel_tblseq, rel_time_range, rel_log_schema, rel_log_sequence
              FROM emaj.emaj_relation
              WHERE rel_group = r_group.grph_group
                AND rel_kind = 'r'
                AND rel_time_range @> coalesce(v_startTimeId, v_1stMarkAfterStopTimeId)
          LOOP
            SELECT tbl_log_seq_last_val INTO v_beginLastValue
              FROM emaj.emaj_table
              WHERE tbl_schema = r_rel.rel_schema
                AND tbl_name = r_rel.rel_tblseq
                AND tbl_time_id = greatest(coalesce(v_startTimeId, v_1stMarkAfterStopTimeId), lower(r_rel.rel_time_range));
            IF FOUND THEN
              IF upper_inf(r_rel.rel_time_range) THEN
                EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM %I.%I',
                               r_rel.rel_log_schema, r_rel.rel_log_sequence)
                  INTO v_endLastValue;
              ELSE
                SELECT tbl_log_seq_last_val INTO v_endLastValue
                  FROM emaj.emaj_table
                  WHERE tbl_schema = r_rel.rel_schema
                    AND tbl_name = r_rel.rel_tblseq
                    AND tbl_time_id = upper(r_rel.rel_time_range);
              END IF;
              IF FOUND THEN
                v_nbChange = v_nbChange + v_endLastValue - v_beginLastValue;
              END IF;
            END IF;
          END LOOP;
        END IF;
-- Create the log session.
-- If the start time is unknown, record the group creation time.
        INSERT INTO emaj.emaj_log_session VALUES
          (r_group.grph_group, int8range(coalesce(v_startTimeId, v_groupCreationTimeId), NULL, '[]'), v_nbMark, v_nbChange);
        UPDATE emaj.emaj_group_hist
          SET grph_log_sessions = grph_log_sessions + 1
          WHERE grph_group = r_group.grph_group
            AND upper_inf(grph_time_range);
      END IF;
    END LOOP;
  END;
$do$;

-- Populate emaj_log_session.
DO
$do$
  DECLARE
    v_version                TEXT;
    v_startDatetime          TIMESTAMPTZ;
    v_txid                   BIGINT;
    r_hist                   RECORD;
  BEGIN
-- Scan the EMAJ_INSTALL events
    FOR r_hist IN
      SELECT hist_datetime, hist_event, substring(hist_object FROM 'E-Maj (.*)') AS version, hist_txid
        FROM emaj.emaj_tmp_hist
        WHERE hist_function = 'EMAJ_INSTALL'
        ORDER BY hist_id
      LOOP
      CASE r_hist.hist_event
        WHEN 'BEGIN' THEN
-- This is a version upgrade start.
          UPDATE emaj.emaj_version_hist
            SET verh_time_range = TSTZRANGE(lower(verh_time_range), r_hist.hist_datetime, '[]')
            WHERE upper_inf(verh_time_range);
          v_startDatetime = r_hist.hist_datetime;
          v_version = r_hist.version;
          v_txid = r_hist.hist_txid;
        WHEN 'END' THEN
-- This is the version upgrade end.
          IF r_hist.version <> v_version OR r_hist.hist_txid < v_txid then
            RAISE EXCEPTION 'E-Maj upgrade: Internal error while populating the new emaj_veresion_hist table.';
          ELSE
            INSERT INTO emaj.emaj_version_hist VALUES
              (r_hist.version, TSTZRANGE(r_hist.hist_datetime, NULL, '[]'), r_hist.hist_datetime - v_startDatetime, r_hist.hist_txid);
          END IF;
        ELSE
-- This is an initial installation. (the initial installation duration will remain unknown)
          INSERT INTO emaj.emaj_version_hist VALUES
            (r_hist.version, TSTZRANGE(r_hist.hist_datetime, NULL, '[]'), NULL, r_hist.hist_txid);
      END CASE;
    END LOOP;
  END;
$do$;

DROP TABLE emaj_tmp_timed_event;
DROP MATERIALIZED VIEW emaj.emaj_tmp_event CASCADE;
DROP MATERIALIZED VIEW emaj.emaj_tmp_time_stamp CASCADE;
DROP MATERIALIZED VIEW emaj.emaj_tmp_hist CASCADE;

--
-- Drop the emaj_mark.mark_is_deleted column
--
ALTER TABLE emaj.emaj_mark DROP COLUMN mark_is_deleted;

--
-- Drop the emaj_group.group_creation_time_id column.
--
ALTER TABLE emaj.emaj_group DROP COLUMN group_creation_time_id;

--
-- Drop the emaj_relation_change.rlchg_rlbk_id column
--
ALTER TABLE emaj.emaj_relation_change DROP COLUMN rlchg_rlbk_id;

--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--
SELECT pg_catalog.pg_extension_config_dump('emaj_version_hist','WHERE NOT upper_inf(verh_time_range)');
SELECT pg_catalog.pg_extension_config_dump('emaj_param','');
SELECT pg_catalog.pg_extension_config_dump('emaj_group_hist','');
SELECT pg_catalog.pg_extension_config_dump('emaj_log_session','');

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------
DROP TYPE emaj._log_stat_type CASCADE;
DROP TYPE emaj._detailed_log_stat_type CASCADE;

ALTER TYPE emaj.emaj_detailed_log_stat_type ALTER ATTRIBUTE stat_role TYPE TEXT;
ALTER TYPE emaj.emaj_detailed_log_stat_type ALTER ATTRIBUTE stat_verb TYPE TEXT;

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
DROP FUNCTION IF EXISTS emaj._set_time_stamp(P_TIMESTAMPTYPE CHAR(1));
DROP FUNCTION IF EXISTS emaj._check_marks_range(P_GROUPNAMES TEXT[],INOUT P_FIRSTMARK TEXT,INOUT P_LASTMARK TEXT,P_FINITEUPPERBOUND BOOLEAN,OUT P_FIRSTMARKTIMEID BIGINT,OUT P_LASTMARKTIMEID BIGINT,OUT P_FIRSTMARKTS TIMESTAMPTZ,OUT P_LASTMARKTS TIMESTAMPTZ,OUT P_FIRSTMARKEMAJGID BIGINT,OUT P_LASTMARKEMAJGID BIGINT);
DROP FUNCTION IF EXISTS emaj._log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj._detailed_log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._emaj_param_change_fnct()
RETURNS TRIGGER LANGUAGE plpgsql AS
$_emaj_param_change_fnct$
  BEGIN
    IF TG_OP = 'DELETE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('', 'DELETED PARAMETER', OLD.param_key);
      RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'UPDATED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('', 'INSERTED PARAMETER', NEW.param_key,
                CASE WHEN NEW.param_key = 'dblink_user_password' THEN '<masked data>'
                     ELSE coalesce(NEW.param_value_text, NEW.param_value_numeric::TEXT,
                          NEW.param_value_boolean::TEXT, NEW.param_value_interval::TEXT)
                END);
      RETURN NEW;
    ELSIF TG_OP = 'TRUNCATE' THEN
      RAISE EXCEPTION '_emaj_param_change_fnct: TRUNCATE the emaj_param table is not allowed. Use DELETE instead.';
    END IF;
    RETURN NULL;
  END;
$_emaj_param_change_fnct$;

CREATE OR REPLACE FUNCTION emaj._set_time_stamp(p_function TEXT, p_timeEvent CHAR(1))
RETURNS BIGINT LANGUAGE SQL AS
$$
-- This function creates a new time stamp in the emaj_time_stamp table, records it into the emaj_hist table (except for rollback events)
--   and returns its identifier.
WITH inserted_time_stamp AS (
  INSERT INTO emaj.emaj_time_stamp (time_last_emaj_gid, time_event)
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END, p_timeEvent
      FROM emaj.emaj_global_seq
    RETURNING time_id, time_clock_timestamp
  ), inserted_hist AS (
  INSERT INTO emaj.emaj_hist (hist_datetime, hist_function, hist_event, hist_object)
    SELECT time_clock_timestamp, p_function, 'TIME STAMP SET', time_id::TEXT
      FROM inserted_time_stamp
      WHERE p_timeEvent <> 'R'
  )
  SELECT time_id FROM inserted_time_stamp;
$$;

CREATE OR REPLACE FUNCTION emaj._check_json_param_conf(p_paramsJson JSON)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_check_json_param_conf$
-- This function verifies that the JSON structure that contains a parameter configuration is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them.
-- It is called by the _import_param_conf() function.
-- The function is also directly called by Emaj_web.
-- This function checks that:
--   - the "parameters" attribute exists
--   - "key" attribute are defined and are known parameters
--   - no unknow attribute are listed
--   - parameters are not described several times
-- Input: the JSON structure to check
-- Output: set of error messages
  DECLARE
    v_parameters             JSON;
    v_paramNumber            INT;
    v_key                    TEXT;
    r_param                  RECORD;
  BEGIN
-- Extract the "parameters" json path and check that the attribute exists.
    v_parameters = p_paramsJson #> '{"parameters"}';
    IF v_parameters IS NULL THEN
      RETURN QUERY
        VALUES (101, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                'The JSON structure does not contain any "parameters" array.');
    ELSE
-- Check that all keywords of the "parameters" structure are valid.
      v_paramNumber = 0;
      FOR r_param IN
        SELECT param
          FROM json_array_elements(v_parameters) AS t(param)
      LOOP
        v_paramNumber = v_paramNumber + 1;
-- Check the "key" attribute exists in the json structure.
        v_key = r_param.param ->> 'key';
        IF v_key IS NULL THEN
          RETURN QUERY
            VALUES (102, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_paramNumber,
                    format('The #%s parameter has no "key" attribute or a "key" set to null.',
                           v_paramNumber::TEXT));
        END IF;
-- Check that the structure only contains "key" and "value" attributes.
        RETURN QUERY
          SELECT 103, 1, v_key, attr, NULL::TEXT, NULL::TEXT, NULL::INT,
               format('For the parameter "%s", the attribute "%s" is unknown.',
                      v_key, attr)
            FROM (
              SELECT attr
                FROM json_object_keys(r_param.param) AS x(attr)
                WHERE attr NOT IN ('key', 'value')
              ) AS t;
-- Check the key is valid.
        IF v_key NOT IN ('dblink_user_password', 'history_retention', 'alter_log_table',
                         'avg_row_rollback_duration', 'avg_row_delete_log_duration', 'avg_fkey_check_duration',
                         'fixed_step_rollback_duration', 'fixed_table_rollback_duration', 'fixed_dblink_rollback_duration') THEN
          RETURN QUERY
            VALUES (104, 1, v_key, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                 format('"%s" is not a known E-Maj parameter.',
                        v_key));
        END IF;
      END LOOP;
-- Check that parameters are not configured more than once in the JSON structure.
      RETURN QUERY
        SELECT 105, 1, "key", NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
             format('The JSON structure references several times the parameter "%s".',
                    "key")
          FROM (
            SELECT "key", count(*)
              FROM json_to_recordset(v_parameters) AS x("key" TEXT)
              GROUP BY "key"
              HAVING count(*) > 1
            ) AS t;
    END IF;
--
    RETURN;
  END;
$_check_json_param_conf$;

CREATE OR REPLACE FUNCTION emaj._check_mark_name(p_groupNames TEXT[], p_mark TEXT, p_checkActive BOOLEAN DEFAULT FALSE)
RETURNS TEXT LANGUAGE plpgsql AS
$_check_mark_name$
-- This function verifies that a mark name exists for one or several groups.
-- It processes the EMAJ_LAST_MARK keyword.
-- When several groups are supplied, it checks that the mark represents the same point in time for all groups.
-- Input: array of group names, name of the mark to check, boolean to ask for a mark is active check
-- Output: internal name of the mark
  DECLARE
    v_markName               TEXT = p_mark;
    v_groupList              TEXT;
    v_count                  INTEGER;
  BEGIN
-- Process the 'EMAJ_LAST_MARK' keyword, if needed.
    IF p_mark = 'EMAJ_LAST_MARK' THEN
-- Detect groups that have no recorded mark.
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM
          (  SELECT unnest(p_groupNames)
           EXCEPT
             SELECT mark_group
               FROM emaj.emaj_mark
          ) AS t(group_name);
      IF v_count > 0 THEN
        IF v_count = 1 THEN
          RAISE EXCEPTION '_check_mark_name: The group "%" has no mark.', v_groupList;
        ELSE
          RAISE EXCEPTION '_check_mark_name: The groups "%" have no mark.', v_groupList;
        END IF;
      END IF;
      IF array_length(p_groupNames, 1) > 1 THEN
-- In multi-group operations, verify that the last mark of each group has been set at the same time.
        SELECT count(DISTINCT mark_time_id) INTO v_count
          FROM
            (SELECT mark_group, max(mark_time_id) AS mark_time_id
               FROM emaj.emaj_mark
               WHERE mark_group = ANY (p_groupNames)
               GROUP BY 1
            ) AS t;
        IF v_count > 1 THEN
          RAISE EXCEPTION '_check_mark_name: The EMAJ_LAST_MARK does not represent the same point in time for all groups.';
        END IF;
      END IF;
-- Get the name of the last mark for the first group in the array, as we now know that all groups share the same last mark.
      SELECT mark_name INTO v_markName
        FROM emaj.emaj_mark
        WHERE mark_group = p_groupNames[1]
        ORDER BY mark_time_id DESC
        LIMIT 1;
    ELSE
-- For usual mark name (i.e. not EMAJ_LAST_MARK),
-- ... Check that the mark exists for all groups.
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM
          (  SELECT unnest(p_groupNames)
           EXCEPT
             SELECT mark_group
               FROM emaj.emaj_mark
               WHERE mark_name = v_markName
          ) AS t(group_name);
      IF v_count > 0 THEN
        IF v_count = 1 THEN
          RAISE EXCEPTION '_check_mark_name: The mark "%" does not exist for the group "%".', v_markName, v_groupList;
        ELSE
          RAISE EXCEPTION '_check_mark_name: The mark "%" does not exist for the groups "%".', v_markName, v_groupList;
        END IF;
      END IF;
-- ... Check that the mark represents the same point in time for all groups.
      SELECT count(DISTINCT mark_time_id) INTO v_count
        FROM emaj.emaj_mark
        WHERE mark_name = v_markName
          AND mark_group = ANY (p_groupNames);
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_mark_name: The mark "%" does not represent the same point in time for all groups.', v_markName;
      END IF;
    END IF;
-- If requested, check the mark is active for all groups.
    IF p_checkActive THEN
      SELECT string_agg(mark_group,', ' ORDER BY mark_group), count(*)
        INTO v_groupList, v_count
        FROM emaj.emaj_mark
             JOIN emaj.emaj_log_session ON (lses_group = mark_group AND lses_time_range @> mark_time_id)
        WHERE mark_name = v_markName
          AND mark_group = ANY(p_groupNames)
          AND NOT upper_inf(lses_time_range);
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_mark_name: For the group "%", the mark "%" was set before the latest group start.',
                        v_groupList, v_markName;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_mark_name: For the groups "%", the mark "%" was set before the latest group start.',
                        v_groupList, v_markName;
      END IF;
    END IF;
--
    RETURN v_markName;
  END;
$_check_mark_name$;

CREATE OR REPLACE FUNCTION emaj._check_marks_range(p_groupNames TEXT[], INOUT p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                   p_finiteUpperBound BOOLEAN DEFAULT FALSE, p_checkLogSession BOOLEAN DEFAULT TRUE,
                                                   OUT p_firstMarkTimeId BIGINT, OUT p_lastMarkTimeId BIGINT,
                                                   OUT p_firstMarkTs TIMESTAMPTZ, OUT p_lastMarkTs TIMESTAMPTZ,
                                                   OUT p_firstMarkEmajGid BIGINT, OUT p_lastMarkEmajGid BIGINT)
LANGUAGE plpgsql AS
$_check_marks_range$
-- This function verifies that a marks range is valid for one or several groups and return useful data about both marks.
-- It checks that both marks defining the bounds exist and are in chronological order.
-- If required, it warns if the mark range is not contained by a log session.
-- It processes the EMAJ_LAST_MARK keyword.
-- A last mark (upper bound) set to NULL means "the current state". In this case, no specific checks is performed.
-- When several groups are supplied, it checks that the marks represent the same point in time for all groups.
-- Input: array of group names, name of the first mark, name of the last mark,
--        2 booleans to perform or not additional checks
-- Output: name, time id, clock timestamp and emaj_gid for both marks
  DECLARE
    v_groupName              TEXT;
  BEGIN
-- Check that the first mark is not NULL or empty.
    IF p_firstMark IS NULL OR p_firstMark = '' THEN
      RAISE EXCEPTION '_check_marks_range: The first mark cannot be NULL or empty.';
    END IF;
-- Checks the supplied first mark.
    SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_firstMark) INTO p_firstMark;
-- Get some additional data about the first mark.
-- (use the first group of the array, as we are now sure that all groups share the same mark).
    SELECT mark_time_id, time_clock_timestamp, time_last_emaj_gid INTO p_firstMarkTimeId, p_firstMarkTs, p_firstMarkEmajGid
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupNames[1]
        AND mark_name = p_firstMark;
    IF p_lastMark IS NULL OR p_lastMark = '' THEN
      IF p_finiteUpperBound THEN
        RAISE EXCEPTION '_check_marks_range: The last mark cannot be NULL or empty.';
      END IF;
    ELSE
-- The last mark is not NULL or empty, so check it.
      SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_lastMark) INTO p_lastMark;
-- Get some additional data about the last mark (that may be NULL)
-- (use the first group of the array, as we are now sure that all groups share the same mark).
      SELECT mark_time_id, time_clock_timestamp, time_last_emaj_gid INTO p_lastMarkTimeId, p_lastMarkTs, p_lastMarkEmajGid
        FROM emaj.emaj_mark
             JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
        WHERE mark_group = p_groupNames[1]
          AND mark_name = p_lastMark;
-- And check that the last mark has been set after the first mark.
      IF p_firstMarkTimeId > p_lastMarkTimeId THEN
        RAISE EXCEPTION '_check_marks_range: The start mark "%" (%) has been set after the end mark "%" (%).',
          p_firstMark, p_firstMarkTs, p_lastMark, p_lastMarkTs;
      END IF;
    END IF;
-- If required, warn if the mark range is not contained by a single log session for any tables group.
    IF p_checkLogSession THEN
      FOREACH v_groupName IN ARRAY p_groupNames
      LOOP
        IF p_lastMark IS NULL OR p_lastMark = '' THEN
          PERFORM 0
            FROM emaj.emaj_log_session
            WHERE lses_group = v_groupName
              AND lses_time_range @> int8range(p_firstMarkTimeId, NULL, '[]');
          IF NOT FOUND THEN
            RAISE WARNING 'Since mark "%", the tables group "%" has not been always in logging state. '
                          'Some data changes may not have been recorded.', p_firstMark, v_groupName;
          END IF;
        ELSE
          PERFORM 0
            FROM emaj.emaj_log_session
            WHERE lses_group = v_groupName
              AND lses_time_range @> int8range(p_firstMarkTimeId, p_lastMarkTimeId, '[]');
          IF NOT FOUND THEN
            RAISE WARNING 'Between marks "%" and "%", the tables group "%" has not been always in logging state. '
                          'Some data changes may not have been recorded.', p_firstMark, p_lastMark, v_groupName;
          END IF;
        END IF;
      END LOOP;
    END IF;
--
    RETURN;
  END;
$_check_marks_range$;

CREATE OR REPLACE FUNCTION emaj._build_path_name(p_dir TEXT, p_file TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$_build_path_name$
-- This function build a path name from a directory name and a file names.
-- Some characters of the file name are translated in order to manipulate files on the OS more easily.
-- Both names are concatenated with a / character between.
SELECT p_dir || '/' || translate(p_file, E' /\\|$<>*\'"', '__________');
$_build_path_name$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(p_schema TEXT, p_tables TEXT[], p_group TEXT, p_properties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB p_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
  DECLARE
    v_function               TEXT;
    v_groupIsRollbackable    BOOLEAN;
    v_groupIsLogging         BOOLEAN;
    v_priority               INT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_ignoredTriggers        TEXT[];
    v_ignoredTrgProfiles     TEXT[];
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_logSchema              TEXT;
    v_selectedIgnoredTrgs    TEXT[];
    v_selectConditions       TEXT;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_nbAssignedTbl          INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters.
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_group], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_rollbackable, group_is_logging INTO v_groupIsRollbackable, v_groupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_group;
-- Check the supplied schema exists and is not an E-Maj schema.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" does not exist.', p_schema;
    END IF;
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_schema
            WHERE sch_name = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_tables: The schema "%" is an E-Maj schema.', p_schema;
    END IF;
-- Check tables.
    IF NOT p_arrayFromRegex THEN
-- From the tables array supplied by the user, remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that application tables exist.
      WITH tables AS (
        SELECT unnest(p_tables) AS table_name
      )
      SELECT string_agg(quote_ident(table_name), ', ') INTO v_list
        FROM
          (SELECT table_name
             FROM tables
             WHERE NOT EXISTS
                     (SELECT 0
                        FROM pg_catalog.pg_class
                             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        WHERE nspname = p_schema
                          AND relname = table_name
                          AND relkind IN ('r','p')
                     )
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard partitioned application tables (only elementary partitions can be managed by E-Maj).
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'p';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are partitionned tables (only elementary partitions are supported'
                        ' by E-Maj).', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some partitionned tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
            (  SELECT unnest(p_tables)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_table);
      END IF;
    END IF;
-- Check or discard TEMP tables.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'r'
        AND relpersistence = 't';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) are TEMP tables.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some TEMP tables (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
           (  SELECT unnest(p_tables)
            EXCEPT
              SELECT unnest(v_array)
           ) AS t(remaining_table);
      END IF;
    END IF;
-- If the group is ROLLBACKABLE, perform additional checks or filters (a PK, not UNLOGGED).
    IF v_groupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_assign_tables');
    END IF;
-- Check or discard tables already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', '), array_agg(rel_tblseq) INTO v_list, v_array
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_tables)
        AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_tables: In schema %, some tables (%) already belong to a group.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_tables: Some tables already belonging to a group (%) are not selected.', v_list;
        -- remove these tables from the tables to process
        SELECT array_agg(remaining_table) INTO p_tables
          FROM
            (  SELECT unnest(p_tables)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_table);
      END IF;
    END IF;
-- Check and extract the tables JSON properties.
    IF p_properties IS NOT NULL THEN
      SELECT * INTO v_priority, v_logDatTsp, v_logIdxTsp, v_ignoredTriggers, v_ignoredTrgProfiles
        FROM emaj._check_json_table_properties(p_properties);
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(array[p_group], p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_assign_tables: No table to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
--   Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--   vacuum operation.
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
--   And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Create new log schemas if needed.
      v_logSchema = 'emaj_' || p_schema;
      IF NOT EXISTS
           (SELECT 0
              FROM emaj.emaj_schema
              WHERE sch_name = v_logSchema
           ) THEN
-- Check that the schema doesn't already exist.
        IF EXISTS
             (SELECT 0
                FROM pg_catalog.pg_namespace
                WHERE nspname = v_logSchema
             ) THEN
          RAISE EXCEPTION '_assign_tables: The schema "%" should not exist. Drop it manually.',v_logSchema;
        END IF;
-- Create the schema.
        PERFORM emaj._create_log_schema(v_logSchema, CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END);
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively create the log components for each table.
--   Build the SQL conditions to use in order to build the array of "triggers to ignore at rollback time" for each table.
      IF v_ignoredTriggers IS NOT NULL OR v_ignoredTrgProfiles IS NOT NULL THEN
--   Build the condition on trigger names using the ignored_triggers parameters.
        IF v_ignoredTriggers IS NOT NULL THEN
          v_selectConditions = 'tgname = ANY (' || quote_literal(v_ignoredTriggers) || ') OR ';
        ELSE
          v_selectConditions = '';
        END IF;
--   Build the regexp conditions on trigger names using the ignored_triggers_profile parameters.
        IF v_ignoredTrgProfiles IS NOT NULL THEN
          SELECT v_selectConditions || string_agg('tgname ~ ' || quote_literal(profile), ' OR ')
            INTO v_selectConditions
            FROM unnest(v_ignoredTrgProfiles) AS profile;
        ELSE
          v_selectConditions = v_selectConditions || 'FALSE';
        END IF;
      END IF;
-- Process each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Check that the triggers listed in ignored_triggers property exists for the table.
        SELECT string_agg(quote_ident(trigger_name), ', ') INTO v_list
          FROM
            (  SELECT trigger_name
                 FROM unnest(v_ignoredTriggers) AS trigger_name
             EXCEPT
               SELECT tgname
                 FROM pg_catalog.pg_trigger
                      JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                      JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                 WHERE nspname = p_schema
                   AND relname = v_oneTable
                   AND tgconstraint = 0
                   AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
            ) AS t;
        IF v_list IS NOT NULL THEN
          RAISE EXCEPTION '_assign_tables: some triggers (%) have not been found in the table %.%.',
                          v_list, quote_ident(p_schema), quote_ident(v_oneTable);
        END IF;
-- Build the array of "triggers to ignore at rollback time".
        IF v_selectConditions IS NOT NULL THEN
          EXECUTE format(
            $$SELECT array_agg(tgname ORDER BY tgname)
                FROM pg_catalog.pg_trigger
                     JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                     JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                WHERE nspname = %L
                  AND relname = %L
                  AND tgconstraint = 0
                  AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
                  AND (%s)
            $$, p_schema, v_oneTable, v_selectConditions)
            INTO v_selectedIgnoredTrgs;
        END IF;
-- Create the table.
        PERFORM emaj._add_tbl(p_schema, v_oneTable, p_group, v_priority, v_logDatTsp, v_logIdxTsp, v_selectedIgnoredTrgs,
                              v_groupIsLogging, v_timeId, v_function);
        v_nbAssignedTbl = v_nbAssignedTbl + 1;
      END LOOP;
-- Enable previously disabled event triggers
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the group characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (
              SELECT count(*)
                FROM emaj.emaj_relation
                WHERE rel_group = group_name
                  AND upper_inf(rel_time_range)
                  AND rel_kind = 'r'
                             )
        WHERE group_name = p_group;
-- If the group is logging, check foreign keys with tables outside the groups (otherwise the check will be done at the group start time).
      IF v_groupIsLogging THEN
        PERFORM emaj._check_fk_groups(array[p_group]);
      END IF;
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbAssignedTbl || ' tables assigned to the group ' || p_group);
--
    RETURN v_nbAssignedTbl;
  END;
$_assign_tables$;

CREATE OR REPLACE FUNCTION emaj._remove_tables(p_schema TEXT, p_tables TEXT[], p_mark TEXT, p_multiTable BOOLEAN,
                                               p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_tables$
-- The function effectively removes tables from their tables group.
-- Inputs: schema, array of table names, mark to set if for logging groups,
--         boolean to indicate whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively removed to the tables group
  DECLARE
    v_function               TEXT;
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_oneTable               TEXT;
    v_logSchema              TEXT;
    v_nbRemovedTbl           INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the tables list.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that the tables currently belong to a tables group (not necessarily the same for all tables).
      WITH all_supplied_tables AS (
        SELECT unnest(p_tables) AS table_name),
           tables_in_group AS (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range)
                              )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(table_name), ', ') INTO v_list
        FROM
          (  SELECT table_name
               FROM all_supplied_tables
           EXCEPT
             SELECT rel_tblseq FROM tables_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_remove_tables: some tables (%) do not currently belong to any tables group.', v_list;
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these tables, if any.
-- It locks the tables groups so that no other operation simultaneously occurs these groups.
    WITH tables_group AS (
      SELECT group_name, group_is_logging
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(p_tables)
          AND upper_inf(rel_time_range)
        FOR UPDATE OF emaj_group
                         )
    SELECT (SELECT array_agg(group_name)
              FROM tables_group),
           (SELECT array_agg(group_name)
              FROM tables_group
              WHERE group_is_logging)
      INTO v_groups, v_loggingGroups;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_remove_tables: No table to process.';
    ELSE
      v_logSchema = 'emaj_' || p_schema;
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the log components for each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Get some characteristics of the group that holds the table.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneTable
            AND upper_inf(rel_time_range);
-- Drop this table.
        PERFORM emaj._remove_tbl(p_schema, v_oneTable, v_groupName, v_groupIsLogging, v_timeId, v_function);
        v_nbRemovedTbl = v_nbRemovedTbl + 1;
      END LOOP;
-- Drop the log schema if it is now useless.
      IF NOT EXISTS
           (SELECT 0
              FROM emaj.emaj_relation
              WHERE rel_log_schema = v_logSchema
           ) THEN
-- Drop the schema.
        EXECUTE format('DROP SCHEMA %I',
                       v_logSchema);
-- And record the schema drop into the emaj_schema and the emaj_hist tables.
        DELETE FROM emaj.emaj_schema
          WHERE sch_name = v_logSchema;
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES (CASE WHEN p_multiTable THEN 'REMOVE_TABLES' ELSE 'REMOVE_TABLE' END, 'LOG_SCHEMA DROPPED', quote_ident(v_logSchema));
      END IF;
-- Enable previously disabled event triggers.
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table = (
              SELECT count(*)
                FROM emaj.emaj_relation
                WHERE rel_group = group_name
                  AND upper_inf(rel_time_range)
                  AND rel_kind = 'r'
                             )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbRemovedTbl || ' tables removed from their groups');
--
    RETURN v_nbRemovedTbl;
  END;
$_remove_tables$;

CREATE OR REPLACE FUNCTION emaj._move_tables(p_schema TEXT, p_tables TEXT[], p_newGroup TEXT, p_mark TEXT, p_multiTable BOOLEAN,
                                             p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_move_tables$
-- The function effectively moves tables from their tables group to another tables group.
-- Inputs: schema, array of table names, new group name, mark to set if for logging groups,
--         boolean to indicate whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively moved to the tables group
  DECLARE
    v_function               TEXT;
    v_newGroupIsRollbackable BOOLEAN;
    v_newGroupIsLogging      BOOLEAN;
    v_list                   TEXT;
    v_uselessTables          TEXT[];
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_nbAuditOnlyGroups      INT;
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_oneTable               TEXT;
    v_nbMovedTbl             INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'MOVE_TABLES' ELSE 'MOVE_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_newGroup], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_rollbackable, group_is_logging INTO v_newGroupIsRollbackable, v_newGroupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_newGroup;
-- Check the tables list.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that the tables currently belong to a tables group (not necessarily the same for all table).
      WITH all_supplied_tables AS (
        SELECT unnest(p_tables) AS table_name
        ),
           tables_in_group AS (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range)
        )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(table_name), ', ' ORDER BY table_name) INTO v_list
        FROM
          (  SELECT table_name
               FROM all_supplied_tables
           EXCEPT
             SELECT rel_tblseq
               FROM tables_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_move_tables: some tables (%) do not currently belong to any tables group.', v_list;
      END IF;
-- Remove tables that already belong to the new group.
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
        INTO v_list, v_uselessTables
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(p_tables)
          AND upper_inf(rel_time_range)
          AND rel_group = p_newGroup;
      IF v_list IS NOT NULL THEN
        RAISE WARNING '_move_tables: some tables (%) already belong to the tables group %.', v_list, p_newGroup;
        SELECT array_agg(tbl) INTO p_tables
          FROM unnest(p_tables) AS tbl
          WHERE tbl <> ALL(v_uselessTables);
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these tables, if any, and count the number of AUDIT_ONLY groups.
-- It locks the target and source tables groups so that no other operation simultaneously occurs these groups
-- (the CTE is needed for the FOR UPDATE clause not allowed when aggregate functions).
    WITH tables_group AS (
      SELECT group_name, group_is_logging, group_is_rollbackable FROM emaj.emaj_group
        WHERE group_name = p_newGroup OR
              group_name IN
               (SELECT DISTINCT rel_group FROM emaj.emaj_relation
                  WHERE rel_schema = p_schema
                    AND rel_tblseq = ANY(p_tables)
                    AND upper_inf(rel_time_range))
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging),
           count(group_name) FILTER (WHERE NOT group_is_rollbackable AND group_name <> p_newGroup)
      INTO v_groups, v_loggingGroups, v_nbAuditOnlyGroups
      FROM tables_group;
-- If at least 1 source tables group is of type AUDIT_ONLY and the target tables group is ROLLBACKABLE, add some checks on tables.
-- They may be incompatible with ROLLBACKABLE groups.
    IF v_nbAuditOnlyGroups > 0 AND v_newGroupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_move_tables');
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_move_tables: No table to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively move each table.
      FOREACH v_oneTable IN ARRAY p_tables
      LOOP
-- Get some characteristics of the group that holds the table before the move.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneTable
            AND upper_inf(rel_time_range);
-- Move this table.
        PERFORM emaj._move_tbl(p_schema, v_oneTable, v_groupName, v_groupIsLogging, p_newGroup, v_newGroupIsLogging, v_timeId, v_function);
        v_nbMovedTbl = v_nbMovedTbl + 1;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_table =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'r'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbMovedTbl || ' tables moved to the tables group ' || p_newGroup);
--
    RETURN v_nbMovedTbl;
  END;
$_move_tables$;

CREATE OR REPLACE FUNCTION emaj._modify_tables(p_schema TEXT, p_tables TEXT[], p_changedProperties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_modify_tables$
-- The function effectively modify the assignment properties of tables.
-- Inputs: schema, array of table names, properties as JSON structure
--         mark to set for logging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively modified
-- The JSONB v_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties can be set to NULL to delete a previously set value
  DECLARE
    v_function               TEXT;
    v_priorityChanged        BOOLEAN;
    v_logDatTspChanged       BOOLEAN;
    v_logIdxTspChanged       BOOLEAN;
    v_ignoredTrgChanged      BOOLEAN;
    v_newPriority            INT;
    v_newLogDatTsp           TEXT;
    v_newLogIdxTsp           TEXT;
    v_ignoredTriggers        TEXT[];
    v_ignoredTrgProfiles     TEXT[];
    v_list                   TEXT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_selectConditions       TEXT;
    v_isTableChanged         BOOLEAN;
    v_newIgnoredTriggers     TEXT[];
    v_nbChangedTbl           INT = 0;
    r_rel                    RECORD;
  BEGIN
    v_function = CASE WHEN p_multiTable THEN 'MODIFY_TABLES' ELSE 'MODIFY_TABLE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters.
-- Check tables.
    IF NOT p_arrayFromRegex THEN
-- From the tables array supplied by the user, remove duplicates values, NULL and empty strings from the supplied table names array.
      SELECT array_agg(DISTINCT table_name) INTO p_tables
        FROM unnest(p_tables) AS table_name
        WHERE table_name IS NOT NULL AND table_name <> '';
-- Check that the tables currently belong to a tables group (not necessarily the same for all tables).
      WITH all_supplied_tables AS (
        SELECT unnest(p_tables) AS table_name),
           tables_in_group AS (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range))
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(table_name), ', ') INTO v_list
        FROM
          (  SELECT table_name
               FROM all_supplied_tables
           EXCEPT
             SELECT rel_tblseq
               FROM tables_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_modify_tables: some tables (%) do not currently belong to any tables group.', v_list;
      END IF;
    END IF;
-- Determine which properties are listed in the json parameter.
    v_priorityChanged = p_changedProperties ? 'priority';
    v_logDatTspChanged = p_changedProperties ? 'log_data_tablespace';
    v_logIdxTspChanged = p_changedProperties ? 'log_index_tablespace';
    v_ignoredTrgChanged = p_changedProperties ? 'ignored_triggers' OR p_changedProperties ? 'ignored_triggers_profiles';
-- Check and extract the tables JSON properties.
    IF p_changedProperties IS NOT NULL THEN
      SELECT * INTO v_newPriority, v_newLogDatTsp, v_newLogIdxTsp, v_ignoredTriggers, v_ignoredTrgProfiles
        FROM emaj._check_json_table_properties(p_changedProperties);
    END IF;
-- Get the lists of groups and logging groups holding these tables, if any.
-- The FOR UPDATE clause locks the tables groups so that no other operation simultaneously occurs on these groups
-- (the CTE is needed for the FOR UPDATE clause not allowed when aggregate functions).
    WITH tables_group AS (
      SELECT group_name, group_is_logging
        FROM emaj.emaj_group
        WHERE group_name IN
               (SELECT DISTINCT rel_group
                  FROM emaj.emaj_relation
                  WHERE rel_schema = p_schema
                    AND rel_tblseq = ANY(p_tables)
                    AND upper_inf(rel_time_range)
               )
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging)
      INTO v_groups, v_loggingGroups
      FROM tables_group;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
-- When no tables are finaly selected, just warn.
      RAISE WARNING '_modified_tables: No table to process.';
    ELSIF p_changedProperties IS NULL OR p_changedProperties = '{}' THEN
      RAISE WARNING '_modified_tables: No property change to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
--  vacuum operation.
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- And set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Build the SQL conditions to use in order to build the array of "triggers to ignore at rollback time" for each table.
      IF v_ignoredTriggers IS NOT NULL OR v_ignoredTrgProfiles IS NOT NULL THEN
--   Build the condition on trigger names using the ignored_triggers parameters.
        IF v_ignoredTriggers IS NOT NULL THEN
          v_selectConditions = 'tgname = ANY (' || quote_literal(v_ignoredTriggers) || ') OR ';
        ELSE
          v_selectConditions = '';
        END IF;
--   Build the regexp conditions on trigger names using the ignored_triggers_profile parameters.
        IF v_ignoredTrgProfiles IS NOT NULL THEN
          SELECT v_selectConditions || string_agg('tgname ~ ' || quote_literal(profile), ' OR ')
            INTO v_selectConditions
            FROM unnest(v_ignoredTrgProfiles) AS profile;
        ELSE
          v_selectConditions = v_selectConditions || 'FALSE';
        END IF;
      END IF;
-- Process the changes for each table, if any.
      FOR r_rel IN
        SELECT rel_tblseq, rel_time_range, rel_log_schema, rel_priority, rel_log_table, rel_log_index, rel_log_dat_tsp,
               rel_log_idx_tsp, rel_ignored_triggers, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = ANY(p_tables)
            AND upper_inf(rel_time_range)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        v_isTableChanged = FALSE;
-- Change the priority, if needed.
        IF v_priorityChanged AND
            (r_rel.rel_priority <> v_newPriority
            OR (r_rel.rel_priority IS NULL AND v_newPriority IS NOT NULL)
            OR (r_rel.rel_priority IS NOT NULL AND v_newPriority IS NULL)) THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_priority_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_priority, v_newPriority,
                                            v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the log data tablespace, if needed.
        IF v_logDatTspChanged AND coalesce(v_newLogDatTsp, '') <> coalesce(r_rel.rel_log_dat_tsp, '') THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_log_data_tsp_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_table,
                                                r_rel.rel_log_dat_tsp, v_newLogDatTsp, v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the log index tablespace, if needed.
        IF v_logIdxTspChanged AND coalesce(v_newLogIdxTsp, '') <> coalesce(r_rel.rel_log_idx_tsp, '') THEN
          v_isTableChanged = TRUE;
          PERFORM emaj._change_log_index_tsp_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_log_schema, r_rel.rel_log_index,
                                                 r_rel.rel_log_idx_tsp, v_newLogIdxTsp, v_timeId, r_rel.rel_group, v_function);
        END IF;
-- Change the ignored_trigger array if needed.
        IF v_ignoredTrgChanged THEN
--   Compute the new list of "triggers to ignore at rollback time".
          IF v_selectConditions IS NOT NULL THEN
            EXECUTE format(
              $$SELECT array_agg(tgname ORDER BY tgname)
                  FROM pg_catalog.pg_trigger
                       JOIN pg_catalog.pg_class ON (tgrelid = pg_class.oid)
                       JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
                  WHERE nspname = %L
                    AND relname = %L
                    AND tgconstraint = 0
                    AND tgname NOT IN ('emaj_log_trg','emaj_trunc_trg')
                    AND (%s)
              $$, p_schema, r_rel.rel_tblseq, v_selectConditions)
              INTO v_newIgnoredTriggers;
          END IF;
          IF (r_rel.rel_ignored_triggers <> v_newIgnoredTriggers
             OR (r_rel.rel_ignored_triggers IS NULL AND v_newIgnoredTriggers IS NOT NULL)
             OR (r_rel.rel_ignored_triggers IS NOT NULL AND v_newIgnoredTriggers IS NULL)) THEN
            v_isTableChanged = TRUE;
--   If changes must be recorded, call the dedicated function.
            PERFORM emaj._change_ignored_triggers_tbl(p_schema, r_rel.rel_tblseq, r_rel.rel_ignored_triggers, v_newIgnoredTriggers,
                                                      v_timeId, r_rel.rel_group, v_function);
          END IF;
        END IF;
--
        IF v_isTableChanged THEN
          v_nbChangedTbl = v_nbChangedTbl + 1;
        END IF;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId
        WHERE group_name = ANY(v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbChangedTbl || ' tables effectively modified');
--
    RETURN v_nbChangedTbl;
  END;
$_modify_tables$;

CREATE OR REPLACE FUNCTION emaj._assign_sequences(p_schema TEXT, p_sequences TEXT[], p_group TEXT, p_mark TEXT,
                                                  p_multiSequence BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_assign_sequences$
-- The function effectively assigns sequences into a tables group.
-- Inputs: schema, array of sequence names, group name,
--         mark to set for lonnging groups, a boolean indicating whether several sequences need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of sequences effectively assigned to the tables group
-- The JSONB v_properties parameter has currenlty only one field '{"priority":...}' the properties being NULL by default
  DECLARE
    v_function               TEXT;
    v_groupIsLogging         BOOLEAN;
    v_list                   TEXT;
    v_array                  TEXT[];
    v_timeId                 BIGINT;
    v_markName               TEXT;
    v_oneSequence            TEXT;
    v_nbAssignedSeq          INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'ASSIGN_SEQUENCES' ELSE 'ASSIGN_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check supplied parameters
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_group], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_group;
-- Check the supplied schema exists and is not an E-Maj schema.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" does not exist.', p_schema;
    END IF;
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_schema
            WHERE sch_name = p_schema
         ) THEN
      RAISE EXCEPTION '_assign_sequences: The schema "%" is an E-Maj schema.', p_schema;
    END IF;
-- Check sequences.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the sequence names array supplied by the user.
      SELECT array_agg(DISTINCT sequence_name) INTO p_sequences
        FROM unnest(p_sequences) AS sequence_name
        WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- Check that application sequences exist.
      WITH sequences AS (
        SELECT unnest(p_sequences) AS sequence_name)
      SELECT string_agg(quote_ident(sequence_name), ', ') INTO v_list
        FROM
          (SELECT sequence_name
             FROM sequences
             WHERE NOT EXISTS
                    (SELECT 0
                       FROM pg_catalog.pg_class
                            JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                       WHERE nspname = p_schema
                         AND relname = sequence_name
                         AND relkind = 'S')
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_assign_sequences: In schema %, some sequences (%) do not exist.', quote_ident(p_schema), v_list;
      END IF;
    END IF;
-- Check or discard sequences already assigned to a group.
    SELECT string_agg(quote_ident(rel_tblseq), ', '), array_agg(rel_tblseq) INTO v_list, v_array
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_sequences)
        AND upper_inf(rel_time_range);
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '_assign_sequences: In schema %, some sequences (%) already belong to a group.', quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '_assign_sequences: Some sequences already belonging to a group (%) are not selected.', v_list;
        -- remove these sequences from the sequences to process
        SELECT array_agg(remaining_sequence) INTO p_sequences
          FROM
            (  SELECT unnest(p_sequences)
             EXCEPT
               SELECT unnest(v_array)
            ) AS t(remaining_sequence);
      END IF;
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(array[p_group], p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL OR p_sequences = '{}' THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_assign_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_groupIsLogging THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(ARRAY[p_group], 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively create the log components for each table.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
        PERFORM emaj._add_seq(p_schema, v_oneSequence, p_group, v_groupIsLogging, v_timeId, v_function);
        v_nbAssignedSeq = v_nbAssignedSeq + 1;
      END LOOP;
-- Adjust the group characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = p_group;
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbAssignedSeq || ' sequences assigned to the group ' || p_group);
--
    RETURN v_nbAssignedSeq;
  END;
$_assign_sequences$;

CREATE OR REPLACE FUNCTION emaj._remove_sequences(p_schema TEXT, p_sequences TEXT[], p_mark TEXT, p_multiSequence BOOLEAN,
                                                  p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_remove_sequences$
-- The function effectively removes sequences from their sequences group.
-- Inputs: schema, array of sequence names, mark to set if for logging groups,
--         a boolean to indicate whether several sequences need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of sequences effectively assigned to the sequences group
  DECLARE
    v_function               TEXT;
    v_list                   TEXT;
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_eventTriggers          TEXT[];
    v_oneSequence            TEXT;
    v_nbRemovedSeq           INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'REMOVE_SEQUENCES' ELSE 'REMOVE_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the sequences array.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the supplied sequence names array.
      SELECT array_agg(DISTINCT sequence_name) INTO p_sequences
        FROM unnest(p_sequences) AS sequence_name
        WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- Check that the sequences currently belong to a tables group (not necessarily the same one).
      WITH all_supplied_sequences AS
        (SELECT unnest(p_sequences) AS sequence_name
        ),
           sequences_in_group AS
        (SELECT rel_tblseq
           FROM emaj.emaj_relation
           WHERE rel_schema = p_schema
             AND rel_tblseq = ANY(p_sequences)
             AND upper_inf(rel_time_range)
        )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(sequence_name), ', ') INTO v_list
        FROM
          (  SELECT sequence_name
               FROM all_supplied_sequences
           EXCEPT
             SELECT rel_tblseq
               FROM sequences_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_remove_sequences: some sequences (%) do not currently belong to any tables group.', v_list;
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these sequences, if any.
-- It locks the tables groups so that no other operation simultaneously occurs these groups.
    WITH tables_group AS
      (SELECT group_name, group_is_logging
         FROM emaj.emaj_relation
              JOIN emaj.emaj_group ON (group_name = rel_group)
         WHERE rel_schema = p_schema
           AND rel_tblseq = ANY(p_sequences)
           AND upper_inf(rel_time_range)
         FOR UPDATE OF emaj_group
      )
    SELECT (SELECT array_agg(group_name)
              FROM tables_group
           ),
           (SELECT array_agg(group_name)
              FROM tables_group
              WHERE group_is_logging
           )
      INTO v_groups, v_loggingGroups;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_remove_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, FALSE, TRUE, NULL, v_timeId);
      END IF;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
      SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the log components for each sequence.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
-- Get some characteristics of the group that holds the sequence.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneSequence
            AND upper_inf(rel_time_range);
-- Drop this sequence from its group.
        PERFORM emaj._remove_seq(p_schema, v_oneSequence, v_groupName, v_groupIsLogging, v_timeId, v_function);
        v_nbRemovedSeq = v_nbRemovedSeq + 1;
      END LOOP;
-- Enable previously disabled event triggers.
      PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbRemovedSeq || ' sequences removed from their groups');
--
    RETURN v_nbRemovedSeq;
  END;
$_remove_sequences$;

CREATE OR REPLACE FUNCTION emaj._move_sequences(p_schema TEXT, p_sequences TEXT[], p_newGroup TEXT, p_mark TEXT, p_multiSequence BOOLEAN,
                                             p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql AS
$_move_sequences$
-- The function effectively moves sequences from their tables group to another tables group.
-- Inputs: schema, array of sequence names, new group name, mark to set if for logging groups,
--         boolean to indicate whether several sequences need to be processed,
--         a boolean indicating whether the sequences array has been built from regex filters
-- Outputs: number of sequences effectively moved to the tables group
  DECLARE
    v_function               TEXT;
    v_newGroupIsLogging      BOOLEAN;
    v_list                   TEXT;
    v_uselessSequences       TEXT[];
    v_markName               TEXT;
    v_timeId                 BIGINT;
    v_groups                 TEXT[];
    v_loggingGroups          TEXT[];
    v_groupName              TEXT;
    v_groupIsLogging         BOOLEAN;
    v_oneSequence            TEXT;
    v_nbMovedSeq             INT = 0;
  BEGIN
    v_function = CASE WHEN p_multiSequence THEN 'MOVE_SEQUENCES' ELSE 'MOVE_SEQUENCE' END;
-- Insert the begin entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES (v_function, 'BEGIN');
-- Check the group name and if ok, get some properties of the group.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_newGroup], p_mayBeNull := FALSE, p_lockGroups := TRUE);
    SELECT group_is_logging INTO v_newGroupIsLogging
      FROM emaj.emaj_group
      WHERE group_name = p_newGroup;
-- Check the sequences list.
    IF NOT p_arrayFromRegex THEN
-- Remove duplicates values, NULL and empty strings from the supplied sequence names array.
      SELECT array_agg(DISTINCT sequence_name) INTO p_sequences
        FROM unnest(p_sequences) AS sequence_name
        WHERE sequence_name IS NOT NULL AND sequence_name <> '';
-- Check that the sequences currently belong to a tables group (not necessarily the same for all sequences).
      WITH all_supplied_sequences AS
        (SELECT unnest(p_sequences) AS sequence_name
        ),
           sequences_in_group AS
        (SELECT rel_tblseq
           FROM emaj.emaj_relation
           WHERE rel_schema = p_schema
             AND rel_tblseq = ANY(p_sequences)
             AND upper_inf(rel_time_range)
        )
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(sequence_name), ', ' ORDER BY sequence_name) INTO v_list
        FROM
          (  SELECT sequence_name
               FROM all_supplied_sequences
           EXCEPT
             SELECT rel_tblseq
               FROM sequences_in_group
          ) AS t;
      IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_move_sequences: some sequences (%) do not currently belong to any tables group.', v_list;
      END IF;
-- Remove sequences that already belong to the new group.
      SELECT string_agg(quote_ident(p_schema) || '.' || quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq), array_agg(rel_tblseq)
        INTO v_list, v_uselessSequences
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(p_sequences)
          AND upper_inf(rel_time_range)
          AND rel_group = p_newGroup;
      IF v_list IS NOT NULL THEN
        RAISE WARNING '_move_sequences: some sequences (%) already belong to the tables group %.', v_list, p_newGroup;
        SELECT array_agg(seq) INTO p_sequences
          FROM unnest(p_sequences) AS seq
          WHERE seq <> ALL(v_uselessSequences);
      END IF;
    END IF;
-- Get the lists of groups and logging groups holding these sequences, if any.
-- It locks the tables groups so that no other operation simultaneously occurs these groups
-- (the CTE is needed for the FOR UPDATE clause not allowed when aggregate functions).
    WITH tables_group AS
      (SELECT group_name, group_is_logging
         FROM emaj.emaj_group
         WHERE group_name = p_newGroup
            OR group_name IN
                 (SELECT DISTINCT rel_group
                    FROM emaj.emaj_relation
                    WHERE rel_schema = p_schema
                      AND rel_tblseq = ANY(p_sequences)
                      AND upper_inf(rel_time_range)
                 )
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging)
      INTO v_groups, v_loggingGroups
      FROM tables_group;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_sequences IS NULL THEN
-- When no sequences are finaly selected, just warn.
      RAISE WARNING '_move_sequences: No sequence to process.';
    ELSE
-- Get the time stamp of the operation.
      SELECT emaj._set_time_stamp(v_function, 'A') INTO v_timeId;
-- For LOGGING groups, lock all tables to get a stable point.
      IF v_loggingGroups IS NOT NULL THEN
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation,
        PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', FALSE);
-- ... and set the mark, using the same time identifier.
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
      END IF;
-- Effectively move each sequence.
      FOREACH v_oneSequence IN ARRAY p_sequences
      LOOP
-- Get some characteristics of the group that holds the sequence before the move.
        SELECT rel_group, group_is_logging INTO v_groupName, v_groupIsLogging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_schema = p_schema
            AND rel_tblseq = v_oneSequence
            AND upper_inf(rel_time_range);
-- Move this sequence.
        PERFORM emaj._move_seq(p_schema, v_oneSequence, v_groupName, v_groupIsLogging, p_newGroup, v_newGroupIsLogging, v_timeId,
                               v_function);
        v_nbMovedSeq = v_nbMovedSeq + 1;
      END LOOP;
-- Adjust the groups characteristics.
      UPDATE emaj.emaj_group
        SET group_last_alter_time_id = v_timeId,
            group_nb_sequence =
              (SELECT count(*)
                 FROM emaj.emaj_relation
                 WHERE rel_group = group_name
                   AND upper_inf(rel_time_range)
                   AND rel_kind = 'S'
              )
        WHERE group_name = ANY (v_groups);
    END IF;
-- Insert the end entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbMovedSeq || ' sequences moved to the tables group ' || p_newGroup);
--
    RETURN v_nbMovedSeq;
  END;
$_move_sequences$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(p_groupName TEXT, p_isRollbackable BOOLEAN DEFAULT TRUE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates a group, for the moment empty.
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
-- Output: 1 = number of created groups
  DECLARE
    v_function               TEXT = 'CREATE_GROUP';
    v_timeId                 BIGINT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'BEGIN', p_groupName, CASE WHEN p_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- Check that the group name is valid.
    IF p_groupName IS NULL OR p_groupName = '' THEN
      RAISE EXCEPTION 'emaj_create_group: The group name can''t be NULL or empty.';
    END IF;
-- Check that the group is not yet recorded in emaj_group table
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_group
            WHERE group_name = p_groupName
         ) THEN
      RAISE EXCEPTION 'emaj_create_group: The group "%" already exists.', p_groupName;
    END IF;
-- OK
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'C') INTO v_timeId;
-- Insert the row describing the group into the emaj_group and emaj_group_hist tables
-- (The group_is_rlbk_protected boolean column is always initialized as not group_is_rollbackable).
    INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_is_logging,
                                 group_is_rlbk_protected, group_nb_table, group_nb_sequence)
      VALUES (p_groupName, p_isRollbackable, FALSE, NOT p_isRollbackable, 0, 0);
    INSERT INTO emaj.emaj_group_hist (grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
      VALUES (p_groupName, int8range(v_timeId, NULL, '[]'), p_isRollbackable, 0);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'END', p_groupName);
    RETURN 1;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_drop_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT = 'DROP_GROUP';
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE, p_checkIdle := TRUE);
-- Effectively drop the group.
    SELECT emaj._drop_group(p_groupName, FALSE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_drop_group$;
COMMENT ON FUNCTION emaj.emaj_drop_group(TEXT) IS
$$Drops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_force_drop_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_force_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- It differs from emaj_drop_group by the fact that:
--   - the group may be in LOGGING state,
--   - a missing component in the drop processing does not generate any error.
-- This allows to drop a group that is not consistent, following hasardeous operations.
-- This function should not be used, except if the emaj_drop_group fails.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT = 'FORCE_DROP_GROUP';
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Effectively drop the group.
    SELECT emaj._drop_group(p_groupName, TRUE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_force_drop_group$;
COMMENT ON FUNCTION emaj.emaj_force_drop_group(TEXT) IS
$$Drops an E-Maj group, even in LOGGING state.$$;

CREATE OR REPLACE FUNCTION emaj._drop_group(p_groupName TEXT, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group.
-- It also drops log schemas that are not useful any more.
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT;
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbRel                  INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
    v_function = CASE WHEN p_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END;
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'D') INTO v_timeId;
-- Register into emaj_relation_change the tables and sequences removal from their group, for completeness.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      SELECT v_timeId, rel_schema, rel_tblseq,
             CASE WHEN rel_kind = 'r' THEN 'REMOVE_TABLE'::emaj._relation_change_kind_enum
                                      ELSE 'REMOVE_SEQUENCE'::emaj._relation_change_kind_enum END,
             p_groupName
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND upper_inf(rel_time_range)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Delete the emaj objects and references for each table and sequences of the group.
    FOR r_rel IN
      SELECT *
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
    LOOP
      PERFORM CASE r_rel.rel_kind
                WHEN 'r' THEN emaj._drop_tbl(r_rel, v_timeId)
                WHEN 'S' THEN emaj._drop_seq(r_rel, v_timeId)
              END;
    END LOOP;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any other created group).
    PERFORM emaj._drop_log_schemas(v_function, p_isForced);
-- Delete group row from the emaj_group table.
-- By cascade, it also deletes rows from emaj_mark.
    DELETE FROM emaj.emaj_group
      WHERE group_name = p_groupName
      RETURNING group_nb_table + group_nb_sequence INTO v_nbRel;
-- Update the last log session for the group to set the time range upper bound
    UPDATE emaj.emaj_log_session
      SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[]')
      WHERE lses_group = p_groupName
        AND upper_inf(lses_time_range);
-- Update the last group history row to set the time range upper bound
    UPDATE emaj.emaj_group_hist
      SET grph_time_range = int8range(lower(grph_time_range), v_timeId, '[]')
      WHERE grph_group = p_groupName
        AND upper_inf(grph_time_range);
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
--
    RETURN v_nbRel;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_forget_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_forget_group$
-- This function deletes all traces of a dropped group from emaj_group_hist and emaj_log_sessions tables.
-- Input: group name
-- Output: number of deleted rows
  DECLARE
    v_nbDeletedSession       INT = 0;
    v_nbDeletedHistory       INT = 0;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('FORGET_GROUP', 'BEGIN', p_groupName);
-- Check that the group is not recorded in emaj_group table anymore
    IF EXISTS
         (SELECT 0
            FROM emaj.emaj_group
            WHERE group_name = p_groupName
         ) THEN
      RAISE EXCEPTION 'emaj_forget_group: The group "%" still exists.', p_groupName;
    END IF;
-- OK
-- Delete rows from emaj_log_session.
    DELETE FROM emaj.emaj_log_session
      WHERE lses_group = p_groupName;
    GET DIAGNOSTICS v_nbDeletedSession = ROW_COUNT;
-- Delete rows from emaj_group_hist.
    DELETE FROM emaj.emaj_group_hist
      WHERE grph_group = p_groupName;
    GET DIAGNOSTICS v_nbDeletedHistory = ROW_COUNT;
-- Warn if the group has not been found in any history table.
    IF v_nbDeletedSession + v_nbDeletedHistory = 0 THEN
      RAISE WARNING 'emaj_forget_group: the tables group "%" has not been found in history tables', p_groupName;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('FORGET_GROUP', 'END', p_groupName,
              v_nbDeletedSession || ' rows deleted from emaj_log_session and ' ||
              v_nbDeletedHistory || ' rows deleted from emaj_group_hist');
--
    RETURN v_nbDeletedSession + v_nbDeletedHistory;
  END;
$emaj_forget_group$;
COMMENT ON FUNCTION emaj.emaj_forget_group(TEXT) IS
$$Removes traces of a dropped group from histories.$$;

CREATE OR REPLACE FUNCTION emaj._export_groups_conf(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$_export_groups_conf$
-- This function generates a JSON formatted structure representing the current configuration of some or all tables groups.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the tables groups configuration in JSON format
  DECLARE
    v_groupsText             TEXT;
    v_unknownGroupsList      TEXT;
    v_groupsJson             JSON;
    r_group                  RECORD;
    r_table                  RECORD;
    r_sequence               RECORD;
  BEGIN
-- Build the header of the JSON structure.
    v_groupsText = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || current_timestamp || E'",\n';
-- Check the group names array, if supplied. All the listed groups must exist.
    IF p_groups IS NOT NULL THEN
      SELECT string_agg(group_name, ', ') INTO v_unknownGroupsList
        FROM
          (SELECT *
             FROM unnest(p_groups) AS grp(group_name)
             WHERE NOT EXISTS
                    (SELECT group_name
                       FROM emaj.emaj_group
                       WHERE emaj_group.group_name = grp.group_name
                    )
          ) AS t;
      IF v_unknownGroupsList IS NOT NULL THEN
        RAISE EXCEPTION '_export_groups_conf: The tables groups % are unknown.', v_unknownGroupsList;
      END IF;
    END IF;
-- Build the tables groups description.
    v_groupsText = v_groupsText
                || E'  "tables_groups": [\n';
    FOR r_group IN
      SELECT group_name, group_is_rollbackable, group_comment, group_nb_table, group_nb_sequence
        FROM emaj.emaj_group
        WHERE (p_groups IS NULL OR group_name = ANY(p_groups))
        ORDER BY group_name
    LOOP
      v_groupsText = v_groupsText
                  || E'    {\n'
                  ||  '      "group": ' || to_json(r_group.group_name) || E',\n'
                  ||  '      "is_rollbackable": ' || to_json(r_group.group_is_rollbackable) || E',\n';
      IF r_group.group_comment IS NOT NULL THEN
        v_groupsText = v_groupsText
                  ||  '      "comment": ' || to_json(r_group.group_comment) || E',\n';
      END IF;
      IF r_group.group_nb_table > 0 THEN
-- Build the tables list, if any.
        v_groupsText = v_groupsText
                    || E'      "tables": [\n';
        FOR r_table IN
          SELECT rel_schema, rel_tblseq, rel_priority, rel_log_dat_tsp, rel_log_idx_tsp, rel_ignored_triggers
            FROM emaj.emaj_relation
            WHERE rel_kind = 'r'
              AND upper_inf(rel_time_range)
              AND rel_group = r_group.group_name
            ORDER BY rel_schema, rel_tblseq
        LOOP
          v_groupsText = v_groupsText
                      || E'        {\n'
                      ||  '          "schema": ' || to_json(r_table.rel_schema) || E',\n'
                      ||  '          "table": ' || to_json(r_table.rel_tblseq) || E',\n'
                      || coalesce('          "priority": '|| to_json(r_table.rel_priority) || E',\n', '')
                      || coalesce('          "log_data_tablespace": '|| to_json(r_table.rel_log_dat_tsp) || E',\n', '')
                      || coalesce('          "log_index_tablespace": '|| to_json(r_table.rel_log_idx_tsp) || E',\n', '')
                      || coalesce('          "ignored_triggers": ' || array_to_json(r_table.rel_ignored_triggers) || E',\n', '')
                      || E'        },\n';
        END LOOP;
        v_groupsText = v_groupsText
                    || E'      ],\n';
      END IF;
      IF r_group.group_nb_sequence > 0 THEN
-- Build the sequences list, if any.
        v_groupsText = v_groupsText
                    || E'      "sequences": [\n';
        FOR r_sequence IN
          SELECT rel_schema, rel_tblseq
            FROM emaj.emaj_relation
            WHERE rel_kind = 'S'
              AND upper_inf(rel_time_range)
              AND rel_group = r_group.group_name
            ORDER BY rel_schema, rel_tblseq
        LOOP
          v_groupsText = v_groupsText
                      || E'        {\n'
                      ||  '          "schema": ' || to_json(r_sequence.rel_schema) || E',\n'
                      ||  '          "sequence": ' || to_json(r_sequence.rel_tblseq) || E',\n'
                      || E'        },\n';
        END LOOP;
        v_groupsText = v_groupsText
                    || E'      ],\n';
      END IF;
      v_groupsText = v_groupsText
                  || E'    },\n';
    END LOOP;
    v_groupsText = v_groupsText
                || E'  ]\n';
-- Build the trailer and remove illicite commas at the end of arrays and attributes lists.
    v_groupsText = v_groupsText
                || E'}\n';
    v_groupsText = regexp_replace(v_groupsText, E',(\n *(\]|}))', '\1', 'g');
-- Test the JSON format by casting the text structure to json and report a warning in case of problem
-- (this should not fail, unless the function code is bogus).
    BEGIN
      v_groupsJson = v_groupsText::JSON;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '_export_groups_conf: The generated JSON structure is not properly formatted. '
                        'Please report the bug to the E-Maj project.';
    END;
--
    RETURN v_groupsJson;
  END;
$_export_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_exec(p_json JSON, p_groups TEXT[], p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf_exec$
-- This function completes a tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- Non existing groups are created empty.
-- The _import_groups_conf_alter() function is used to process the assignement, the move, the removal or the attributes change for tables
-- and sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process
--        - a boolean indicating whether tables groups to import may already exist
--        - the mark name to set for tables groups in logging state
-- Output: the number of created or altered tables groups
  DECLARE
    v_function               TEXT = 'IMPORT_GROUPS';
    v_timeId                 BIGINT;
    v_groupsJson             JSON;
    v_nbGroup                INT;
    v_comment                TEXT;
    v_isRollbackable         BOOLEAN;
    v_loggingGroups          TEXT[];
    v_markName               TEXT;
    r_group                  RECORD;
  BEGIN
-- Get a time stamp id of type 'I' for the operation.
    SELECT emaj._set_time_stamp(v_function, 'I') INTO v_timeId;
-- Extract the "tables_groups" json path.
    v_groupsJson = p_json #> '{"tables_groups"}';
-- In a third pass over the JSON structure:
--   - create empty groups for those which does not exist yet,
--   - adjust the comment on the groups, if needed.
    v_nbGroup = 0;
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(v_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
      v_nbGroup = v_nbGroup + 1;
-- Create the tables group if it does not exist yet.
      SELECT group_comment INTO v_comment
        FROM emaj.emaj_group
        WHERE group_name = r_group.groupJson ->> 'group';
      IF NOT FOUND THEN
        v_isRollbackable = coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE);
        INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable,
                                     group_is_logging, group_is_rlbk_protected, group_nb_table, group_nb_sequence, group_comment)
          VALUES (r_group.groupJson ->> 'group', v_isRollbackable,
                                     FALSE, NOT v_isRollbackable, 0, 0, r_group.groupJson ->> 'comment');
        INSERT INTO emaj.emaj_group_hist (grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
          VALUES (r_group.groupJson ->> 'group', int8range(v_timeId, NULL, '[]'), v_isRollbackable, 0);
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
          VALUES (v_function, 'GROUP CREATED', r_group.groupJson ->> 'group',
                  CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
      ELSE
-- If the group exists, adjust the comment if needed.
        IF coalesce(v_comment, '') <> coalesce(r_group.groupJson ->> 'comment', '') THEN
          UPDATE emaj.emaj_group
            SET group_comment = r_group.groupJson ->> 'comment'
            WHERE group_name = r_group.groupJson ->> 'group';
        END IF;
      END IF;
    END LOOP;
-- Lock the group names to avoid concurrent operation on these groups.
    PERFORM 0
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groups)
      FOR UPDATE;
-- Build the list of groups that are in logging state.
    SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groups)
        AND group_is_logging;
-- If some groups are in logging state, check and set the supplied mark name and lock the groups.
    IF v_loggingGroups IS NOT NULL THEN
      SELECT emaj._check_new_mark(p_groups, p_mark) INTO v_markName;
-- Lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
      PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', TRUE);
-- And set the mark, using the same time identifier.
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, TRUE, TRUE, NULL, v_timeId);
    END IF;
-- Process the tmp_app_table and tmp_app_sequence content change.
    PERFORM emaj._import_groups_conf_alter(p_groups, p_mark, v_timeId);
-- Check foreign keys with tables outside the groups in logging state.
    PERFORM emaj._check_fk_groups(v_loggingGroups);
-- The temporary tables are not needed anymore. So drop them.
    DROP TABLE tmp_app_table;
    DROP TABLE tmp_app_sequence;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbGroup || ' created or altered tables groups');
--
    RETURN v_nbGroup;
  END;
$_import_groups_conf_exec$;

CREATE OR REPLACE FUNCTION emaj._start_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark.
-- It also delete oldest rows in emaj_hist table.
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function,
--        boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and
-- sequences.
  DECLARE
    v_function               TEXT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_eventTriggers          TEXT[];
    r_tblsq                  RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','),
              CASE WHEN p_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE, p_checkIdle := TRUE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- If there is at least 1 group to process, go on.
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Get a time stamp id of type 'S' for the operation.
      SELECT emaj._set_time_stamp(v_function, 'S') INTO v_timeId;
-- Check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(p_groupNames);
-- If requested by the user, call the emaj_reset_groups() function to erase remaining traces from previous logs.
      IF p_resetLog THEN
        PERFORM emaj._reset_groups(p_groupNames);
-- Drop the log schemas that would have been emptied by the _reset_groups() call.
        SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
        PERFORM emaj._drop_log_schemas(CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, FALSE);
        PERFORM emaj._enable_event_triggers(v_eventTriggers);
      END IF;
-- Check the supplied mark name (the check must be performed after the _reset_groups() call to allow to reuse an old mark name that is
-- being deleted.
      IF p_mark IS NULL OR p_mark = '' THEN
        p_mark = 'START_%';
      END IF;
      SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
--   one sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
--   risk of deadlock.
      PERFORM emaj._lock_groups(p_groupNames,'SHARE ROW EXCLUSIVE',p_multiGroup);
-- Enable all log triggers for the groups.
-- For each relation currently belonging to the groups,
      FOR r_tblsq IN
        SELECT rel_kind, quote_ident(rel_schema) || '.' || quote_ident(rel_tblseq) AS full_relation_name
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, enable the emaj log and truncate triggers.
          PERFORM emaj._handle_trigger_fk_tbl('ENABLE_TRIGGER', r_tblsq.full_relation_name, 'emaj_log_trg', 'ALWAYS');
          PERFORM emaj._handle_trigger_fk_tbl('ENABLE_TRIGGER', r_tblsq.full_relation_name, 'emaj_trunc_trg', 'ALWAYS');
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
-- Update the state of the group row from the emaj_group table.
      UPDATE emaj.emaj_group
        SET group_is_logging = TRUE
        WHERE group_name = ANY (p_groupNames);
-- Insert log sessions start into emaj_log_session...
--   lses_marks is already set to 1 as it will not be incremented at the first mark set.
      INSERT INTO emaj.emaj_log_session
        SELECT group_name, int8range(v_timeId, NULL, '[]'), 1, 0
          FROM emaj.emaj_group
          WHERE group_name = ANY (p_groupNames);
-- ... and update the last group history row to increment the number of log sessions
      UPDATE emaj.emaj_group_hist
        SET grph_log_sessions = grph_log_sessions + 1
        WHERE grph_group = ANY (p_groupNames)
          AND upper_inf(grph_time_range);
-- Set the first mark for each group.
      PERFORM emaj._set_mark_groups(p_groupNames, v_markName, p_multiGroup, TRUE, NULL, v_timeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT;
    v_groupList              TEXT;
    v_count                  INT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    v_group                  TEXT;
    v_lsesTimeRange          INT8RANGE;
    r_schema                 RECORD;
    r_tblsq                  RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','));
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := TRUE)
      INTO p_groupNames;
-- For all already IDLE groups, generate a warning message and remove them from the list of the groups to process.
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*)
      INTO v_groupList, v_count
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
       AND NOT group_is_logging;
    IF v_count = 1 THEN
      RAISE WARNING '_stop_groups: The group "%" is already in IDLE state.', v_groupList;
    END IF;
    IF v_count > 1 THEN
      RAISE WARNING '_stop_groups: The groups "%" are already in IDLE state.', v_groupList;
    END IF;
-- Process the LOGGING groups.
    SELECT array_agg(DISTINCT group_name)
      INTO p_groupNames
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
        AND group_is_logging;
    IF p_groupNames IS NOT NULL THEN
-- Check and process the supplied mark name (except if the function is called by emaj_force_stop_group()).
      IF NOT p_isForced THEN
        IF p_mark IS NULL OR p_mark = '' THEN
          p_mark = 'STOP_%';
        END IF;
        SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
      END IF;
-- OK (no error detected and at least one group in logging state)
-- Get a time stamp id of type 'X' for the operation.
      SELECT emaj._set_time_stamp(v_function, 'X') INTO v_timeId;
-- Lock all tables to get a stable point.
-- One sets the locks at the beginning of the operation (rather than let the ALTER TABLE statements set their own locks) to decrease the
-- risk of deadlock.
      PERFORM emaj._lock_groups(p_groupNames,'SHARE ROW EXCLUSIVE',p_multiGroup);
-- Verify that all application schemas for the groups still exists.
      FOR r_schema IN
        SELECT DISTINCT rel_schema
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND NOT EXISTS
                 (SELECT nspname
                    FROM pg_catalog.pg_namespace
                    WHERE nspname = rel_schema
                 )
          ORDER BY rel_schema
      LOOP
        IF p_isForced THEN
          RAISE WARNING '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist any more.', r_schema.rel_schema;
        END IF;
      END LOOP;
-- For each relation currently belonging to the groups to process...
      FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- If it is a table, check the table still exists,
          IF NOT EXISTS
               (SELECT 0
                  FROM pg_catalog.pg_class
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                  WHERE nspname = r_tblsq.rel_schema
                    AND relname = r_tblsq.rel_tblseq
               ) THEN
            IF p_isForced THEN
              RAISE WARNING '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            ELSE
              RAISE EXCEPTION '_stop_groups: The table "%.%" does not exist any more.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            END IF;
          ELSE
-- ... and disable the emaj log and truncate triggers.
-- Errors are captured so that emaj_force_stop_group() can be silently executed.
            v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
            BEGIN
              PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_log_trg');
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
            BEGIN
              PERFORM emaj._handle_trigger_fk_tbl('DISABLE_TRIGGER', v_fullTableName, 'emaj_trunc_trg');
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist any more.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
          END IF;
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
      IF NOT p_isForced THEN
-- If the function is not called by emaj_force_stop_group(), set the stop mark for each group,
        PERFORM emaj._set_mark_groups(p_groupNames, v_markName, p_multiGroup, TRUE, NULL, v_timeId);
-- and set the number of log rows to 0 for these marks.
        UPDATE emaj.emaj_mark m
          SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (p_groupNames)
            AND mark_time_id = v_timeId;
      END IF;
-- Process each tables group separately to ...
      FOREACH v_group IN ARRAY p_groupNames
      LOOP
-- Get the latest log session of the tables group.
        SELECT lses_time_range
          INTO v_lsesTimeRange
          FROM emaj.emaj_log_session
          WHERE lses_group = v_group
          ORDER BY lses_time_range DESC
          LIMIT 1;
-- Set all marks as 'DELETED' to avoid any further rollback and remove marks protection against rollback, if any.
        UPDATE emaj.emaj_mark
          SET mark_is_rlbk_protected = FALSE
          WHERE mark_group = v_group
            AND mark_time_id >= lower(v_lsesTimeRange);
-- Update the log session to set the time range upper bound
        UPDATE emaj.emaj_log_session
          SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[]')
          WHERE lses_group = v_group
            AND lses_time_range = v_lsesTimeRange;
      END LOOP;
-- Update the emaj_group table to set the groups state and the rollback protections.
      UPDATE emaj.emaj_group
        SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (p_groupNames);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_eventToRecord BOOLEAN,
                                                 p_loggedRlbkTargetMark TEXT DEFAULT NULL, p_timeId BIGINT DEFAULT NULL,
                                                 p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates 1) the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into
-- all log tables between this previous mark and the new mark and 2) the current log session.
-- The function is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks,
-- like functions that start, stop or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
--        name of the rollback target mark when this mark is created by the logged_rollback functions (NULL by default)
--        time stamp identifier to reuse (NULL by default) (this parameter is set when the mark is a rollback start mark)
--        dblink schema when the mark is set by a rollback operation and dblink connection are used (NULL by default)
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_function               TEXT;
    v_nbSeq                  INT;
    v_group                  TEXT;
    v_lsesTimeRange          INT8RANGE;
    v_latestMarkTimeId       BIGINT;
    v_nbChanges              BIGINT;
    v_nbTbl                  INT;
    v_stmt                   TEXT;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END;
-- If requested by the calling function, record the set mark begin in emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (v_function, 'BEGIN', array_to_string(p_groupNames,','), p_mark);
    END IF;
-- Get the time stamp of the operation, if not supplied as input parameter.
    IF p_timeId IS NULL THEN
      SELECT emaj._set_time_stamp(v_function, 'M') INTO p_timeId;
    END IF;
-- Record sequences state as early as possible (no lock protects them from other transactions activity).
-- The join on pg_namespace and pg_class filters the potentially dropped application sequences.
    WITH seq AS                          -- selected sequences
      (SELECT rel_schema, rel_tblseq
         FROM emaj.emaj_relation
              JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
              JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'S'
           AND rel_group = ANY (p_groupNames)
      )
    INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,
                sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)
      SELECT t.*
        FROM seq,
             LATERAL emaj._get_current_sequence_state(rel_schema, rel_tblseq, p_timeId) AS t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- Record the number of log rows for the previous last mark of each selected group.
    FOREACH v_group IN ARRAY p_groupNames
    LOOP
-- Get the latest log session of the tables group.
      SELECT lses_time_range
        INTO v_lsesTimeRange
        FROM emaj.emaj_log_session
        WHERE lses_group = v_group
        ORDER BY lses_time_range DESC
        LIMIT 1;
      IF p_timeId > lower(v_lsesTimeRange) OR lower(v_lsesTimeRange) IS NULL THEN
-- This condition excludes marks set at start_group time, for which there is nothing to do.
--   The lower bound may be null when the log session has been created by the emaj version upgrade processing and the last start_group
--   call has not been found into the history.
-- Get the latest mark for the tables group.
        SELECT mark_time_id
          INTO v_latestMarkTimeId
          FROM emaj.emaj_mark
          WHERE mark_group = v_group
          ORDER BY mark_time_id DESC
          LIMIT 1;
-- Compute the number of changes for tables since this latest mark
        SELECT coalesce(sum(emaj._log_stat_tbl(emaj_relation, greatest(v_latestMarkTimeId, lower(rel_time_range)),NULL)), 0)
          INTO v_nbChanges
          FROM emaj.emaj_relation
          WHERE rel_group = v_group
            AND rel_kind = 'r'
            AND upper_inf(rel_time_range);
-- Update the latest mark statistics.
        UPDATE emaj.emaj_mark
          SET mark_log_rows_before_next = v_nbChanges
          WHERE mark_group = v_group
            AND mark_time_id = v_latestMarkTimeId;
-- Update the current log session statistics.
        UPDATE emaj.emaj_log_session
          SET lses_marks = lses_marks + 1,
              lses_log_rows = lses_log_rows + v_nbChanges
          WHERE lses_group = v_group
            AND lses_time_range = v_lsesTimeRange;
      END IF;
    END LOOP;
-- For tables currently belonging to the groups, record their state and their log sequence last_value.
    INSERT INTO emaj.emaj_table (tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val)
      SELECT rel_schema, rel_tblseq, p_timeId, reltuples, relpages, last_value
        FROM emaj.emaj_relation
             LEFT OUTER JOIN pg_catalog.pg_namespace ON (nspname = rel_schema)
             LEFT OUTER JOIN pg_catalog.pg_class ON (relname = rel_tblseq AND relnamespace = pg_namespace.oid),
             LATERAL emaj._get_log_sequence_last_value(rel_log_schema, rel_log_sequence) AS last_value
        WHERE upper_inf(rel_time_range)
          AND rel_group = ANY (p_groupNames)
          AND rel_kind = 'r';
    GET DIAGNOSTICS v_nbTbl = ROW_COUNT;
-- Record the mark for each group into the emaj_mark table.
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, p_mark, p_timeId, FALSE, p_loggedRlbkTargetMark
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
        ORDER BY group_name;
-- Before exiting, cleanup the state of the pending rollback events from the emaj_rlbk table.
-- It uses a dblink connection when the mark to set comes from a rollback operation that uses dblink connections.
    v_stmt = 'SELECT emaj._cleanup_rollback_state()';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, p_dblinkSchema);
-- If requested by the calling function, record the set mark end into emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (v_function, 'END', array_to_string(p_groupNames,','), p_mark);
    END IF;
--
    RETURN v_nbSeq + v_nbTbl;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_delete_before_mark_group$
-- This function deletes all logs and marks set before a given mark.
-- The function is called by the emaj_delete_before_mark_group(), emaj_delete_mark_group() functions.
-- It deletes rows corresponding to the marks to delete from emaj_mark and emaj_sequence.
-- It deletes rows from emaj_relation corresponding to old versions that become unreacheable.
-- It deletes rows from all concerned log tables.
-- To complete, the function deletes oldest rows from emaj_hist.
-- Input: group name, name of the new first mark.
-- Output: number of deleted marks, number of tables effectively processed (for which at least one log row has been deleted)
  DECLARE
    v_eventTriggers          TEXT[];
    v_markGlobalSeq          BIGINT;
    v_markTimeId             BIGINT;
    v_nbMark                 INT;
    r_rel                    RECORD;
  BEGIN
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Retrieve the timestamp and the emaj_gid value and the time stamp id of the target new first mark.
    SELECT time_last_emaj_gid, mark_time_id INTO v_markGlobalSeq, v_markTimeId
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
--
-- First process all obsolete time ranges for the group.
--
-- Drop obsolete old log tables.
    FOR r_rel IN
        -- log tables for the group, whose end time stamp is older than the new first mark time stamp
        SELECT DISTINCT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND rel_group = p_groupName
            AND upper(rel_time_range) <= v_markTimeId
      EXCEPT
        -- unless they are also used for more recent time range, or are also linked to other groups
        SELECT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND (upper(rel_time_range) > v_markTimeId
                 OR upper_inf(rel_time_range)
                 OR rel_group <> p_groupName)
        ORDER BY 1,2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- Delete emaj_table rows corresponding to obsolete relation time range that will be deleted just later.
-- The related emaj_seq_hole rows will be deleted just later ; they are not directly linked to an emaj_relation row.
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation r1
      WHERE rel_group = p_groupName
        AND rel_kind = 'r'
        AND tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND upper(rel_time_range) <= v_markTimeId
        AND (tbl_time_id < v_markTimeId                   -- all tables states prior the mark time
             OR (tbl_time_id = v_markTimeId               -- and the tables state of the mark time
                 AND NOT EXISTS                           --   if it is not the lower bound of an adjacent time range
                       (SELECT 0
                          FROM emaj.emaj_relation r2
                          WHERE r2.rel_schema = r1.rel_schema
                            AND r2.rel_tblseq = r1.rel_tblseq
                            AND lower(r2.rel_time_range) = v_markTimeId
                       )));
-- Keep a trace of the relation group ownership history and finaly delete from the emaj_relation table the relation that ended before
-- the new first mark.
    WITH deleted AS
      (DELETE FROM emaj.emaj_relation
         WHERE rel_group = p_groupName
           AND upper(rel_time_range) <= v_markTimeId
         RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
        FROM deleted;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any created group).
    PERFORM emaj._drop_log_schemas('DELETE_BEFORE_MARK_GROUP', FALSE);
--
-- Then process the current relation time range for the group.
--
-- Delete rows from all log tables.
    FOR r_rel IN
      SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND rel_kind = 'r'
          AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_markTimeId)
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
-- Delete log rows prior to the new first mark.
      EXECUTE format('DELETE FROM %s WHERE emaj_gid <= $1',
                     r_rel.log_table_name)
        USING v_markGlobalSeq;
    END LOOP;
-- Process emaj_seq_hole content.
-- Delete all existing holes, if any, before the mark.
-- It may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
-- but is safe enough for rollbacks.
    DELETE FROM emaj.emaj_seq_hole
      USING emaj.emaj_relation
      WHERE rel_group = p_groupName
        AND rel_kind = 'r'
        AND rel_schema = sqhl_schema
        AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id < v_markTimeId;
-- Now the sequences related to the mark to delete can be suppressed.
-- Delete first application sequences related data for the group.
-- The sequence state at time range bounds are kept (if the mark comes from a logging group alter operation).
    DELETE FROM emaj.emaj_sequence
      USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema
        AND sequ_name = rel_tblseq
        AND rel_time_range @> sequ_time_id
        AND rel_group = p_groupName
        AND rel_kind = 'S'
        AND sequ_time_id < v_markTimeId
        AND lower(rel_time_range) <> sequ_time_id;
-- Delete then tables related data for the group.
-- The tables state at time range bounds are kept.
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation
      WHERE tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND rel_time_range @> tbl_time_id
        AND rel_group = p_groupName
        AND rel_kind = 'r'
        AND tbl_time_id < v_markTimeId
        AND lower(rel_time_range) <> tbl_time_id;
-- And that may have one of the deleted marks as target mark from a previous logged rollback operation.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = p_groupName
        AND mark_time_id >= v_markTimeId
        AND mark_logged_rlbk_target_mark IN
             (SELECT mark_name
                FROM emaj.emaj_mark
                WHERE mark_group = p_groupName
                  AND mark_time_id < v_markTimeId
             );
-- Delete oldest marks.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id < v_markTimeId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Purge the history tables, if needed (even if no mark as been really dropped).
    PERFORM emaj._purge_histories();
--
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE OR REPLACE FUNCTION emaj._rlbk_check(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN, isRollbackSimulation BOOLEAN)
RETURNS TEXT LANGUAGE plpgsql AS
$_rlbk_check$
-- This functions performs checks on group names and mark names supplied as parameter for the emaj_rollback_groups()
-- and emaj_estimate_rollback_groups() functions.
-- It returns the real mark name, or NULL if the groups array is NULL or empty.
  DECLARE
    v_markName               TEXT;
    v_aGroupName             TEXT;
    v_markTimeId             BIGINT;
    v_protectedMarksList     TEXT;
  BEGIN
-- Check the group names and states.
    IF isRollbackSimulation THEN
      SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := FALSE, p_lockGroups := FALSE,
                                     p_checkLogging := TRUE, p_checkRollbackable := TRUE) INTO p_groupNames;
    ELSE
      SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                     p_checkLogging := TRUE, p_checkRollbackable := TRUE, p_checkUnprotected := TRUE) INTO p_groupNames;
    END IF;
    IF p_groupNames IS NOT NULL THEN
-- Check the mark name.
      SELECT emaj._check_mark_name(p_groupNames := p_groupNames, p_mark := p_mark, p_checkActive := TRUE) INTO v_markName;
      IF NOT isRollbackSimulation THEN
-- Check that for each group that the rollback wouldn't delete protected marks (check disabled for rollback simulation).
        FOREACH v_aGroupName IN ARRAY p_groupNames LOOP
--   Get the target mark time id,
          SELECT mark_time_id INTO v_markTimeId
            FROM emaj.emaj_mark
            WHERE mark_group = v_aGroupName
              AND mark_name = v_markName;
--   ... and look at the protected mark.
          SELECT string_agg(mark_name,', ' ORDER BY mark_name) INTO v_protectedMarksList
            FROM
              (SELECT mark_name
                 FROM emaj.emaj_mark
                 WHERE mark_group = v_aGroupName
                   AND mark_time_id > v_markTimeId
                   AND mark_is_rlbk_protected
                 ORDER BY mark_time_id
              ) AS t;
          IF v_protectedMarksList IS NOT NULL THEN
            RAISE EXCEPTION '_rlbk_check: Protected marks (%) for the group "%" block the rollback to the mark "%".',
              v_protectedMarksList, v_aGroupName, v_markName;
          END IF;
        END LOOP;
      END IF;
-- If the isAlterGroupAllowed flag is not explicitely set to true, check that the rollback would not cross any structure change for
-- the groups.
      IF p_isAlterGroupAllowed IS NULL OR NOT p_isAlterGroupAllowed THEN
        SELECT mark_time_id INTO v_markTimeId
          FROM emaj.emaj_mark
          WHERE mark_group = p_groupNames[1]
            AND mark_name = v_markName;
        IF EXISTS
             (SELECT 0
                FROM emaj.emaj_relation_change
                WHERE rlchg_time_id > v_markTimeId
                  AND (rlchg_group = ANY (p_groupNames) OR rlchg_new_group = ANY (p_groupNames))
             ) THEN
          RAISE EXCEPTION '_rlbk_check: This rollback operation would cross some previous structure group change operations,'
                          ' which is not allowed by the current function parameters.';
        END IF;
      END IF;
    END IF;
--
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(p_rlbkId INT, p_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_start_mark$
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
-- Before setting the mark, it checks no update has been recorded between the planning step and the locks set
-- for tables for which no rollback was needed at planning time.
-- It also sets the rollback status to EXECUTING.
  DECLARE
    v_function               TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_dblinkSchema           TEXT;
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_timeId                 BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_markTimeId             BIGINT;
    v_markName               TEXT;
    v_errorMsg               TEXT;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END;
-- Get the dblink usage characteristics for the current rollback.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema INTO v_isDblinkUsed, v_dblinkSchema
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get a time stamp for the rollback operation and record it into emaj_hist
--   (the _set_time_stamp() function doesn't trace events of type 'R' in emaj_hist for visibility reason.)
    v_stmt = 'SELECT emaj._set_time_stamp(''' || v_function || ''', ''R'')';
    SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'TIME STAMP SET', v_timeId::TEXT);
-- Update the emaj_rlbk table to record the time stamp and adjust the rollback status.
    v_stmt = 'UPDATE emaj.emaj_rlbk' ||
             ' SET rlbk_time_id = ' || v_timeId || ', rlbk_end_locking_datetime = time_clock_timestamp, rlbk_status = ''EXECUTING''' ||
             ' FROM emaj.emaj_time_stamp' ||
             ' WHERE time_id = ' || v_timeId || ' AND rlbk_id = ' || p_rlbkId || ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_end_locking_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_rlbkDatetime
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get some mark attributes from emaj_mark.
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Check that no update has been recorded between planning time and lock time for tables that did not need to
-- be rolled back at planning time.
-- This may occur and cannot be avoided because tables cannot be locked before processing the rollback planning.
-- Sessions must lock the tables they will rollback and the planning processing distribute those tables to sessions.
    IF EXISTS
         (SELECT 0
            FROM
              (SELECT *
                 FROM emaj.emaj_relation
                 WHERE upper_inf(rel_time_range)
                   AND rel_group = ANY (v_groupNames)
                   AND rel_kind = 'r'
                   AND NOT EXISTS
                         (SELECT NULL
                            FROM emaj.emaj_rlbk_plan
                            WHERE rlbp_schema = rel_schema
                              AND rlbp_table = rel_tblseq
                              AND rlbp_rlbk_id = p_rlbkId
                              AND rlbp_step = 'RLBK_TABLE'
                         )
              ) AS t
            WHERE emaj._log_stat_tbl(t, greatest(v_markTimeId, lower(rel_time_range)), NULL) > 0
         ) THEN
      v_errorMsg = 'the rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables'
                   ' to process.';
      PERFORM emaj._rlbk_error(p_rlbkId, v_errorMsg, 'rlbk#1');
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If rollback is a "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time.
      v_markName = 'RLBK_' || v_mark || '_' || substring(to_char(v_rlbkDatetime, 'HH24.MI.SS.US') from 1 for 13) || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, p_multiGroup, TRUE, NULL, v_timeId, v_dblinkSchema);
    END IF;
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_start_mark(): ' || SQLERRM, 'rlbk#1');
      RAISE;
  END;
$_rlbk_start_mark$;

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
    v_function               TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_isAlterGroupAllowed    BOOLEAN;
    v_nbTbl                  INT;
    v_effNbTbl               INT;
    v_nbSeq                  INT;
    v_effNbSeq               INT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_rlbkDatetime           TIMESTAMPTZ;
    v_markTimeId             BIGINT;
    v_stmt                   TEXT;
    v_ctrlDuration           INTERVAL;
    v_messages               TEXT[] = ARRAY[]::TEXT[];
    v_markName               TEXT;
    v_msg                    TEXT;
    v_msgList                TEXT;
    r_msg                    RECORD;
  BEGIN
    v_function = CASE WHEN p_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END;
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_is_alter_group_allowed, rlbk_nb_table, rlbk_eff_nb_table, rlbk_nb_sequence,
           rlbk_eff_nb_sequence, rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_end_locking_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_isAlterGroupAllowed, v_nbTbl, v_effNbTbl, v_nbSeq,
           v_effNbSeq, v_dblinkSchema, v_isDblinkUsed, v_rlbkDatetime
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- Get the mark timestamp for the 1st group (they all share the same timestamp).
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- If "unlogged" rollback, delete all marks later than the now rolled back mark and the associated sequences.
    IF NOT v_isLoggedRlbk THEN
-- Get the highest mark time id of the mark used for rollback, for all groups.
-- Delete the marks that are suppressed by the rollback (the related sequences have been already deleted), with a trace in the history,
      WITH deleted AS
        (DELETE FROM emaj.emaj_mark
           WHERE mark_group = ANY (v_groupNames)
             AND mark_time_id > v_markTimeId
           RETURNING mark_time_id, mark_group, mark_name
        ),
           sorted_deleted AS                                        -- the sort is performed to produce stable results in regression tests
        (SELECT mark_group, mark_name
           FROM deleted
           ORDER BY mark_time_id, mark_group
        )
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT v_function, 'MARK DELETED', mark_group, 'mark ' || mark_name || ' is deleted'
        FROM sorted_deleted;
-- ... and reset the mark_log_rows_before_next column for the new groups latest marks.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND mark_time_id = v_markTimeId;
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
--   copy elementary steps for RLBK_TABLE, RLBK_SEQUENCES, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_rlbk_id,' ||
             '      rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan' ||
             '    WHERE rlbp_rlbk_id = ' || p_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''RLBK_SEQUENCES'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
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
    v_messages = array_append(v_messages,
                              'Notice: ' || format ('Rollback id = %s.', p_rlbkId::TEXT));
    IF v_nbTbl > 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: ' || format ('%s / %s tables effectively processed.', v_effNbTbl::TEXT, v_nbTbl::TEXT));
    END IF;
    IF v_nbSeq > 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: ' || format ('%s / %s sequences effectively processed.', v_effNbSeq::TEXT, v_nbSeq::TEXT));
    END IF;
    IF v_nbTbl = 0 AND v_nbSeq = 0 THEN
      v_messages = array_append(v_messages,
                                'Notice: no table and sequence to process');
    END IF;
-- And then the WARNING messages for any elementary action from group structure change that has not been rolled back.
    FOR r_msg IN
-- Steps are splitted into 2 groups to filter them differently.
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               (CASE
                  WHEN rlchg_change_kind = 'ADD_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment state ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND NOT new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'ADD_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND NOT new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_tx_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  END)::TEXT AS message
          FROM
-- Suppress duplicate ADD_TABLE / MOVE_TABLE / REMOVE_TABLE or ADD_SEQUENCE / MOVE_SEQUENCE / REMOVE_SEQUENCE for same table or sequence,
-- by keeping the most recent changes.
            (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind, new_group_is_rolledback
               FROM
                 (SELECT rlchg_schema, rlchg_tblseq, rlchg_time_id, rlchg_change_kind,
                         (rlchg_new_group = ANY (v_groupNames)) AS new_group_is_rolledback,
                         rank() OVER (PARTITION BY rlchg_schema, rlchg_tblseq ORDER BY rlchg_time_id DESC) AS rlchg_rank
                    FROM emaj.emaj_relation_change
                    WHERE rlchg_time_id > v_markTimeId
                      AND (rlchg_group = ANY (v_groupNames) OR rlchg_new_group = ANY (v_groupNames))
                      AND rlchg_tblseq <> ''
                      AND rlchg_change_kind IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                  ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2, emaj.emaj_time_stamp
          WHERE rlchg_time_id = time_id
      UNION
        SELECT rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq,
               'Tables group change not rolled back: ' ||
               (CASE rlchg_change_kind
                  WHEN 'CHANGE_PRIORITY' THEN
                    'E-Maj priority for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_DATA_TABLESPACE' THEN
                    'log data tablespace for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_LOG_INDEX_TABLESPACE' THEN
                    'log index tablespace for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
                  WHEN 'CHANGE_IGNORED_TRIGGERS' THEN
                    'ignored triggers list for ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq)
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
                      AND rlchg_change_kind NOT IN
                            ('ADD_TABLE','ADD_SEQUENCE','REMOVE_TABLE','REMOVE_SEQUENCE','MOVE_TABLE','MOVE_SEQUENCE')
                 ) AS t1
               WHERE rlchg_rank = 1
            ) AS t2
        ORDER BY rlchg_time_id, rlchg_change_kind, rlchg_schema, rlchg_tblseq
    LOOP
      v_messages = array_append(v_messages, 'Warning: ' || r_msg.message);
    END LOOP;
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
-- Update the emaj_rlbk table to adjust the rollback status and set the output messages.
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
      VALUES (v_function, 'END', array_to_string(v_groupNames,','), 'Rollback_id ' || p_rlbkId);
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

CREATE OR REPLACE FUNCTION emaj._reset_groups(p_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql AS
$_reset_groups$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences images.
-- It is called by emaj_reset_group(), _start_groups() and _import_groups_conf_alter() functions.
-- Input: group names array
-- Output: number of processed tables and sequences
-- There is no check of the groups state (this is done by callers).
  DECLARE
    v_eventTriggers          TEXT[];
    v_batchSize     CONSTANT INT = 100;
    v_tableList              TEXT;
    v_nbTbl                  INT;
    r_rel                    RECORD;
  BEGIN
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Delete all marks for the groups from the emaj_mark table.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = ANY (p_groupNames);
-- Delete emaj_table rows related to the tables of the groups.
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation r1
      WHERE tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND rel_group = ANY (p_groupNames)
        AND rel_kind = 'r'
        AND ((tbl_time_id <@ rel_time_range               -- all log sequences inside the relation time range
             AND (tbl_time_id <> lower(rel_time_range)    -- except the lower bound if
                  OR NOT EXISTS                           --   it is the upper bound of another time range for another group
                       (SELECT 0
                          FROM emaj.emaj_relation r2
                          WHERE r2.rel_schema = tbl_schema
                            AND r2.rel_tblseq = tbl_name
                            AND upper(r2.rel_time_range) = tbl_time_id
                            AND NOT (r2.rel_group = ANY (p_groupNames)) )))
            OR (tbl_time_id = upper(rel_time_range)        -- but including the upper bound if
                  AND NOT EXISTS                           --   it is not the lower bound of another time range (for any group)
                        (SELECT 0
                           FROM emaj.emaj_relation r3
                           WHERE r3.rel_schema = tbl_schema
                             AND r3.rel_tblseq = tbl_name
                             AND lower(r3.rel_time_range) = tbl_time_id
                        )
               ));
-- Delete all sequence holes for the tables of the groups.
-- It may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
-- but is safe enough for rollbacks.
    DELETE FROM emaj.emaj_seq_hole
      USING emaj.emaj_relation
      WHERE rel_schema = sqhl_schema
        AND rel_tblseq = sqhl_table
        AND rel_group = ANY (p_groupNames)
        AND rel_kind = 'r';
-- Drop obsolete log tables, but keep those linked to other groups.
    FOR r_rel IN
        SELECT DISTINCT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'
            AND NOT upper_inf(rel_time_range)
      EXCEPT
        SELECT rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND (upper_inf(rel_time_range) OR NOT rel_group = ANY (p_groupNames))
        ORDER BY 1,2
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END LOOP;
-- Delete emaj_sequence rows related to the sequences of the groups.
    DELETE FROM emaj.emaj_sequence
      USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema
        AND sequ_name = rel_tblseq
        AND rel_group = ANY (p_groupNames)
        AND rel_kind = 'S'
        AND ((sequ_time_id <@ rel_time_range               -- all application sequences inside the relation time range
             AND (sequ_time_id <> lower(rel_time_range)    -- except the lower bound if
                  OR NOT EXISTS                            --   it is the upper bound of another time range for another group
                       (SELECT 0
                          FROM emaj.emaj_relation r2
                          WHERE r2.rel_schema = sequ_schema
                            AND r2.rel_tblseq = sequ_name
                            AND upper(r2.rel_time_range) = sequ_time_id
                            AND NOT (r2.rel_group = ANY (p_groupNames))
                       )))
          OR (sequ_time_id = upper(rel_time_range)         -- including the upper bound if
                  AND NOT EXISTS                           --   it is not the lower bound of another time range for another group
                       (SELECT 0
                          FROM emaj.emaj_relation r3
                          WHERE r3.rel_schema = sequ_schema
                            AND r3.rel_tblseq = sequ_name
                            AND lower(r3.rel_time_range) = sequ_time_id
                       ))
            );
-- Keep a trace of the relation group ownership history
-- and finaly delete the old versions of emaj_relation rows (those with a not infinity upper bound).
    WITH deleted AS
      (DELETE FROM emaj.emaj_relation
         WHERE rel_group = ANY (p_groupNames)
           AND NOT upper_inf(rel_time_range)
         RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
        FROM deleted;
-- Truncate remaining log tables for application tables.
-- For performance reason, execute one single TRUNCATE statement for every v_batchSize tables.
    v_tableList = NULL;
    v_nbTbl = 0;
    FOR r_rel IN
      SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS full_relation_name
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groupNames)
          AND rel_kind = 'r'
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_tableList = coalesce(v_tableList || ',' || r_rel.full_relation_name, r_rel.full_relation_name);
      v_nbTbl = v_nbtbl + 1;
      IF v_nbTbl >= v_batchSize THEN
        EXECUTE 'TRUNCATE ' || v_tableList;
        v_nbTbl = 0;
        v_tableList = NULL;
      END IF;
    END LOOP;
    IF v_tableList IS NOT NULL THEN
      EXECUTE 'TRUNCATE ' || v_tableList;
    END IF;
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Purge the history tables, if needed.
    PERFORM emaj._purge_histories();
--
    RETURN sum(group_nb_table)+sum(group_nb_sequence)
      FROM emaj.emaj_group
      WHERE group_name = ANY (p_groupNames);
  END;
$_reset_groups$;

CREATE OR REPLACE FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$_log_stat_groups$
-- This function effectively returns statistics about logged data changes between 2 marks or between a mark and the current state for 1
-- or several groups.
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function, the 2 mark names defining a
--          range
--   a NULL value or an empty string as last_mark indicates the current state
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark
--   parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of stat rows by table (including tables without any data change)
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
  BEGIN
-- Check the groups name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT checked.p_firstMark, checked.p_lastMark, p_firstMarkTimeId, p_lastMarkTimeId, p_firstMarkTs, p_lastMarkTs
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark) checked;
-- For each table of the group, get the number of log rows and return the statistics.
-- Shorten the timeframe if the table did not belong to the group on the entire requested time frame.
      RETURN QUERY
        SELECT rel_group, rel_schema, rel_tblseq,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN p_firstMark
                    ELSE coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = lower(rel_time_range)
                                AND mark_group = rel_group
                           ),'[deleted mark]')
                 END AS stat_first_mark,
               CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                    WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMarkTs
                    ELSE (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = lower(rel_time_range)
                         )
                 END AS stat_first_mark_datetime,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN coalesce(
                                (SELECT mark_name
                                   FROM emaj.emaj_mark
                                   WHERE mark_time_id = upper(rel_time_range)
                                     AND mark_group = rel_group
                                ),'[deleted mark]')
                    ELSE p_lastMark
                 END AS stat_last_mark,
               CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                    WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                         THEN (SELECT time_clock_timestamp
                                 FROM emaj.emaj_time_stamp
                                 WHERE time_id = upper(rel_time_range)
                              )
                    ELSE v_lastMarkTs
                 END AS stat_last_mark_datetime,
               CASE WHEN v_firstMarkTimeId IS NULL THEN 0                                       -- group just created but without any mark
                    ELSE emaj._log_stat_tbl(emaj_relation,
                                            CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                   THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                            CASE WHEN NOT upper_inf(rel_time_range)
                                                   AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                   THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
                 END AS nb_rows
          FROM emaj.emaj_relation
          WHERE rel_group = ANY(p_groupNames)
            AND rel_kind = 'r'                                                                  -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)        --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range;
    ELSE
      RETURN;
    END IF;
  END;
$_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$_detailed_log_stat_groups$
-- This function effectively returns statistics on logged data changes executed between 2 marks as viewed through the log tables for one
-- or several groups.
-- It provides muche precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: groups name array, a boolean indicating whether the calling function is a multi_groups function,
--        the 2 mark names defining a range
--   a NULL value or an empty string as last_mark indicates the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: set of stat rows by table, user and SQL type
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_lowerBoundMark         TEXT;
    v_lowerBoundMarkTs       TIMESTAMPTZ;
    v_lowerBoundGid          BIGINT;
    v_upperBoundMark         TEXT;
    v_upperBoundMarkTs       TIMESTAMPTZ;
    v_upperBoundGid          BIGINT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE);
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT *
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark);
-- For each table currently belonging to the group, count the number of operations per type (INSERT, UPDATE and DELETE) and role.
      FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_group, rel_time_range, rel_log_schema, rel_log_table
          FROM emaj.emaj_relation
          WHERE rel_group = ANY(p_groupNames)
            AND rel_kind = 'r'                                                                         -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range
      LOOP
-- Compute the lower bound for this table.
        IF v_firstMarkTimeId >= lower(r_tblsq.rel_time_range) THEN
-- Usual case: the table belonged to the group at statistics start mark.
          v_lowerBoundMark = p_firstMark;
          v_lowerBoundMarkTs = v_firstMarkTs;
          v_lowerBoundGid = v_firstEmajGid;
        ELSE
-- Special case: the table has been added to the group after the statistics start mark.
          SELECT mark_name INTO v_lowerBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = lower(r_tblsq.rel_time_range)
              AND mark_group = r_tblsq.rel_group;
          IF v_lowerBoundMark IS NULL THEN
-- The mark set at alter_group time may have been deleted.
            v_lowerBoundMark = '[deleted mark]';
          END IF;
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_lowerBoundMarkTs, v_lowerBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = lower(r_tblsq.rel_time_range);
        END IF;
-- Compute the upper bound for this table.
        IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- No supplied end mark and the table has not been removed from its group => the current state.
          v_upperBoundMark = NULL;
          v_upperBoundMarkTs = NULL;
          v_upperBoundGid = NULL;
        ELSIF NOT upper_inf(r_tblsq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_tblsq.rel_time_range) < v_lastMarkTimeId) THEN
-- Special case: the table has been removed from its group before the statistics end mark.
          SELECT mark_name INTO v_upperBoundMark
            FROM emaj.emaj_mark
            WHERE mark_time_id = upper(r_tblsq.rel_time_range)
              AND mark_group = r_tblsq.rel_group;
          IF v_upperBoundMark IS NULL THEN
-- The mark set at alter_group time may have been deleted.
            v_upperBoundMark = '[deleted mark]';
          END IF;
          SELECT time_clock_timestamp, time_last_emaj_gid INTO v_upperBoundMarkTs, v_upperBoundGid
            FROM emaj.emaj_time_stamp
            WHERE time_id = upper(r_tblsq.rel_time_range);
        ELSE
-- Usual case: the table belonged to the group at statistics end mark.
          v_upperBoundMark = p_lastMark;
          v_upperBoundMarkTs = v_lastMarkTs;
          v_upperBoundGid = v_lastEmajGid;
        END IF;
-- Build the statement.
        v_stmt= 'SELECT ' || quote_literal(r_tblsq.rel_group) || '::TEXT AS stat_group, '
             || quote_literal(r_tblsq.rel_schema) || '::TEXT AS stat_schema, '
             || quote_literal(r_tblsq.rel_tblseq) || '::TEXT AS stat_table, '
             || quote_literal(v_lowerBoundMark) || '::TEXT AS stat_first_mark, '
             || quote_literal(v_lowerBoundMarkTs) || '::TIMESTAMPTZ AS stat_first_mark_datetime, '
             || coalesce(quote_literal(v_upperBoundMark),'NULL') || '::TEXT AS stat_last_mark, '
             || coalesce(quote_literal(v_upperBoundMarkTs),'NULL') || '::TIMESTAMPTZ AS stat_last_mark_datetime, '
             || ' emaj_user::TEXT AS stat_user,'
             || ' CASE emaj_verb WHEN ''INS'' THEN ''INSERT'''
             ||                ' WHEN ''UPD'' THEN ''UPDATE'''
             ||                ' WHEN ''DEL'' THEN ''DELETE'''
             ||                ' WHEN ''TRU'' THEN ''TRUNCATE'''
             ||                             ' ELSE ''?'' END AS stat_verb,'
             || ' count(*) AS stat_rows'
             || ' FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table)
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'') AND NOT (emaj_verb = ''TRU'' AND emaj_tuple = '''')'
             || ' AND emaj_gid > '|| v_lowerBoundGid
             || coalesce(' AND emaj_gid <= '|| v_upperBoundGid, '')
             || ' GROUP BY stat_user, stat_verb'
             || ' ORDER BY stat_user, stat_verb';
-- Execute the statement.
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END LOOP;
    END IF;
-- Final return.
    RETURN;
  END;
$_detailed_log_stat_groups$;

CREATE OR REPLACE FUNCTION emaj._sequence_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_type LANGUAGE plpgsql AS
$_sequence_stat_groups$
-- This function effectively returns statistics on sequences changes recorded between 2 marks or between a mark and the current state for
-- one or several groups.
-- Input: group names array, a boolean indicating whether the calling function is a multi_groups function, both mark names defining a
--          range
--   a NULL value or an empty string as last_mark indicates the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark for the groups.
-- Output: set of stats by sequence (including unchanged sequences).
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_lowerTimeId            BIGINT;
    v_upperTimeId            BIGINT;
    r_seq                    emaj.emaj_relation%ROWTYPE;
    r_stat                   emaj.emaj_sequence_stat_type%ROWTYPE;
  BEGIN
-- Check the groups name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT checked.p_firstMark, checked.p_lastMark, p_firstMarkTimeId, p_lastMarkTimeId, p_firstMarkTs, p_lastMarkTs
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark,
                                     p_checkLogSession := FALSE) AS checked;
-- For each sequence of the group, get and return the statistics.
      FOR r_seq IN
          SELECT *
            FROM emaj.emaj_relation
            WHERE rel_group = ANY(p_groupNames)
              AND rel_kind = 'S'                                                                  -- sequences belonging to the groups
              AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)        --   at the requested time frame
              AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
            ORDER BY rel_schema, rel_tblseq, rel_time_range
      LOOP
        r_stat.stat_group = r_seq.rel_group;
        r_stat.stat_schema = r_seq.rel_schema;
        r_stat.stat_sequence = r_seq.rel_tblseq;
        r_stat.stat_first_mark = p_firstMark;
        r_stat.stat_first_mark_datetime = v_firstMarkTs;
        r_stat.stat_last_mark = p_lastMark;
        r_stat.stat_last_mark_datetime = v_lastMarkTs;
        v_lowerTimeId = v_firstMarkTimeId;
        v_upperTimeId = v_lastMarkTimeId;
-- Shorten the time frame if the sequence did not belong to the group on the entire requested time frame.
        IF v_firstMarkTimeId < lower(r_seq.rel_time_range) THEN
          v_lowerTimeId = lower(r_seq.rel_time_range);
          r_stat.stat_first_mark = coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = v_lowerTimeId
                                AND mark_group = r_seq.rel_group
                           ),'[deleted mark]');
          r_stat.stat_first_mark_datetime = (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = v_lowerTimeId
                         );
        END IF;
        IF NOT upper_inf(r_seq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_seq.rel_time_range) < v_lastMarkTimeId) THEN
          v_upperTimeId = upper(r_seq.rel_time_range);
          r_stat.stat_last_mark = coalesce(
                           (SELECT mark_name
                              FROM emaj.emaj_mark
                              WHERE mark_time_id = v_upperTimeId
                                AND mark_group = r_seq.rel_group
                           ),'[deleted mark]');
          r_stat.stat_last_mark_datetime = (SELECT time_clock_timestamp
                            FROM emaj.emaj_time_stamp
                            WHERE time_id = v_upperTimeId
                         );
        END IF;
-- Get the stats for the sequence.
        SELECT p_increments, p_hasStructureChanged
          INTO r_stat.stat_increments, r_stat.stat_has_structure_changed
          FROM emaj._sequence_stat_seq(r_seq, v_lowerTimeId, v_upperTimeId);
-- Return the stat row for the sequence.
        RETURN NEXT r_stat;
      END LOOP;
    END IF;
    RETURN;
  END;
$_sequence_stat_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_optionsList TEXT,
                                                        p_tblseqs TEXT[], p_dir TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_dump_changes_group$
-- This function reads log tables and sequences states to export into files the data changes recorded between 2 marks for a group.
-- The function performs COPY TO statements, using the options provided by the caller.
-- Some options may be set to customize the changes dump:
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - COPY_OPTIONS=(options) sets the options to use for COPY TO statements
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - NO_EMPTY_FILES ... remove empty files (requires the adminpack extension to be installed)
--   - ORDER_BY=PK|TIME defines the data sort criteria in the output for tables (default depends on the consolidation level)
--   - SEQUENCES_ONLY filters only sequences
--   - TABLES_ONLY filters only tables
-- Complex options such as lists or directory names must be set between ().
-- It's users responsability to create the directory  before the function call (with proper permissions allowing the cluster to
--   write into).
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        the absolute pathname of the directory where the files are to be created.
-- Output: Message with the number of generated files (for tables and sequences, including the _INFO file).
  DECLARE
    v_copyOptions            TEXT;
    v_noEmptyFiles           BOOLEAN;
    v_nbFile                 INT = 1;
    v_pathName               TEXT;
    v_copyResult             INT;
    v_stmt                   TEXT;
    r_sql                    RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DUMP_CHANGES_GROUP', 'BEGIN', p_groupName,
       'From mark ' || coalesce(p_firstMark, '') || ' to ' || coalesce(p_lastMark, '') ||
       coalesce(' towards ' || p_dir, ''));
-- Call the _gen_sql_dump_changes_group() function to proccess options and get the SQL statements.
    SELECT p_copyOptions, p_noEmptyFiles, g.p_lastMark
      INTO v_copyOptions, v_noEmptyFiles, p_lastMark
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, FALSE) g;
-- Test the supplied output directory and copy options.
    IF p_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_dump_changes_group: The directory parameter cannot be NULL.';
    END IF;
    PERFORM emaj._copy_to_file('(SELECT '''')', p_dir || '/_INFO', v_copyOptions);
-- Execute each generated SQL statement.
    FOR r_sql IN
      SELECT sql_stmt_number, sql_schema, sql_tblseq, sql_file_name_suffix, sql_text
        FROM emaj_temp_sql
        WHERE sql_line_number = 1
        ORDER BY sql_stmt_number
      LOOP
        IF r_sql.sql_text ~ '^(SET|RESET)' THEN
-- The SET or RESET statements are executed as is.
          EXECUTE r_sql.sql_text;
        ELSE
-- Otherwise, dump the log table or the sequence states.
          v_pathName = emaj._build_path_name(p_dir, r_sql.sql_schema || '_' || r_sql.sql_tblseq || r_sql.sql_file_name_suffix);
          v_copyResult = emaj._copy_to_file('(' || r_sql.sql_text || ')', v_pathName, v_copyOptions, v_noEmptyFiles);
          v_nbFile = v_nbFile + v_copyResult;
-- Keep a trace of the dump execution.
          UPDATE emaj_temp_sql
            SET sql_result = v_copyResult
            WHERE sql_stmt_number = r_sql.sql_stmt_number AND sql_line_number = 1;
        END IF;
    END LOOP;
-- Create the _INFO file to keep information about the operation.
-- It contains 3 first rows with general information and then 1 row per effectively written file, describing the file content.
    v_stmt = '(SELECT ' || quote_literal('Dump logged changes for the group "' || p_groupName || '" between mark "' || p_firstMark ||
                                         '" and mark "' || p_lastMark || '"') ||
             ' UNION ALL' ||
             ' SELECT ' || quote_literal(coalesce('  using options "' || p_optionsList || '"', ' without option')) ||
             ' UNION ALL' ||
             ' SELECT ' || quote_literal('  started at ' || statement_timestamp()) ||
             ' UNION ALL' ||
             ' SELECT ''File '' || '
                      'translate(sql_schema || ''_'' || sql_tblseq || sql_file_name_suffix, E'' /\\$<>*'', ''_______'')'
                      ' || '' covers '' || sql_rel_kind || '' "'' || sql_schema || ''.'' || sql_tblseq || ''" from mark "'''
                      ' || sql_first_mark || ''" to mark "'' || sql_last_mark || ''"'''
                'FROM emaj_temp_sql WHERE sql_line_number = 1 AND sql_result = 1)';
    PERFORM emaj._copy_to_file(v_stmt, p_dir || '/_INFO');
-- Drop the temporary table.
    DROP TABLE IF EXISTS emaj_temp_sql;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DUMP_CHANGES_GROUP', 'END', p_groupName, v_nbFile || ' generated files');
-- Return a formated message.
    RETURN format('%s files have been created in %s', v_nbFile, p_dir);
  END;
$emaj_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[],TEXT) IS
$$Dump recorded changes between two marks for application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                            p_optionsList TEXT, p_tblseqs TEXT[], p_genSqlOnly BOOLEAN,
                                                            OUT p_nbStmt INT, OUT p_copyOptions TEXT, OUT p_noEmptyFiles BOOLEAN,
                                                            OUT p_isPsqlCopy BOOLEAN)
LANGUAGE plpgsql AS
$_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- It is called by both emaj_gen_sql_dump_changes_group() and emaj_dump_changes_group() functions to prepare the SQL statements to be
--   stored or executed.
-- The function checks the supplied parameters, including the options that may be common or specific to both calling functions.
-- Input: group name, 2 mark names defining the time range,
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        a boolean indentifying the calling function.
-- Output: the number of generated SQL statements, excluding comments, but including SET or RESET statements, if any.
--         the COPY_OPTIONS and NO_EMPTY_FILES options needed by the emaj_dump_changes_group() function,
--         a flag for generated psql \copy meta-commands needed by the emaj_gen_sql_dump_changes_group() function.
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_optionsList            TEXT;
    v_options                TEXT[];
    v_option                 TEXT;
    v_colsOrder              TEXT;
    v_consolidation          TEXT;
    v_copyOptions            TEXT;
    v_psqlCopyDir            TEXT;
    v_psqlCopyOptions        TEXT;
    v_emajColumnsList        TEXT;
    v_isPsqlCopy             BOOLEAN = FALSE;
    v_noEmptyFiles           BOOLEAN = FALSE;
    v_orderBy                TEXT;
    v_sequencesOnly          BOOLEAN = FALSE;
    v_sqlFormat              TEXT = 'RAW';
    v_tablesOnly             BOOLEAN = FALSE;
    v_tableWithoutPkList     TEXT;
    v_nbStmt                 INT = 0;
    v_relFirstMark           TEXT;
    v_relLastMark            TEXT;
    v_relFirstEmajGid        BIGINT;
    v_relLastEmajGid         BIGINT;
    v_relFirstTimeId         BIGINT;
    v_relLastTimeId          BIGINT;
    v_stmt                   TEXT;
    v_comment                TEXT;
    v_copyOutputFile         TEXT;
    r_rel                    RECORD;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the marks range and get some data about both marks.
    SELECT * INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
      FROM emaj._check_marks_range(p_groupNames := ARRAY[p_groupName], p_firstMark := p_firstMark, p_lastMark := p_lastMark,
                                   p_finiteUpperBound := TRUE);
-- Analyze the options parameter.
    IF p_optionsList IS NOT NULL THEN
      v_optionsList = p_optionsList;
      IF NOT p_genSqlOnly THEN
-- Extract the COPY_OPTIONS list, if any, before removing spaces.
        v_copyOptions = (regexp_match(v_optionsList, 'COPY_OPTIONS\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_copyOptions IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_copyOptions, '');
        END IF;
      END IF;
      IF p_genSqlOnly THEN
-- Extract the PSQL_COPY_DIR and PSQL_COPY_OPTIONS options, if any, before removing spaces.
        v_psqlCopyDir = (regexp_match(v_optionsList, 'PSQL_COPY_DIR\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_psqlCopyDir IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_psqlCopyDir, '');
        END IF;
        v_psqlCopyOptions = (regexp_match(v_optionsList, 'PSQL_COPY_OPTIONS\s*?=\s*?\((.*?)\)', 'i'))[1];
        IF v_psqlCopyOptions IS NOT NULL THEN
          v_optionsList = replace(v_optionsList, v_psqlCopyOptions, '');
        END IF;
      END IF;
-- Remove spaces, tabs and newlines from the options list.
      v_optionsList = regexp_replace(v_optionsList, '\s', '', 'g');
-- Extract the option values list, if any.
      v_emajColumnsList = (regexp_match(v_optionsList, 'EMAJ_COLUMNS\s*?=\s*?\((.*?)\)', 'i'))[1];
      IF v_emajColumnsList IS NOT NULL THEN
        v_optionsList = replace(v_optionsList, v_emajColumnsList, '');
      END IF;
-- Process each option from the comma separated list.
      v_options = regexp_split_to_array(upper(v_optionsList), ',');
      FOREACH v_option IN ARRAY v_options
      LOOP
        CASE
          WHEN v_option LIKE 'COLS_ORDER=%' THEN
            CASE
              WHEN v_option = 'COLS_ORDER=LOG_TABLE' THEN
                v_colsOrder = 'LOG_TABLE';
              WHEN v_option = 'COLS_ORDER=PK' THEN
                v_colsOrder = 'PK';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COLS_ORDER option only accepts '
                                'LOG_TABLE or PK values).',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'CONSOLIDATION=%' THEN
            CASE
              WHEN v_option = 'CONSOLIDATION=NONE' THEN
                v_consolidation = 'NONE';
              WHEN v_option = 'CONSOLIDATION=PARTIAL' THEN
                v_consolidation = 'PARTIAL';
              WHEN v_option = 'CONSOLIDATION=FULL' THEN
                v_consolidation = 'FULL';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The CONSOLIDATION option only accepts '
                                'NONE or PARTIAL or FULL values).',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'COPY_OPTIONS=%' AND NOT p_genSqlOnly THEN
            IF v_option <> 'COPY_OPTIONS=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COPY options must be set between ().',
                              v_option;
            END IF;
-- Check the copy options parameter doesn't contain unquoted semicolon that could be used for sql injection.
            IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Unquoted semi-column in COPY options is illegal.';
            END IF;
            v_copyOptions = '(' || v_copyOptions || ')';
          WHEN v_option LIKE 'EMAJ_COLUMNS=%' THEN
            CASE
              WHEN v_option = 'EMAJ_COLUMNS=ALL' THEN
                v_emajColumnsList = '*';
              WHEN v_option = 'EMAJ_COLUMNS=MIN' THEN
                v_emajColumnsList = 'MIN';
              WHEN v_option = 'EMAJ_COLUMNS=()' THEN
                IF v_emajColumnsList NOT ILIKE '%emaj_tuple%' THEN
                  RAISE EXCEPTION '_gen_sql_dump_changes_group: In the EMAJ_COLUMN option, the "emaj_tuple" column must be part '
                                  'of the columns list.';
                END IF;
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The EMAJ_COLUMNS option only accepts '
                                'ALL or MIN values or a (columns list).',
                                v_option;
            END CASE;
          WHEN v_option = 'NO_EMPTY_FILES' AND NOT p_genSqlOnly THEN
            v_noEmptyFiles = TRUE;
          WHEN v_option LIKE 'ORDER_BY=%' THEN
            CASE
              WHEN v_option = 'ORDER_BY=PK' THEN
                v_orderBy = 'PK';
              WHEN v_option = 'ORDER_BY=TIME' THEN
                v_orderBy = 'TIME';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The ORDER_BY option only accepts '
                                'PK or TIME values.',
                                v_option;
            END CASE;
          WHEN v_option LIKE 'PSQL_COPY_DIR%' AND p_genSqlOnly THEN
            v_isPsqlCopy = TRUE;
            IF v_option <> 'PSQL_COPY_DIR=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The directory name must be set between ().',
                              v_option;
            END IF;
          WHEN v_option LIKE 'PSQL_COPY_OPTIONS=%' AND p_genSqlOnly THEN
            IF v_option <> 'PSQL_COPY_OPTIONS=()' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The COPY options list must be set between ().',
                               v_option;
            END IF;
          WHEN v_option = 'SEQUENCES_ONLY' THEN
            v_sequencesOnly = TRUE;
          WHEN v_option LIKE 'SQL_FORMAT=%' AND p_genSqlOnly THEN
            CASE
              WHEN v_option = 'SQL_FORMAT=RAW' THEN
                v_sqlFormat = 'RAW';
              WHEN v_option = 'SQL_FORMAT=PRETTY' THEN
                v_sqlFormat = 'PRETTY';
              ELSE
                RAISE EXCEPTION '_gen_sql_dump_changes_group: Error on the option "%". The SQL_FORMAT option only accepts '
                                'RAW or PRETTY values.',
                                v_option;
            END CASE;
          WHEN v_option = 'TABLES_ONLY' THEN
            v_tablesOnly = TRUE;
          ELSE
            IF v_option <> '' THEN
              RAISE EXCEPTION '_gen_sql_dump_changes_group: The option "%" is unknown.', v_option;
            END IF;
        END CASE;
      END LOOP;
-- Validate the relations between options.
-- SEQUENCES_ONLY and TABLES_ONLY are not compatible.
      IF v_sequencesOnly AND v_tablesOnly THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: SEQUENCES_ONLY and TABLES_ONLY options are mutually exclusive.';
      END IF;
-- PSQL_COPY_OPTIONS needs a PSQL_COPY_DIR to be set;
      IF v_psqlCopyOptions IS NOT NULL AND NOT v_isPsqlCopy THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: the PSQL_COPY_OPTIONS option needs a PSQL_COPY_DIR option to be set.';
      END IF;
-- PSQL_COPY_DIR and FORMAT=PRETTY are not compatible (for a psql \copy, the statement must be one a single line).
      IF v_isPsqlCopy AND v_sqlFormat = 'PRETTY' THEN
        RAISE EXCEPTION '_gen_sql_dump_changes_group: PSQL_COPY_DIR and FORMAT=PRETTY options are mutually exclusive.';
      END IF;
-- When one or several options need PRIMARY KEYS, check that all selected tables have a PK.
      IF v_consolidation IN ('PARTIAL', 'FULL') OR v_colsOrder = 'PK' OR v_orderBy = 'PK' THEN
        SELECT string_agg(table_name, ', ')
          INTO v_tableWithoutPkList
          FROM (
            SELECT rel_schema || '.' || rel_tblseq AS table_name
              FROM emaj.emaj_relation
              WHERE rel_group = p_groupName
                AND rel_kind = 'r' AND NOT v_sequencesOnly
                AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))
                AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
                AND rel_pk_cols IS NULL
              ORDER BY rel_schema, rel_tblseq, rel_time_range
            ) AS t;
        IF v_tableWithoutPkList IS NOT NULL THEN
          RAISE EXCEPTION '_gen_sql_dump_changes_group: A CONSOLIDATION level set to PARTIAL or FULL or a COLS_ORDER set to PK or an '
                          'ORDER_BY set to PK cannot support tables without primary key. And no primary key is defined for tables "%"',
                          v_tableWithoutPkList;
        END IF;
      END IF;
-- Reject the empty files removing if the adminpack extension is not installed.
      IF v_noEmptyFiles THEN
        PERFORM 1 FROM pg_catalog.pg_extension WHERE extname = 'adminpack';
        IF NOT FOUND THEN
          RAISE WARNING 'emaj_dump_changes_group: the NO_EMPTY_FILES option cannot be satisfied because the adminpack extension is not '
                        'installed.';
          v_noEmptyFiles = FALSE;
        END IF;
      END IF;
    END IF;
-- If table/sequence names to filter are supplied, check them.
    IF p_tblseqs IS NOT NULL THEN
      p_tblseqs = emaj._check_tblseqs_filter(p_tblseqs, ARRAY[p_groupName], v_firstMarkTimeId, v_lastMarkTimeId, FALSE);
    END IF;
-- End of checks.
-- Set options default values.
    v_consolidation = coalesce(v_consolidation, 'NONE');
    v_copyOptions = coalesce(v_copyOptions, '');
    v_colsOrder = coalesce(v_colsOrder, CASE WHEN v_consolidation = 'NONE' THEN 'LOG_TABLE' ELSE 'PK' END);
    v_emajColumnsList = coalesce(v_emajColumnsList, CASE WHEN v_consolidation = 'NONE' THEN '*' ELSE 'emaj_tuple' END);
    v_orderBy = coalesce(v_orderBy, CASE WHEN v_consolidation = 'NONE' THEN 'TIME' ELSE 'PK' END);
-- Resolve the MIN value for the EMAJ_COLUMNS option, depending on the final consolidation level.
    IF v_emajColumnsList = 'MIN' THEN
      v_emajColumnsList = CASE WHEN v_consolidation = 'NONE' THEN 'emaj_gid,emaj_tuple' ELSE 'emaj_tuple' END;
    END IF;
-- Set the ORDER_BY clause if not explicitely done in the supplied options.
    v_orderBy = coalesce(v_orderBy, CASE WHEN v_consolidation = 'NONE' THEN 'TIME' ELSE 'PK' END);
-- Create a temporary table to hold the SQL statements.
    DROP TABLE IF EXISTS emaj_temp_sql CASCADE;
    CREATE TEMP TABLE emaj_temp_sql (
      sql_stmt_number        INT,                 -- SQL statement number
      sql_line_number        INT,                 -- line number for the statement (0 for an initial comment)
      sql_rel_kind           TEXT,                -- either "table" or "sequence"
      sql_schema             TEXT,                -- the application schema
      sql_tblseq             TEXT,                -- the table or sequence name
      sql_first_mark         TEXT,                -- the first mark name
      sql_last_mark          TEXT,                -- the last mark name
      sql_group              TEXT,                -- the group name
      sql_nb_changes         BIGINT,              -- the estimated number of changes to process (NULL for sequences)
      sql_file_name_suffix   TEXT,                -- the file name suffix to use to build the output file name if the statement
                                                  --   has to be executed by a COPY statement or a \copy meta-command
      sql_text               TEXT,                -- the generated sql text
      sql_result             BIGINT               -- a column available for caller usage (if needed, some other can be added by the
                                                  --   caller with an ALTER TABLE)
    );
-- Add an initial comment reporting the supplied options.
    v_comment = format('-- Generated SQL for dumping changes in tables group "%s" %sbetween marks "%s" and "%s"%s',
                       p_groupName, CASE WHEN p_tblseqs IS NOT NULL THEN '(subset) ' ELSE '' END, p_firstMark, p_lastMark,
                       CASE WHEN p_optionsList IS NOT NULL AND p_optionsList <> '' THEN ' using options: ' || p_optionsList ELSE '' END);
    INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
      VALUES (0, 0, v_comment);
-- If the requested consolidation level is FULL, then add a SET statement to disable nested-loop nodes in the execution plan.
--   This solves a performance issue with the generated SQL statements for log tables analysis.
    IF v_consolidation = 'FULL' THEN
      v_nbStmt = v_nbStmt + 1;
      INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
        VALUES (v_nbStmt, 1, 'SET enable_nestloop = FALSE;');
    END IF;
-- Process each log table or sequence from the emaj_relation table that enters in the marks range, starting with tables.
    FOR r_rel IN
      SELECT rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind,
             rel_log_schema, rel_log_table, rel_emaj_verb_attnum, rel_pk_cols,
             CASE WHEN rel_kind = 'r' THEN 'table' ELSE 'sequence' END AS kind,
             count(*) OVER (PARTITION BY rel_schema, rel_tblseq) AS nb_time_range,
             row_number() OVER (PARTITION BY rel_schema, rel_tblseq ORDER BY rel_time_range) AS time_range_rank,
             CASE WHEN rel_kind = 'S'
                    THEN NULL
                    ELSE emaj._log_stat_tbl(emaj_relation,
                                            CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                   THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                            CASE WHEN NOT upper_inf(rel_time_range)
                                                   AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                   THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
                 END AS nb_changes
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND ((rel_kind = 'r' AND NOT v_sequencesOnly) OR (rel_kind = 'S' AND NOT v_tablesOnly))
          AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))
          AND rel_time_range && int8range(v_firstMarkTimeId, v_lastMarkTimeId,'[)')
        ORDER BY rel_kind DESC, rel_schema, rel_tblseq, rel_time_range
    LOOP
-- Compute the real mark and gid range for the relation (the relation time range can be shorter that the requested mark range).
      IF lower(r_rel.rel_time_range) <= v_firstMarkTimeId THEN
        v_relFirstMark = p_firstMark;
        v_relFirstEmajGid = v_firstEmajGid;
        v_relFirstTimeId = v_firstMarkTimeId;
      ELSE
        v_relFirstMark = coalesce((SELECT mark_name
                                     FROM emaj.emaj_mark
                                     WHERE mark_time_id = lower(r_rel.rel_time_range)
                                       AND mark_group = r_rel.rel_group
                                  ),'[deleted mark]');
        SELECT time_last_emaj_gid INTO STRICT v_firstEmajGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = lower(r_rel.rel_time_range);
        v_relFirstTimeId = lower(r_rel.rel_time_range);
      END IF;
      IF upper_inf(r_rel.rel_time_range) OR upper(r_rel.rel_time_range) >= v_lastMarkTimeId THEN
        v_relLastMark = p_lastMark;
        v_relLastEmajGid = v_lastEmajGid;
        v_relLastTimeId = v_lastMarkTimeId;
      ELSE
        v_relLastMark = coalesce((SELECT mark_name
                                     FROM emaj.emaj_mark
                                     WHERE mark_time_id = upper(r_rel.rel_time_range)
                                       AND mark_group = r_rel.rel_group
                                  ),'[deleted mark]');
        SELECT time_last_emaj_gid INTO STRICT v_lastEmajGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = upper(r_rel.rel_time_range);
        v_relLastTimeId = upper(r_rel.rel_time_range);
      END IF;
      v_nbStmt = v_nbStmt + 1;
-- Generate the comment and the statement for the table or sequence.
      IF r_rel.rel_kind = 'r' THEN
        v_comment = format('-- Dump changes for table %s.%s between marks "%s" and "%s" (%s changes)',
                           r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark, r_rel.nb_changes);
        v_stmt = emaj._gen_sql_dump_changes_tbl(r_rel.rel_log_schema, r_rel.rel_log_table, r_rel.rel_emaj_verb_attnum,
                                                r_rel.rel_pk_cols, v_relFirstEmajGid, v_relLastEmajGid, v_consolidation,
                                                v_emajColumnsList, v_colsOrder, v_orderBy);
      ELSE
        v_comment = format('-- Dump changes for sequence %s.%s between marks "%s" and "%s"',
                           r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark);
        v_stmt = emaj._gen_sql_dump_changes_seq(r_rel.rel_schema, r_rel.rel_tblseq,
                                                v_relFirstTimeId, v_relLastTimeId, v_consolidation);
      END IF;
-- If the output is a psql script, build the output file name for the \copy command.
      IF v_isPsqlCopy THEN
-- As several files may be generated for a single table or sequence, add a "_nn" to the file name suffix.
        v_copyOutputFile = emaj._build_path_name(v_psqlCopyDir, r_rel.rel_schema || '_' || r_rel.rel_tblseq ||
                             CASE WHEN r_rel.nb_time_range > 1 THEN '_' || r_rel.time_range_rank ELSE '' END || '.changes');
        v_stmt = '\copy (' || v_stmt || ') TO ' || quote_literal(v_copyOutputFile) || coalesce(' (' || v_psqlCopyOptions || ')', '');
      ELSE
        IF p_genSqlOnly THEN
          v_stmt = v_stmt || ';';
        END IF;
      END IF;
-- Record the comment on line 0.
      INSERT INTO emaj_temp_sql
        VALUES (v_nbStmt, 0, r_rel.kind, r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark, r_rel.rel_group,
                r_rel.nb_changes, NULL, v_comment);
-- Record the statement on 1 or several rows, depending on the SQL_FORMAT option.
-- In raw format, newlines and consecutive spaces are removed.
      IF v_sqlFormat = 'RAW' THEN
        v_stmt = replace(v_stmt, E'\n', ' ');
        v_stmt = regexp_replace(v_stmt, '\s{2,}', ' ', 'g');
      END IF;
      INSERT INTO emaj_temp_sql
        SELECT v_nbStmt, row_number() OVER (), r_rel.kind, r_rel.rel_schema, r_rel.rel_tblseq, v_relFirstMark, v_relLastMark,
               r_rel.rel_group, r_rel.nb_changes,
               CASE WHEN r_rel.nb_time_range > 1 THEN '_' || r_rel.time_range_rank ELSE '' END || '.changes', line
          FROM regexp_split_to_table(v_stmt, E'\n') AS line;
    END LOOP;
-- If the requested consolidation level is FULL, then add a RESET statement to revert the previous 'SET enable_nestloop = FALSE'.
    IF v_consolidation = 'FULL' THEN
      v_nbStmt = v_nbStmt + 1;
      INSERT INTO emaj_temp_sql (sql_stmt_number, sql_line_number, sql_text)
        VALUES (v_nbStmt, 1, 'RESET enable_nestloop;');
    END IF;
-- Return output parameters.
  p_nbStmt = v_nbStmt;
  p_copyOptions = v_copyOptions;
  p_noEmptyFiles = v_noEmptyFiles;
  p_isPsqlCopy = v_isPsqlCopy;
  RETURN;
  END;
$_gen_sql_dump_changes_group$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_tbl(p_logSchema TEXT, p_logTable TEXT, p_emajVerbAttnum INT, p_pkCols TEXT[],
                                                          p_firstEmajGid BIGINT, p_lastEmajGid BIGINT, p_consolidationLevel TEXT,
                                                          p_emajColumnsList TEXT, p_colsOrder TEXT, p_orderBy TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_gen_sql_dump_changes_tbl$
-- This function builds a SQL statement that snaps a log table subset, with or without consolidation.
-- Input: the log schema and table names to process, with its emaj_verb attribute number and its PK columns array,
--        the emaj sequence range corresponding to the selected mark range,
--        the requested consolidation level (NONE or PARTIAL or FULL),
--        the list of emaj columns to add to the application columns,
--        the criteria to use for the columns order (LOG_TABLE or PK)
--        the criteria to use for the ORDER BY clause (TIME or PK).
-- Output: the formatted SQL statement.
-- When CONSOLIDATION=NONE, the SQL statements return all rows from the log tables corresponding to the marks range.
-- When CONSOLIDATION=PARTIAL or FULL, there are at the most 1 row of type OLD and 1 row of type NEW for each primary key value,
--   representing the net changes for this primary key value, without taking care of the columns content.
-- When CONSOLIDATION=FULL, changes that produce the same row content are not visible.
  DECLARE
    v_logTableName           TEXT;
    v_stmt                   TEXT;
    v_allAppCols             TEXT[];
    v_allAppColumnsList      TEXT;
    v_allEmajCols            TEXT[];
    v_colsWithoutEqualOp     TEXT[];
    v_pkCols                 TEXT[];
    v_col                    TEXT;
    v_pkColsList             TEXT;
    v_prefixedPkColsList     TEXT;
    v_pkConditions           TEXT;
    v_nonPkCols              TEXT[];
    v_prefixedNonPkColsList  TEXT;
    v_columnsList            TEXT;
    v_extraEmajColumnsList   TEXT;
    v_isEmajgidInList        BOOLEAN;
    v_orderByColumns         TEXT;
    v_r1PkColumns            TEXT;
    v_r1R2PkCond             TEXT;
    v_r1R2NonPkCond          TEXT;
    v_conditions             TEXT;
    v_template               TEXT;
  BEGIN
-- Build columns arrays (identifier quoted).
    v_logTableName = quote_ident(p_logSchema) || '.' || quote_ident(p_logTable);
    v_stmt = 'SELECT array_agg(quote_ident(attname)) FILTER (WHERE attnum < %s),'
             '       string_agg(''tbl.'' || quote_ident(attname), '','') FILTER (WHERE attnum < %s),'
             '       array_agg(quote_ident(attname)) FILTER (WHERE attnum >= %s),'
             '       array_agg(quote_ident(attname)) FILTER (WHERE no_equal_operator)'
             '  FROM ('
             '  SELECT attname, attnum, oprname IS NULL AS no_equal_operator'
             '    FROM pg_catalog.pg_attribute'
             '         JOIN pg_catalog.pg_type ON (atttypid=pg_type.oid)'
             '         LEFT OUTER JOIN pg_catalog.pg_operator ON (pg_type.oid = oprleft AND oprright = oprleft AND oprname = ''='')'
             '    WHERE attrelid = %L::regclass'
             '      AND attnum > 0 AND NOT attisdropped'
             '  ORDER BY attnum) AS t';
    EXECUTE format(v_stmt,
                   p_emajVerbAttnum, p_emajVerbAttnum, p_emajVerbAttnum, v_logTableName)
      INTO v_allAppCols, v_allAppColumnsList, v_allEmajCols, v_colsWithoutEqualOp;
    v_pkCols = array_agg(quote_ident(col)) FROM unnest (p_pkCols) AS col;
    SELECT array_agg(col ORDER BY rownum), string_agg('tbl.' || col, ',' ORDER BY rownum)
      INTO v_nonPkCols, v_prefixedNonPkColsList
      FROM (SELECT col, row_number() OVER () AS rownum FROM unnest(v_allAppCols) AS col WHERE col <> ALL(v_pkCols)) AS t;
-- Check the emaj columns from the EMAJ_COLUMNS option, for this table (emaj columns may differ from a table to another).
    IF p_emajColumnsList <> '*' THEN
      FOREACH v_col IN ARRAY string_to_array(p_emajColumnsList, ',')
        LOOP
        IF quote_ident(v_col) <> ALL(v_allEmajCols) THEN
          RAISE EXCEPTION '_gen_sql_dump_changes_tbl: The emaj column "%" from the EMAJ_COLUMNS option (%) is not valid for table %.%.',
                          v_col, p_emajColumnsList, p_logSchema, p_logTable;
        END IF;
      END LOOP;
    END IF;
-- Build the PK columns lists and conditions.
    v_pkColsList = array_to_string(v_pkCols, ',');
    v_prefixedPkColsList = 'tbl.' || array_to_string(v_pkCols, ',tbl.');
    SELECT string_agg('tbl.' || attname || ' = keys.' || attname, ' AND ')
      INTO v_pkConditions
      FROM unnest(v_pkCols) AS attname;
-- Build the columns list.
    IF p_colsOrder = 'LOG_TABLE' THEN
      IF p_emajColumnsList = '*' THEN
        v_columnsList = 'tbl.*';
      ELSE
        v_columnsList = v_allAppColumnsList || ',' || p_emajColumnsList;
      END IF;
    ELSE           -- COLS_ORDER=PK
      IF p_emajColumnsList = '*' THEN
        p_emajColumnsList = array_to_string(v_allEmajCols, ',');
      END IF;
      v_extraEmajColumnsList = replace(replace(p_emajColumnsList, 'emaj_tuple,', ''), 'emaj_tuple', '');
      v_isEmajgidInList = (position('emaj_gid' IN v_extraEmajColumnsList) > 0);
      IF v_isEmajgidInList THEN
        v_extraEmajColumnsList = replace(replace(v_extraEmajColumnsList, 'emaj_gid,', ''), 'emaj_gid', '');
      END IF;
      v_columnsList = v_prefixedPkColsList ||
                      CASE WHEN v_isEmajgidInList THEN ',emaj_gid,emaj_tuple' ELSE ',emaj_tuple' END ||
                      CASE WHEN v_prefixedNonPkColsList <> '' THEN ',' || v_prefixedNonPkColsList ELSE '' END ||
                      CASE WHEN v_extraEmajColumnsList <> '' THEN ',' || v_extraEmajColumnsList ELSE '' END;
    END IF;
-- Build the conditions on emaj_gid.
    v_conditions = 'emaj_gid > ' || p_firstEmajGid || coalesce(' AND emaj_gid <= ' || p_lastEmajGid, '');
-- Build the ORDER BY columns list.
    IF p_orderBy = 'TIME' THEN
      v_orderByColumns = 'emaj_gid, emaj_tuple DESC';
    ELSE
      v_orderByColumns = 'tbl.' || array_to_string(v_pkCols, ',tbl.') || ', emaj_gid, emaj_tuple DESC';
    END IF;
-- Build the final statement.
    CASE p_consolidationLevel
      WHEN 'NONE' THEN
        v_template =
          E'SELECT %s\n'
           '  FROM %I.%I tbl\n'
           '  WHERE %s\n'
           '    AND emaj_tuple IN (''OLD'',''NEW'')\n'
           '  ORDER BY %s';
        v_stmt = format(v_template,
                        v_columnsList, p_logSchema, p_logTable, v_conditions, v_orderByColumns);
      WHEN 'PARTIAL' THEN
        v_template =
          E'WITH keys AS (\n'
           '  SELECT %s, min(emaj_gid) AS min_gid, max(emaj_gid) AS max_gid\n'
           '    FROM %I.%I\n'
           '    WHERE %s\n'
           '      AND emaj_tuple IN (''OLD'',''NEW'')\n'
           '    GROUP BY %s\n'
           '  ) \n'
           'SELECT %s\n'
           '  FROM %I.%I tbl\n'
           '       JOIN keys ON (%s)\n'
           '  WHERE (tbl.emaj_tuple = ''OLD'' AND tbl.emaj_gid = keys.min_gid)\n'
           '     OR (tbl.emaj_tuple = ''NEW'' AND tbl.emaj_gid = keys.max_gid)\n'
           '  ORDER BY %s';
        v_stmt = format(v_template,
                        v_pkColsList, p_logSchema, p_logTable, v_conditions, v_pkColsList,
                        v_columnsList, p_logSchema, p_logTable, v_pkConditions, v_orderByColumns);
      WHEN 'FULL' THEN
-- Some additional SQL pieces for full consolidation.
        v_r1PkColumns = 'r1.' || array_to_string(v_pkCols, ',r1.');
        SELECT string_agg(condition, ' AND ') INTO v_r1R2PkCond
          FROM (
            SELECT 'r1.' || col || '=r2.' || col
              FROM unnest(v_pkCols) AS col
            ) AS t(condition);
        SELECT string_agg(condition, ' AND ') INTO v_r1R2NonPkCond
          FROM (
            SELECT CASE
                     WHEN col = ANY(v_colsWithoutEqualOp) THEN     -- columns without '=' operator are casted into TEXT for the comparison
                          '(r1.' || col || '::text=r2.' || col || '::text OR (r1.' || col || ' IS NULL AND r2.' || col || ' IS NULL))'
                     ELSE '(r1.' || col || '=r2.' || col || ' OR (r1.' || col || ' IS NULL AND r2.' || col || ' IS NULL))'
                   END
              FROM unnest(v_nonPkCols) AS col
            ) AS t(condition);
-- And the final statement.
        v_template =
          E'WITH keys AS (\n'
           '  SELECT %s, min(emaj_gid) AS min_gid, max(emaj_gid) AS max_gid\n'
           '    FROM %I.%I\n'
           '    WHERE %s\n'
           '      AND emaj_tuple IN (''OLD'',''NEW'')\n'
           '    GROUP BY %s\n'
           '  ),\n'
           '     consolidated AS (\n'
           '  SELECT tbl.*\n'
           '    FROM %I.%I tbl\n'
           '         JOIN keys ON (%s)\n'
           '    WHERE (tbl.emaj_tuple = ''OLD'' AND tbl.emaj_gid = keys.min_gid)\n'
           '       OR (tbl.emaj_tuple = ''NEW'' AND tbl.emaj_gid = keys.max_gid)\n'
           '  ),\n'
           '     unchanged_keys AS (\n'
           '  SELECT %s\n'
           '    FROM consolidated r1\n'
           '         JOIN consolidated r2 ON (%s)\n'
           '    WHERE r1.emaj_tuple = ''OLD'' AND r2.emaj_tuple = ''NEW''\n'
           '      AND %s\n'
           '  )\n'
           '  SELECT %s\n'
           '    FROM consolidated tbl\n'
           '    WHERE NOT EXISTS (SELECT 0 FROM unchanged_keys keys WHERE %s)\n'
           '    ORDER BY %s';
        v_stmt = format(v_template,
                        v_pkColsList, p_logSchema, p_logTable, v_conditions, v_pkColsList,
                        p_logSchema, p_logTable, v_pkConditions,
                        v_r1PkColumns, v_r1R2PkCond, v_r1R2NonPkCond,
                        v_columnsList, v_pkConditions, v_orderByColumns);
    END CASE;
    IF v_stmt IS NULL THEN
      RAISE EXCEPTION '_gen_sql_dump_changes_tbl: Internal error - the generated statement is NULL.';
    END IF;
    RETURN v_stmt;
  END;
$_gen_sql_dump_changes_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(p_groupName TEXT, p_dir TEXT, p_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_snap_group$
-- This function creates a file for each table and sequence belonging to the group.
-- For tables, these files contain all rows sorted on primary key.
-- For sequences, they contain a single row describing the sequence.
-- To do its job, the function performs COPY TO statement, with all default parameters.
-- For table without primary key, rows are sorted on all columns.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability:
--   - to create the directory (with proper permissions allowing the cluster to write into) before the emaj_snap_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name,
--        the absolute pathname of the directory where the files are to be created and the options to used in the COPY TO statements
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_colList                TEXT;
    v_pathName               TEXT;
    v_stmt                   TEXT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'BEGIN', p_groupName, p_dir);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the supplied directory is not null.
    IF p_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_group: The directory parameter cannot be NULL.';
    END IF;
-- Check the copy options parameter doesn't contain unquoted ; that could be used for sql injection.
    IF regexp_replace(p_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: The COPY options parameter format is invalid.';
    END IF;
-- For each table/sequence of the emaj_relation table.
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = p_groupName
        ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_pathName = emaj._build_path_name(p_dir, r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap');
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- It is a table.
--   Build the order by column list.
          IF EXISTS
               (SELECT 0
                  FROM pg_catalog.pg_class
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                       JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                  WHERE contype = 'p'
                    AND nspname = r_tblsq.rel_schema
                    AND relname = r_tblsq.rel_tblseq
               ) THEN
--   The table has a pkey.
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList
              FROM
                (SELECT attname
                   FROM pg_catalog.pg_attribute
                        JOIN pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
                   WHERE attnum = ANY (indkey)
                     AND indrelid = v_fullTableName::regclass
                     AND indisprimary
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          ELSE
--   The table has no pkey.
            SELECT string_agg(quote_ident(attname), ',') INTO v_colList
              FROM
                (SELECT attname
                   FROM pg_catalog.pg_attribute
                   WHERE attrelid = v_fullTableName::regclass
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          END IF;
--   Dump the table
          v_stmt= '(SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ')';
          PERFORM emaj._copy_to_file(v_stmt, v_pathName, p_copyOptions);
        WHEN 'S' THEN
-- If it is a sequence, the statement has no order by.
          v_stmt = '(SELECT sequencename, rel.last_value, start_value, increment_by, max_value, '
                || 'min_value, cache_size, cycle, rel.is_called '
                || 'FROM ' || v_fullTableName || ' rel, pg_catalog.pg_sequences '
                || 'WHERE schemaname = '|| quote_literal(r_tblsq.rel_schema) || ' AND sequencename = '
                || quote_literal(r_tblsq.rel_tblseq) ||')';
--    Dump the sequence properties.
          PERFORM emaj._copy_to_file(v_stmt, v_pathName, p_copyOptions);
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || transaction_timestamp()) || ')';
    PERFORM emaj._copy_to_file(v_stmt, p_dir || '/_INFO', NULL);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_version()
RETURNS TEXT LANGUAGE SQL STABLE AS
$$
-- This function returns the current emaj extension version.
SELECT verh_version FROM emaj.emaj_version_hist WHERE upper_inf(verh_time_range);
$$;
COMMENT ON FUNCTION emaj.emaj_get_version() IS
$$Returns the current emaj version.$$;

CREATE OR REPLACE FUNCTION emaj._purge_histories(p_retentionDelay INTERVAL DEFAULT NULL)
RETURNS VOID LANGUAGE plpgsql AS
$_purge_histories$
-- This function purges the emaj histories by deleting all rows prior the 'history_retention' parameter, but
--   without deleting event traces neither after the oldest mark or after the oldest not committed or aborted rollback operation.
-- It purges oldest rows from the following tables:
--    emaj_hist, emaj_log_sessions, emaj_group_hist, emaj_rel_hist, emaj_relation_change, emaj_rlbk_session and emaj_rlbk_plan.
-- The function is called at start group time and when oldest marks are deleted.
-- It is also called by the emaj_purge_histories() function.
-- A retention delay >= 100 years means infinite.
-- Input: retention delay ; if supplied, it overloads the history_retention parameter from the emaj_param table.
  DECLARE
    v_delay                  INTERVAL;
    v_datetimeLimit          TIMESTAMPTZ;
    v_maxTimeId              BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbDeletedRows          BIGINT;
    v_nbPurgedRlbk           BIGINT;
    v_nbPurgedRelChanges     BIGINT;
    v_wording                TEXT = '';
  BEGIN
-- Compute the retention delay to use.
    SELECT coalesce(p_retentionDelay,
                    (SELECT param_value_interval
                       FROM emaj.emaj_param
                       WHERE param_key = 'history_retention'
                    ),'1 YEAR')
      INTO v_delay;
-- Immediately exit if the delay is infinity.
    IF v_delay >= INTERVAL '100 years' THEN
      RETURN;
    END IF;
-- Compute the timestamp limit.
    SELECT least(
                                         -- compute the timestamp limit from the retention delay value
        (SELECT current_timestamp - v_delay),
                                         -- get the transaction timestamp of the oldest known mark
        (SELECT min(time_tx_timestamp)
           FROM emaj.emaj_mark
                JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)),
                                         -- get the transaction timestamp of the oldest non committed or aborted rollback
        (SELECT min(time_tx_timestamp)
           FROM emaj.emaj_rlbk
                JOIN emaj.emaj_time_stamp ON (time_id = rlbk_time_id)
           WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED')))
      INTO v_datetimeLimit;
-- Get the greatest timestamp identifier corresponding to the timeframe to purge, if any.
    SELECT max(time_id) INTO v_maxTimeId
      FROM emaj.emaj_time_stamp
      WHERE time_tx_timestamp < v_datetimeLimit;
-- Delete oldest rows from emaj_hist.
    DELETE FROM emaj.emaj_hist
      WHERE hist_datetime < v_datetimeLimit;
    GET DIAGNOSTICS v_nbDeletedRows = ROW_COUNT;
    IF v_nbDeletedRows > 0 THEN
      v_wording = v_nbDeletedRows || ' emaj_hist rows deleted';
    END IF;
-- Delete oldest rows from emaj_log_session.
    DELETE FROM emaj.emaj_log_session
      WHERE upper(lses_time_range) - 1 < v_maxTimeId;
    GET DIAGNOSTICS v_nbDeletedRows = ROW_COUNT;
    IF v_nbDeletedRows > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbDeletedRows || ' log session rows deleted';
    END IF;
-- Delete oldest rows from emaj_group_hist.
    DELETE FROM emaj.emaj_group_hist
      WHERE upper(grph_time_range) - 1 < v_maxTimeId;
    GET DIAGNOSTICS v_nbDeletedRows = ROW_COUNT;
    IF v_nbDeletedRows > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbDeletedRows || ' group history rows deleted';
    END IF;
-- Delete oldest rows from emaj_rel_hist.
    DELETE FROM emaj.emaj_rel_hist
      WHERE upper(relh_time_range) < v_maxTimeId;
    GET DIAGNOSTICS v_nbDeletedRows = ROW_COUNT;
    IF v_nbDeletedRows > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbDeletedRows || ' relation history rows deleted';
    END IF;
-- Purge the emaj_relation_change table.
    WITH deleted_relation_change AS
      (DELETE FROM emaj.emaj_relation_change
         WHERE rlchg_time_id <= v_maxTimeId
         RETURNING rlchg_time_id
      )
      SELECT COUNT (DISTINCT rlchg_time_id) INTO v_nbPurgedRelChanges
        FROM deleted_relation_change;
    IF v_nbPurgedRelChanges > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedRelChanges || ' relation changes deleted';
    END IF;
-- Get the greatest rollback identifier to purge.
    SELECT max(rlbk_id) INTO v_maxRlbkId
      FROM emaj.emaj_rlbk
      WHERE rlbk_time_id <= v_maxTimeId;
-- And purge the emaj_rlbk_plan and emaj_rlbk_session tables.
    IF v_maxRlbkId IS NOT NULL THEN
      DELETE FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id <= v_maxRlbkId;
      WITH deleted_rlbk AS
        (DELETE FROM emaj.emaj_rlbk_session
           WHERE rlbs_rlbk_id <= v_maxRlbkId
           RETURNING rlbs_rlbk_id
        )
        SELECT COUNT(DISTINCT rlbs_rlbk_id) INTO v_nbPurgedRlbk
          FROM deleted_rlbk;
      v_wording = v_wording || ' ; ' || v_nbPurgedRlbk || ' rollback events deleted';
    END IF;
-- Record the purge into the history if there are significant data.
    IF v_wording <> '' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_wording)
        VALUES ('PURGE_HISTORIES', v_wording);
    END IF;
--
    RETURN;
  END;
$_purge_histories$;

CREATE OR REPLACE FUNCTION emaj._export_param_conf()
RETURNS JSON LANGUAGE plpgsql AS
$_export_param_conf$
-- This function generates a JSON formatted structure representing the parameters registered in the emaj_param table.
-- All parameters are extracted, except the "emaj_version" key that is directly linked to the extension and thus is not updatable.
-- The E-Maj version is already displayed in the generated comment at the beginning of the structure.
-- Output: the parameters content in JSON format
  DECLARE
    v_params                 TEXT;
    v_paramsJson             JSON;
    r_param                  RECORD;
  BEGIN
-- Build the header of the JSON structure.
    v_params = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || current_timestamp || E'",\n' ||
               E'  "_comment": "Known parameter keys: dblink_user_password, history_retention (default = 1 year), alter_log_table, '
                'avg_row_rollback_duration (default = 00:00:00.0001), avg_row_delete_log_duration (default = 00:00:00.00001), '
                'avg_fkey_check_duration (default = 00:00:00.00002), fixed_step_rollback_duration (default = 00:00:00.0025), '
                'fixed_table_rollback_duration (default = 00:00:00.001) and fixed_dblink_rollback_duration (default = 00:00:00.004).",\n';
-- Build the parameters description.
    v_params = v_params || E'  "parameters": [\n';
    FOR r_param IN
      SELECT param_key AS key,
             coalesce(to_json(param_value_text),
                      to_json(param_value_interval),
                      to_json(param_value_boolean),
                      to_json(param_value_numeric),
                      'null') as value
        FROM emaj.emaj_param
             JOIN (VALUES (1::INT, 'dblink_user_password'), (2, 'history_retention'), (3, 'alter_log_table'),
                          (10, 'avg_row_rollback_duration'), (11, 'avg_row_delete_log_duration'),
                          (12, 'avg_fkey_check_duration'), (13, 'fixed_step_rollback_duration'),
                          (14, 'fixed_table_rollback_duration'), (15, 'fixed_dblink_rollback_duration')
                  ) AS p(rank,key) ON (p.key = param_key)
        ORDER BY rank
    LOOP
      v_params = v_params || E'    {\n'
                          ||  '      "key": ' || to_json(r_param.key) || E',\n'
                          ||  '      "value": ' || r_param.value || E'\n'
                          || E'    },\n';
    END LOOP;
    v_params = v_params || E'  ]\n';
-- Build the trailer and remove illicite commas at the end of arrays and attributes lists.
    v_params = v_params || E'}\n';
    v_params = regexp_replace(v_params, E',(\n *(\]|}))', '\1', 'g');
-- Test the JSON format by casting the text structure to json and report a warning in case of problem
-- (this should not fail, unless the function code is bogus).
    BEGIN
      v_paramsJson = v_params::JSON;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '_export_param_conf: The generated JSON structure is not properly formatted. '
                        'Please report the bug to the E-Maj project.';
    END;
--
    RETURN v_paramsJson;
  END;
$_export_param_conf$;

CREATE OR REPLACE FUNCTION emaj._import_param_conf(p_json JSON, p_deleteCurrentConf BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_import_param_conf$
-- This function processes a JSON formatted structure representing the E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- The "emaj_version" parameter key is always left unchanged because it is a constant linked to the extension itself.
-- The expected JSON structure must contain an array like:
-- { "parameters": [
--      { "key": "...", "value": "..." },
--      { ... }
--    ] }
-- If the "value" attribute is missing or null, the parameter is removed from the emaj_param table, and the parameter will be set at
--   its default value
-- Input: - the parameter configuration structure in JSON format
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_parameters             JSON;
    v_nbParam                INT;
    v_key                    TEXT;
    v_value                  TEXT;
    r_msg                    RECORD;
    r_param                  RECORD;
  BEGIN
-- Performs various checks on the parameters content described in the supplied JSON structure.
    FOR r_msg IN
      SELECT rpt_message
        FROM emaj._check_json_param_conf(p_json)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_int_var_1
    LOOP
      RAISE WARNING '_import_param_conf : %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_param_conf: One or several errors have been detected in the supplied JSON structure.';
    END IF;
-- OK
    v_parameters = p_json #> '{"parameters"}';
-- If requested, delete the existing parameters.
-- The trigger on emaj_param records the deletions into emaj_hist.
    IF p_deleteCurrentConf THEN
      DELETE FROM emaj.emaj_param;
    END IF;
-- Process each parameter.
    v_nbParam = 0;
    FOR r_param IN
        SELECT param
          FROM json_array_elements(v_parameters) AS t(param)
      LOOP
-- Get each parameter from the list.
        v_key = r_param.param ->> 'key';
        v_value = r_param.param ->> 'value';
        v_nbParam = v_nbParam + 1;
-- If there is no value to set, deleted the parameter, if it exists.
        IF v_value IS NULL THEN
          DELETE FROM emaj.emaj_param
            WHERE param_key = v_key;
        ELSE
-- Insert or update the parameter in the emaj_param table, selecting the right parameter value column type depending on the key.
          IF v_key IN ('dblink_user_password', 'alter_log_table') THEN
            INSERT INTO emaj.emaj_param (param_key, param_value_text)
              VALUES (v_key, v_value)
              ON CONFLICT (param_key) DO
                UPDATE SET param_value_text = v_value
                  WHERE EXCLUDED.param_key = v_key;
          ELSIF v_key IN ('history_retention', 'avg_row_rollback_duration', 'avg_row_delete_log_duration', 'avg_fkey_check_duration',
                         'fixed_step_rollback_duration', 'fixed_table_rollback_duration', 'fixed_dblink_rollback_duration') THEN
            INSERT INTO emaj.emaj_param (param_key, param_value_interval)
              VALUES (v_key, v_value::INTERVAL)
              ON CONFLICT (param_key) DO
                UPDATE SET param_value_interval = v_value::INTERVAL
                  WHERE EXCLUDED.param_key = v_key;
          END IF;
        END IF;
      END LOOP;
--
    RETURN v_nbParam;
  END;
$_import_param_conf$;

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

GRANT EXECUTE ON FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_marks_range(p_groupNames TEXT[], INOUT p_firstMark TEXT, INOUT p_lastMark TEXT,
                          p_finiteUpperBound BOOLEAN, p_checkLogSession BOOLEAN,
                          OUT p_firstMarkTimeId BIGINT, OUT p_lastMarkTimeId BIGINT,
                          OUT p_firstMarkTs TIMESTAMPTZ, OUT p_lastMarkTs TIMESTAMPTZ,
                          OUT p_firstMarkEmajGid BIGINT, OUT p_lastMarkEmajGid BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_version() TO emaj_viewer;

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

-- Delete the version id in the emaj_param table.
ALTER TABLE emaj.emaj_param DISABLE TRIGGER emaj_param_change_trg;
DELETE FROM emaj.emaj_param WHERE param_key = 'emaj_version';
ALTER TABLE emaj.emaj_param ENABLE TRIGGER emaj_param_change_trg;

-- Update the previous versions from emaj_version_hist and insert the new version into this same table.
-- The upgrade start time (as recorded in emaj_hist) is used as upper time bound of the previous version.
WITH start_time_data AS (
  SELECT hist_datetime AS start_time, clock_timestamp() - hist_datetime AS duration
    FROM emaj.emaj_hist
    ORDER BY hist_id DESC
    LIMIT 1
  ), updated_versions AS (
  UPDATE emaj.emaj_version_hist
    SET verh_time_range = TSTZRANGE(lower(verh_time_range), start_time, '[]')
    FROM start_time_data
    WHERE upper_inf(verh_time_range)
  )
  INSERT INTO emaj.emaj_version_hist (verh_version, verh_time_range, verh_install_duration)
    SELECT '<devel>', TSTZRANGE(clock_timestamp(), null, '[]'), duration
      FROM start_time_data;

-- Insert the upgrade end record into the emaj_hist table.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.3.1 completed');

-- Post installation checks.
DO
$tmp$
  DECLARE
  BEGIN
-- Check the max_prepared_transactions GUC value.
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback '
                    'is possible.', current_setting('max_prepared_transactions');
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
