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

-- Table containing the log sessions, i.e. the history of groups starts and stops.
CREATE TABLE emaj.emaj_log_session (
  lses_group                   TEXT        NOT NULL,       -- group name
  lses_time_range              INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
  lses_group_creation_time_id  BIGINT,                     -- group's creation time id
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
--

CREATE MATERIALIZED VIEW emaj.emaj_tmp_hist AS (
  SELECT *
    FROM emaj.emaj_hist
    WHERE hist_function IN
            ('CREATE_GROUP', 'DROP_GROUP', 'IMPORT_GROUPS',
             'ASSIGN_TABLE', 'REMOVE_TABLE', 'MOVE_TABLE', 'MODIFY_TABLE',
             'ASSIGN_TABLES', 'REMOVE_TABLES', 'MOVE_TABLES', 'MODIFY_TABLES',
             'ASSIGN_SEQUENCE', 'REMOVE_SEQUENCE', 'MOVE_SEQUENCE',
             'ASSIGN_SEQUENCES', 'REMOVE_SEQUENCES', 'MOVE_SEQUENCES',
             'START_GROUP', 'STOP_GROUP', 'START_GROUPS', 'STOP_GROUPS',
             'SET_MARK_GROUP', 'SET_MARK_GROUPS', 'ROLLBACK_GROUP', 'ROLLBACK_GROUPS')
      AND hist_event IN ('BEGIN', 'END')
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
    GROUP BY 1,2,3,4
    ORDER BY b.hist_id
  );

CREATE TEMPORARY TABLE emaj_tmp_timed_event (
  event_hist_id_begin        BIGINT,
  event_hist_id_end          BIGINT,
  event_time_id              INTEGER,
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
-- Once this events history is built, update the stable emaj tables
--

-- emaj_time_stamp: update the time_event columns for marks generated at start group or stop group events.
--   Set the time_event to 'S' or 'X' instead of 'M' for the start group or stop group operations respectively.
UPDATE emaj.emaj_time_stamp
  SET time_event = CASE WHEN event_function IN ('START_GROUP', 'START_GROUPS') THEN 'S' ELSE 'X' END
  FROM emaj_tmp_timed_event
  WHERE event_function IN ('START_GROUP', 'START_GROUPS', 'STOP_GROUP', 'STOP_GROUPS')
    AND time_id = event_time_id
    AND time_event = 'M';

-- emaj_log_session: create the log session rows.
DO
$do$
  DECLARE
    v_createTimeId           BIGINT;
    v_startTimeId            BIGINT;
    v_1stMarkAfterStopTimeId BIGINT;
    v_nbMark                 INTEGER;
    v_nbChange               BIGINT;
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    r_group                  RECORD;
    r_event                  RECORD;
    r_rel                    RECORD;
  BEGIN
    RAISE NOTICE 'Starting the rows generation for the new emaj_log_session table.';
    RAISE NOTICE 'Based on existing history and logs content, #marks and #changes statistics may be inaccurate.';
-- Process each existing group in sequence (log sessions for dropped group are not rebuilt).
    FOR r_group IN
      SELECT group_name, group_creation_time_id, group_is_logging
        FROM emaj.emaj_group
        ORDER BY group_name
    LOOP
      RAISE NOTICE '==> Processing group "%" (%)',
                   r_group.group_name, CASE WHEN r_group.group_is_logging THEN 'LOGGING' ELSE 'STOPPED' END;
-- Initialize variables.
      v_nbMark = 0;
      v_createTimeId = NULL;
      v_startTimeId = NULL;
      v_1stMarkAfterStopTimeId = NULL;
-- Look at each create/start/stop/set_mark event for the group.
      FOR r_event IN
        SELECT *, substring(event_function, '#"%#"_GROUP%', '#') AS abbrev_function
          FROM emaj_tmp_timed_event
          WHERE event_function IN ('CREATE_GROUP', 'START_GROUP', 'START_GROUPS',
                                   'STOP_GROUP', 'STOP_GROUPS', 'SET_MARK_GROUP','SET_MARK_GROUPS')
            AND event_object ~ ('(^|,)' || r_group.group_name || '(,|$)')
          ORDER BY event_hist_id_end
      LOOP
        CASE r_event.abbrev_function
          WHEN 'CREATE' THEN
-- A group creation => keep the creation time id.
            v_createTimeId = r_event.event_time_id;
          WHEN 'START' THEN
-- A group start => keep the start time id.
            v_nbMark = 1;
            v_startTimeId = r_event.event_time_id;
          WHEN 'STOP' THEN
-- A group stop => generate a log session if there is at least 1 mark and reset variables.
            IF v_nbMark > 0 THEN
              v_startTimeId = coalesce(v_startTimeId, v_1stMarkAfterStopTimeId);
              IF v_startTimeId > r_group.group_creation_time_id THEN
                v_createTimeId = r_group.group_creation_time_id;
              END IF;
-- Get the changes statistics on the time frame.
              v_nbChange = NULL;
              FOR r_rel IN
                SELECT rel_schema, rel_tblseq, rel_time_range
                  FROM emaj.emaj_relation
                  WHERE rel_group = r_group.group_name
                    AND rel_kind = 'r'
                    AND rel_time_range && int8range(v_startTimeId, r_event.event_time_id, '[)')
              LOOP
                SELECT tbl_log_seq_last_val INTO v_beginLastValue
                  FROM emaj.emaj_table
                  WHERE tbl_schema = r_rel.rel_schema
                    AND tbl_name = r_rel.rel_tblseq
                    AND tbl_time_id = greatest(v_startTimeId, lower(r_rel.rel_time_range));
                IF FOUND THEN
                  SELECT tbl_log_seq_last_val INTO v_endLastValue
                    FROM emaj.emaj_table
                    WHERE tbl_schema = r_rel.rel_schema
                      AND tbl_name = r_rel.rel_tblseq
                      AND tbl_time_id = least(r_event.event_time_id, upper(r_rel.rel_time_range));
                  IF FOUND THEN
                    v_nbChange = coalesce(v_nbChange, 0) + v_endLastValue - v_beginLastValue;
                  END IF;
                END IF;
              END LOOP;
-- Create the log session.
              RAISE NOTICE '  --> Create a log session for time interval [% , %) (% marks, % changes)',
                v_startTimeId, r_event.event_time_id, v_nbMark, v_nbChange;
              INSERT INTO emaj.emaj_log_session VALUES
                (r_group.group_name, int8range(v_startTimeId, r_event.event_time_id, '[)'), v_createTimeId, v_nbMark, v_nbChange);
            END IF;
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
      IF r_group.group_is_logging THEN
        IF v_startTimeId IS NOT NULL OR v_1stMarkAfterStopTimeId IS NOT NULL THEN
-- If the group is in logging state and a start group or a mark set is known, create the current log session.
          v_startTimeId = coalesce(v_startTimeId, v_1stMarkAfterStopTimeId);
          IF v_startTimeId > r_group.group_creation_time_id THEN
            v_createTimeId = r_group.group_creation_time_id;
          END IF;
          v_nbChange = NULL;
          FOR r_rel IN
            SELECT rel_schema, rel_tblseq, rel_time_range, rel_log_schema, rel_log_sequence
              FROM emaj.emaj_relation
              WHERE rel_group = r_group.group_name
                AND rel_kind = 'r'
                AND rel_time_range @> v_startTimeId
          LOOP
            SELECT tbl_log_seq_last_val INTO v_beginLastValue
              FROM emaj.emaj_table
              WHERE tbl_schema = r_rel.rel_schema
                AND tbl_name = r_rel.rel_tblseq
                AND tbl_time_id = greatest(v_startTimeId, lower(r_rel.rel_time_range));
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
                v_nbChange = coalesce(v_nbChange, 0) + v_endLastValue - v_beginLastValue;
              END IF;
            END IF;
          END LOOP;
-- Create the log session.
          RAISE NOTICE '  --> Create a log session for time interval [% , NULL) (% marks, % changes)',
            v_startTimeId, v_nbMark, v_nbChange;
          INSERT INTO emaj.emaj_log_session VALUES
            (r_group.group_name, int8range(v_startTimeId, NULL, '[)'), v_createTimeId, v_nbMark, v_nbChange);
        ELSE
          RAISE WARNING '*** Tables group % is logging, but the last group start event has not been found. No log session is generated.',
                        r_group.group_name;
        END IF;
      ELSE
-- If the group is stopped but the last group start is not followed by a group stop, warn and do not created a log session.
        IF v_startTimeId IS NOT NULL THEN
          RAISE WARNING '*** Tables group % is stopped, but the last group stop event has not been found. No log session is generated.',
                         r_group.group_name;
        END IF;
      END IF;
    END LOOP;
  END;
$do$;

DROP TABLE IF EXISTS emaj_tmp_timed_event;
DROP MATERIALIZED VIEW IF EXISTS emaj.emaj_tmp_hist CASCADE;

--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--
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
DROP FUNCTION IF EXISTS emaj._check_marks_range(P_GROUPNAMES TEXT[],INOUT P_FIRSTMARK TEXT,INOUT P_LASTMARK TEXT,P_FINITEUPPERBOUND BOOLEAN,OUT P_FIRSTMARKTIMEID BIGINT,OUT P_LASTMARKTIMEID BIGINT,OUT P_FIRSTMARKTS TIMESTAMPTZ,OUT P_LASTMARKTS TIMESTAMPTZ,OUT P_FIRSTMARKEMAJGID BIGINT,OUT P_LASTMARKEMAJGID BIGINT);
DROP FUNCTION IF EXISTS emaj._log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);
DROP FUNCTION IF EXISTS emaj._detailed_log_stat_groups(P_GROUPNAMES TEXT[],P_MULTIGROUP BOOLEAN,P_FIRSTMARK TEXT,P_LASTMARK TEXT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
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
              AND lses_time_range @> int8range(p_firstMarkTimeId, NULL, '[)');
          IF NOT FOUND THEN
            RAISE WARNING 'Since mark "%", the tables group "%" has not been always in logging state. '
                          'Some data changes may not have been recorded.', p_firstMark, v_groupName;
          END IF;
        ELSE
          PERFORM 0
            FROM emaj.emaj_log_session
            WHERE lses_group = v_groupName
              AND lses_time_range @> int8range(p_firstMarkTimeId, p_lastMarkTimeId, '[)');
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

CREATE OR REPLACE FUNCTION emaj._drop_group(p_groupName TEXT, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group.
-- It also drops log schemas that are not useful any more.
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
  DECLARE
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbRel                  INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp('D') INTO v_timeId;
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
    PERFORM emaj._drop_log_schemas(CASE WHEN p_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END, p_isForced);
-- Delete group row from the emaj_group table.
-- By cascade, it also deletes rows from emaj_mark.
    DELETE FROM emaj.emaj_group
      WHERE group_name = p_groupName
      RETURNING group_nb_table + group_nb_sequence INTO v_nbRel;
-- Update the last log session for the group to set the time range upper bound, if it is infinity
    UPDATE emaj.emaj_log_session
      SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[)')
      WHERE lses_group = p_groupName
        AND upper_inf(lses_time_range);
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
--
    RETURN v_nbRel;
  END;
$_drop_group$;

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
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_eventTriggers          TEXT[];
    r_tblsq                  RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','),
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
      SELECT emaj._set_time_stamp('S') INTO v_timeId;
-- Check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(p_groupNames);
-- Purge the history tables, if needed.
      PERFORM emaj._purge_histories();
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
-- Insert log sessions start in emaj_log_session.
--   lses_marks is already set to 1 as it will not be incremented at the first mark set.
      INSERT INTO emaj.emaj_log_session
        SELECT group_name, int8range(v_timeId, NULL, '[)'), group_creation_time_id, 1, 0
          FROM emaj.emaj_group
          WHERE group_name = ANY (p_groupNames);
-- Set the first mark for each group.
      PERFORM emaj._set_mark_groups(p_groupNames, v_markName, p_multiGroup, TRUE, NULL, v_timeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'START_GROUPS' ELSE 'START_GROUP' END, 'END', array_to_string(p_groupNames,','),
              v_nbTblSeq || ' tables/sequences processed');
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
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and
-- sequences.
  DECLARE
    v_groupList              TEXT;
    v_count                  INT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    r_schema                 RECORD;
    r_tblsq                  RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'BEGIN', array_to_string(p_groupNames,','));
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
    SELECT array_agg(DISTINCT group_name)
      INTO p_groupNames
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames)
        AND group_is_logging;
-- Process the LOGGING groups.
    IF p_groupNames IS NOT NULL THEN
-- Check and process the supplied mark name (except if the function is called by emaj_force_stop_group()).
      IF p_mark IS NULL OR p_mark = '' THEN
        p_mark = 'STOP_%';
      END IF;
      IF NOT p_isForced THEN
        SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
      END IF;
-- OK (no error detected and at least one group in logging state)
-- Get a time stamp id of type 'X' for the operation.
      SELECT emaj._set_time_stamp('X') INTO v_timeId;
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
-- For each relation currently belonging to the groups to process.
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
            AND (mark_group, mark_time_id) IN                        -- select only last mark of each concerned group
                  (SELECT mark_group, max(mark_time_id)
                     FROM emaj.emaj_mark
                     WHERE mark_group = ANY (p_groupNames)
                       AND NOT mark_is_deleted
                     GROUP BY mark_group
                  );
      END IF;
-- Set all marks for the groups from the emaj_mark table as 'DELETED' to avoid any further rollback and remove protection, if any.
      UPDATE emaj.emaj_mark
        SET mark_is_deleted = TRUE, mark_is_rlbk_protected = FALSE
        WHERE mark_group = ANY (p_groupNames)
          AND NOT mark_is_deleted;
-- Update the state of the groups rows from the emaj_group table (the rollback protection of rollbackable groups is reset).
      UPDATE emaj.emaj_group
        SET group_is_logging = FALSE, group_is_rlbk_protected = NOT group_is_rollbackable
        WHERE group_name = ANY (p_groupNames);
-- Update the log sessions to set the time range upper bound
      UPDATE emaj.emaj_log_session
        SET lses_time_range = int8range(lower(lses_time_range), v_timeId, '[)')
        WHERE lses_group = ANY (p_groupNames)
          AND upper_inf(lses_time_range);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'STOP_GROUPS'
                   WHEN NOT p_multiGroup AND NOT p_isForced THEN 'STOP_GROUP'
                   ELSE 'FORCE_STOP_GROUP' END,
              'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
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
-- It also updates the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into all
-- log tables between this previous mark and the new mark.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like
-- functions that start or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
--        name of the rollback target mark when this mark is created by the logged_rollback functions (NULL by default)
--        time stamp identifier to reuse (NULL by default) (this parameter is set when the mark is a rollback start mark)
--        dblink schema when the mark is set by a rollback operation and dblink connection are used (NULL by default)
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    v_stmt                   TEXT;
  BEGIN
-- If requested by the calling function, record the set mark begin in emaj_hist.
    IF p_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS'
                     ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','), p_mark);
    END IF;
-- Get the time stamp of the operation, if not supplied as input parameter.
    IF p_timeId IS NULL THEN
      SELECT emaj._set_time_stamp('M') INTO p_timeId;
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
-- The statement updates no row in case of emaj_start_group(s)
-- With the same statistics, update the counters of the last log session for each groups
    WITH stat_group1 AS                        -- for each group, get the time id of the last active mark
      (SELECT mark_group, max(mark_time_id) AS last_mark_time_id
         FROM emaj.emaj_mark
         WHERE NOT mark_is_deleted
           AND mark_group = ANY(p_groupNames)
         GROUP BY mark_group
      ),
         stat_group2 AS                        -- compute the number of logged changes for all tables currently belonging to these groups
      (SELECT mark_group, last_mark_time_id, coalesce(
                (SELECT sum(emaj._log_stat_tbl(emaj_relation, greatest(last_mark_time_id, lower(rel_time_range)),NULL))
                   FROM emaj.emaj_relation
                   WHERE rel_group = mark_group
                     AND rel_kind = 'r'
                     AND upper_inf(rel_time_range)
                ), 0) AS mark_stat
        FROM stat_group1
      ),
         update_mark AS                        -- update emaj_mark
      (UPDATE emaj.emaj_mark m
         SET mark_log_rows_before_next = mark_stat
         FROM stat_group2 s
         WHERE s.mark_group = m.mark_group
           AND s.last_mark_time_id = m.mark_time_id
      )
    UPDATE emaj.emaj_log_session l             -- update emaj_log_session
      SET lses_marks = lses_marks + 1,
          lses_log_rows = lses_log_rows + mark_stat
      FROM stat_group2 s
      WHERE s.mark_group = l.lses_group
        AND upper_inf(lses_time_range);
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
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_deleted, mark_is_rlbk_protected, mark_logged_rlbk_target_mark)
      SELECT group_name, p_mark, p_timeId, FALSE, FALSE, p_loggedRlbkTargetMark
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
        VALUES (CASE WHEN p_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(p_groupNames,','), p_mark);
    END IF;
--
    RETURN v_nbSeq + v_nbTbl;
  END;
$_set_mark_groups$;

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

CREATE OR REPLACE FUNCTION emaj._purge_histories(p_retentionDelay INTERVAL DEFAULT NULL)
RETURNS VOID LANGUAGE plpgsql AS
$_purge_histories$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter, but
--   without deleting event traces neither after the oldest active mark or after the oldest not committed or aborted rollback operation.
-- It also purges oldest rows from the emaj_exec_plan, emaj_rlbk_session and emaj_rlbk_plan tables, using the same rules.
-- The function is called at start group time and when oldest marks are deleted.
-- It is also called by the emaj_purge_histories() function.
-- A retention delay >= 100 years means infinite.
-- Input: retention delay ; if supplied, it overloads the history_retention parameter from the emaj_param table.
  DECLARE
    v_delay                  INTERVAL;
    v_datetimeLimit          TIMESTAMPTZ;
    v_maxTimeId              BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbPurgedHist           BIGINT;
    v_nbPurgedLogSession     BIGINT;
    v_nbPurgedRelHist        BIGINT;
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
                                         -- get the transaction timestamp of the oldest non deleted mark for all groups
        (SELECT min(time_tx_timestamp)
           FROM emaj.emaj_mark
                JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
           WHERE NOT mark_is_deleted),
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
    GET DIAGNOSTICS v_nbPurgedHist = ROW_COUNT;
    IF v_nbPurgedHist > 0 THEN
      v_wording = v_nbPurgedHist || ' emaj_hist rows deleted';
    END IF;
-- Delete oldest rows from emaj_log_session.
    DELETE FROM emaj.emaj_log_session
      WHERE upper(lses_time_range) < v_maxTimeId;
    GET DIAGNOSTICS v_nbPurgedLogSession = ROW_COUNT;
    IF v_nbPurgedLogSession > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedLogSession || ' log session rows deleted';
    END IF;
-- Delete oldest rows from emaj_rel_hist.
    DELETE FROM emaj.emaj_rel_hist
      WHERE upper(relh_time_range) < v_maxTimeId;
    GET DIAGNOSTICS v_nbPurgedRelHist = ROW_COUNT;
    IF v_nbPurgedRelHist > 0 THEN
      v_wording = v_wording || ' ; ' || v_nbPurgedRelHist || ' relation history rows deleted';
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
