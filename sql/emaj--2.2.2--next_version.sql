--
-- E-Maj: migration from 2.2.2 to <NEXT_VERSON>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- complain if this script is executed in psql, rather than via an ALTER EXTENSION statement
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

------------------------------------
--                                --
-- checks                         --
--                                --
------------------------------------
-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_groupList              TEXT;
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current user (%) is not a superuser.', current_user;
    END IF;
-- the emaj version registered in emaj_param must be '2.2.2'
    SELECT param_value_text INTO v_emajVersion FROM emaj.emaj_param WHERE param_key = 'emaj_version';
    IF v_emajVersion <> '2.2.2' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 2.2.2',v_emajVersion;
    END IF;
-- the installed postgres version must be at least 9.2
    IF current_setting('server_version_num')::int < 90200 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL version should be at least 9.2.', current_setting('server_version');
    END IF;
-- no existing group must have been created with a postgres version prior 8.4
    SELECT string_agg(group_name, ', ') INTO v_groupList FROM emaj.emaj_group
      WHERE cast(to_number(substring(group_pg_version FROM E'^(\\d+)'),'99') * 100 +
                 to_number(substring(group_pg_version FROM E'^\\d+\\.(\\d+)'),'99') AS INTEGER) < 804;
    IF v_groupList IS NOT NULL THEN
      RAISE EXCEPTION 'E-Maj upgrade: groups "%" have been created with a too old postgres version (< 8.4). Drop these groups before upgrading. ',v_groupList;
    END IF;
  END;
$do$;

-- OK, the upgrade operation can start...

-- lock emaj_group table to avoid any concurrent E-Maj activity
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- disable the event triggers
SELECT emaj._disable_event_triggers();

----------------------------------------------
--                                          --
-- emaj enums, tables, views and sequences  --
--                                          --
----------------------------------------------

-- index on emaj_mark used to speedup statistics functions, when many marks have been set
CREATE INDEX emaj_mark_idx1 ON emaj.emaj_mark (mark_time_id);

DROP TYPE emaj.emaj_log_stat_type CASCADE;
CREATE TYPE emaj.emaj_log_stat_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_table                   TEXT,                       -- table name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the mark representing the lower bound of the time range
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the mark representing the upper bound of the time range
  stat_rows                    BIGINT                      -- estimated number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_log_stat_type IS
$$Represents the structure of rows returned by the emaj_log_stat_group() function.$$;

DROP TYPE emaj.emaj_detailed_log_stat_type CASCADE;
CREATE TYPE emaj.emaj_detailed_log_stat_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_table                   TEXT,                       -- table name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the mark representing the lower bound of the time range
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the mark representing the upper bound of the time range
  stat_role                    VARCHAR(32),                -- user having generated update events
  stat_verb                    VARCHAR(6),                 -- type of SQL statement (INSERT/UPDATE/DELETE)
  stat_rows                    BIGINT                      -- real number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_detailed_log_stat_type IS
$$Represents the structure of rows returned by the emaj_detailed_log_stat_group() function.$$;

--
-- add created or recreated tables and sequences to the list of content to save by pg_dump
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
-- recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script


--<begin_functions>                              pattern used by the tool that extracts and insert the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing)
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: group name, the 2 mark names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: set of log rows by table (including tables with 0 rows to rollback)
  DECLARE
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_log_stat_group: The group "%" does not exist.', v_groupName;
    END IF;
-- if first mark is NULL or empty, retrieve the name, timestamp and last sequ_hole id of the first recorded mark for the group
    IF v_firstMark IS NULL OR v_firstMark = '' THEN
--   if no mark exists for the group (just after emaj_create_group() or emaj_reset_group() functions call), v_realFirstMark remains NULL
      SELECT mark_name, mark_time_id, time_clock_timestamp INTO v_realFirstMark, v_firstMarkTimeId, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName
        ORDER BY mark_id LIMIT 1;
    ELSE
-- else, check and retrieve the name and the timestamp id of the supplied first mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: The start mark "%" is unknown for the group "%".', v_firstMark, v_groupName;
      END IF;
      SELECT mark_time_id, time_clock_timestamp INTO v_firstMarkTimeId, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- if a last mark name is supplied, check and retrieve the name, and the timestamp id of the supplied end mark for the group
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: The end mark "%" is unknown for the group "%".', v_lastMark, v_groupName;
      END IF;
      SELECT mark_time_id, time_clock_timestamp INTO v_lastMarkTimeId, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName AND mark_name = v_realLastMark;
-- if last mark is null or empty, v_realLastMark, v_lastMarkTimeId and v_lastMarkTs remain NULL
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkTimeId IS NOT NULL AND v_firstMarkTimeId > v_lastMarkTimeId THEN
      RAISE EXCEPTION 'emaj_log_stat_group: The start mark "%" (%) has been set after the end mark "%" (%).', v_realFirstMark, v_firstMarkTs, v_realLastMark, v_lastMarkTs;
    END IF;
-- for each table of the group, get the number of log rows and return the statistics
-- shorten the timeframe if the table did not belong to the group on the entire requested time frame
    RETURN QUERY
      SELECT rel_group, rel_schema, rel_tblseq,
             CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                  WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_realFirstMark
                  ELSE (SELECT mark_name FROM emaj.emaj_mark
                          WHERE mark_time_id = lower(rel_time_range) AND mark_group = v_groupName)
               END AS stat_first_mark,
             CASE WHEN v_firstMarkTimeId IS NULL THEN NULL
                  WHEN v_firstMarkTimeId >= lower(rel_time_range) THEN v_firstMarkTs
                  ELSE (SELECT time_clock_timestamp FROM emaj.emaj_time_stamp
                          WHERE time_id = lower(rel_time_range))
               END AS stat_first_mark_datetime,
             CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                  WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                       THEN (SELECT mark_name FROM emaj.emaj_mark
                               WHERE mark_time_id = upper(rel_time_range) AND mark_group = v_groupName)
                  ELSE v_realLastMark
               END AS stat_last_mark,
             CASE WHEN v_lastMarkTimeId IS NULL AND upper_inf(rel_time_range) THEN NULL
                  WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                       THEN (SELECT time_clock_timestamp FROM emaj.emaj_time_stamp
                               WHERE time_id = upper(rel_time_range))
                  ELSE v_lastMarkTs
               END AS stat_last_mark_datetime,
             CASE WHEN v_firstMarkTimeId IS NULL THEN 0                                              -- group just created but without any mark
                  ELSE emaj._log_stat_tbl(emaj_relation,
                                          CASE WHEN v_firstMarkTimeId >= lower(rel_time_range)
                                                 THEN v_firstMarkTimeId ELSE lower(rel_time_range) END,
                                          CASE WHEN NOT upper_inf(rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(rel_time_range) < v_lastMarkTimeId)
                                                 THEN upper(rel_time_range) ELSE v_lastMarkTimeId END)
               END AS nb_rows
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'r'                                             -- tables belonging to the groups
          AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
          AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
        ORDER BY rel_schema, rel_tblseq, rel_time_range;
 END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged events for an E-Maj group between 2 marks.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks as viewed through the log tables
-- It provides more information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of updates by user and table
  DECLARE
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
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
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: The group "%" does not exist.', v_groupName;
    END IF;
-- if first mark is NULL or empty, retrieve the name, timestamp and last sequ_hole id of the first recorded mark for the group
    IF v_firstMark IS NULL OR v_firstMark = '' THEN
      SELECT mark_name, mark_time_id, time_last_emaj_gid, time_clock_timestamp
        INTO v_realFirstMark, v_firstMarkTimeId, v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE time_id = mark_time_id AND mark_group = v_groupName
        ORDER BY mark_id LIMIT 1;
    ELSE
-- else, check and retrieve the name and the timestamp id of the supplied first mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: The start mark "%" is unknown for the group "%".', v_firstMark, v_groupName;
      END IF;
      SELECT mark_name, mark_time_id, time_last_emaj_gid, time_clock_timestamp
        INTO v_realFirstMark, v_firstMarkTimeId, v_firstEmajGid, v_firstMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- if no mark has been found (this is the case just after emaj_create_group() or emaj_reset_group() functions call), just exit
    IF v_realFirstMark IS NULL THEN
      RETURN;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the global sequence value and the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_detailed_log_stat_group: The end mark "%" is unknown for the group "%".', v_lastMark, v_groupName;
      END IF;
      SELECT mark_time_id, time_last_emaj_gid, time_clock_timestamp INTO v_lastMarkTimeId, v_lastEmajGid, v_lastMarkTs
        FROM emaj.emaj_mark, emaj.emaj_time_stamp
        WHERE mark_time_id = time_id AND mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark (v_realFirstMark is known to be not null)
    IF v_realLastMark IS NOT NULL AND v_firstMarkTimeId > v_lastMarkTimeId THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: The start mark "%" (%) has been set after the end mark "%" (%).', v_realFirstMark, v_firstMarkTs, v_realLastMark, v_lastMarkTs;
    END IF;
-- for each table currently belonging to the group
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_time_range, rel_log_schema, rel_log_table FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r'                                             -- tables belonging to the groups
            AND (upper_inf(rel_time_range) OR upper(rel_time_range) > v_firstMarkTimeId)               --   at the requested time frame
            AND (v_lastMarkTimeId IS NULL OR lower(rel_time_range) < v_lastMarkTimeId)
          ORDER BY rel_schema, rel_tblseq, rel_time_range
        LOOP
-- count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the lower bound for this table
      IF v_firstMarkTimeId >= lower(r_tblsq.rel_time_range) THEN
-- usual case: the table belonged to the group at statistics start mark
        v_lowerBoundMark = v_realFirstMark;
        v_lowerBoundMarkTs = v_firstMarkTs;
        v_lowerBoundGid = v_firstEmajGid;
      ELSE
-- special case: the table has been added to the group after the statistics start mark
        SELECT mark_name INTO v_lowerBoundMark
          FROM emaj.emaj_mark
          WHERE mark_time_id = lower(r-tblsq.rel_time_range) AND mark_group = v_groupName;
        SELECT time_clock_timestamp, time_last_emaj_gid INTO v_lowerBoundMarkTs, v_lowerBoundGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = lower(r_tblsq.rel_time_range);
      END IF;
-- compute the upper bound for this table
      IF v_lastMarkTimeId IS NULL AND upper_inf(r_tblsq.rel_time_range) THEN
-- no supplied end mark and the table has not been removed from its group => the current situation
        v_upperBoundMark = NULL;
        v_upperBoundMarkTs = NULL;
        v_upperBoundGid = NULL;
      ELSIF NOT upper_inf(r_tblsq.rel_time_range) AND (v_lastMarkTimeId IS NULL OR upper(r_tblsq.rel_time_range) < v_lastMarkTimeId) THEN
-- special case: the table has been removed from its group before the statistics end mark
        SELECT mark_name INTO v_upperBoundMark
          FROM emaj.emaj_mark
          WHERE mark_time_id = upper(r_tblsq.rel_time_range) AND mark_group = v_groupName;
        SELECT time_clock_timestamp, time_last_emaj_gid INTO v_upperBoundMarkTs, v_upperBoundGid
          FROM emaj.emaj_time_stamp
          WHERE time_id = upper(r_tblsq.rel_time_range);
      ELSE
-- usual case: the table belonged to the group at statistics end mark
        v_upperBoundMark = v_realLastMark;
        v_upperBoundMarkTs = v_lastMarkTs;
        v_upperBoundGid = v_lastEmajGid;
      END IF;
-- build the statement
      v_stmt= 'SELECT ' || quote_literal(v_groupName) || '::TEXT AS stat_group, '
           || quote_literal(r_tblsq.rel_schema) || '::TEXT AS stat_schema, '
           || quote_literal(r_tblsq.rel_tblseq) || '::TEXT AS stat_table, '
           || quote_literal(v_lowerBoundMark) || '::TEXT AS stat_first_mark, '
           || quote_literal(v_lowerBoundMarkTs) || '::TIMESTAMPTZ AS stat_first_mark_datetime, '
           || coalesce(quote_literal(v_upperBoundMark),'NULL') || '::TEXT AS stat_last_mark, '
           || coalesce(quote_literal(v_upperBoundMarkTs),'NULL') || '::TIMESTAMPTZ AS stat_last_mark_datetime, '
           || ' emaj_user AS stat_user,'
           || ' CASE emaj_verb WHEN ''INS'' THEN ''INSERT'''
           ||                ' WHEN ''UPD'' THEN ''UPDATE'''
           ||                ' WHEN ''DEL'' THEN ''DELETE'''
           ||                             ' ELSE ''?'' END::VARCHAR(6) AS stat_verb,'
           || ' count(*) AS stat_rows'
           || ' FROM ' || quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_log_table)
           || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')'
           || ' AND emaj_gid > '|| v_lowerBoundGid
           || coalesce(' AND emaj_gid <= '|| v_upperBoundGid, '')
           || ' GROUP BY stat_group, stat_schema, stat_table, stat_user, stat_verb'
           || ' ORDER BY stat_user, stat_verb';
-- and execute the statement
      FOR r_stat IN EXECUTE v_stmt LOOP
        RETURN NEXT r_stat;
      END LOOP;
    END LOOP;
    RETURN;
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks.$$;

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

GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;

------------------------------------
--                                --
-- complete the upgrade           --
--                                --
------------------------------------

-- enable the event triggers
DO
$tmp$
  DECLARE
    v_event_trigger_array    TEXT[];
  BEGIN
    IF emaj._pg_version_num() >= 90300 THEN
-- build the event trigger names array from the pg_event_trigger table
      SELECT coalesce(array_agg(evtname),ARRAY[]::TEXT[]) INTO v_event_trigger_array
        FROM pg_catalog.pg_event_trigger WHERE evtname LIKE 'emaj%' AND evtenabled = 'D';
-- call the _enable_event_triggers() function
      PERFORM emaj._enable_event_triggers(v_event_trigger_array);
    END IF;
  END;
$tmp$;

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

-- update the version id in the emaj_param table
UPDATE emaj.emaj_param SET param_value_text = '<NEXT_VERSON>' WHERE param_key = 'emaj_version';

-- insert the upgrade record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <NEXT_VERSON>', 'Upgrade from 2.2.2 completed');

-- post installation checks
DO
$tmp$
  DECLARE
  BEGIN
-- check the max_prepared_transactions GUC value
    IF current_setting('max_prepared_transactions')::int <= 1 THEN
      RAISE WARNING 'E-Maj upgrade: as the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel rollback is possible.', current_setting('max_prepared_transactions');
    END IF;
  END;
$tmp$;

RESET default_tablespace;
SET client_min_messages TO default;
