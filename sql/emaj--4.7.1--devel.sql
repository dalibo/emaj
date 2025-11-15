--
-- E-Maj: migration from 4.7.1 to <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This script upgrades an existing installation of E-Maj extension.
--

-- Complain if this script is executed in psql, rather than via an ALTER EXTENSION statement.
\echo Use "ALTER EXTENSION emaj UPDATE TO..." to upgrade the E-Maj extension. \quit

--SET client_min_messages TO WARNING;
SET client_min_messages TO NOTICE;

----------------------------------------------------------------
--                                                            --
--                           Checks                           --
--                                                            --
----------------------------------------------------------------

-- Check that the upgrade conditions are met.
DO
$do$
  DECLARE
    v_emajVersion            TEXT;
    v_txid                   TEXT;
    v_nbNoError              INT;
    v_nbWarning              INT;
  BEGIN
-- The current emaj version must be '4.7.1'.
    SELECT emaj.emaj_get_version() INTO v_emajVersion;
    IF v_emajVersion <> '4.7.1' THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current E-Maj version (%) is not 4.7.1',v_emajVersion;
    END IF;
-- The installed postgres version must be at least 12.
    IF current_setting('server_version_num')::int < 120000 THEN
      RAISE EXCEPTION 'E-Maj upgrade: the current PostgreSQL version (%) is not compatible with the new E-Maj version. The PostgreSQL '
                      'version should be at least 12.', current_setting('server_version');
    END IF;
-- Check the E-Maj environment state, if not yet done by a previous upgrade in the same transaction.
    SELECT current_setting('emaj.upgrade_verify_txid', TRUE) INTO v_txid;
    IF v_txid IS NULL OR v_txid <> txid_current()::TEXT THEN
      BEGIN
        SELECT count(msg) FILTER (WHERE msg = 'No error detected'),
               count(msg) FILTER (WHERE msg LIKE 'Warning:%')
          INTO v_nbNoError, v_nbWarning
          FROM emaj.emaj_verify_all() AS t(msg);
      EXCEPTION
-- Errors during the emaj_verify_all() execution are trapped. The emaj_verify_all() code may be incompatible with the current PG version.
        WHEN OTHERS THEN -- do nothing
      END;
      IF v_nbNoError = 0 THEN
        RAISE EXCEPTION 'E-Maj upgrade: the E-Maj environment is damaged. Please fix the issue before upgrading. '
                        'You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details. '
                        'An "ALTER EXTENSION emaj UPDATE TO ''%'';" statement may be required before.', v_emajVersion;
      END IF;
      IF v_nbWarning > 0 THEN
        RAISE WARNING 'E-Maj upgrade: the E-Maj environment health check reports warning. '
                      'You may execute "SELECT * FROM emaj.emaj_verify_all();" to get more details.';
      END IF;
      IF v_nbWarning IS NOT NULL THEN
        PERFORM set_config('emaj.upgrade_verify_txid', txid_current()::TEXT, TRUE);
      END IF;
    END IF;

  END;
$do$;

----------------------------------------------------------------
--                                                            --
--                       Upgrade start                        --
--                                                            --
----------------------------------------------------------------

-- Insert the upgrade begin record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','BEGIN','E-Maj <devel>', 'Upgrade from 4.7.1 started');

-- Lock emaj_group table to avoid any concurrent E-Maj activity.
LOCK TABLE emaj.emaj_group IN EXCLUSIVE MODE;

-- Disable the event triggers during the upgrade operation.
SELECT emaj._disable_event_triggers();

----------------------------------------------------------------
--                                                            --
--       Enumerated types, tables, sequences and views        --
--                                                            --
----------------------------------------------------------------

--
-- Add created or recreated tables and sequences to the list of content to save by pg_dump.
--

----------------------------------------------------------------
--                                                            --
--                      Composite types                       --
--                                                            --
----------------------------------------------------------------

----------------------------------------------------------------
--                                                            --
--                         Functions                          --
--                                                            --
----------------------------------------------------------------
-- Recreate functions that have been previously dropped in the tables structure upgrade step and will not be recreated later in this script.


--<begin_functions>                              pattern used by the tool that extracts and inserts the functions definition
------------------------------------------------------------------
-- drop obsolete functions or functions with modified interface --
------------------------------------------------------------------
DROP FUNCTION IF EXISTS emaj.emaj_import_groups_configuration(P_JSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj.emaj_import_groups_configuration(P_LOCATION TEXT,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf(P_JSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_LOCATION TEXT,P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_prepare(P_GROUPSJSON JSON,P_GROUPS TEXT[],P_ALLOWGROUPSUPDATE BOOLEAN,P_LOCATION TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_check(P_GROUPNAMES TEXT[]);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_exec(P_JSON JSON,P_GROUPS TEXT[],P_MARK TEXT);
DROP FUNCTION IF EXISTS emaj._import_groups_conf_alter(P_GROUPNAMES TEXT[],P_MARK TEXT,P_TIMEID BIGINT);

------------------------------------------------------------------
-- create new or modified functions                             --
------------------------------------------------------------------
CREATE OR REPLACE FUNCTION emaj._clean_array(p_array TEXT[])
RETURNS TEXT[] LANGUAGE SQL IMMUTABLE AS
$$
-- This function cleans up a text array by removing duplicates, NULL and empty strings.
  SELECT array_agg(DISTINCT element)
    FROM unnest(p_array) AS element
    WHERE element IS NOT NULL AND element <> '';
$$;

CREATE OR REPLACE FUNCTION emaj._build_tblseqs_array_from_regexp(p_schema TEXT, p_relkind TEXT, p_includeFilter TEXT, p_excludeFilter TEXT,
                                                                 p_exceptionIfMissing BOOLEAN)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_build_tblseqs_array_from_regexp$
-- The function builds the names array of tables or sequences belonging to any tables groups, based on include and exclude regexp filters.
-- Depending on the p_exceptionIfMissing parameter, it warns or raises an error if the schema doesn't exist.
-- (WARNING only is used by removal functions so that it is possible to remove from its group relations of a dropped or renamed schema)
-- Inputs: schema,
--         relation kind ('r' or 'S'),
--         2 patterns to filter table names (one to include and another to exclude),
--         boolean indicating whether a missing schema must raise an exception or just a warning.
-- Outputs: tables or sequences names array
  BEGIN
-- Check that the schema exists.
    PERFORM emaj._check_schema(p_schema, p_exceptionIfMissing, FALSE);
-- Build and return the list of relations names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    RETURN array_agg(rel_tblseq)
      FROM (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND (p_includeFilter IS NOT NULL AND p_includeFilter <> '' AND rel_tblseq ~ p_includeFilter)
            AND (p_excludeFilter IS NULL OR p_excludeFilter = '' OR rel_tblseq !~ p_excludeFilter)
            AND rel_kind = p_relkind
            AND upper_inf(rel_time_range)
          ORDER BY rel_tblseq
           ) AS t;
  END;
$_build_tblseqs_array_from_regexp$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_tables(p_schema TEXT, p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                    p_group TEXT, p_properties JSONB DEFAULT NULL, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_tables$
-- The function assigns tables on name regexp pattern into a tables group.
-- Inputs: schema name, 2 patterns to filter table names (one to include and another to exclude) , assignment group name,
--         assignment properties (optional), mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group
  DECLARE
    v_tables                 TEXT[];
  BEGIN
-- Build the list of tables names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    SELECT array_agg(relname) INTO v_tables
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND (p_tablesIncludeFilter IS NOT NULL AND p_tablesIncludeFilter <> '' AND relname ~ p_tablesIncludeFilter)
             AND (p_tablesExcludeFilter IS NULL OR p_tablesExcludeFilter = '' OR relname !~ p_tablesExcludeFilter)
             AND relkind IN ('r', 'p')
           ORDER BY relname
        ) AS t;
-- Call the _assign_tables() function for execution.
    RETURN emaj._assign_tables(p_schema, v_tables, p_group, p_properties, p_mark, TRUE, TRUE);
  END;
$emaj_assign_tables$;
COMMENT ON FUNCTION emaj.emaj_assign_tables(TEXT,TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign tables on name patterns into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_assigned_group_table(p_schema TEXT, p_table TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_assigned_group_table$
-- The function reports the tables group a table is assigned to.
-- It returns NULL if the table is not currently assigned to a group.
-- Inputs: schema name, table name.
-- Outputs: tables group currently owning the table.
  DECLARE
    v_group                  TEXT;
  BEGIN
-- Check the supplied schema exists and is not an E-Maj schema.
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check that application table exist.
    PERFORM 0
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = p_table
        AND relkind IN ('r','p');
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_assigned_group_table: In schema %, the table % does not exist.',
                      quote_ident(p_schema), quote_ident(p_table);
    END IF;
-- Get the tables group the table is assignned to.
    SELECT rel_group INTO v_group
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND rel_kind = 'r'
        AND upper_inf(rel_time_range);
--
    RETURN v_group;
  END;
$emaj_get_assigned_group_table$;
COMMENT ON FUNCTION emaj.emaj_get_assigned_group_table(TEXT,TEXT) IS
$$Returns the tables group a table is assigned to.$$;

CREATE OR REPLACE FUNCTION emaj._move_tbl(p_schema TEXT, p_table TEXT, p_oldGroup TEXT, p_oldGroupIsLogging BOOLEAN, p_newGroup TEXT,
                                          p_newGroupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_tbl$
-- The function changes the group ownership of a table. It is called during an alter group or a dynamic assignment operation.
-- Required inputs: schema and table to move, old and new group names and their logging state,
--                  time stamp id of the operation, main calling function.
  DECLARE
    v_logSchema              TEXT;
    v_logSequence            TEXT;
    v_currentLogTable        TEXT;
    v_currentLogIndex        TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_namesSuffix            TEXT;
  BEGIN
-- Get the current relation characteristics.
    SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_sequence,
           coalesce('TABLESPACE ' || quote_ident(rel_log_dat_tsp),''),
           coalesce('USING INDEX TABLESPACE ' || quote_ident(rel_log_idx_tsp),'')
      INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logSequence,
           v_dataTblSpace,
           v_idxTblSpace
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
-- Compute the suffix to add to the log table and index names (_1, _2, ...), by looking at the existing names.
    SELECT '_' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_namesSuffix
      FROM
          (SELECT (regexp_match(rel_log_table,'_(\d+)$'))[1]::INT AS suffix
           FROM emaj.emaj_relation
           WHERE rel_schema = p_schema
             AND rel_tblseq = p_table
        ) AS t;
-- Rename the log table and its index (they may have been dropped).
    EXECUTE format('ALTER TABLE IF EXISTS %I.%I RENAME TO %I',
                   v_logSchema, v_currentLogTable, v_currentLogTable || v_namesSuffix);
    EXECUTE format('ALTER INDEX IF EXISTS %I.%I RENAME TO %I',
                   v_logSchema, v_currentLogIndex, v_currentLogIndex || v_namesSuffix);
-- Update emaj_relation to reflect the log table and index rename for all concerned rows.
    UPDATE emaj.emaj_relation
      SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND rel_log_table = v_currentLogTable;
-- Create the new log table, by copying the just renamed table structure.
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS) %s',
                    v_logSchema, v_currentLogTable, v_logSchema, v_currentLogTable || v_namesSuffix, v_dataTblSpace);
-- Add the primary key.
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAiNT %I PRIMARY KEY (emaj_gid, emaj_tuple) %s',
                    v_logSchema, v_currentLogTable, v_currentLogIndex, v_idxTblSpace);
-- Set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE format('ALTER TABLE ONLY %I.%I CLUSTER ON %I',
                   v_logSchema, v_currentLogTable, v_currentLogIndex);
-- Grant appropriate rights to both emaj roles.
    EXECUTE format('ALTER TABLE %I.%I OWNER TO emaj_adm',
                   v_logSchema, v_currentLogTable);
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO emaj_viewer',
                   v_logSchema, v_currentLogTable);
-- Register the end of the previous relation time frame and create a new relation time frame with the new group.
    UPDATE emaj.emaj_relation
      SET rel_time_range = int8range(lower(rel_time_range),p_timeId,'[)')
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind, rel_priority, rel_log_schema,
                                    rel_log_table, rel_log_dat_tsp, rel_log_index, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
                                    rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
                                    rel_sql_rlbk_columns,
                                    rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
                                    rel_log_seq_last_value)
      SELECT rel_schema, rel_tblseq, int8range(p_timeId, NULL, '[)'), p_newGroup, rel_kind, rel_priority, rel_log_schema,
             v_currentLogTable, rel_log_dat_tsp, v_currentLogIndex, rel_log_idx_tsp, rel_log_sequence, rel_log_function,
             rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols, rel_emaj_verb_attnum, rel_has_always_ident_col,
             rel_sql_rlbk_columns,
             rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions,
             rel_log_seq_last_value
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper(rel_time_range) = p_timeId;
-- If the table is moved from an idle group to a group in logging state,
    IF NOT p_oldGroupIsLogging AND p_newGroupIsLogging THEN
-- ... get the log schema and sequence for the new relation,
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- ... and record the new log sequence state in the emaj_table table for the current operation mark.
      INSERT INTO emaj.emaj_table (tbl_schema, tbl_name, tbl_time_id, tbl_tuples, tbl_pages, tbl_log_seq_last_val)
        SELECT p_schema, p_table, p_timeId, reltuples, relpages, last_value
          FROM pg_catalog.pg_class
               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace),
               LATERAL emaj._get_log_sequence_last_value(v_logSchema, v_logSequence) AS last_value
          WHERE nspname = p_schema
            AND relname = p_table;
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group, rlchg_new_group)
      VALUES (p_timeId, p_schema, p_table, 'MOVE_TABLE', p_oldGroup, p_newGroup);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TABLE MOVED', quote_ident(p_schema) || '.' || quote_ident(p_table),
              'From the ' || CASE WHEN p_oldGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_oldGroup ||
              ' to the ' || CASE WHEN p_newGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_newGroup);
--
    RETURN;
  END;
$_move_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequences(p_schema TEXT, p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                      p_group TEXT, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequences$
-- The function assigns sequences on name regexp pattern into a tables group.
-- Inputs: schema name, 2 patterns to filter sequence names (one to include and another to exclude), assignment group name,
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group
  DECLARE
    v_sequences              TEXT[];
  BEGIN
-- Build the list of sequences names satisfying the pattern.
-- Empty strings as inclusion or exclusion pattern are processed as NULL
    SELECT array_agg(relname) INTO v_sequences
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND (p_sequencesIncludeFilter IS NOT NULL AND p_sequencesIncludeFilter <> '' AND relname ~ p_sequencesIncludeFilter)
             AND (p_sequencesExcludeFilter IS NULL OR p_sequencesExcludeFilter = '' OR relname !~ p_sequencesExcludeFilter)
             AND relkind = 'S'
           ORDER BY relname
        ) AS t;
-- OK, call the _assign_sequences() function for execution.
    RETURN emaj._assign_sequences(p_schema, v_sequences, p_group, p_mark, TRUE, TRUE);
  END;
$emaj_assign_sequences$;
COMMENT ON FUNCTION emaj.emaj_assign_sequences(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Assign sequences on name patterns into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_assigned_group_sequence(p_schema TEXT, p_sequence TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_assigned_group_sequence$
-- The function reports the tables group a sequence is assigned to.
-- It returns NULL if the sequence is not currently assigned to a group.
-- Inputs: schema name, sequence name.
-- Outputs: tables group currently owning the sequence.
  DECLARE
    v_group                  TEXT;
  BEGIN
-- Check the supplied schema exists and is not an E-Maj schema.
    PERFORM emaj._check_schema(p_schema, TRUE, TRUE);
-- Check that application sequence exist.
    PERFORM 0
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = p_sequence
        AND relkind = 'S';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_assigned_group_sequence: In schema %, the sequence % does not exist.',
                      quote_ident(p_schema), quote_ident(p_sequence);
    END IF;
-- Get the tables group the sequence is assignned to.
    SELECT rel_group INTO v_group
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_sequence
        AND rel_kind = 'S'
        AND upper_inf(rel_time_range);
--
    RETURN v_group;
  END;
$emaj_get_assigned_group_sequence$;
COMMENT ON FUNCTION emaj.emaj_get_assigned_group_sequence(TEXT,TEXT) IS
$$Returns the tables group a sequence is assigned to.$$;

CREATE OR REPLACE FUNCTION emaj._get_current_seq(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT)
RETURNS emaj.emaj_sequence LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_get_current_seq$
-- This function reads the current characteristics of a sequence and returns it in an emaj_sequence format.
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
  DECLARE
    r_seq                    emaj.emaj_sequence%ROWTYPE;
  BEGIN
    EXECUTE format(
        'SELECT nspname, relname, %s, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
        '  FROM %I.%I sq,'
        '     pg_catalog.pg_sequence s'
        '     JOIN pg_catalog.pg_class c ON (c.oid = s.seqrelid)'
        '     JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)'
        '  WHERE nspname = %L AND relname = %L',
        p_timeId, p_schema, p_sequence, p_schema, p_sequence)
      INTO STRICT r_seq;
    RETURN r_seq;
  END;
$_get_current_seq$;

CREATE OR REPLACE FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                   OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN)
LANGUAGE plpgsql AS
$_sequence_stat_seq$
-- This function compares the state of a single sequence between 2 time stamps or between a time stamp and the current state.
-- It is called by the _sequence_stat_group() function.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time stamp ids defining the time range to examine (a end time stamp id set to NULL indicates the current state).
-- Output: number of sequence increments between both time stamps for the sequence
--         a boolean indicating whether any structure property has been modified between both time stamps.
  DECLARE
    r_beginSeq               emaj.emaj_sequence%ROWTYPE;
    r_endSeq                 emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence characteristics at begin time id.
    SELECT *
      INTO r_beginSeq
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_beginTimeId;
-- Get the sequence characteristics at end time id.
    IF p_endTimeId IS NOT NULL THEN
      SELECT *
        INTO r_endSeq
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq
          AND sequ_time_id = p_endTimeId;
    ELSE
      r_endSeq = emaj._get_current_seq(r_rel.rel_schema, r_rel.rel_tblseq, 0);
    END IF;
-- Compute the statistics
    p_increments = (r_endSeq.sequ_last_val - r_beginSeq.sequ_last_val) / r_beginSeq.sequ_increment
                   - CASE WHEN r_endSeq.sequ_is_called THEN 0 ELSE 1 END
                   + CASE WHEN r_beginSeq.sequ_is_called THEN 0 ELSE 1 END;
    p_hasStructureChanged = r_beginSeq.sequ_start_val <> r_endSeq.sequ_start_val
                         OR r_beginSeq.sequ_increment <> r_endSeq.sequ_increment
                         OR r_beginSeq.sequ_max_val <> r_endSeq.sequ_max_val
                         OR r_beginSeq.sequ_min_val <> r_endSeq.sequ_min_val
                         OR r_beginSeq.sequ_is_cycled <> r_endSeq.sequ_is_cycled;
    RETURN;
  END;
$_sequence_stat_seq$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_seq(r_rel emaj.emaj_relation, p_firstMarkTimeId BIGINT, p_lastMarkTimeId BIGINT, p_nbSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_gen_sql_seq$
-- This function generates a SQL statement to set the final characteristics of a sequence.
-- The statement is stored into a temporary table created by the _gen_sql_groups() calling function.
-- If the sequence has not been changed between both marks, no statement is generated.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time id at requested start and end marks,
--        the number of already processed sequences
-- Output: number of generated SQL statements (0 or 1)
  DECLARE
    v_endTimeId              BIGINT;
    v_rqSeq                  TEXT;
    ref_seq_rec              emaj.emaj_sequence%ROWTYPE;
    trg_seq_rec              emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence characteristics at start mark.
    SELECT *
      INTO ref_seq_rec
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_firstMarkTimeId;
-- Get the sequence characteristics at end mark or the current state.
    IF p_lastMarkTimeId IS NULL AND upper_inf(r_rel.rel_time_range) THEN
-- No supplied last mark and the sequence currently belongs to its group, so get the current sequence characteritics.
      trg_seq_rec = emaj._get_current_seq(r_rel.rel_schema, r_rel.rel_tblseq, 0);
    ELSE
-- A last mark is supplied, or the sequence does not belong to its group anymore, so get the sequence characteristics
-- from the emaj_sequence table.
      v_endTimeId = CASE WHEN upper_inf(r_rel.rel_time_range) OR p_lastMarkTimeId < upper(r_rel.rel_time_range)
                           THEN p_lastMarkTimeId
                         ELSE upper(r_rel.rel_time_range) END;
      SELECT *
        INTO trg_seq_rec
        FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq
          AND sequ_time_id = v_endTimeId;
    END IF;
-- Build the ALTER SEQUENCE clause.
    SELECT emaj._build_alter_seq(ref_seq_rec, trg_seq_rec) INTO v_rqSeq;
-- Insert into the temp table and return 1 if at least 1 characteristic needs to be changed.
    IF v_rqSeq <> '' THEN
      v_rqSeq = 'ALTER SEQUENCE ' || quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq) || ' ' || v_rqSeq || ';';
      EXECUTE 'INSERT INTO emaj_temp_script '
              '  SELECT NULL, -1 * $1, txid_current(), $2'
        USING p_nbSeq + 1, v_rqSeq;
      RETURN 1;
    END IF;
-- Otherwise return 0.
    RETURN 0;
  END;
$_gen_sql_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_group(p_groupName TEXT, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_drop_group$
-- This function effectively processes a tables group deletion.
-- It also drops log schemas that are not useful anymore.
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
  DECLARE
    v_function               TEXT;
    v_eventTriggers          TEXT[];
    v_timeId                 BIGINT;
    v_nbRel                  INT;
  BEGIN
    v_function = CASE WHEN p_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END;
-- Get the time stamp of the operation.
    SELECT emaj._set_time_stamp(v_function, 'D') INTO v_timeId;
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Effectively drop the tables group.
    SELECT emaj._drop_groups(ARRAY[p_groupName], v_timeId) INTO v_nbRel;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any other created group).
    PERFORM emaj._drop_log_schemas(v_function, p_isForced);
-- Enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
--
    RETURN v_nbRel;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj._drop_groups(p_groups TEXT[], p_timeId BIGINT)
RETURNS INT LANGUAGE plpgsql AS
$_drop_groups$
-- This function effectively deletes several groups and their content.
-- It is called by emaj_drop_group() and emaj_import_groups_configuration() functions.
-- Input: group names array, time id of the operation
-- Output: number of processed tables and sequences
  DECLARE
    v_nbRel                  INT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- Register into emaj_relation_change the tables and sequences removal from their group, for completeness.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      SELECT p_timeId, rel_schema, rel_tblseq,
             CASE WHEN rel_kind = 'r' THEN 'REMOVE_TABLE'::emaj._relation_change_kind_enum
                                      ELSE 'REMOVE_SEQUENCE'::emaj._relation_change_kind_enum END,
             rel_group
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND upper_inf(rel_time_range)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range;
-- Delete the emaj objects and references for each table and sequences of the groups.
    FOR r_rel IN
      SELECT *
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
        ORDER BY rel_priority, rel_schema, rel_tblseq, rel_time_range
    LOOP
      PERFORM CASE r_rel.rel_kind
                WHEN 'r' THEN emaj._drop_tbl(r_rel, p_timeId)
                WHEN 'S' THEN emaj._drop_seq(r_rel, p_timeId)
              END;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Count the number of tables annd sequences owned by the groups.
    SELECT sum(group_nb_table + group_nb_sequence)
      INTO v_nbRel
      FROM emaj.emaj_group
      WHERE group_name = ANY (p_groups);
-- Delete groups row from the emaj_group table.
-- By cascade, it also deletes rows from emaj_mark.
    DELETE FROM emaj.emaj_group
      WHERE group_name = ANY (p_groups);
-- Update the last log session for the groups to set the time range upper bound
    UPDATE emaj.emaj_log_session
      SET lses_time_range = int8range(lower(lses_time_range), p_timeId, '[]')
      WHERE lses_group = ANY (p_groups)
        AND upper_inf(lses_time_range);
-- Update the last group history rows to set the time range upper bound
    UPDATE emaj.emaj_group_hist
      SET grph_time_range = int8range(lower(grph_time_range), p_timeId, '[]')
      WHERE grph_group = ANY (p_groups)
        AND upper_inf(grph_time_range);
--
    RETURN v_nbRel;
  END;
$_drop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of existing group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_groups(TEXT, TEXT) IS
$$Builds a groups array, filtered on their names.$$;

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
-- Build the comment heading the JSON structure.
    v_groupsText = E'{\n  "_comment": "Generated on database ' || current_database() || ' with emaj version ' ||
                           emaj.emaj_get_version() || ', at ' || statement_timestamp();
    IF p_groups IS NULL THEN
      v_groupsText = v_groupsText || E', including all tables groups",\n';
    ELSE
      v_groupsText = v_groupsText || E', including a tables groups subset",\n';
    END IF;
-- Check the group names array, if supplied. All the listed groups must exist.
    IF p_groups IS NOT NULL THEN
      SELECT string_agg(group_name, ', ' ORDER BY group_name)
        INTO v_unknownGroupsList
        FROM unnest(p_groups) AS grp(group_name)
        WHERE NOT EXISTS
               (SELECT group_name
                  FROM emaj.emaj_group
                  WHERE emaj_group.group_name = grp.group_name
               );
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

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_json JSON, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%',
                                                                 p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_groups_configuration$
-- This function import a supplied JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--        - an optional mark name to set for tables groups in logging state (IMPORT_% by default)
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
  BEGIN
-- Just process the tables groups.
    RETURN emaj._import_groups_conf(p_json, p_groups, p_allowGroupsUpdate, NULL, p_mark, p_dropOtherGroups);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(JSON,TEXT[],BOOLEAN, TEXT, BOOLEAN) IS
$$Import a json structure describing tables groups to create or alter.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%',
                                                                 p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_groups_configuration$
-- This function imports a file containing a JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - input file location
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--        - an optional mark name to set for tables groups in logging state (IMPORT_% by default)
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_groupsText             TEXT;
    v_json                   JSON;
  BEGIN
-- Read the input file and put its content into a temporary table.
    CREATE TEMP TABLE t (groups TEXT);
    EXECUTE format ('COPY t FROM %L',
                    p_location);
-- Aggregate the lines into a single text variable.
    SELECT string_agg(groups, E'\n') INTO v_groupsText
      FROM t;
    DROP TABLE t;
-- Verify that the file content is a valid json structure.
    BEGIN
      v_json = v_groupsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_groups_configuration: The file content is not a valid JSON content.';
    END;
-- Proccess the tables groups and return the result.
    RETURN emaj._import_groups_conf(v_json, p_groups, p_allowGroupsUpdate, p_location, p_mark, p_dropOtherGroups);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(TEXT,TEXT[],BOOLEAN, TEXT, BOOLEAN) IS
$$Create or alter tables groups configuration from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf(p_json JSON, p_groups TEXT[], p_allowGroupsUpdate BOOLEAN,
                                                    p_location TEXT, p_mark TEXT, p_dropOtherGroups BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf$
-- This function processes a JSON formatted structure representing the tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- The expected JSON structure must contain an array like:
-- { "tables_groups": [
--   {
--     "group": "myGroup1", "is_rollbackable": true, "comment": "This is group #1",
--     "tables": [
--       {
--       "schema": "myschema1", "table": "mytbl1",
--       "priority": 1, "log_data_tablespace": "tblspc1", "log_index_tablespace": "tblspc2",
--       "ignored_triggers": [
--         { "trigger": "xxx" },
--         ...
--         ]
--       },
--       {
--       ...
--       }
--     ],
--     "sequences": [
--       {
--       "schema": "myschema1", "sequence": "mytbl1",
--       },
--       {
--       ...
--       }
--     ],
--   },
--   ...
--   ]
-- }
-- For tables groups, "group_is_rollbackable" and "group_comment" attributes are optional.
-- For tables, "priority", "log_data_tablespace" and "log_index_tablespace" attributes are optional.
-- A tables group may have no "tables" or "sequences" arrays.
-- A table may have no "ignored_triggers" array.
-- The function replaces the content of the tmp_app_table table for the imported tables groups by the content of the JSON configuration.
-- Non existing groups are created empty.
-- The _alter_groups() function is used to process the assignement, the move, the removal or the attributes change for tables and
-- sequences.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - a boolean indicating whether tables groups to import may already exist
--        - the input file name, if any, to record in the emaj_hist table (NULL if direct import)
--        - the mark name to set for tables groups in logging state
--        - the request for the drop of all existing groups that are not in the imported configuration
-- Output: the number of created or altered tables groups
  DECLARE
    v_groupsJson             JSON;
    r_msg                    RECORD;
  BEGIN
-- Performs various checks on the groups content described in the supplied JSON structure.
    FOR r_msg IN
      SELECT rpt_message
        FROM emaj._check_json_groups_conf(p_json)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3, rpt_int_var_1
    LOOP
      RAISE WARNING '_import_groups_conf (1): %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the supplied JSON structure.';
    END IF;
-- Extract the "tables_groups" json path.
    v_groupsJson = p_json #> '{"tables_groups"}';
-- If not supplied by the caller, materialize the groups array, by aggregating all groups of the JSON structure.
    IF p_groups IS NULL THEN
      SELECT array_agg("group") INTO p_groups
        FROM json_to_recordset(v_groupsJson) AS x("group" TEXT);
    END IF;
-- Prepare the groups configuration import. This may report some other issues with the groups content.
    FOR r_msg IN
      SELECT rpt_message
        FROM emaj._import_groups_conf_prepare(p_json, p_groups, p_allowGroupsUpdate, p_location, p_dropOtherGroups)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3
    LOOP
      RAISE WARNING '_import_groups_conf (2): %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the JSON groups configuration.';
    END IF;
-- OK
    RETURN emaj._import_groups_conf_exec(p_json, p_groups, p_mark, p_dropOtherGroups);
 END;
$_import_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(p_groupsJson JSON, p_groups TEXT[],
                                                    p_allowGroupsUpdate BOOLEAN, p_location TEXT, p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepares the effective tables groups configuration import.
-- It is called by _import_groups_conf() and by Emaj_web
-- At the end of the function, the tmp_app_table table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: diagnostic records
  DECLARE
    v_prevCMM                TEXT;
    v_rollbackableGroups     TEXT[];
    v_ignoredTriggers        TEXT[];
    r_group                  RECORD;
    r_table                  RECORD;
    r_sequence               RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('IMPORT_GROUPS', 'BEGIN', array_to_string(p_groups, ', '), 'Input file: ' || quote_literal(p_location));
-- Extract the "tables_groups" json path.
    p_groupsJson = p_groupsJson #> '{"tables_groups"}';
-- Check that all tables groups listed in the p_groups array exist in the JSON structure.
    RETURN QUERY
      SELECT 250, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                   format('The tables group "%s" to import is not referenced in the JSON structure.',
                          group_name)
        FROM
          (  SELECT group_name
               FROM unnest(p_groups) AS g(group_name)
           EXCEPT
             SELECT "group"
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT)
          ) AS t;
    IF FOUND THEN
      RETURN;
    END IF;
-- If the p_allowGroupsUpdate flag is FALSE, check that no tables group already exists.
    IF NOT p_allowGroupsUpdate THEN
      RETURN QUERY
        SELECT 251, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('The tables group "%s" already exists.',
                            group_name)
          FROM
            (SELECT "group" AS group_name
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT), emaj.emaj_group
               WHERE group_name = "group"
                 AND "group" = ANY (p_groups)
            ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    ELSE
-- If the p_allowGroupsUpdate flag is TRUE, check that existing tables groups have the same type than in the JSON structure.
      RETURN QUERY
        SELECT 252, 1, group_name, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                     format('Changing the type of the tables group "%s" is not allowed. '
                            'You may drop this tables group before importing the configuration.',
                            group_name)
          FROM
            (SELECT "group" AS group_name
               FROM json_to_recordset(p_groupsJson) AS x("group" TEXT, "is_rollbackable" BOOLEAN)
                    JOIN emaj.emaj_group ON (group_name = "group")
               WHERE "group" = ANY (p_groups)
                 AND group_is_rollbackable <> coalesce(is_rollbackable, true)
            ) AS t;
      IF FOUND THEN
        RETURN;
      END IF;
    END IF;
-- Drop temporary tables in case of...
    v_prevCMM = pg_catalog.current_setting('client_min_messages');
    SET client_min_messages TO WARNING;
    DROP TABLE IF EXISTS tmp_app_table, tmp_app_sequence;
    PERFORM pg_catalog.set_config ('client_min_messages', v_prevCMM, FALSE);
-- Create the temporary table that will hold the application tables configured in imported groups.
    CREATE TEMP TABLE tmp_app_table (
      tmp_group            TEXT NOT NULL,
      tmp_schema           TEXT NOT NULL,
      tmp_tbl_name         TEXT NOT NULL,
      tmp_priority         INTEGER,
      tmp_log_dat_tsp      TEXT,
      tmp_log_idx_tsp      TEXT,
      tmp_ignored_triggers TEXT[]
      );
-- Create the temporary table that will hold the application sequences configured in imported groups.
    CREATE TEMP TABLE tmp_app_sequence (
      tmp_schema           TEXT NOT NULL,
      tmp_seq_name         TEXT NOT NULL,
      tmp_group            TEXT
      );
-- In a second pass over the JSON structure, populate the tmp_app_table and tmp_app_sequence temporary tables.
    v_rollbackableGroups = '{}';
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(p_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
-- Build the rollbackable groups array.
      IF coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE) THEN
        v_rollbackableGroups = array_append(v_rollbackableGroups, r_group.groupJson ->> 'group');
      END IF;
-- Insert tables into tmp_app_table.
      FOR r_table IN
        SELECT value AS tableJson
          FROM json_array_elements(r_group.groupJson -> 'tables')
      LOOP
--   Prepare the trigger names array for the table,
        SELECT array_agg("value" ORDER BY "value") INTO v_ignoredTriggers
          FROM json_array_elements_text(r_table.tableJson -> 'ignored_triggers') AS t;
--   ... and insert
        INSERT INTO tmp_app_table(tmp_group, tmp_schema, tmp_tbl_name,
                                  tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp, tmp_ignored_triggers)
          VALUES (r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table',
                  (r_table.tableJson ->> 'priority')::INT, r_table.tableJson ->> 'log_data_tablespace',
                  r_table.tableJson ->> 'log_index_tablespace', v_ignoredTriggers);
      END LOOP;
-- Insert sequences into tmp_app_sequence.
      FOR r_sequence IN
        SELECT value AS sequenceJson
          FROM json_array_elements(r_group.groupJson -> 'sequences')
      LOOP
        INSERT INTO tmp_app_sequence(tmp_schema, tmp_seq_name, tmp_group)
          VALUES (r_sequence.sequenceJson ->> 'schema', r_sequence.sequenceJson ->> 'sequence', r_group.groupJson ->> 'group');
      END LOOP;
    END LOOP;
-- Add an index on each temporary table.
    CREATE INDEX ON tmp_app_table (tmp_schema, tmp_tbl_name);
    CREATE INDEX ON tmp_app_sequence (tmp_schema, tmp_seq_name);
-- Check that no table or sequence is referenced in several different groups or in its group.
    RETURN QUERY
      SELECT 270, 1, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::TEXT, nb_group::INT,
             format('The table %s.%s is referenced in %s different tables groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_tbl_name), nb_group)
        FROM (SELECT tmp_schema, tmp_tbl_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_table
                GROUP BY tmp_schema, tmp_tbl_name
                HAVING count(DISTINCT tmp_group) > 1) AS t1
      UNION
      SELECT 271, 1, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::TEXT, nb_group::INT,
             format('The sequence %s.%s is referenced in %s different tables groups.',
                    quote_ident(tmp_schema), quote_ident(tmp_seq_name), nb_group)
        FROM (SELECT tmp_schema, tmp_seq_name, count(DISTINCT tmp_group) AS nb_group
                FROM tmp_app_sequence
                GROUP BY tmp_schema, tmp_seq_name
                HAVING count(DISTINCT tmp_group) > 1) AS t2
      UNION
      SELECT 272, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is referenced several times.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM (SELECT tmp_group, tmp_schema, tmp_tbl_name, count(*)
                FROM tmp_app_table
                GROUP BY tmp_group, tmp_schema, tmp_tbl_name
                HAVING count(*) > 1) AS t3
      UNION
      SELECT 273, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the sequence %s.%s is referenced several times.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM (SELECT tmp_group, tmp_schema, tmp_seq_name, count(*)
                FROM tmp_app_sequence
                GROUP BY tmp_group, tmp_schema, tmp_seq_name
                HAVING count(*) > 1) AS t4;
    IF FOUND THEN
      RETURN;
    END IF;
-- Check that the tmp_app_table and tmp_app_sequence tables content is ok for the imported groups.
    RETURN QUERY
      SELECT *
        FROM emaj._import_groups_conf_check(p_groups, p_dropOtherGroups)
        WHERE ((rpt_text_var_1 = ANY (p_groups) AND rpt_severity = 1)
            OR (rpt_text_var_1 = ANY (v_rollbackableGroups) AND rpt_severity = 2))
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3;
--
    RETURN;
  END;
$_import_groups_conf_prepare$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_check(p_groupNames TEXT[], p_dropOtherGroups BOOLEAN)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_check$
-- This function verifies that the content of tables group as defined into the tmp_app_table and tmp_app_sequence tables is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them, depending on the tables group type.
-- It is called by the _import_groups_conf_prepare() function.
-- This function checks that the referenced application tables and sequences:
--  - exist, and only once
--  - are not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
-- It also checks that:
--  - tables are not TEMPORARY
--  - for rollbackable groups, tables are not UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for tables, configured tablespaces exist
-- Input: name array of the tables groups to check
--        flag to request for extra groups drop
-- Output: _report_message_type records representing diagnostic messages
--         the rpt_severity is set to 1 if the error blocks any type group creation or alter,
--                                 or 2 if the error only blocks ROLLBACKABLE groups creation
  BEGIN
-- Check that all application tables listed for the group really exist.
    RETURN QUERY
      SELECT 1, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE tmp_schema = nspname AND tmp_tbl_name = relname
                     AND relkind IN ('r','p')
                );
-- Check that no application table is a partitioned table (only elementary partitions can be managed by E-Maj).
    RETURN QUERY
      SELECT 2, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'p';
-- Check no application schema listed for the group in the tmp_app_table table is an E-Maj schema.
    RETURN QUERY
      SELECT 3, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
-- Check that no table of the checked groups already belongs to other created groups.
    IF NOT p_dropOtherGroups THEN
      RETURN QUERY
        SELECT 4, 1, tmp_group, tmp_schema, tmp_tbl_name, rel_group, NULL::INT,
               format('In tables group "%s", the table %s.%s is already assigned to the group "%s".',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(rel_group))
          FROM tmp_app_table
               JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_tbl_name AND upper_inf(rel_time_range))
          WHERE NOT rel_group = ANY (p_groupNames);
    END IF;
-- Check that no table is a TEMP table.
    RETURN QUERY
      SELECT 5, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is a TEMPORARY table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r' AND relpersistence = 't';
-- Check that the log data tablespaces for tables exist.
    RETURN QUERY
      SELECT 12, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_dat_tsp, NULL::INT,
             format('In tables group "%s" and for the table %s.%s, the data log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_dat_tsp))
        FROM tmp_app_table
        WHERE tmp_log_dat_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_dat_tsp
                );
-- Check that the log index tablespaces for tables exist.
    RETURN QUERY
      SELECT 13, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_idx_tsp, NULL::INT,
             format('In tables group "%s" and for the table %s.%s, the index log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_idx_tsp))
        FROM tmp_app_table
        WHERE tmp_log_idx_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_idx_tsp
                );
-- Check that all listed triggers exist,
    RETURN QUERY
      SELECT 15, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_trigger, NULL::INT,
             format('In tables group "%s" and for the table %I.%I, the trigger %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_trigger))
        FROM
          (SELECT tmp_group, tmp_schema, tmp_tbl_name, unnest(tmp_ignored_triggers) AS tmp_trigger
             FROM tmp_app_table
                  JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
             WHERE relkind = 'r'
          ) AS t
        WHERE NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_trigger ON (tgrelid = pg_class.oid)
                   WHERE nspname = tmp_schema AND relname = tmp_tbl_name AND tgname = tmp_trigger
                     AND NOT tgisinternal
                );
-- ... and are not emaj triggers.
    RETURN QUERY
      SELECT 16, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_trigger, NULL::INT,
             format('In tables group "%s" and for the table %I.%I, the trigger %I is an E-Maj trigger.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_trigger))
        FROM
          (SELECT tmp_group, tmp_schema, tmp_tbl_name, unnest(tmp_ignored_triggers) AS tmp_trigger
             FROM tmp_app_table
                  JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
             WHERE relkind = 'r'
          ) AS t
        WHERE tmp_trigger IN ('emaj_trunc_trg', 'emaj_log_trg');
-- Check that no table is an unlogged table (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 20, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s is an UNLOGGED table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND relpersistence = 'u';
-- Check every table has a primary key (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 22, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the table %s.%s has no PRIMARY KEY.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                   WHERE contype = 'p' AND nspname = tmp_schema AND relname = tmp_tbl_name
                );
-- Check that all application sequences listed for the group really exist.
    RETURN QUERY
      SELECT 31, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the sequence %s.%s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM tmp_app_sequence
        WHERE NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE tmp_schema = nspname
                     AND tmp_seq_name = relname
                     AND relkind = 'S'
                );
-- Check that no sequence of the checked groups already belongs to other created groups.
    IF NOT p_dropOtherGroups THEN
      RETURN QUERY
        SELECT 32, 1, tmp_group, tmp_schema, tmp_seq_name, rel_group, NULL::INT,
               format('In tables group "%s", the sequence %s.%s is already assigned to the group %s.',
                      tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name), quote_ident(rel_group))
          FROM tmp_app_sequence
               JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_seq_name AND upper_inf(rel_time_range))
          WHERE NOT rel_group = ANY (p_groupNames);
    END IF;
-- Check no application schema listed for the group in the tmp_app_sequence table is an E-Maj schema.
    RETURN QUERY
      SELECT 33, 1, tmp_group, tmp_schema, tmp_seq_name, NULL::TEXT, NULL::INT,
             format('In tables group "%s", the sequence %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name))
        FROM tmp_app_sequence
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
--
    RETURN;
  END;
$_import_groups_conf_check$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_exec(p_json JSON, p_groups TEXT[], p_mark TEXT,
                                                         p_dropOtherGroups BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$_import_groups_conf_exec$
-- This function completes a tables groups configuration import.
-- It is called by _import_groups_conf() and by Emaj_web
-- Non existing groups are created empty.
-- The _import_groups_conf_alter() function is used to process the assignement, the move, the removal or the attributes change for tables
-- and sequences and the extra groups drop.
-- Input: - the tables groups configuration structure in JSON format
--        - the array of group names to process
--        - a boolean indicating whether tables groups to import may already exist
--        - the mark name to set for tables groups in logging state
--        - an optional boolean to ask for the drop of all existing groups that are not in the imported configuration (FALSE by default)
-- Output: the number of created or altered tables groups
  DECLARE
    v_function               TEXT = 'IMPORT_GROUPS';
    v_timeId                 BIGINT;
    v_groupsJson             JSON;
    v_nbGroup                INT;
    v_comment                TEXT;
    v_isRollbackable         BOOLEAN;
    v_loggingGroups          TEXT[];
    v_groupsToDrop           TEXT[];
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
-- Lock the concerned groups to avoid concurrent operation on these groups.
    PERFORM 0
      FROM emaj.emaj_group
      WHERE p_dropOtherGroups OR group_name = ANY (p_groups)
      FOR UPDATE;
-- Build the groups to drop array, if requested.
    IF p_dropOtherGroups THEN
      SELECT array_agg(group_name ORDER BY group_name) INTO v_groupsToDrop
        FROM emaj.emaj_group
        WHERE NOT group_name = ANY (p_groups);
    END IF;
-- Build the in logging state groups array.
    SELECT array_agg(group_name ORDER BY group_name) INTO v_loggingGroups
      FROM emaj.emaj_group
      WHERE (p_dropOtherGroups OR group_name = ANY (p_groups))
        AND group_is_logging;
-- If some groups are in logging state, check and set the supplied mark name and lock the groups.
    IF v_loggingGroups IS NOT NULL THEN
      SELECT emaj._check_new_mark(p_groups, p_mark) INTO v_markName;
-- Lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
      PERFORM emaj._lock_groups(v_loggingGroups, 'ROW EXCLUSIVE', TRUE);
-- And set the mark, using the same time identifier.
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
    END IF;
-- Process the tmp_app_table and tmp_app_sequence content change.
    PERFORM emaj._import_groups_conf_alter(p_groups, p_mark, v_timeId, v_groupsToDrop);
-- Check foreign keys with tables outside the groups in logging state.
    PERFORM emaj._check_fk_groups(v_loggingGroups);
-- Drop the now useless temporary tables.
    DROP TABLE tmp_app_table;
    DROP TABLE tmp_app_sequence;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES (v_function, 'END', v_nbGroup || ' created or altered tables groups');
--
    RETURN v_nbGroup;
  END;
$_import_groups_conf_exec$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_alter(p_groupNames TEXT[], p_mark TEXT, p_timeId BIGINT, p_groupsToDrop TEXT[])
RETURNS VOID LANGUAGE plpgsql AS
$_import_groups_conf_alter$
-- This function effectively alters the tables groups to import.
-- It uses the content of tmp_app_table and tmp_app_sequence temporary tables and calls the appropriate elementary functions
-- It is called by the _import_groups_conf_exec() function.
-- Input: group names array,
--        the mark name to set on groups in logging state
--        the timestamp id
--        the groups to drop array
  DECLARE
    v_eventTriggers          TEXT[];
    v_function               TEXT = 'IMPORT_GROUPS';
  BEGIN
-- Disable event triggers that protect emaj components and keep in memory these triggers name.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Create the needed log schemas.
    PERFORM emaj._create_log_schemas('IMPORT_GROUPS', p_timeId);
-- Remove the tables that do not belong to the groups anymore.
    PERFORM emaj._remove_tbl(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
            AND upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND NOT EXISTS
                  (SELECT NULL
                     FROM tmp_app_table
                     WHERE tmp_schema = rel_schema
                     AND tmp_tbl_name = rel_tblseq
                  )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Remove the sequences that do not belong to the groups anymore.
    PERFORM emaj._remove_seq(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM emaj.emaj_relation
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
            AND upper_inf(rel_time_range)
            AND rel_kind = 'S'
            AND NOT EXISTS
                  (SELECT NULL
                     FROM tmp_app_sequence
                     WHERE tmp_schema = rel_schema
                       AND tmp_seq_name = rel_tblseq
                  )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Repair the tables that are damaged or out of sync E-Maj components.
    PERFORM emaj._repair_tbl(rel_schema, rel_tblseq, rel_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, group_is_logging
          FROM                                   -- all damaged or out of sync tables
            (SELECT DISTINCT ver_schema, ver_tblseq
               FROM emaj._verify_groups(p_groupNames, FALSE)
            ) AS t
            JOIN emaj.emaj_relation ON (rel_schema = ver_schema AND rel_tblseq = ver_tblseq AND upper_inf(rel_time_range))
            JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
            JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the priority level when requested
-- (the later operations will be executed with the new priorities).
    PERFORM emaj._change_priority_tbl(rel_schema, rel_tblseq, rel_priority, tmp_priority, p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_priority, tmp_priority, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND ( (rel_priority IS NULL AND tmp_priority IS NOT NULL) OR
                  (rel_priority IS NOT NULL AND tmp_priority IS NULL) OR
                  (rel_priority <> tmp_priority) )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the log data tablespaces.
    PERFORM emaj._change_log_data_tsp_tbl(rel_schema, rel_tblseq, rel_log_schema, rel_log_table, rel_log_dat_tsp, tmp_log_dat_tsp,
                                          p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_log_table, rel_log_dat_tsp, tmp_log_dat_tsp, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'
            AND coalesce(rel_log_dat_tsp,'') <> coalesce(tmp_log_dat_tsp,'')
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the log index tablespaces.
    PERFORM emaj._change_log_index_tsp_tbl(rel_schema, rel_tblseq, rel_log_schema, rel_log_index, rel_log_idx_tsp, tmp_log_idx_tsp,
                                          p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_log_index, rel_log_idx_tsp, tmp_log_idx_tsp, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
               JOIN emaj.emaj_group ON (group_name = rel_group)
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'
            AND coalesce(rel_log_idx_tsp,'') <> coalesce(tmp_log_idx_tsp,'')
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the arrays of triggers to ignore at rollback time for tables.
    PERFORM emaj._change_ignored_triggers_tbl(rel_schema, rel_tblseq, rel_ignored_triggers, tmp_ignored_triggers,
                                              p_timeId, rel_group, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_ignored_triggers, tmp_ignored_triggers, rel_group
          FROM emaj.emaj_relation
               JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND ( (rel_ignored_triggers IS NULL AND tmp_ignored_triggers IS NOT NULL) OR
                  (rel_ignored_triggers IS NOT NULL AND tmp_ignored_triggers IS NULL) OR
                  (rel_ignored_triggers <> tmp_ignored_triggers) )
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the group ownership of tables.
    PERFORM emaj._move_tbl(rel_schema, rel_tblseq, rel_group, old_group_is_logging, tmp_group, new_group_is_logging,
                           p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, old_group.group_is_logging AS old_group_is_logging,
                                       tmp_group, new_group.group_is_logging AS new_group_is_logging
          FROM emaj.emaj_relation
              JOIN tmp_app_table ON (tmp_schema = rel_schema AND tmp_tbl_name = rel_tblseq)
              JOIN emaj.emaj_group old_group ON (old_group.group_name = rel_group)
              JOIN emaj.emaj_group new_group ON (new_group.group_name = tmp_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'r'
            AND rel_group = ANY (p_groupNames)
            AND rel_group <> tmp_group
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Change the group ownership of sequences.
    PERFORM emaj._move_seq(rel_schema, rel_tblseq, rel_group, old_group_is_logging, tmp_group, new_group_is_logging,
                           p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT rel_schema, rel_tblseq, rel_group, old_group.group_is_logging AS old_group_is_logging,
                                       tmp_group, new_group.group_is_logging AS new_group_is_logging
          FROM emaj.emaj_relation
              JOIN tmp_app_sequence ON (tmp_schema = rel_schema AND tmp_seq_name = rel_tblseq)
              JOIN emaj.emaj_group old_group ON (old_group.group_name = rel_group)
              JOIN emaj.emaj_group new_group ON (new_group.group_name = tmp_group)
          WHERE upper_inf(rel_time_range)
            AND rel_kind = 'S'
            AND rel_group = ANY (p_groupNames)
            AND rel_group <> tmp_group
          ORDER BY rel_priority, rel_schema, rel_tblseq
           ) AS t;
-- Add tables to the groups.
    PERFORM emaj._add_tbl(tmp_schema, tmp_tbl_name, tmp_group, tmp_priority, tmp_log_dat_tsp,
                          tmp_log_idx_tsp, tmp_ignored_triggers, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT tmp_schema, tmp_tbl_name, tmp_group, tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp,
               tmp_ignored_triggers, group_is_logging
          FROM tmp_app_table
               JOIN emaj.emaj_group ON (group_name = tmp_group)
          WHERE NOT EXISTS
                  (SELECT NULL
                     FROM emaj.emaj_relation
                     WHERE rel_schema = tmp_schema
                       AND rel_tblseq = tmp_tbl_name
                       AND upper_inf(rel_time_range)
                       AND rel_kind = 'r'
                       AND rel_group = ANY (p_groupNames)
                  )
          ORDER BY tmp_priority, tmp_schema, tmp_tbl_name
           ) AS t;
-- Add sequences to the groups.
    PERFORM emaj._add_seq(tmp_schema, tmp_seq_name, tmp_group, group_is_logging, p_timeId, 'IMPORT_GROUPS')
      FROM (
        SELECT tmp_schema, tmp_seq_name, tmp_group, group_is_logging
          FROM tmp_app_sequence
               JOIN emaj.emaj_group ON (group_name = tmp_group)
          WHERE NOT EXISTS
                  (SELECT NULL
                     FROM emaj.emaj_relation
                     WHERE rel_schema = tmp_schema
                       AND rel_tblseq = tmp_seq_name
                       AND upper_inf(rel_time_range)
                       AND rel_kind = 'S'
                       AND rel_group = ANY (p_groupNames)
                  )
          ORDER BY tmp_schema, tmp_seq_name
           ) AS t;
-- Drop the not imported tables groups, if requested.
    IF p_groupsToDrop IS NOT NULL THEN
      PERFORM emaj._drop_groups(p_groupsToDrop, p_timeId);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        SELECT v_function, 'GROUP DROPPED', group_name
          FROM unnest(p_groupsToDrop) AS group_name;
    END IF;
-- Drop the E-Maj log schemas that are now useless (i.e. not used by any created group).
    PERFORM emaj._drop_log_schemas('IMPORT_GROUPS', FALSE);
-- Re-enable previously disabled event triggers.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Update some attributes in the emaj_group table.
    UPDATE emaj.emaj_group
      SET group_last_alter_time_id = p_timeId,
          group_nb_table =
            (SELECT count(*)
               FROM emaj.emaj_relation
               WHERE rel_group = group_name
                 AND upper_inf(rel_time_range)
                 AND rel_kind = 'r'
             ),
          group_nb_sequence =
            (SELECT count(*)
               FROM emaj.emaj_relation
               WHERE rel_group = group_name
                 AND upper_inf(rel_time_range)
                 AND rel_kind = 'S'
            )
      WHERE group_name = ANY (p_groupNames);
--
    RETURN;
  END;
$_import_groups_conf_alter$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_logging_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of logging group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE group_is_logging
    AND (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_logging_groups(TEXT, TEXT) IS
$$Builds a logging groups array, filtered on their names.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_idle_groups(p_includeFilter TEXT DEFAULT NULL, p_excludeFilter TEXT DEFAULT NULL)
RETURNS TEXT[] LANGUAGE SQL STABLE AS
$$
-- This function returns the array of idle group names, with optional regexp to select or exclude some of them.
-- Groups are sorted in alphabetic order.
SELECT array_agg(group_name ORDER BY group_name)
  FROM emaj.emaj_group
  WHERE NOT group_is_logging
    AND (p_includeFilter IS NULL OR group_name ~ p_includeFilter)
    AND (p_excludeFilter IS NULL OR group_name !~ p_excludeFilter);
$$;
COMMENT ON FUNCTION emaj.emaj_get_idle_groups(TEXT, TEXT) IS
$$Builds a idle groups array, filtered on their names.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_comment TEXT, p_multiGroup BOOLEAN,
                                                 p_eventToRecord BOOLEAN, p_loggedRlbkTargetMark TEXT DEFAULT NULL,
                                                 p_timeId BIGINT DEFAULT NULL, p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates 1) the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into
-- all log tables between this previous mark and the new mark and 2) the current log session.
-- The function is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks,
-- like functions that start, stop or rollback groups.
-- Input: group names array, mark to set, comment,
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
    r_seq                    RECORD;
    r_currSeq                emaj.emaj_sequence%ROWTYPE;
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
    v_nbSeq = 0;
    FOR r_seq IN
      SELECT rel_schema, rel_tblseq
        FROM emaj.emaj_relation
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'S'
          AND rel_group = ANY (p_groupNames)
    LOOP
      r_currSeq = emaj._get_current_seq(r_seq.rel_schema, r_seq.rel_tblseq, p_timeId);
      INSERT INTO emaj.emaj_sequence VALUES (r_currSeq.*);
      v_nbSeq = v_nbSeq + 1;
    END LOOP;
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
    INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_time_id, mark_is_rlbk_protected, mark_comment, mark_logged_rlbk_target_mark)
      SELECT group_name, p_mark, p_timeId, FALSE, p_comment, p_loggedRlbkTargetMark
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

CREATE OR REPLACE FUNCTION emaj._log_stat_sequence(p_schema TEXT, p_sequence TEXT, p_startTimeId BIGINT, p_endTimeId BIGINT)
RETURNS SETOF emaj.emaj_log_stat_sequence_type LANGUAGE plpgsql AS
$_log_stat_sequence$
-- This function returns statistics about a single sequence, for the time period framed by a supplied time_id slice.
-- It is called by the various emaj_log_stat_sequence() function.
-- For each mark interval, possibly for different tables groups, it returns the number of increments and a flag to show any sequence
--   properties changes.
-- Input: schema and sequence names
--        start and end time_id.
-- Output: set of stats by time slice.
  DECLARE
    v_needCurrentState       BOOLEAN;
    r_endSeq                 emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Get the sequence current state, if it still belongs to an active group.
    v_needCurrentState = EXISTS(
      SELECT 0
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND upper_inf(rel_time_range)
          AND group_is_logging
          AND p_endTimeId IS NULL
      );
    IF v_needCurrentState THEN
      r_endSeq = emaj._get_current_seq(p_schema, p_sequence, 0);
    END IF;
-- Compute and return the statistics by scanning the sequence history in the emaj_sequence table.
    RETURN QUERY
      WITH event AS (
        SELECT sequ_time_id, lower(rel_time_range), sequ_last_val, sequ_start_val, sequ_increment, sequ_max_val,
               sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called,
               rel_group, rel_time_range, time_clock_timestamp, time_event,
               coalesce(mark_name, '[deleted mark]') AS mark_name,
               (time_event = 'S' OR sequ_time_id = lower(rel_time_range)) AS log_start,
               (time_event = 'X' OR (NOT upper_inf(rel_time_range) AND sequ_time_id = upper(rel_time_range))) AS log_stop
          FROM emaj.emaj_sequence
               JOIN emaj.emaj_relation ON (rel_schema = sequ_schema AND rel_tblseq = sequ_name)
               JOIN emaj.emaj_time_stamp ON (time_id = sequ_time_id)
               LEFT OUTER JOIN emaj.emaj_mark ON (mark_group = rel_group AND mark_time_id = sequ_time_id)
          WHERE sequ_schema = p_schema
            AND sequ_name = p_sequence
            AND (sequ_time_id <@ rel_time_range OR sequ_time_id = upper(rel_time_range))
            AND (p_startTimeId IS NULL OR sequ_time_id >= p_startTimeId)
            AND (p_endTimeId IS NULL OR sequ_time_id <= p_endTimeId)
        UNION
-- ... and add the sequence current state, if the upper bound is not fixed and if the sequence belongs to an active group yet.
        SELECT NULL, NULL, r_endSeq.sequ_last_val, r_endSeq.sequ_start_val, r_endSeq.sequ_increment, r_endSeq.sequ_max_val,
               r_endSeq.sequ_min_val, r_endSeq.sequ_cache_val, r_endSeq.sequ_is_cycled, r_endSeq.sequ_is_called,
               rel_group, rel_time_range, clock_timestamp(), '', '[current state]', false, false
          FROM emaj.emaj_relation
          WHERE v_needCurrentState
            AND rel_schema = p_schema
            AND rel_tblseq = p_sequence
            AND upper_inf(rel_time_range)
        ORDER BY 1, 2
        ), time_slice AS (
-- Transform elementary time events into time slices
         SELECT sequ_time_id AS start_time_id, lead(sequ_time_id) OVER () AS end_time_id,
                sequ_last_val AS start_last_val, lead(sequ_last_val) OVER () AS end_last_val,
                sequ_start_val AS start_start_val, lead(sequ_start_val) OVER () AS end_start_val,
                sequ_increment AS start_increment, lead(sequ_increment) OVER () AS end_increment,
                sequ_max_val AS start_max_val, lead(sequ_max_val) OVER () AS end_max_val,
                sequ_min_val AS start_min_val, lead(sequ_min_val) OVER () AS end_min_val,
                sequ_cache_val AS start_cache_val, lead(sequ_cache_val) OVER () AS end_cache_val,
                sequ_is_cycled AS start_is_cycled, lead(sequ_is_cycled) OVER () AS end_is_cycled,
                sequ_is_called AS start_is_called, lead(sequ_is_called) OVER () AS end_is_called,
                rel_group,
                rel_time_range AS start_rel_time_range, lead(rel_time_range) OVER () AS end_rel_time_range,
                time_clock_timestamp AS start_timestamp, lead(time_clock_timestamp) OVER () AS end_timestamp,
                time_event AS start_time_event, lead(time_event) OVER () AS end_time_event,
                mark_name AS start_mark_name, lead(mark_name) OVER () AS end_mark_name,
                log_start AS start_log_start, lead(log_start) OVER () AS end_log_start,
                log_stop AS start_log_stop, lead(log_stop) OVER () AS end_log_stop
           FROM event
        )
-- Filter time slices and compute statistics aggregates
      SELECT rel_group AS stat_group, start_mark_name AS stat_first_mark, start_timestamp AS stat_first_mark_datetime,
             start_time_id AS stat_first_time_id, start_log_start AS stat_is_log_start, end_mark_name AS stat_last_mark,
             end_timestamp AS stat_last_mark_datetime, end_time_id AS stat_last_time_id, end_log_stop AS stat_is_log_stop,
             (end_last_val - start_last_val) / start_increment
               + CASE WHEN start_is_called THEN 0 ELSE 1 END
               - CASE WHEN end_is_called THEN 0 ELSE 1 END AS stat_increments,
             (start_start_val <> end_start_val OR start_increment <> end_increment OR start_max_val <> end_max_val
                OR start_min_val <> end_min_val OR start_is_cycled <> end_is_cycled) AS stat_has_structure_changed,
             count(rlbk_id)::INT AS stat_rollbacks
        FROM time_slice
             LEFT OUTER JOIN emaj.emaj_rlbk ON (rel_group = ANY (rlbk_groups) AND
                                                rlbk_time_id >= start_time_id AND (end_time_id IS NULL OR rlbk_time_id < end_time_id))
        WHERE end_timestamp IS NOT NULL                    -- time slice not starting with the very last event
          AND start_rel_time_range = end_rel_time_range    -- same rel_time_range on the slice
          AND NOT (start_log_stop AND end_log_start)       -- not a logging hole
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        ORDER BY start_time_id;
  END;
$_log_stat_sequence$;

--<end_functions>                                pattern used by the tool that extracts and inserts the functions definition

----------------------------------------------------------------
--                                                            --
--                       Event triggers                       --
--                                                            --
----------------------------------------------------------------

----------------------------------------------------------------
--                                                            --
--                 Rights on emaj components                  --
--                                                            --
----------------------------------------------------------------

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA emaj FROM PUBLIC;

GRANT ALL ON ALL TABLES IN SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL SEQUENCES IN SCHEMA emaj TO emaj_adm;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA emaj TO emaj_adm;

GRANT SELECT ON ALL TABLES IN SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA emaj TO emaj_viewer;
REVOKE SELECT ON TABLE emaj.emaj_param FROM emaj_viewer;

GRANT EXECUTE ON FUNCTION emaj._get_current_seq(p_schema TEXT, p_sequence TEXT, p_timeId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_schema(p_schema TEXT, p_exceptionIfMissing BOOLEAN, p_checkNotEmaj BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_assigned_group_table(p_schema TEXT, p_table TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_assigned_group_sequence(p_schema TEXT, p_sequence TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_logging_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_idle_groups(p_includeFilter TEXT, p_excludeFilter TEXT) TO emaj_viewer;

----------------------------------------------------------------
--                                                            --
--                    Complete the upgrade                    --
--                                                            --
----------------------------------------------------------------

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

-- Insert the upgrade end record in the operation history.
INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
  VALUES ('EMAJ_INSTALL','END','E-Maj <devel>', 'Upgrade from 4.7.1 completed');

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
