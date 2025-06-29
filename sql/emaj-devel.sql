--
-- E-Maj : logs and rollbacks table changes : Version <devel>
--
-- This software is distributed under the GNU General Public License.
--
-- This psql script creates the emaj environment, for cases when the "CREATE EXTENSION" syntax is not possible.
-- Use the "CREATE EXTENSION" syntax when possible.
--
-- This script may be executed by a non SUPERUSER role. But in this case, the installation role must be
--   the owner of application tables and sequences that will constitute the future tables groups.
--
-- The E-Maj technical tables will be installed into the default tablespace.
-- The user executing the installation may set it to a particular value using a "set default_tablespace to <name>;" statement.
--
-- The emaj extension also installs the dblink and btree_gist extensions into the database if they are not already installed.


-- Perform some checks and create emaj roles.
DO LANGUAGE plpgsql
$do$
  BEGIN
-- Check postgres version is >= 11.
    IF current_setting('server_version_num')::INT < 110000 THEN
      RAISE EXCEPTION 'E-Maj installation: The current postgres version (%) is too old for this E-Maj version. It should be at least 11.',
        current_setting('server_version');
    END IF;
-- Create both emaj_adm and emaj_viewer roles (NOLOGIN), if they do not exist.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_roles
            WHERE rolname = 'emaj_adm'
         ) THEN
      CREATE ROLE emaj_adm;
      COMMENT ON ROLE emaj_adm IS
        $$This role may be granted to other roles in charge of E-Maj administration.$$;
    END IF;
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_roles
            WHERE rolname = 'emaj_viewer'
         ) THEN
      CREATE ROLE emaj_viewer;
      COMMENT ON ROLE emaj_viewer IS
        $$This role may be granted to other roles allowed to view E-Maj objects content.$$;
    END IF;
    RETURN;
  END;
$do$;

BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS emaj CASCADE;
CREATE SCHEMA emaj;

CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS btree_gist;
COMMENT ON SCHEMA emaj IS
$$Contains all E-Maj related objects.$$;

------------------------------------------------
--                                            --
-- emaj enum types, sequences and tables      --
--                                            --
------------------------------------------------

-- Table containing the history of all installed E-Maj versions.
-- It is the very first created structure, in order to record the installation duration.
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
INSERT INTO emaj.emaj_version_hist (verh_version, verh_time_range) VALUES ('<devel>', TSTZRANGE(clock_timestamp(), null, '[]'));
COMMENT ON TABLE emaj.emaj_version_hist IS
$$Contains E-Maj versions history.$$;

-- Enum of the possible values for the rlchg_change_kind column of the emaj_relation_change table.
CREATE TYPE emaj._relation_change_kind_enum AS ENUM (
  'REMOVE_TABLE',
  'REMOVE_SEQUENCE',
  'REPAIR_TABLE',
  'CHANGE_LOG_DATA_TABLESPACE',
  'CHANGE_LOG_INDEX_TABLESPACE',
  'CHANGE_PRIORITY',
  'CHANGE_IGNORED_TRIGGERS',
  'MOVE_TABLE',
  'MOVE_SEQUENCE',
  'ADD_TABLE',
  'ADD_SEQUENCE',
  'RENAME_SCHEMA',
  'RENAME_TABLE',
  'RENAME_SEQUENCE'
  );

-- Enum of the possible values for the rollback status columns.
CREATE TYPE emaj._rlbk_status_enum AS ENUM (
  'PLANNING',                -- the emaj rollback is in the initial planning phase
  'LOCKING',                 -- the emaj rollback is acquiring locks on tables
  'EXECUTING',               -- the emaj rollback is in the main executing phase
  'COMPLETED',               -- the emaj rollback is completed but the status of its transaction is not yet known
  'COMMITTED',               -- the emaj rollback transaction is known as committed
  'ABORTED'                  -- the emaj rollback transaction is known as aborted
  );

-- Enum of the possible values for the rollback steps.
CREATE TYPE emaj._rlbk_step_enum AS ENUM (
  'LOCK_TABLE',              -- set a lock on a table
  'RLBK_SEQUENCES',          -- rollback all sequences at once
  'DIS_APP_TRG',             -- disable an application trigger
  'SET_ALWAYS_APP_TRG',      -- set an application trigger as an ALWAYS trigger (to fire even in a 'replica' session_replication_role)
  'DIS_LOG_TRG',             -- disable a log trigger
  'DROP_FK',                 -- drop a foreign key
  'SET_FK_DEF',              -- set a foreign key deferred
  'RLBK_TABLE',              -- rollback a table
  'DELETE_LOG',              -- delete rows from a log table
  'SET_FK_IMM',              -- set a foreign key immediate
  'ADD_FK',                  -- recreate a foreign key
  'ENA_APP_TRG',             -- enable an application trigger
  'SET_LOCAL_APP_TRG',       -- set an application trigger as a regular trigger, to reset the SET_ALWAYS_APP_TRG action
  'ENA_LOG_TRG',             -- enable a log trigger
  'CTRL+DBLINK',             -- pseudo step representing the periods between 2 steps execution, when dblink is used
  'CTRL-DBLINK'              -- pseudo step representing the periods between 2 steps execution, when dblink is not used
  );

-- The emaj_global_seq sequence provides a unique identifier for all rows inserted into all emaj log tables of the database.
-- It is used to order all these rows in insertion time order for rollback as well as other purposes.
-- (So this order is not based on system time that can be unsafe).
-- The sequence is created with the following  (default) characteristics:
-- - increment = 1
-- - no cache (to keep the delivered nextval value in time order)
-- - no cycle (would the end of the sequence be reached, no new log row would be accepted)
CREATE SEQUENCE emaj.emaj_global_seq;
COMMENT ON SEQUENCE emaj.emaj_global_seq IS
$$Global sequence to identifiy all rows of emaj log tables.$$;

-- Table containing E-maj parameters.
CREATE TABLE emaj.emaj_param (
  param_key                    TEXT        NOT NULL,       -- parameter key
  param_value_text             TEXT,                       -- value if type is text, otherwise NULL
  param_value_numeric          NUMERIC,                    -- value if type is numeric, otherwise NULL
  param_value_boolean          BOOLEAN,                    -- value if type is boolean, otherwise NULL
  param_value_interval         INTERVAL,                   -- value if type is interval, otherwise NULL
  PRIMARY KEY (param_key)
  );
COMMENT ON TABLE emaj.emaj_param IS
$$Contains E-Maj parameters.$$;

-- Table containing the history of all E-Maj events.
CREATE TABLE emaj.emaj_hist (
  hist_id                      BIGINT      NOT NULL        -- internal id
                                           GENERATED ALWAYS AS IDENTITY,
  hist_datetime                TIMESTAMPTZ NOT NULL
                               DEFAULT clock_timestamp(),  -- insertion time
  hist_function                TEXT        NOT NULL,       -- main E-Maj function generating the event
  hist_event                   TEXT,                       -- type of event (often BEGIN or END)
  hist_object                  TEXT,                       -- object supporting the event (often the group name)
  hist_wording                 TEXT,                       -- additional comment
  hist_user                    TEXT
                               DEFAULT session_user,       -- the user who call the E-Maj function
  hist_txid                    BIGINT
                               DEFAULT txid_current(),     -- and its tx_id
  PRIMARY KEY (hist_id)
  );
COMMENT ON TABLE emaj.emaj_hist IS
$$Contains E-Maj events history.$$;

-- Table containing the time stamps of major E-Maj events.
-- These stamps, used as time references in other internal tables, are insensitive to system time fluctuations and transaction wrapaound.
CREATE TABLE emaj.emaj_time_stamp (
  time_id                      BIGINT      NOT NULL        -- internal id
                                           GENERATED ALWAYS AS IDENTITY,
  time_clock_timestamp         TIMESTAMPTZ NOT NULL        -- insertion clock time
                               DEFAULT clock_timestamp(),
  time_stmt_timestamp          TIMESTAMPTZ NOT NULL        -- insertion statement start time
                               DEFAULT statement_timestamp(),
  time_tx_timestamp            TIMESTAMPTZ NOT NULL        -- insertion transaction start time
                               DEFAULT transaction_timestamp(),
  time_txid                    BIGINT                      -- id of the tx that has generated the time stamp
                               DEFAULT txid_current(),
  time_last_emaj_gid           BIGINT,                     -- last value of the E-Maj global sequence
  time_event                   CHAR(1),                    -- event type that has generated the time stamp
                                                           --   C(reate group), D(rop group), A(lter group), I(mport)
                                                           --   S(tart), M(ark setting), R(ollback), X(stop)
  PRIMARY KEY (time_id)
  );
COMMENT ON TABLE emaj.emaj_time_stamp IS
$$Contains the time stamps of major E-Maj events.$$;

-- Table containing the defined groups.
-- Rows are created at emaj_create_group time and deleted at emaj_drop_group time.
CREATE TABLE emaj.emaj_group (
  group_name                   TEXT        NOT NULL,
  group_is_rollbackable        BOOLEAN     NOT NULL,       -- false for 'AUDIT_ONLY' and true for 'ROLLBACKABLE' groups
  group_pg_version             TEXT        NOT NULL        -- postgres version at emaj_create_group() time
                               DEFAULT substring (version() FROM E'PostgreSQL\\s([.,0-9,A-Z,a-z]*)'),
  group_last_alter_time_id     BIGINT,                     -- time stamp of the last group structure change
                                                           --   (NULL at group creation time)
  group_is_logging             BOOLEAN     NOT NULL,       -- are log triggers activated ?
  group_is_rlbk_protected      BOOLEAN     NOT NULL,       -- is the group currently protected against rollback ?
                                                           --   (always true for AUDIT_ONLY groups)
  group_nb_table               INT,                        -- current number of tables
  group_nb_sequence            INT,                        -- current number of sequences
  group_comment                TEXT,                       -- optional user comment
  PRIMARY KEY (group_name),
  FOREIGN KEY (group_last_alter_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_group IS
$$Contains created E-Maj groups.$$;

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

-- Table containing the emaj and log schemas.
CREATE TABLE emaj.emaj_schema (
  sch_name                     TEXT        NOT NULL,       -- schema name
  sch_time_id                  BIGINT,                     -- insertion time (NULL for emaj)
  PRIMARY KEY (sch_name),
  FOREIGN KEY (sch_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_schema IS
$$Contains the E-Maj related schemas (emaj and all schemas hosting log tables, sequences and functions).$$;

-- Table containing the relations (tables and sequences) of created tables groups.
CREATE TABLE emaj.emaj_relation (
  rel_schema                   TEXT        NOT NULL,       -- schema name containing the relation
  rel_tblseq                   TEXT        NOT NULL,       -- application table or sequence name
  rel_time_range               INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
  rel_group                    TEXT        NOT NULL,       -- name of the group that owns the relation
  rel_kind                     TEXT,                       -- similar to the relkind column of pg_class table
                                                           --   ('r' = table, 'S' = sequence)
-- Next columns are specific for tables and remain NULL for sequences.
  rel_priority                 INTEGER,                    -- priority level of processing inside the group
  rel_log_schema               TEXT,                       -- schema for the log table, functions and sequence
  rel_log_table                TEXT,                       -- name of the log table associated
  rel_log_dat_tsp              TEXT,                       -- tablespace for the log table
  rel_log_index                TEXT,                       -- name of the index of the log table
  rel_log_idx_tsp              TEXT,                       -- tablespace for the log index
  rel_log_sequence             TEXT,                       -- name of the log sequence
  rel_log_function             TEXT,                       -- name of the function associated to the log trigger
                                                           -- created on the application table
  rel_ignored_triggers         TEXT[],                     -- names array of application trigger to ignore at rollback time
  rel_pk_cols                  TEXT[],                     -- PK columns names array
  rel_gen_expr_cols            TEXT[],                     -- generated as expression columns names array
  rel_emaj_verb_attnum         SMALLINT,                   -- column number (attnum) of the log table's emaj_verb column in the
                                                           --   pg_attribute table
  rel_has_always_ident_col     BOOLEAN,                    -- are there any "generated always as identity" column ?
  rel_sql_rlbk_columns         TEXT,                       -- piece of sql used to rollback: list of the columns (excluding GENERATED
                                                           --   ALWAYS AS expression columns)
  rel_sql_gen_ins_col          TEXT,                       -- piece of sql used for SQL generation: list of columns to insert
  rel_sql_gen_ins_val          TEXT,                       -- piece of sql used for SQL generation: list of column values to insert
  rel_sql_gen_upd_set          TEXT,                       -- piece of sql used for SQL generation: set clause for updates
  rel_sql_gen_pk_conditions    TEXT,                       -- piece of sql used for SQL generation: equality conditions on the pk columns
  rel_log_seq_last_value       BIGINT,                     -- last value of the log sequence when the table is removed from the group
                                                           --   (NULL otherwise)
  PRIMARY KEY (rel_schema, rel_tblseq, rel_time_range),
  FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name),
  FOREIGN KEY (rel_log_schema) REFERENCES emaj.emaj_schema (sch_name),
  EXCLUDE USING gist (rel_schema WITH =, rel_tblseq WITH =, rel_time_range WITH &&)
  );
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;
-- Index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration.
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- Index on emaj_relation used to speedup _verify_all_schemas() with large E-Maj configuration.
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- Table containing the history of relations - groups relationship.
CREATE TABLE emaj.emaj_rel_hist (
  relh_schema                  TEXT        NOT NULL,       -- schema name containing the relation
  relh_tblseq                  TEXT        NOT NULL,       -- application table or sequence name
  relh_time_range              INT8RANGE   NOT NULL,       -- range of time id representing the validity time range
                                           CHECK (NOT upper_inf(relh_time_range)),
  relh_group                   TEXT        NOT NULL,       -- name of the group that owned the relation
                                                           --   (the group may not exist in emaj_group anymore)
  relh_kind                    TEXT,                       -- similar to the relkind column of pg_class table
                                                           --   ('r' = table, 'S' = sequence)
  PRIMARY KEY (relh_schema, relh_tblseq, relh_time_range),
  EXCLUDE USING gist (relh_schema WITH =, relh_tblseq WITH =, relh_time_range WITH &&)
  );
-- Functional index on emaj_rel_hist used to speedup the history purge function.
CREATE INDEX emaj_rel_hist_idx1 ON emaj.emaj_rel_hist ((upper(relh_time_range)));
COMMENT ON TABLE emaj.emaj_rel_hist IS
$$Contains the history of groups content.$$;

-- Table containing the relation changes, i.e. the events that lead to changes in the tables groups structure.
CREATE TABLE emaj.emaj_relation_change (
  rlchg_time_id                 BIGINT      NOT NULL,       -- time stamp id of the change operation
  rlchg_schema                  TEXT        NOT NULL,       -- current or old schema name
  rlchg_tblseq                  TEXT        NOT NULL,       -- current or old table or sequence name, depending on the step
  rlchg_change_kind             emaj._relation_change_kind_enum
                                            NOT NULL,       -- kind of change among the enum list
  rlchg_group                   TEXT        NOT NULL,       -- current or old group that owns the table or the sequence
-- Depending on rlchg_change_kind, non concerned columns for current or new values remain NULL.
-- But concerned columns can also be explicitely set to NULL.
  rlchg_new_schema              TEXT        ,               -- new schema name, if changed
  rlchg_new_tblseq              TEXT        ,               -- new table or sequence name, if changed
  rlchg_new_group               TEXT        ,               -- new group name, if changed
  rlchg_priority                INT         ,               -- current or old priority level (tables are processed in ascending
                                                           --    order, with NULL last)
  rlchg_new_priority            INT         ,               -- new priority level, if changed
  rlchg_log_data_tsp            TEXT        ,               -- current or old log data tablespace
  rlchg_new_log_data_tsp        TEXT        ,               -- new log data tablespace, if changed
  rlchg_log_index_tsp           TEXT        ,               -- current or old log index tablespace
  rlchg_new_log_index_tsp       TEXT        ,               -- new log index tablespace, if changed
  rlchg_ignored_triggers        TEXT[]      ,               -- current or old array of triggers to ignore at rollback time
  rlchg_new_ignored_triggers    TEXT[]      ,               -- new array of triggers to ignore at rollback time, if changed
  PRIMARY KEY (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind),
  FOREIGN KEY (rlchg_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_relation_change IS
$$Contains the changes in relations registered in tables groups.$$;

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

-- Table containing the marks.
CREATE TABLE emaj.emaj_mark (
  mark_group                   TEXT        NOT NULL,       -- group for which the mark has been set
  mark_name                    TEXT        NOT NULL,       -- mark name
  mark_time_id                 BIGINT      NOT NULL,       -- time stamp of the mark creation, used as a reference
                                                           --   for other tables like emaj_sequence and all log tables
  mark_is_rlbk_protected       BOOLEAN     NOT NULL,       -- boolean to indicate whether the mark is protected against rollbacks (false
                                                           -- by default)
  mark_comment                 TEXT,                       -- optional user comment
  mark_log_rows_before_next    BIGINT,                     -- number of log rows recorded for the group between the mark
                                                           -- and the next one (NULL if last mark)
                                                           -- used to speedup marks list display in the Emaj_web client
  mark_logged_rlbk_target_mark TEXT,                       -- for marks generated by logged_rollback functions, name of the rollback target
                                                           -- mark
  PRIMARY KEY (mark_group, mark_name),
  FOREIGN KEY (mark_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE,
  FOREIGN KEY (mark_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_mark IS
$$Contains marks set on E-Maj tables groups.$$;
-- Index on emaj_mark used to speedup statistics functions, when many marks have been set.
CREATE INDEX emaj_mark_idx1 ON emaj.emaj_mark (mark_time_id);

-- Table containing the application sequences properties at mark time.
CREATE TABLE emaj.emaj_sequence (
  sequ_schema                  TEXT        NOT NULL,       -- schema that owns the sequence
  sequ_name                    TEXT        NOT NULL,       -- sequence name
  sequ_time_id                 BIGINT      NOT NULL,       -- time stamp when the sequence characteristics have been recorded
                                                           --   the same time stamp id as referenced in emaj_mark table
  sequ_last_val                BIGINT      NOT NULL,       -- sequence last value
  sequ_start_val               BIGINT      NOT NULL,       -- sequence start value
  sequ_increment               BIGINT      NOT NULL,       -- sequence increment
  sequ_max_val                 BIGINT      NOT NULL,       -- sequence max value
  sequ_min_val                 BIGINT      NOT NULL,       -- sequence min value
  sequ_cache_val               BIGINT      NOT NULL,       -- sequence cache value
  sequ_is_cycled               BOOLEAN     NOT NULL,       -- sequence flag 'is cycled ?'
  sequ_is_called               BOOLEAN     NOT NULL,       -- sequence flag 'is called ?'
  PRIMARY KEY (sequ_schema, sequ_name, sequ_time_id),
  FOREIGN KEY (sequ_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_sequence IS
$$Contains application sequences properties at E-Maj set_mark times.$$;

-- Table containing some tables properties at mark time, including the log sequence last value.
CREATE TABLE emaj.emaj_table (
  tbl_schema                   TEXT        NOT NULL,       -- schema that owns the table
  tbl_name                     TEXT        NOT NULL,       -- table name
  tbl_time_id                  BIGINT      NOT NULL,       -- time stamp when the table characteristics have been recorded
                                                           --   the same time stamp id as referenced in emaj_mark table
  tbl_pages                    INT,                        -- estimated number of pages
  tbl_tuples                   FLOAT,                      -- estimated number of rows
  tbl_log_seq_last_val         BIGINT      NOT NULL,       -- log sequence last value
  PRIMARY KEY (tbl_schema, tbl_name, tbl_time_id),
  FOREIGN KEY (tbl_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_table IS
$$Contains tables properties at E-Maj set_mark times.$$;

-- Table containing the holes in sequences log.
-- These holes are due to rollback operations or rollback consolidation that produce holes in log sequences.
-- They are recorded to give better results in functions that estimate the number of updates using the sequence values recorded at
-- set mark times.
CREATE TABLE emaj.emaj_seq_hole (
  sqhl_schema                  TEXT        NOT NULL,       -- schema that owns the application table
  sqhl_table                   TEXT        NOT NULL,       -- application table for which a sequence hole is recorded in the associated
                                                           -- log table
  sqhl_begin_time_id           BIGINT      NOT NULL,       -- time stamp id of the lower range limit of the hole
  sqhl_end_time_id             BIGINT      NOT NULL,       -- time stamp id of the upper range limit of the hole
  sqhl_hole_size               BIGINT      NOT NULL,       -- hole size computed as the difference of 2 sequence last-values
  PRIMARY KEY (sqhl_schema, sqhl_table, sqhl_begin_time_id),
  FOREIGN KEY (sqhl_begin_time_id) REFERENCES emaj.emaj_time_stamp (time_id),
  FOREIGN KEY (sqhl_end_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_seq_hole IS
$$Contains description of holes in sequence values for E-Maj log tables.$$;

-- Table containing rollback events.
CREATE TABLE emaj.emaj_rlbk (
  rlbk_id                      INT         NOT NULL        -- rollback id
                                           GENERATED BY DEFAULT AS IDENTITY,
  rlbk_groups                  TEXT[]      NOT NULL,       -- groups array to rollback
  rlbk_mark                    TEXT        NOT NULL,       -- mark to rollback to (the original value at rollback time)
  rlbk_mark_time_id            BIGINT      NOT NULL,       -- time stamp id of the mark to rollback to
  rlbk_time_id                 BIGINT,                     -- time stamp id at the rollback exec start
  rlbk_is_logged               BOOLEAN     NOT NULL,       -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN,                    -- flag allowing to rollback to a mark set before alter group operations
                                                           -- (NULL with old rollback functions)
  rlbk_comment                 TEXT,                       -- comment about this rollback
  rlbk_nb_session              INT         NOT NULL,       -- number of requested rollback sessions
  rlbk_nb_table                INT,                        -- number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences in groups
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_eff_nb_sequence         INT,                        -- number of sequences with attributes to change
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_begin_hist_id           BIGINT,                     -- hist_id of the rollback BEGIN event in the emaj_hist
                                                           --   used to know if the rollback has been committed or not
  rlbk_dblink_schema           TEXT,                       -- schema that holds the dblink extension
  rlbk_is_dblink_used          BOOLEAN,                    -- boolean indicating whether dblink connection are used
  rlbk_start_datetime          TIMESTAMPTZ,                -- clock timestamp of the rollback start
  rlbk_end_planning_datetime   TIMESTAMPTZ,                -- clock timestamp of the planning step end
  rlbk_end_locking_datetime    TIMESTAMPTZ,                -- clock timestamp of the locking step end
  rlbk_end_datetime            TIMESTAMPTZ,                -- clock time the rollback has been completed,
  rlbk_messages                TEXT[],                     -- result messages array
  PRIMARY KEY (rlbk_id),
  FOREIGN KEY (rlbk_time_id) REFERENCES emaj.emaj_time_stamp (time_id),
  FOREIGN KEY (rlbk_mark_time_id) REFERENCES emaj.emaj_time_stamp (time_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk IS
$$Contains description of rollback events.$$;
-- Partial index on emaj_rlbk targeting in progress rollbacks (not yet committed or marked as aborted).
CREATE INDEX emaj_rlbk_idx1 ON emaj.emaj_rlbk (rlbk_status)
    WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED');

-- Table containing rollback events sessions.
CREATE TABLE emaj.emaj_rlbk_session (
  rlbs_rlbk_id                 INT         NOT NULL,       -- rollback id
  rlbs_session                 INT         NOT NULL,       -- session number (from 1 to rlbk_nb_session)
  rlbs_txid                    BIGINT      NOT NULL,       -- id of the tx that executes this rollback session
  rlbs_start_datetime          TIMESTAMPTZ NOT NULL,       -- rollback session start timestamp
  rlbs_end_datetime            TIMESTAMPTZ,                -- clock time the rollback session has been completed,
                                                           --   NULL if rollback is in progress
  PRIMARY KEY (rlbs_rlbk_id, rlbs_session),
  FOREIGN KEY (rlbs_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_session IS
$$Contains description of rollback events sessions.$$;

-- Table containing the elementary steps of rollback operations.
CREATE TABLE emaj.emaj_rlbk_plan (
  rlbp_rlbk_id                 INT         NOT NULL,       -- rollback id
  rlbp_step                    emaj._rlbk_step_enum
                                           NOT NULL,       -- kind of elementary step in the rollback processing
  rlbp_schema                  TEXT        NOT NULL,       -- schema object of the step or ''
  rlbp_table                   TEXT        NOT NULL,       -- table name or ''
  rlbp_object                  TEXT        NOT NULL,       -- foreign key name for step on foreign key, trigger name for step on trigger
                                                           -- or ''
  rlbp_batch_number            INT,                        -- identifies a set of tables linked by foreign keys
  rlbp_session                 INT,                        -- session number the step is affected to
  rlbp_object_def              TEXT,                       -- foreign key definition used to recreate it or NULL
  rlbp_app_trg_type            TEXT,                       -- type of an application trigger to enable ('', 'ALWAYS', 'REPLICA') or NULL
  rlbp_is_repl_role_replica    BOOLEAN,                    -- for a RLBK_TABLE step, TRUE if the session_replication_role is replica,
                                                           -- NULL for other steps
  rlbp_target_time_id          BIGINT,                     -- for RLBK_TABLE and DELETE_LOG, time_id to rollback to
                                                           -- NULL for other steps
  rlbp_estimated_quantity      BIGINT,                     -- for RLBK_TABLE, estimated number of updates to rollback
                                                           -- for RLBK_SEQUENCES, number of sequences in the rolled back groups
                                                           -- for DELETE_LOG, estimated number of rows to delete
                                                           -- for fkeys, estimated number of keys to check
  rlbp_estimated_duration      INTERVAL,                   -- estimated elapse time for the step processing
  rlbp_estimate_method         INT,                        -- method used to compute the estimated duration
                                                           --  1: use rollback stats with volume in same order of magnitude
                                                           --  2: use all previous rollback stats
                                                           --  3: use only parameters (from emaj_param or default values)
  rlbp_start_datetime          TIMESTAMPTZ,                -- clock start time of the step, NULL is not yet started
  rlbp_quantity                BIGINT,                     -- for RLBK_TABLE, number of effectively rolled back updates
                                                           -- for DELETE_LOG, number of effectively deleted log rows
                                                           -- null for fkeys
  rlbp_duration                INTERVAL,                   -- real elapse time of the step, NULL is not yet completed
  PRIMARY KEY (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object),
  FOREIGN KEY (rlbp_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_plan IS
$$Contains description of elementary steps for rollback operations.$$;

-- Table containing statistics about previously executed rollback operations
-- and used to estimate rollback durations.
-- Depending on the step, it contains 1 row per elementary step (like 'RLBK_TABLE' or 'DELETE_LOG'),
-- or 1 row per type of step for 1 rollback operation (like 'DROP_FK', or 'DIS_LOG_TRG').
CREATE TABLE emaj.emaj_rlbk_stat (
  rlbt_step                    emaj._rlbk_step_enum
                                           NOT NULL,       -- kind of elementary step in the rollback processing
  rlbt_schema                  TEXT        NOT NULL,       -- schema object of the step
  rlbt_table                   TEXT        NOT NULL,       -- table name
  rlbt_object                  TEXT        NOT NULL,       -- foreign key name for step on foreign key, or trigger name for step on
                                                           -- triggers, or ''
  rlbt_rlbk_id                 INT         NOT NULL,       -- rollback id
  rlbt_quantity                BIGINT      NOT NULL,       -- depending on the step, either estimated quantity processed by the
                                                           -- elementary step or number of executed steps
  rlbt_duration                INTERVAL    NOT NULL,       -- duration or sum of durations of the elementary step(s)
  PRIMARY KEY (rlbt_step, rlbt_schema, rlbt_table, rlbt_object, rlbt_rlbk_id),
  FOREIGN KEY (rlbt_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_stat IS
$$Contains statistics about previous E-Maj rollback durations.$$;

------------------------------------
--                                --
-- emaj composite types           --
--                                --
------------------------------------

-- Composite types usable by end-user.

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

CREATE TYPE emaj.emaj_detailed_log_stat_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_table                   TEXT,                       -- table name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the mark representing the lower bound of the time range
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the mark representing the upper bound of the time range
  stat_role                    TEXT,                       -- user having generated update events
  stat_verb                    TEXT,                       -- type of SQL statement (INSERT/UPDATE/DELETE/TRUNCATE)
  stat_rows                    BIGINT                      -- real number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_detailed_log_stat_type IS
$$Represents the structure of rows returned by the emaj_detailed_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_sequence_stat_type AS (
  stat_group                   TEXT,                       -- group name owning the schema.table
  stat_schema                  TEXT,                       -- schema name
  stat_sequence                TEXT,                       -- sequence name
  stat_first_mark              TEXT,                       -- mark representing the lower bound of the time range
  stat_first_mark_datetime     TIMESTAMPTZ,                -- clock timestamp of the mark representing the lower bound of the time range
  stat_last_mark               TEXT,                       -- mark representing the upper bound of the time range
  stat_last_mark_datetime      TIMESTAMPTZ,                -- clock timestamp of the mark representing the upper bound of the time range
  stat_increments              BIGINT,                     -- number of sequence increments for this sequence during the time interval
  stat_has_structure_changed   BOOLEAN                     -- TRUE if any property other than last_value has changed
  );
COMMENT ON TYPE emaj.emaj_sequence_stat_type IS
$$Represents the structure of rows returned by the emaj_sequence_stat_group() function.$$;

CREATE TYPE emaj.emaj_rollback_activity_type AS (
  rlbk_id                      INT,                        -- rollback id
  rlbk_groups                  TEXT[],                     -- groups array to rollback
  rlbk_mark                    TEXT,                       -- mark to rollback to
  rlbk_mark_datetime           TIMESTAMPTZ,                -- timestamp of the mark as recorded into emaj_mark
  rlbk_is_logged               BOOLEAN,                    -- rollback type: true = logged rollback
  rlbk_is_alter_group_allowed  BOOLEAN,                    -- flag allowing to rollback to a mark set before alter group operations
  rlbk_comment                 TEXT,                       -- comment about this rollback
  rlbk_nb_session              INT,                        -- number of requested sessions
  rlbk_nb_table                INT,                        -- number of tables in groups
  rlbk_nb_sequence             INT,                        -- number of sequences in groups
  rlbk_eff_nb_table            INT,                        -- number of tables with rows to rollback
  rlbk_eff_nb_sequence         INT,                        -- number of sequences with attributes to change
  rlbk_status                  emaj._rlbk_status_enum,     -- rollback status
  rlbk_start_datetime          TIMESTAMPTZ,                -- clock timestamp of the rollback start
  rlbk_planning_duration       INTERVAL,                   -- planning phase duration, if completed
  rlbk_locking_duration        INTERVAL,                   -- tables locking phase duration, if completed
  rlbk_elapse                  INTERVAL,                   -- elapse time since the begining of the execution
  rlbk_remaining               INTERVAL,                   -- estimated remaining time to complete the rollback
  rlbk_completion_pct          SMALLINT                    -- estimated percentage of the rollback operation
  );
COMMENT ON TYPE emaj.emaj_rollback_activity_type IS
$$Represents the structure of rows returned by the emaj_rollback_activity() function.$$;

CREATE TYPE emaj.emaj_consolidable_rollback_type AS (
  cons_group                   TEXT,                       -- group name
  cons_target_rlbk_mark_name   TEXT,                       -- name of the mark used as target of the logged rollback operation
  cons_target_rlbk_mark_time_id BIGINT,                    -- timestamp of the mark used as target of the logged rollback operation
  cons_end_rlbk_mark_name      TEXT,                       -- name of the mark set at the end of the logged rollback operation
  cons_end_rlbk_mark_time_id   BIGINT,                     -- timestamp of the mark set at the end of the logged rollback operation
  cons_rows                    BIGINT,                     -- estimated number of update events that can be consolidated for the rollback
  cons_marks                   INT                         -- number of marks that would be deleted by a consolidation
  );
COMMENT ON TYPE emaj.emaj_consolidable_rollback_type IS
$$Represents the structure of rows returned by the emaj_get_consolidable_rollbacks() function.$$;

-- Composite types used by emaj internal functions.

CREATE TYPE emaj._verify_groups_type AS (                -- this type is not used by functions called by users
  ver_schema                   TEXT,
  ver_tblseq                   TEXT,
  ver_group                    TEXT,
  ver_msg                      TEXT
  );
COMMENT ON TYPE emaj._verify_groups_type IS
$$Represents the structure of rows returned by the internal _verify_groups() function.$$;

CREATE TYPE emaj._report_message_type AS (
  rpt_msg_type                 INT,                        -- message number
                                                           -- range 1 - 33 used by _import_groups_conf_check
                                                           -- range 101 - 105 used by _check_json_param_conf
                                                           -- range 201 - 232 used by _check_json_groups_conf
                                                           -- range 250 - 261 used by _import_groups_conf_prepare
  rpt_severity                 INT,                        -- severity level
                                                           -- 0 : notice
                                                           -- 1 : blocking error
                                                           -- 2 : error not blocking an audit_only group creation
                                                           -- 3 : warning
  rpt_text_var_1               TEXT,                       -- textual variable #1
  rpt_text_var_2               TEXT,                       -- textual variable #2
  rpt_text_var_3               TEXT,                       -- textual variable #3
  rpt_text_var_4               TEXT,                       -- textual variable #4
  rpt_int_var_1                INT,                        -- integer variable #1
  rpt_message                  TEXT                        -- the english formatted error message
  );
COMMENT ON TYPE emaj._report_message_type IS
$$Represents a generic notice, warning or error message structure that can be translated by external clients.$$;

------------------------------------
--                                --
-- Parameters                     --
--                                --
------------------------------------
-- All parameters are optional. They may be set by E-Maj administrators, if needed.

-- The 'dblink_user_password' parameter defines the role and its associated password, if any, to establish a dblink
-- connection for the monitoring of rollback operations.
--   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('dblink_user_password','user=<user> password=<password>');

-- The 'history_retention' parameter defines the time interval when a row remains in the emaj history and rollback tables
--   default = 1 year.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','1 year'::INTERVAL);

-- The 'alter_log_table' parameter allows to adjust the log tables content by adding extra information column.
--   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('alter_log_table',
--     'ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr(),
--      ADD COLUMN emaj_appname TEXT DEFAULT current_setting(''application_name'')');

-- 6 parameters are used by the emaj_estimate_rollback_group(s) and the rollback functions as default values to compute the approximate
-- duration of a rollback operation.
-- The avg_row_rollback_duration parameter defines the average duration needed to rollback a row.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','100 microsecond'::INTERVAL);
-- The avg_row_delete_log_duration parameter defines the average duration needed to delete log rows.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','10 microsecond'::INTERVAL);
-- The avg_fkey_check_duration parameter defines the average duration needed to check a foreign key.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_fkey_check_duration','20 microsecond'::INTERVAL);
-- The fixed_step_rollback_duration parameter defines the fixed cost for any elementary rollback step.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_step_rollback_duration','2.5 millisecond'::INTERVAL);
-- The fixed_table_rollback_duration parameter defines the fixed rollback cost for any table or sequence belonging to a group.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','1 millisecond'::INTERVAL);
-- The fixed_dblink_rollback_duration parameter defines the fixed cost of dblink use for any rollback step.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_dblink_rollback_duration','4 millisecond'::INTERVAL);

-- View readable by emaj_viewer role. It hides the 'dblink_user_password' parameter's value.
CREATE VIEW emaj.emaj_visible_param WITH (security_barrier) AS
  SELECT param_key,
         CASE WHEN param_key = 'dblink_user_password' THEN '<masked data>'
                                                      ELSE param_value_text END AS param_value_text,
         param_value_numeric, param_value_boolean, param_value_interval
    FROM emaj.emaj_param;

------------------------------------
--                                --
-- Triggers on internal tables    --
--                                --
------------------------------------

-- Triggers for changes and truncate on the emaj_param table.
-- It traces changes into the emaj_hist table, masking the 'dblink_user_password' parameter value, if concerned.
-- TRUNCATE verb are blocked to allow the tracing of deleted parameters.

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

CREATE TRIGGER emaj_param_change_trg
  AFTER INSERT OR UPDATE OR DELETE  ON emaj.emaj_param
  FOR EACH ROW EXECUTE PROCEDURE emaj._emaj_param_change_fnct();

CREATE TRIGGER emaj_param_truncate_trg
  BEFORE TRUNCATE ON emaj.emaj_param
  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._emaj_param_change_fnct();

------------------------------------
--                                --
-- Low level Functions            --
--                                --
------------------------------------
CREATE OR REPLACE FUNCTION emaj._pg_version_num()
RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS
$$
-- This function returns as an integer the current postgresql version.
SELECT current_setting('server_version_num')::INT;
$$;

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

CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(p_cnxName TEXT, p_callerRole TEXT, OUT p_status INT, OUT p_schema TEXT)
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_dblink_open_cnx$
-- This function tries to open a named dblink connection.
-- It uses as target: the current cluster (port), the current database and a role defined in the emaj_param table.
-- This connection role must be defined in the emaj_param table with a row having:
--   - param_key = 'dblink_user_password',
--   - param_value_text = 'user=<user> password=<password>' with the rules that apply to usual libPQ connect strings.
-- The password can be omited if the connection doesn't require it.
-- The dblink_connect_u is used to open the connection so that emaj_adm but non superuser roles can access the
--    cluster even when no password is required to log on.
-- The function is directly called by Emaj_web.
-- Input:  connection name
--         caller role to check for permissions
-- Output: integer status return.
--           1 successful connection
--           0 already opened connection
--          -1 dblink is not installed
--          -2 dblink functions are not visible for the session (obsolete)
--          -3 dblink functions execution is not granted to the role
--          -4 the transaction isolation level is not READ COMMITTED
--          -5 no 'dblink_user_password' parameter is defined in the emaj_param table
--          -6 error at dblink_connect() call
--          -7 the dblink user/password from emaj_param has not emaj_adm rights
--         name of the schema that holds the dblink extension (used later to schema qualify all calls to dblink functions)
-- The function is defined as SECURITY DEFINER because reading the unix_socket_directories GUC needs to be at least a member
--   of pg_read_all_settings.
  DECLARE
    v_nbCnx                  INT;
    v_userPassword           TEXT;
    v_connectString          TEXT;
    v_stmt                   TEXT;
    v_isEmajadm              BOOLEAN;
  BEGIN
-- Look for the schema holding the dblink functions.
--   (NULL if the dblink_connect_u function is not available, which should not happen)
    SELECT nspname INTO p_schema
      FROM pg_catalog.pg_proc
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
      WHERE proname = 'dblink_connect_u'
      LIMIT 1;
    IF NOT FOUND THEN
      p_status = -1;                      -- dblink is not installed
    ELSIF NOT has_function_privilege(p_callerRole, quote_ident(p_schema) || '.dblink_connect_u(text, text)', 'execute') THEN
      p_status = -3;                      -- current role has not the execute rights on dblink functions
    ELSIF (p_cnxName LIKE 'rlbk#%' OR p_cnxName = 'emaj_verify_all') AND
          current_setting('transaction_isolation') <> 'read committed' THEN
      p_status = -4;                      -- 'rlbk#*' connection (used for rollbacks) must only come from a
                                          --   READ COMMITTED transaction
    ELSE
      EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                     p_cnxName, p_schema);
      GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
      IF v_nbCnx > 0 THEN
-- Dblink is usable, so search the requested connection name in dblink connections list.
        p_status = 0;                       -- the requested connection is already open
      ELSE
-- So, get the 'dblink_user_password' parameter if exists, from emaj_param.
        SELECT param_value_text INTO v_userPassword
          FROM emaj.emaj_param
          WHERE param_key = 'dblink_user_password';
        IF NOT FOUND THEN
          p_status = -5;                    -- no 'dblink_user_password' parameter is defined in the emaj_param table
        ELSE
-- Build the connect string.
          v_connectString = 'host='
                         || CASE
                              WHEN current_setting('listen_addresses') = ''
                                THEN coalesce(substring(current_setting('unix_socket_directories') from '(.*?)\s*,'),
                                              current_setting('unix_socket_directories'))
                              ELSE 'localhost'
                            END
                         || ' port=' || current_setting('port')
                         || ' dbname=' || current_database()
                         || ' ' || v_userPassword;
-- Try to connect.
          BEGIN
            EXECUTE format('SELECT %I.dblink_connect_u(%L ,%L)',
                           p_schema, p_cnxName, v_connectString);
            p_status = 1;                   -- the connection is successful
-- For E-Maj rollback first connections and test connections, check the role is member of emaj_adm.
            IF (p_cnxName LIKE 'rlbk#1' OR p_cnxName = 'emaj_verify_all') THEN
              v_stmt = 'SELECT pg_has_role(''emaj_adm'', ''MEMBER'') AS is_emaj_adm';
              EXECUTE format('SELECT is_emaj_adm FROM %I.dblink(%L, %L) AS (is_emaj_adm BOOLEAN)',
                             p_schema, p_cnxName, v_stmt)
                INTO v_isEmajadm;
              IF NOT v_isEmajadm THEN
                p_status = -7;              -- the dblink user/password from emaj_param has not emaj_adm rights
                EXECUTE format('SELECT %I.dblink_disconnect(%L)',
                               p_schema, p_cnxName);
              END IF;
            END IF;
          EXCEPTION
            WHEN OTHERS THEN
              p_status = -6;                -- the connection attempt failed
          END;
        END IF;
      END IF;
    END IF;
-- For connections used for rollback operations, record the dblink connection attempt in the emaj_hist table.
    IF substring(p_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('DBLINK_OPEN_CNX', p_cnxName, 'Status = ' || p_status);
    END IF;
--
    RETURN;
  END;
$_dblink_open_cnx$;

CREATE OR REPLACE FUNCTION emaj._dblink_sql_exec(p_cnxName TEXT, p_stmt TEXT, p_dblinkSchema TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_dblink_sql_exec$
-- This function executes a SQL statement, either through an opened dblink connection when a schema name is provided or directly.
-- It returns a bigint value. Consequently, all SQL statements to execute must return an integer numeric value.
-- Input:  connection name
--         sql statement
--         name of the schema that holds the dblink extension
-- Output: the single return value
  DECLARE
    v_returnValue            BIGINT;
  BEGIN
    IF p_dblinkSchema IS NOT NULL THEN
-- A dblink schema is provided, so the connection name can be used to execute the requested SQL statement.
      EXECUTE format('SELECT return_value FROM %I.dblink(%L, %L) AS (return_value BIGINT)',
                     p_dblinkSchema, p_cnxName, p_stmt)
        INTO v_returnValue;
    ELSE
-- The SQL statement has to be directly executed.
      EXECUTE p_stmt INTO v_returnValue;
    END IF;
--
    RETURN v_returnValue;
  END;
$_dblink_sql_exec$;

CREATE OR REPLACE FUNCTION emaj._dblink_close_cnx(p_cnxName TEXT, p_dblinkSchema TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_dblink_close_cnx$
-- This function closes a named dblink connection.
-- The function is directly called by Emaj_web.
-- Input:  connection name
  DECLARE
    v_nbCnx                  INT;
  BEGIN
-- Check the dblink connection exists.
    EXECUTE format('SELECT 0 WHERE %L = ANY (%I.dblink_get_connections())',
                   p_cnxName, p_dblinkSchema);
    GET DIAGNOSTICS v_nbCnx = ROW_COUNT;
    IF v_nbCnx > 0 THEN
-- The connection exists, so disconnect.
      EXECUTE format('SELECT %I.dblink_disconnect(%L)',
                     p_dblinkSchema, p_cnxName);
-- For connections used for rollback operations, record the dblink disconnection in the emaj_hist table.
      IF substring(p_cnxName FROM 1 FOR 5) = 'rlbk#' THEN
        INSERT INTO emaj.emaj_hist (hist_function, hist_object)
          VALUES ('DBLINK_CLOSE_CNX', p_cnxName);
      END IF;
    END IF;
--
    RETURN;
  END;
$_dblink_close_cnx$;

CREATE OR REPLACE FUNCTION emaj._get_default_tablespace()
RETURNS TEXT LANGUAGE plpgsql AS
$_get_default_tablespace$
-- This function returns the name of a default tablespace to use when moving an existing log table or index.
-- Output: tablespace name
-- The function is called at alter group time.
  DECLARE
    v_tablespace             TEXT;
  BEGIN
-- Get the default tablespace set for the current session or set for the entire instance by GUC.
    SELECT setting INTO v_tablespace
      FROM pg_settings
      WHERE name = 'default_tablespace';
    IF v_tablespace = '' THEN
-- Get the default tablespace for the current database (pg_default if no specific tablespace name has been set for the database).
      SELECT spcname INTO v_tablespace
        FROM pg_catalog.pg_database
             JOIN pg_catalog.pg_tablespace ON (pg_tablespace.oid = dattablespace)
        WHERE datname = current_database();
    END IF;
--
    RETURN v_tablespace;
  END;
$_get_default_tablespace$;

CREATE OR REPLACE FUNCTION emaj._check_group_names(p_groupNames TEXT[], p_mayBeNull BOOLEAN, p_lockGroups BOOLEAN,
                                                   p_checkIdle BOOLEAN DEFAULT FALSE, p_checkLogging BOOLEAN DEFAULT FALSE,
                                                   p_checkRollbackable BOOLEAN DEFAULT FALSE, p_checkUnprotected BOOLEAN DEFAULT FALSE)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_group_names$
-- This function performs various checks on a group names array.
-- The NULL, empty strings and duplicate values are removed from the array. If the array is empty raise either an exception or a warning.
-- Checks are then perform to verify:
-- - that all groups exist,
-- - if requested are ROLLBACKABLE,
-- - if requested are in LOGGING or IDLE state,
-- - if requested are not PROTECTED against rollback operations.
-- A SELECT FOR UPDATE is executed if requested, to avoid other sensitive actions in parallel on the same groups.
-- Input: group names array,
--        a boolean that tells whether a NULL array only raise a WARNING,
--        a boolean that tells whether the groups have to be locked,
--        a string that lists the checks to perform, with the following possible values: IDLE, LOGGING, ROLLBACKABLE and UNPROTECTED.
-- Output: validated group names array
  DECLARE
    v_groupList              TEXT;
    v_count                  INT;
  BEGIN
-- Remove duplicates values, NULL and empty strings from the supplied group names array.
    SELECT array_agg(DISTINCT group_name) INTO p_groupNames
      FROM unnest(p_groupNames) AS group_name
      WHERE group_name IS NOT NULL
        AND group_name <> '';
-- Process empty array.
    IF p_groupNames IS NULL THEN
      IF p_mayBeNull THEN
        RAISE WARNING '_check_group_names: No group to process.';
        RETURN NULL;
      ELSE
        RAISE EXCEPTION '_check_group_names: No group to process.';
      END IF;
    END IF;
-- Check that all groups exist.
    SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
      FROM (  SELECT unnest(p_groupNames)
            EXCEPT
              SELECT group_name
                FROM emaj.emaj_group
           ) AS t(group_name);
    IF v_count > 0 THEN
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" does not exist.', v_groupList;
      ELSE
        RAISE EXCEPTION '_check_group_names: The groups "%" do not exist.', v_groupList;
      END IF;
    END IF;
-- Lock the groups if requested.
    IF p_lockGroups THEN
      PERFORM 0
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
        FOR UPDATE;
    END IF;
-- Checks ROLLBACKABLE type, if requested.
    IF p_checkRollbackable THEN
      SELECT string_agg(group_name,', '  ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND NOT group_is_rollbackable;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" has been created as AUDIT_ONLY.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" have been created as AUDIT_ONLY.', v_groupList;
      END IF;
    END IF;
-- Checks IDLE state, if requested
    IF p_checkIdle THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND group_is_logging;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is not in IDLE state.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are not in IDLE state.', v_groupList;
      END IF;
    END IF;
-- Checks LOGGING state, if requested.
    IF p_checkLogging THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND NOT group_is_logging;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is not in LOGGING state.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are not in LOGGING state.', v_groupList;
      END IF;
    END IF;
-- Checks UNPROTECTED type, if requested.
    IF p_checkUnprotected THEN
      SELECT string_agg(group_name,', ' ORDER BY group_name), count(*) INTO v_groupList, v_count
        FROM emaj.emaj_group
        WHERE group_name = ANY(p_groupNames)
          AND group_is_rlbk_protected;
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_group_names: The group "%" is currently protected against rollback operations.', v_groupList;
      END IF;
      IF v_count > 1 THEN
        RAISE EXCEPTION '_check_group_names: The groups "%" are currently protected against rollback operations.', v_groupList;
      END IF;
    END IF;
--
    RETURN p_groupNames;
  END;
$_check_group_names$;

CREATE OR REPLACE FUNCTION emaj._check_json_groups_conf(p_groupsJson JSON)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_check_json_groups_conf$
-- This function verifies that the JSON structure that contains a tables groups configuration is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them.
-- It is called by the _import_groups_conf() function.
-- The function is also directly called by Emaj_web.
-- This function checks that:
--   - the "tables_groups" attribute exists
--   - "groups_name" attribute are defined
--   - no unknow attribute are listed in the group level
--   - the "is_rollbackable" attributes are boolean
--   - the "priority" attributes are numeric
--   - groups are not described several times
-- Input: the JSON structure to check
-- Output: _report_message_type records representing diagnostic messages
  DECLARE
    v_groupNumber            INT;
    v_group                  TEXT;
    v_tblseqNumber           INT;
    v_schema                 TEXT;
    v_tblseq                 TEXT;
    v_triggerNumber          INT;
    v_trigger                TEXT;
    r_group                  RECORD;
    r_table                  RECORD;
    r_trigger                RECORD;
    r_sequence               RECORD;
  BEGIN
-- Extract the "tables_groups" json path and check that the attribute exists.
    p_groupsJson = p_groupsJson #> '{"tables_groups"}';
    IF p_groupsJson IS NULL THEN
      RETURN QUERY
        VALUES (201, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                'The JSON structure does not contain any "tables_groups" array.');
    ELSE
-- Check that all keywords of the JSON structure are valid.
-- Process groups attributes.
      v_groupNumber = 0;
      FOR r_group IN
        SELECT value AS groupJson
          FROM json_array_elements(p_groupsJson)
      LOOP
--   The group_name must be defined.
        v_groupNumber = v_groupNumber + 1;
        v_group = r_group.groupJson ->> 'group';
        IF v_group IS NULL OR v_group = '' THEN
          RETURN QUERY
            VALUES (210, 1, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_groupNumber,
                    format('The tables group #%s has no "group" attribute.',
                           v_groupNumber::TEXT));
        ELSE
--   Other attributes of the group level must be known.
          RETURN QUERY
            SELECT 211, 1, v_group, NULL::TEXT, NULL::TEXT, key, NULL::INT,
                 format('For the tables group "%s", the keyword "%s" is unknown.',
                        v_group, key)
              FROM (
                SELECT key
                  FROM json_object_keys(r_group.groupJson) AS x(key)
                  WHERE key NOT IN ('group', 'is_rollbackable', 'comment', 'tables', 'sequences')
                ) AS t;
--   If it exists, the "is_rollbackable" attribute must be a boolean
          IF r_group.groupJson -> 'is_rollbackable' IS NOT NULL AND
             json_typeof(r_group.groupJson -> 'is_rollbackable') <> 'boolean' THEN
            RETURN QUERY
              VALUES (212, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
                      format('For the tables group "%s", the "is_rollbackable" attribute is not a boolean.',
                             v_group));
          END IF;
-- Process tables attributes.
          v_tblseqNumber = 0;
          FOR r_table IN
            SELECT value AS tableJson
              FROM json_array_elements(r_group.groupJson -> 'tables')
          LOOP
            v_tblseqNumber = v_tblseqNumber + 1;
            v_schema = r_table.tableJson ->> 'schema';
            v_tblseq = r_table.tableJson ->> 'table';
--   The schema and table attributes must exists.
            IF v_schema IS NULL OR v_schema = '' THEN
              RETURN QUERY
                VALUES (220, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_tblseqNumber,
                        format('In the tables group "%s", the table #%s has no "schema" attribute.',
                               v_group, v_tblseqNumber::TEXT));
            ELSIF v_tblseq IS NULL OR v_tblseq = '' THEN
              RETURN QUERY
                VALUES (221, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_tblseqNumber,
                        format('In the tables group "%s", the table #%s has no "table" attribute.',
                               v_group, v_tblseqNumber::TEXT));
            ELSE
--   Attributes of the tables level must exist.
              RETURN QUERY
                SELECT 222, 1, v_group, v_schema, v_tblseq, key, NULL::INT,
                     format('In the tables group "%s" and for the table %I.%I, the keyword "%s" is unknown.',
                            v_group, quote_ident(v_schema), quote_ident(v_tblseq), key)
                  FROM (
                    SELECT key
                      FROM json_object_keys(r_table.tableJson) AS x(key)
                      WHERE key NOT IN ('schema', 'table', 'priority', 'log_data_tablespace',
                                        'log_index_tablespace', 'ignored_triggers')
                    ) AS t;
--   If it exists, the "priority" attribute must be a number.
              IF r_table.tableJson -> 'priority' IS NOT NULL AND
                 json_typeof(r_table.tableJson -> 'priority') <> 'number' THEN
                RETURN QUERY
                  VALUES (223, 1, v_group, v_schema, v_tblseq, NULL::TEXT, NULL::INT,
                          format('In the tables group "%s" and for the table %I.%I, the "priority" attribute is not a number.',
                                 v_group, quote_ident(v_schema), quote_ident(v_tblseq)));
              END IF;
-- Process triggers attributes.
              IF r_table.tableJson -> 'ignored_triggers' IS NOT NULL THEN
                IF json_typeof(r_table.tableJson -> 'ignored_triggers') = 'array' THEN
                  v_triggerNumber = 0;
                  FOR r_trigger IN
                    SELECT value AS triggerJson
                      FROM json_array_elements(r_table.tableJson -> 'ignored_triggers')
                  LOOP
                    v_triggerNumber = v_triggerNumber + 1;
                    IF json_typeof(r_trigger.triggerJson) <> 'string' THEN
                      RETURN QUERY
                        VALUES (226, 1, v_group, v_schema, v_tblseq, NULL::TEXT, v_triggerNumber,
                                format('In the tables group "%s" and for the table %I.%I, the trigger #%s is not a string. '
                                       'This is required since E-Maj 4.0.',
                                       v_group, quote_ident(v_schema), quote_ident(v_tblseq), v_triggerNumber));
                    END IF;
                  END LOOP;
                ELSE
                  RETURN QUERY
                    VALUES (227, 1, v_group, v_schema, v_tblseq, NULL::TEXT, NULL::INT,
                            format('In the tables group "%s" and for the table %I.%I, the "ignored_triggers" attribute is not an array.',
                                   v_group, quote_ident(v_schema), quote_ident(v_tblseq)));
                END IF;
              END IF;
            END IF;
          END LOOP;
-- Process sequences attributes.
          v_tblseqNumber = 0;
          FOR r_sequence IN
            SELECT value AS sequenceJson
              FROM json_array_elements(r_group.groupJson -> 'sequences')
          LOOP
            v_tblseqNumber = v_tblseqNumber + 1;
            v_schema = r_sequence.sequenceJson ->> 'schema';
            v_tblseq = r_sequence.sequenceJson ->> 'sequence';
--   The schema and table attributes must exists.
            IF v_schema IS NULL OR v_schema = '' THEN
              RETURN QUERY
                VALUES (230, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_tblseqNumber,
                        format('In the tables group "%s", the sequence #%s has no "schema" attribute.',
                               v_group, v_tblseqNumber::TEXT));
            ELSIF v_tblseq IS NULL OR v_tblseq = '' THEN
              RETURN QUERY
                VALUES (231, 1, v_group, NULL::TEXT, NULL::TEXT, NULL::TEXT, v_tblseqNumber,
                        format('In the tables group "%s", the sequence #%s has no "sequence" attribute.',
                               v_group, v_tblseqNumber::TEXT));
            ELSE
--   No other attributes of the sequences level must exist.
              RETURN QUERY
                SELECT 232, 1, v_group, v_schema, v_tblseq, key, NULL::INT,
                     format('In the tables group "%s" and for the sequence %I.%I, the keyword "%s" is unknown.',
                            v_group, quote_ident(v_schema), quote_ident(v_tblseq), key)
                  FROM (
                    SELECT key
                      FROM json_object_keys(r_sequence.sequenceJson) AS x(key)
                      WHERE key NOT IN ('schema', 'sequence')
                    ) AS t;
            END IF;
          END LOOP;
        END IF;
      END LOOP;
-- Check that tables groups are not configured more than once in the JSON structure.
      RETURN QUERY
        SELECT 202, 1, "group", NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::INT,
             format('The JSON structure references several times the tables group "%s".',
                    "group")
          FROM (
            SELECT "group", count(*)
              FROM json_to_recordset(p_groupsJson) AS x("group" TEXT)
              GROUP BY "group" HAVING count(*) > 1
            ) AS t;
    END IF;
--
    RETURN;
  END;
$_check_json_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._check_json_table_properties(p_properties JSONB,
                                                             OUT p_priority INT, OUT p_logDatTsp TEXT, OUT p_logIdxTsp TEXT,
                                                             OUT p_ignoredTriggers TEXT[], OUT p_ignoredTriggersProfiles TEXT[])
LANGUAGE plpgsql AS
$_check_json_table_properties$
-- This function verifies that the JSON structure that contains tables properties set at tables assign or modify functions is correct.
-- This function returns the elementary fields
-- Input: the JSON structure to check
-- Output: priority level
--         log data and index tablespaces
--         names array of triggers to ignore at rollback time
--         array of names profiles (i.e. regular expressions) for triggers to ignore at rollback time
  DECLARE
    v_emajTriggers           TEXT[];
    v_extraProperties        JSONB;
  BEGIN
-- Check that the priority is numeric.
    IF p_properties ? 'priority' AND jsonb_typeof(p_properties->'priority') <> 'null' THEN
      IF jsonb_typeof(p_properties->'priority') = 'number' THEN
        p_priority = p_properties->>'priority';
      ELSE
        RAISE EXCEPTION '_check_json_table_properties: the "priority" property must be numeric.';
      END IF;
    END IF;
-- Check that the tablespaces exist, if supplied.
    IF p_properties ? 'log_data_tablespace' AND jsonb_typeof(p_properties->'log_data_tablespace') <> 'null' THEN
      IF jsonb_typeof(p_properties->'log_data_tablespace') = 'string' THEN
        p_logDatTsp = p_properties->>'log_data_tablespace';
        IF NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = p_logDatTsp
                ) THEN
          RAISE EXCEPTION '_check_json_table_properties: the log data tablespace "%" does not exists.', p_logDatTsp;
        END IF;
      ELSE
        RAISE EXCEPTION '_check_json_table_properties: the log data tablespace must be a string.';
      END IF;
    END IF;
    IF p_properties ? 'log_index_tablespace' AND jsonb_typeof(p_properties->'log_index_tablespace') <> 'null' THEN
      IF jsonb_typeof(p_properties->'log_index_tablespace') = 'string' THEN
        p_logIdxTsp = p_properties->>'log_index_tablespace';
        IF NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = p_logIdxTsp
                ) THEN
          RAISE EXCEPTION '_check_json_table_properties: the log index tablespace "%" does not exists.', p_logIdxTsp;
        END IF;
      ELSE
        RAISE EXCEPTION '_check_json_table_properties: the log index tablespace must be a string.';
      END IF;
    END IF;
-- Check the ignored_triggers property.
    IF p_properties ? 'ignored_triggers' AND jsonb_typeof(p_properties->'ignored_triggers') <> 'null' THEN
      IF jsonb_typeof(p_properties->'ignored_triggers') = 'string' THEN
--   The property is a string.
        p_ignoredTriggers = ARRAY[p_properties->>'ignored_triggers'];
        IF p_properties->>'ignored_triggers' IN ('emaj_log_trg', 'emaj_trunc_trg') THEN
          v_emajTriggers = p_ignoredTriggers;
        END IF;
      ELSIF jsonb_typeof(p_properties->'ignored_triggers') = 'array' THEN
--   The property is an array, transform the json array into a native arrays to process.
        WITH trigger AS (
          SELECT trigger_name FROM jsonb_array_elements_text(p_properties->'ignored_triggers') AS trigger_name
          )
        SELECT array_agg(trigger_name),
               array_agg(trigger_name) FILTER (WHERE trigger_name IN ('emaj_log_trg', 'emaj_trunc_trg'))
          INTO p_ignoredTriggers, v_emajTriggers
          FROM trigger;
      ELSE
        RAISE EXCEPTION '_check_json_table_properties: the "ignored_triggers" property must be an array or a string.';
      END IF;
--   Check that triggers listed in the ignored_triggers property are not emaj triggers.
      IF v_emajTriggers IS NOT NULL THEN
        RAISE EXCEPTION '_check_json_table_properties: E-Maj triggers are not allowed in the "ignored_triggers" property.';
      END IF;
    END IF;
-- Check the ignored_triggers_profiles property.
    IF p_properties ? 'ignored_triggers_profiles' AND jsonb_typeof(p_properties->'ignored_triggers_profiles') <> 'null' THEN
      IF jsonb_typeof(p_properties->'ignored_triggers_profiles') = 'string' THEN
--   The property is a string.
        p_ignoredTriggersProfiles = ARRAY[p_properties->>'ignored_triggers_profiles'];
      ELSIF jsonb_typeof(p_properties->'ignored_triggers_profiles') = 'array' THEN
--   The property is an array, transform the json array into a native arrays to process.
        SELECT array_agg(triggers_profile) INTO p_ignoredTriggersProfiles
          FROM jsonb_array_elements_text(p_properties->'ignored_triggers_profiles') AS triggers_profile;
      ELSE
        RAISE EXCEPTION '_check_json_table_properties: the "ignored_triggers_profiles" property must be an array or a string.';
      END IF;
    END IF;
-- Check no properties are unknown.
    v_extraProperties = p_properties - 'priority' - 'log_data_tablespace' - 'log_index_tablespace'
                                     - 'ignored_triggers' - 'ignored_triggers_profiles';
    IF v_extraProperties IS NOT NULL AND v_extraProperties <> '{}' THEN
      RAISE EXCEPTION '_check_json_table_properties: properties "%" are unknown.', v_extraProperties;
    END IF;
--
    RETURN;
  END;
$_check_json_table_properties$;

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

CREATE OR REPLACE FUNCTION emaj._check_tables_for_rollbackable_group(p_schema TEXT, p_tables TEXT[], p_arrayFromRegex BOOLEAN,
                                                                     p_callingFunction TEXT)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_tables_for_rollbackable_group$
-- This function filters or verifies that tables are compatible with ROLLBACKABLE groups.
-- (they must have a PK and no be UNLOGGED or WITH OIDS)
-- Input: schema, array of tables names, boolean indicating whether the tables list is built from regexp, calling function name
-- Output: updated tables name array
  DECLARE
    v_list                   TEXT;
    v_array                  TEXT[];
  BEGIN
-- Check or discard tables without primary key.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class t
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema AND t.relname = ANY(p_tables)
        AND relkind = 'r'
        AND NOT EXISTS
              (SELECT 0
                 FROM pg_catalog.pg_class c
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = c.relnamespace)
                      JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = c.oid)
                           WHERE contype = 'p'
                             AND nspname = p_schema
                             AND c.relname = t.relname
              );
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '%: In schema %, some tables (%) have no PRIMARY KEY.', p_callingFunction, quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '%: Some tables without PRIMARY KEY (%) are not selected.', p_callingFunction, v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
-- Check or discard UNLOGGED tables.
    SELECT string_agg(quote_ident(relname), ', '), array_agg(relname) INTO v_list, v_array
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = p_schema
        AND relname = ANY(p_tables)
        AND relkind = 'r'
        AND relpersistence = 'u';
    IF v_list IS NOT NULL THEN
      IF NOT p_arrayFromRegex THEN
        RAISE EXCEPTION '%: In schema %, some tables (%) are UNLOGGED tables.', p_callingFunction, quote_ident(p_schema), v_list;
      ELSE
        RAISE WARNING '%: Some UNLOGGED tables (%) are not selected.', p_callingFunction, v_list;
        -- remove these tables from the tables to process
        p_tables = array(SELECT unnest(p_tables) EXCEPT SELECT unnest(v_array));
      END IF;
    END IF;
--
    RETURN p_tables;
  END;
$_check_tables_for_rollbackable_group$;

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
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      IF p_exceptionIfMissing THEN
        RAISE EXCEPTION '_build_tblseqs_array_from_regexp: The schema "%" does not exist!', p_schema;
      ELSE
        RAISE WARNING '_build_tblseqs_array_from_regexp: The schema "%" does not exist.', p_schema;
      END IF;
    END IF;
-- Process empty filters as NULL.
    SELECT CASE WHEN p_includeFilter = '' THEN NULL ELSE p_includeFilter END,
           CASE WHEN p_excludeFilter = '' THEN NULL ELSE p_excludeFilter END
      INTO p_includeFilter, p_excludeFilter;
-- Build and return the list of relations names satisfying the pattern.
    RETURN array_agg(rel_tblseq)
      FROM (
        SELECT rel_tblseq
          FROM emaj.emaj_relation
          WHERE rel_schema = p_schema
            AND rel_tblseq ~ p_includeFilter
            AND (p_excludeFilter IS NULL OR rel_tblseq !~ p_excludeFilter)
            AND rel_kind = p_relkind
            AND upper_inf(rel_time_range)
          ORDER BY rel_tblseq
           ) AS t;
  END;
$_build_tblseqs_array_from_regexp$;

CREATE OR REPLACE FUNCTION emaj._check_tblseqs_array(p_schema TEXT, p_tblseqs TEXT[], p_relkind TEXT, p_exceptionIfMissing BOOLEAN)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_tblseqs_array$
-- The function checks a names array of tables or sequences.
-- Depending on the p_exceptionIfMissing parameter, it warns or raises an error if the schema or any table or sequence doesn't exist.
-- (WARNING only is used by removal functions so that it is possible to remove a dropped or renamed relation from its group)
-- It verifies that the schema and the tables or sequences belong to a tables group (not necessary the same one).
-- It returns the names array, duplicates and empty names being removed.
-- Inputs: schema,
--         tables or sequences names array,
--         relation kind ('r' or 'S'),
--         boolean indicating whether a missing schema or relation must raise an exception or just a warning.
-- Outputs: tables or sequences names array.
  DECLARE
    v_relationKind           TEXT;
    v_schemaExists           BOOLEAN = TRUE;
    v_list                   TEXT;
    v_tblseqs                TEXT[];
  BEGIN
-- Setup constant.
    IF p_relkind = 'r' THEN
      v_relationKind = 'tables';
    ELSE
      v_relationKind = 'sequences';
    END IF;
-- Check that the schema exists.
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_namespace
            WHERE nspname = p_schema
         ) THEN
      IF p_exceptionIfMissing THEN
        RAISE EXCEPTION '_check_tblseqs_array: The schema "%" does not exist!', p_schema;
      ELSE
        RAISE WARNING '_check_tblseqs_array: The schema "%" does not exist.', p_schema;
      END IF;
      v_schemaExists = FALSE;
    END IF;
-- Clean up the relation names array: remove duplicates values, NULL and empty strings.
    SELECT array_agg(DISTINCT tblseq) INTO v_tblseqs
      FROM unnest(p_tblseqs) AS tblseq
      WHERE tblseq IS NOT NULL AND tblseq <> '';
-- If the schema exists, check that all relations exist.
    IF v_schemaExists THEN
      WITH tblseqs AS (
        SELECT unnest(v_tblseqs) AS tblseq
        )
      SELECT string_agg(quote_ident(tblseq), ', ') INTO v_list
        FROM
          (SELECT tblseq
             FROM tblseqs
             WHERE NOT EXISTS
                     (SELECT 0
                        FROM pg_catalog.pg_class
                             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        WHERE nspname = p_schema
                          AND relname = tblseq
                          AND relkind = p_relkind
                     )
          ) AS t;
      IF v_list IS NOT NULL THEN
        IF p_exceptionIfMissing THEN
          RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not exist!', p_schema, v_relationKind, v_list;
        ELSE
          RAISE WARNING '_check_tblseqs_array: In schema "%", some % (%) do not exist.', p_schema, v_relationKind, v_list;
        END IF;
      END IF;
    END IF;
-- Check that the relations currently belong to a tables group (not necessarily the same for all relations).
    WITH all_supplied_tblseqs AS (
      SELECT unnest(v_tblseqs) AS tblseq
      ),
         tblseqs_in_groups AS (
      SELECT rel_tblseq
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = ANY(v_tblseqs)
          AND rel_kind = p_relkind
          AND upper_inf(rel_time_range)
      )
    SELECT string_agg(quote_ident(tblseq), ', ') INTO v_list
      FROM
        (  SELECT tblseq
             FROM all_supplied_tblseqs
         EXCEPT
           SELECT rel_tblseq FROM tblseqs_in_groups
        ) AS t;
    IF v_list IS NOT NULL THEN
      RAISE EXCEPTION '_check_tblseqs_array: In schema "%", some % (%) do not currently belong to any tables group.',
                      p_schema, v_relationKind, v_list;
    END IF;
--
    RETURN v_tblseqs;
  END;
$_check_tblseqs_array$;

CREATE OR REPLACE FUNCTION emaj._get_lock_tblseqs_groups(p_schema TEXT, p_tblseqs TEXT[], p_newGroup TEXT,
                                                         OUT p_groups TEXT[], OUT p_loggingGroups TEXT[], OUT p_nbAuditOnlyGroups INT)
LANGUAGE plpgsql AS
$_get_lock_tblseqs_groups$
-- This function gets the lists of groups and logging groups holding a set of tables or sequences of a single schema.
-- It also counts the number of AUDIT_ONLY groups.
-- It is called by functions that change the tables groups structure.
-- For functions moving tables or sequences from a group to another, it handles the target tables group.
-- It locks the target and source tables groups so that no other operation simultaneously occurs for these groups.
-- Input: schema,
--        array of table or sequence names,
--        target tables group for move functions (set to NULL for other functions).
-- Output: tables groups names array, logging tables groups names array, number of AUDIT_ONLY tables groups.
  BEGIN
-- Build the output data.
-- The CTE is needed for the FOR UPDATE clause not allowed when aggregate functions.
    WITH tables_group AS (
      SELECT group_name, group_is_logging, group_is_rollbackable FROM emaj.emaj_group
        WHERE (p_newGroup IS NOT NULL AND group_name = p_newGroup) OR
              group_name IN
               (SELECT DISTINCT rel_group FROM emaj.emaj_relation
                  WHERE rel_schema = p_schema
                    AND rel_tblseq = ANY(p_tblseqs)
                    AND upper_inf(rel_time_range))
        FOR UPDATE OF emaj_group
      )
    SELECT array_agg(group_name ORDER BY group_name),
           array_agg(group_name ORDER BY group_name) FILTER (WHERE group_is_logging),
           count(group_name) FILTER (WHERE NOT group_is_rollbackable AND (p_newGroup IS NULL OR group_name <> p_newGroup))
      INTO p_groups, p_loggingGroups, p_nbAuditOnlyGroups
      FROM tables_group;
--
    RETURN;
  END;
$_get_lock_tblseqs_groups$;

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

CREATE OR REPLACE FUNCTION emaj._check_new_mark(p_groupNames TEXT[], p_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_check_new_mark$
-- This function verifies that a new mark name supplied the user is valid.
-- It processes the possible NULL mark value and the replacement of % wild characters.
-- It also checks that the mark name do not already exist for any group.
-- Input: array of group names, name of the mark to set
-- Output: internal name of the mark
  DECLARE
    v_markName               TEXT = p_mark;
    v_groupList              TEXT;
    v_count                  INTEGER;
  BEGIN
-- Check the mark name is not 'EMAJ_LAST_MARK'.
    IF p_mark = 'EMAJ_LAST_MARK' THEN
      RAISE EXCEPTION '_check_new_mark: "%" is not an allowed name for a new mark.', p_mark;
    END IF;
-- Process null or empty supplied mark name.
    IF v_markName = '' OR v_markName IS NULL THEN
      v_markName = 'MARK_%';
    END IF;
-- Process % wild characters in mark name.
    v_markName = replace(v_markName, '%', substring(to_char(clock_timestamp(), 'HH24.MI.SS.US') from 1 for 13));
-- Check that the mark does not exist for any groups.
    SELECT string_agg(mark_group,', ' ORDER BY mark_group), count(*) INTO v_groupList, v_count
      FROM emaj.emaj_mark
      WHERE mark_name = v_markName
        AND mark_group = ANY(p_groupNames);
    IF v_count > 0 THEN
      IF v_count = 1 THEN
        RAISE EXCEPTION '_check_new_mark: The group "%" already contains a mark named "%".', v_groupList, v_markName;
      ELSE
        RAISE EXCEPTION '_check_new_mark: The groups "%" already contain a mark named "%".', v_groupList, v_markName;
      END IF;
    END IF;
--
    RETURN v_markName;
  END;
$_check_new_mark$;

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

CREATE OR REPLACE FUNCTION emaj._check_tblseqs_filter(INOUT p_tblseqs TEXT[], p_groupNames TEXT[], p_firstMarkTimeId BIGINT,
                                                      p_lastMarkTimeId BIGINT, p_checkInGroupAtStartMark BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql AS
$_check_tblseqs_filter$
-- This function verifies that a schema qualified table/sequence names array is valid for one or several groups and in a marks range.
-- Input: array of table/sequence names, array of group names, time id of the first and last marks,
--        and a boolean indicating whether the tables and sequences must be owned by one group at start mark time
-- Output: the array of table/sequence names, without empty or duplicates (the array is empty if it does not contain any relation
  DECLARE
    v_tblseqErr              TEXT;
    v_count                  INT;
  BEGIN
-- Remove duplicates values, NULL and empty strings from the supplied tables/sequences names array.
    SELECT coalesce(array_agg(DISTINCT table_seq_name), ARRAY[]::TEXT[]) INTO p_tblseqs
      FROM unnest(p_tblseqs) AS table_seq_name
      WHERE table_seq_name IS NOT NULL
        AND table_seq_name <> '';
    IF p_tblseqs = ARRAY[]::TEXT[] THEN
      RAISE WARNING '_check_tblseqs_filter: The table/sequence names array is empty.';
      RETURN;
    END IF;
    IF p_checkInGroupAtStartMark THEN
-- Each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups.
      SELECT string_agg(t,', ' ORDER BY t), count(*) INTO v_tblseqErr, v_count
        FROM
          (  SELECT t
               FROM unnest(p_tblseqs) AS t
           EXCEPT
             SELECT rel_schema || '.' || rel_tblseq
               FROM emaj.emaj_relation
               WHERE rel_time_range @> p_firstMarkTimeId              -- tables/sequences that belong to their group
                 AND rel_group = ANY (p_groupNames)                   -- at the start mark time
          ) AS t2;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_check_tblseqs_filter: % tables/sequences (%) did not belong to any of the selected tables groups '
                        'at start mark time.', v_count, v_tblseqErr;
      END IF;
    ELSE
-- Each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups.
      SELECT string_agg(t,', ' ORDER BY t), count(*) INTO v_tblseqErr, v_count
        FROM
          (  SELECT t
               FROM unnest(p_tblseqs) AS t
           EXCEPT
             SELECT rel_schema || '.' || rel_tblseq
               FROM emaj.emaj_relation
               WHERE rel_time_range && int8range(p_firstMarkTimeId, p_lastMarkTimeId,'[)')
                 AND rel_group = ANY (p_groupNames)
          ) AS t2;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_check_tblseqs_filter: % tables/sequences (%) never belonged to any of the selected tables groups '
                        'during the requested marks range.', v_count, v_tblseqErr;
      END IF;
    END IF;
--
    RETURN;
  END;
$_check_tblseqs_filter$;

CREATE OR REPLACE FUNCTION emaj._truncate_trigger_fnct()
RETURNS TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_truncate_trigger_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables.
-- Before effetively truncating the table, keep a trace of the event in the log table.
-- First, a generic TRU event is recorded (it will be used by the functions that generate SQL script to replay).
-- Then, the content of the table to truncate is copied into the log table, with a 'TRU' emaj_verb and an 'OLD' emaj_tuple (it will be
-- used by the rollback functions)
-- And add the number of recorded rows to the log sequence (to get accurate statistics)
-- The function is declared as SECURITY DEFINER so that any role performing a TRUNCATE on an application table can log the event into
--   E-Maj tables.
  DECLARE
    v_fullLogTableName       TEXT;
    v_fullLogSequenceName    TEXT;
    v_nbRows                 BIGINT;
  BEGIN
    IF (TG_OP = 'TRUNCATE') THEN
-- Get some log object names for the truncated table.
      SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table),
             quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_sequence)
             INTO v_fullLogTableName, v_fullLogSequenceName
        FROM emaj.emaj_relation
        WHERE rel_schema = TG_TABLE_SCHEMA
          AND rel_tblseq = TG_TABLE_NAME
          AND upper_inf(rel_time_range);
-- Log the TRU event into the log table, with emaj_tuple set to an empty string.
      EXECUTE format('INSERT INTO %s (emaj_verb, emaj_tuple) VALUES (''TRU'', '''')',
                     v_fullLogTableName);
-- Log all rows from the table.
      EXECUTE format('INSERT INTO %s SELECT *, ''TRU'', ''OLD'' FROM ONLY %I.%I ',
                     v_fullLogTableName, TG_TABLE_SCHEMA, TG_TABLE_NAME);
      GET DIAGNOSTICS v_nbRows = ROW_COUNT;
      IF v_nbRows > 0 THEN
-- Adjust the log sequence value for the table.
        EXECUTE format('SELECT setval(%L,'
                       '  (SELECT CASE WHEN is_called THEN last_value + %s ELSE last_value + %s - 1 END FROM %s))',
                       v_fullLogSequenceName, v_nbRows, v_nbRows, v_fullLogSequenceName);
      END IF;
    END IF;
-- And effectively TRUNCATE the table.
    RETURN NULL;
  END;
$_truncate_trigger_fnct$;

CREATE OR REPLACE FUNCTION emaj._create_log_schemas(p_function TEXT, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_log_schemas$
-- The function creates all log schemas that will be needed to create new log tables.
-- The function is called at tables groups configuration import.
-- Input: calling function to record into the emaj_hist table and time_id
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted the CREATE privilege on
--   the current database.
  DECLARE
    r_schema                 RECORD;
  BEGIN
    FOR r_schema IN
        SELECT DISTINCT 'emaj_' || tmp_schema AS log_schema
          FROM tmp_app_table
          WHERE NOT EXISTS                                                                -- minus those already created
                  (SELECT 0
                     FROM emaj.emaj_schema
                     WHERE sch_name = 'emaj_' || tmp_schema)
          ORDER BY 1
    LOOP
-- Check that the schema doesn't already exist.
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_namespace
              WHERE nspname = r_schema.log_schema
           ) THEN
        RAISE EXCEPTION '_create_log_schemas: The schema "%" should not exist. Drop it manually.',r_schema.log_schema;
      END IF;
-- Create the schema and give the appropriate rights.
      EXECUTE format('CREATE SCHEMA %I AUTHORIZATION emaj_adm',
                     r_schema.log_schema);
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                     r_schema.log_schema);
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
      INSERT INTO emaj.emaj_schema (sch_name, sch_time_id)
        VALUES (r_schema.log_schema, p_timeId);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (p_function, 'LOG_SCHEMA CREATED', quote_ident(r_schema.log_schema));
    END LOOP;
--
    RETURN;
  END;
$_create_log_schemas$;

CREATE OR REPLACE FUNCTION emaj._drop_log_schemas(p_function TEXT, p_isForced BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_log_schemas$
-- The function looks for log schemas to drop. Drop them if any.
-- Input: calling function to record into the emaj_hist table,
--        boolean telling whether the schema to drop may contain residual objects
  DECLARE
    r_schema                 RECORD;
  BEGIN
-- For each log schema to drop,
    FOR r_schema IN
        SELECT sch_name AS log_schema
          FROM emaj.emaj_schema                           -- the existing schemas
          WHERE sch_name <> 'emaj'
      EXCEPT
        SELECT DISTINCT rel_log_schema
          FROM emaj.emaj_relation                         -- the currently needed schemas (after tables drop)
          WHERE rel_kind = 'r'
            AND rel_log_schema <> 'emaj'
        ORDER BY 1
    LOOP
      IF p_isForced THEN
-- Drop cascade when called by emaj_force_xxx_group().
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',
                       r_schema.log_schema);
      ELSE
-- Otherwise, drop restrict with a trap on the potential error.
        BEGIN
          EXECUTE format('DROP SCHEMA %I',
                         r_schema.log_schema);
          EXCEPTION
-- Trap the 3F000 exception to process case when the schema does not exist anymore.
            WHEN INVALID_SCHEMA_NAME THEN                   -- SQLSTATE '3F000'
              RAISE EXCEPTION '_drop_log_schemas: Internal error (the schema "%" does not exist).',r_schema.log_schema;
-- Trap the 2BP01 exception to generate a more understandable error message.
            WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
              RAISE EXCEPTION '_drop_log_schemas: Cannot drop the schema "%". It probably owns unattended objects.'
                              ' Use the emaj_verify_all() function to get details.', r_schema.log_schema;
        END;
      END IF;
-- Remove the schema from the emaj_schema table.
      DELETE FROM emaj.emaj_schema
        WHERE sch_name = r_schema.log_schema;
-- Record the schema drop in emaj_hist table.
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (p_function,'LOG_SCHEMA DROPPED',quote_ident(r_schema.log_schema));
    END LOOP;
--
    RETURN;
  END;
$_drop_log_schemas$;

CREATE OR REPLACE FUNCTION emaj._build_path_name(p_dir TEXT, p_file TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$_build_path_name$
-- This function build a path name from a directory name and a file names.
-- Some characters of the file name are translated in order to manipulate files on the OS more easily.
-- Both names are concatenated with a / character between.
SELECT p_dir || '/' || translate(p_file, E' /\\|$<>*\'"', '__________');
$_build_path_name$;

---------------------------------------------------
--                                               --
-- Elementary functions for tables and sequences --
--                                               --
---------------------------------------------------

CREATE OR REPLACE FUNCTION emaj.emaj_assign_table(p_schema TEXT, p_table TEXT, p_group TEXT, p_properties JSONB DEFAULT NULL,
                                                  p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_table$
-- The function assigns a table into a tables group.
-- Inputs: schema name, table name, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group, ie. 1
  BEGIN
    RETURN emaj._assign_tables(p_schema, ARRAY[p_table], p_group, p_properties, p_mark, FALSE, FALSE);
  END;
$emaj_assign_table$;
COMMENT ON FUNCTION emaj.emaj_assign_table(TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign a table into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_tables(p_schema TEXT, p_tables TEXT[], p_group TEXT, p_properties JSONB DEFAULT NULL,
                                                   p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_tables$
-- The function assigns several tables at once into a tables group.
-- Inputs: schema, array of table names, assignment group name, assignment properties (optional),
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively assigned to the tables group
  BEGIN
    RETURN emaj._assign_tables(p_schema, p_tables, p_group, p_properties, p_mark, TRUE, FALSE);
  END;
$emaj_assign_tables$;
COMMENT ON FUNCTION emaj.emaj_assign_tables(TEXT,TEXT[],TEXT,JSONB,TEXT) IS
$$Assign several tables into a tables group.$$;

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
-- Process empty filters as NULL
    SELECT CASE WHEN p_tablesIncludeFilter = '' THEN NULL ELSE p_tablesIncludeFilter END,
           CASE WHEN p_tablesExcludeFilter = '' THEN NULL ELSE p_tablesExcludeFilter END
      INTO p_tablesIncludeFilter, p_tablesExcludeFilter;
-- Build the list of tables names satisfying the pattern.
    SELECT array_agg(relname) INTO v_tables
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND relname ~ p_tablesIncludeFilter
             AND (p_tablesExcludeFilter IS NULL OR relname !~ p_tablesExcludeFilter)
             AND relkind IN ('r', 'p')
           ORDER BY relname
        ) AS t;
-- Call the _assign_tables() function for execution.
    RETURN emaj._assign_tables(p_schema, v_tables, p_group, p_properties, p_mark, TRUE, TRUE);
  END;
$emaj_assign_tables$;
COMMENT ON FUNCTION emaj.emaj_assign_tables(TEXT,TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Assign tables on name patterns into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj._assign_tables(p_schema TEXT, p_tables TEXT[], p_group TEXT, p_properties JSONB, p_mark TEXT,
                                               p_multiTable BOOLEAN, p_arrayFromRegex BOOLEAN)
RETURNS INTEGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_assign_tables$
-- The function effectively assigns tables into a tables group.
-- Inputs: schema, array of table names, group name, properties as JSON structure
--         mark to set for lonnging groups, a boolean indicating whether several tables need to be processed,
--         a boolean indicating whether the tables array has been built from regex filters
-- Outputs: number of tables effectively assigned to the tables group
-- The JSONB p_properties parameter has the following structure '{"priority":..., "log_data_tablespace":..., "log_index_tablespace":...}'
--   each properties being NULL by default
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted the CREATE privilege on
--   the current database, needed to create log schemas.
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
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
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
-- Create the schema and give the appropriate rights.
        EXECUTE format('CREATE SCHEMA %I AUTHORIZATION emaj_adm',
                       v_logSchema);
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO emaj_viewer',
                       v_logSchema);
-- And record the schema creation into the emaj_schema and the emaj_hist tables.
        INSERT INTO emaj.emaj_schema (sch_name, sch_time_id)
          VALUES (v_logSchema, v_timeId);
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES (CASE WHEN p_multiTable THEN 'ASSIGN_TABLES' ELSE 'ASSIGN_TABLE' END, 'LOG_SCHEMA CREATED', quote_ident(v_logSchema));
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

CREATE OR REPLACE FUNCTION emaj.emaj_remove_table(p_schema TEXT, p_table TEXT, p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_table$
-- The function removes a table from its tables group.
-- Inputs: schema name, table name, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively removed to the tables group, ie. 1
  BEGIN
    RETURN emaj._remove_tables(p_schema, ARRAY[p_table], p_mark, FALSE, FALSE);
  END;
$emaj_remove_table$;
COMMENT ON FUNCTION emaj.emaj_remove_table(TEXT,TEXT,TEXT) IS
$$Remove a table from its tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_tables(p_schema TEXT, p_tables TEXT[], p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_tables$
-- The function removes several tables at once from their tables group.
-- Inputs: schema, array of table names, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively removed from the tables group
  BEGIN
    RETURN emaj._remove_tables(p_schema, p_tables, p_mark, TRUE, FALSE);
  END;
$emaj_remove_tables$;
COMMENT ON FUNCTION emaj.emaj_remove_tables(TEXT,TEXT[],TEXT) IS
$$Remove several tables from their tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_tables(p_schema TEXT, p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                   p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_tables$
-- The function removes tables on name patterns from their tables group.
-- Inputs: schema, 2 patterns to filter table names (one to include and another to exclude),
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively removed from the tables group
  DECLARE
    v_tables                 TEXT[];
  BEGIN
    v_tables = emaj._build_tblseqs_array_from_regexp(p_schema, 'r', p_tablesIncludeFilter, p_tablesExcludeFilter, FALSE);
-- Call the _remove_tables() function for execution.
    RETURN emaj._remove_tables(p_schema, v_tables, p_mark, TRUE, TRUE);
  END;
$emaj_remove_tables$;
COMMENT ON FUNCTION emaj.emaj_remove_tables(TEXT,TEXT,TEXT,TEXT) IS
$$Remove several tables on name patterns from their tables group.$$;

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
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', FALSE);
    END IF;
-- Get and lock the tables groups and logging groups holding these tables.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, NULL);
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
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_move_table(p_schema TEXT, p_table TEXT, p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_table$
-- The function moves a table from its tables group to another tables group.
-- Inputs: schema name, table name, new group name, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively moved to the new tables group, ie. 1
  BEGIN
    RETURN emaj._move_tables(p_schema, ARRAY[p_table], p_newGroup, p_mark, FALSE, FALSE);
  END;
$emaj_move_table$;
COMMENT ON FUNCTION emaj.emaj_move_table(TEXT,TEXT,TEXT,TEXT) IS
$$Move a table from its tables group to another tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_move_tables(p_schema TEXT, p_tables TEXT[], p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_tables$
-- The function moves several tables at once from their tables group to another tables group.
-- Inputs: schema, array of table names, new group name, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively moved to the new tables group
  BEGIN
    RETURN emaj._move_tables(p_schema, p_tables, p_newGroup, p_mark, TRUE, FALSE);
  END;
$emaj_move_tables$;
COMMENT ON FUNCTION emaj.emaj_move_tables(TEXT,TEXT[],TEXT,TEXT) IS
$$Move several tables from their tables group to another tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_move_tables(p_schema TEXT, p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                 p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_tables$
-- The function moves tables on name patterns from their tables group to another tables group.
-- Inputs: schema, 2 patterns to filter table names (one to include and another to exclude), new group name,
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively moved to the new tables group
  DECLARE
    v_tables                 TEXT[];
  BEGIN
    v_tables = emaj._build_tblseqs_array_from_regexp(p_schema, 'r', p_tablesIncludeFilter, p_tablesExcludeFilter, TRUE);
-- Call the _move_tables() function for execution.
    RETURN emaj._move_tables(p_schema, v_tables, p_newGroup, p_mark, TRUE, TRUE);
  END;
$emaj_move_tables$;
COMMENT ON FUNCTION emaj.emaj_move_tables(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Move several tables on name patterns from their tables group to another tables group.$$;

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
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', TRUE);
    END IF;
-- Remove tables that already belong to the new group.
    SELECT array_agg(rel_tblseq ORDER BY rel_tblseq) FILTER (WHERE rel_group <> p_newGroup),
           string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq) FILTER (WHERE rel_group = p_newGroup)
      INTO p_tables, v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_tables)
        AND upper_inf(rel_time_range);
-- Warn only if the tables list has been supplied by the user.
    IF v_list IS NOT NULL AND NOT p_arrayFromRegex THEN
      RAISE WARNING '_move_tables: In schema "%", some tables (%) already belong to the tables group "%".', p_schema, v_list, p_newGroup;
    END IF;
-- Get and lock the tables groups and logging groups holding these tables, and count the number of AUDIT_ONLY groups.
    SELECT p_groups, p_loggingGroups, p_nbAuditOnlyGroups INTO v_groups, v_loggingGroups, v_nbAuditOnlyGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, p_newGroup);
-- If at least 1 source tables group is of type AUDIT_ONLY and the target tables group is ROLLBACKABLE, add some checks on tables.
-- They may be incompatible with ROLLBACKABLE groups.
    IF v_nbAuditOnlyGroups > 0 AND v_newGroupIsRollbackable THEN
      p_tables = emaj._check_tables_for_rollbackable_group(p_schema, p_tables, p_arrayFromRegex, '_move_tables');
    END IF;
-- Check the supplied mark.
    SELECT emaj._check_new_mark(v_loggingGroups, p_mark) INTO v_markName;
-- OK,
    IF p_tables IS NULL OR p_tables = '{}' THEN
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
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_modify_table(p_schema TEXT, p_table TEXT, p_changedProperties JSONB,
                                                  p_mark TEXT DEFAULT 'MODIFY_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_modify_table$
-- The function modifies the assignment properties of a table.
-- Inputs: schema name, table name, assignment properties changes,
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively modified, ie 0 or 1
  BEGIN
    RETURN emaj._modify_tables(p_schema, ARRAY[p_table], p_changedProperties, p_mark, FALSE, FALSE);
  END;
$emaj_modify_table$;
COMMENT ON FUNCTION emaj.emaj_modify_table(TEXT,TEXT,JSONB,TEXT) IS
$$Modify the assignment properties of a table.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_modify_tables(p_schema TEXT, p_tables TEXT[], p_changedProperties JSONB,
                                                   p_mark TEXT DEFAULT 'MODIFY_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_modify_tables$
-- The function modifies the assignment properties for several tables at once.
-- Inputs: schema, array of table names, assignment properties,
--         mark name to set when logging groups (optional)
-- Outputs: number of tables effectively modified
  BEGIN
    RETURN emaj._modify_tables(p_schema, p_tables, p_changedProperties, p_mark, TRUE, FALSE);
  END;
$emaj_modify_tables$;
COMMENT ON FUNCTION emaj.emaj_modify_tables(TEXT,TEXT[],JSONB,TEXT) IS
$$Modify the assignment properties of several tables.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_modify_tables(p_schema TEXT, p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                   p_properties JSONB, p_mark TEXT DEFAULT 'MODIFY_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_modify_tables$
-- The function modifies the assignment properties for several tables selected on name regexp pattern at once.
-- Inputs: schema name, 2 patterns to filter table names (one to include and another to exclude),
--         assignment properties, mark name to set when logging groups (optional)
-- Outputs: number of tables effectively modified
  DECLARE
    v_tables                 TEXT[];
  BEGIN
    v_tables = emaj._build_tblseqs_array_from_regexp(p_schema, 'r', p_tablesIncludeFilter, p_tablesExcludeFilter, TRUE);
-- Call the _modify_tables() function for execution.
    RETURN emaj._modify_tables(p_schema, v_tables, p_properties, p_mark, TRUE, TRUE);
  END;
$emaj_modify_tables$;
COMMENT ON FUNCTION emaj.emaj_modify_tables(TEXT,TEXT,TEXT,JSONB,TEXT) IS
$$Modify the assignment properties of several tables selected on name patterns.$$;

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
      p_tables = emaj._check_tblseqs_array(p_schema, p_tables, 'r', TRUE);
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
-- Get and lock the tables groups and logging groups holding these tables.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_tables, NULL);
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
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_get_current_log_table(p_app_schema TEXT, p_app_table TEXT,
                                                           OUT log_schema TEXT, OUT log_table TEXT)
LANGUAGE plpgsql AS
$emaj_get_current_log_table$
-- The function returns the current log table for a given application schema and table.
-- It returns NULL values if the table doesn't currently belong to a tables group.
-- Inputs: schema and table names
-- Outputs: schema and table of the currently associated log table
  BEGIN
-- Get the requested data from the emaj_relation table.
    SELECT rel_log_schema, rel_log_table INTO log_schema, log_table
      FROM emaj.emaj_relation
      WHERE rel_schema = p_app_schema
        AND rel_tblseq = p_app_table
        AND upper_inf(rel_time_range);
--
    RETURN;
  END;
$emaj_get_current_log_table$;
COMMENT ON FUNCTION emaj.emaj_get_current_log_table(TEXT,TEXT) IS
$$Retrieve the current log table of a given application table.$$;

CREATE OR REPLACE FUNCTION emaj._create_tbl(p_schema TEXT, p_tbl TEXT, p_groupName TEXT, p_priority INT, p_logDatTsp TEXT,
                                            p_logIdxTsp TEXT, p_ignoredTriggers TEXT[], p_timeId BIGINT, p_groupIsLogging BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table.
-- Input: the application table to process,
--        the group to add it into,
--        the table properties: priority, tablespaces attributes and triggers to ignore at rollback time
--        the time id of the operation,
--        a boolean indicating whether the group is currently in logging state.
-- The objects created in the log schema:
--    - the associated log table, with its own sequence,
--    - the function and trigger that log the tables updates.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on the
--   application table.
  DECLARE
    v_emajNamesPrefix        TEXT;
    v_baseLogTableName       TEXT;
    v_baseLogIdxName         TEXT;
    v_baseLogFnctName        TEXT;
    v_baseSequenceName       TEXT;
    v_logSchema              TEXT;
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_logIdxName             TEXT;
    v_logFnctName            TEXT;
    v_sequenceName           TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_pkCols                 TEXT[];
    v_genExprCols            TEXT[];
    v_rlbkColList            TEXT;
    v_genColList             TEXT;
    v_genValList             TEXT;
    v_genSetList             TEXT;
    v_genPkConditions        TEXT;
    v_nbGenAlwaysIdentCol    INTEGER;
    v_attnum                 SMALLINT;
    v_alter_log_table_param  TEXT;
    v_stmt                   TEXT;
    v_triggerList            TEXT;
  BEGIN
-- The checks on the table properties are performed by the calling functions.
-- Build the prefix of all emaj object to create.
    IF length(p_tbl) <= 50 THEN
-- For not too long table name, the prefix is the table name itself.
      v_emajNamesPrefix = p_tbl;
    ELSE
-- For long table names (over 50 char long), compute the suffix to add to the first 50 characters (#1, #2, ...), by looking at the
-- existing names.
      SELECT substr(p_tbl, 1, 50) || '#' || coalesce(max(suffix) + 1, 1)::TEXT INTO v_emajNamesPrefix
        FROM
          (SELECT (regexp_match(substr(rel_log_table, 51), '#(\d+)'))[1]::INT AS suffix
             FROM emaj.emaj_relation
             WHERE substr(rel_log_table, 1, 50) = substr(p_tbl, 1, 50)
          ) AS t;
    END IF;
-- Build the name of emaj components associated to the application table (non schema qualified and not quoted).
    v_baseLogTableName     = v_emajNamesPrefix || '_log';
    v_baseLogIdxName       = v_emajNamesPrefix || '_log_idx';
    v_baseLogFnctName      = v_emajNamesPrefix || '_log_fnct';
    v_baseSequenceName     = v_emajNamesPrefix || '_log_seq';
-- Build the different name for table, trigger, functions,...
    v_logSchema        = 'emaj_' || p_schema;
    v_fullTableName    = quote_ident(p_schema) || '.' || quote_ident(p_tbl);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogTableName);
    v_logIdxName       = quote_ident(v_baseLogIdxName);
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_baseLogFnctName);
    v_sequenceName     = quote_ident(v_logSchema) || '.' || quote_ident(v_baseSequenceName);
-- Prepare the TABLESPACE clauses for data and index
    v_dataTblSpace = coalesce('TABLESPACE ' || quote_ident(p_logDatTsp),'');
    v_idxTblSpace = coalesce('USING INDEX TABLESPACE ' || quote_ident(p_logIdxTsp),'');
-- Create the log table: it looks like the application table, with some additional technical columns.
    EXECUTE format('DROP TABLE IF EXISTS %s',
                   v_logTableName);
    EXECUTE format('CREATE TABLE %s (LIKE %s,'
                   '  emaj_verb      VARCHAR(3)  NOT NULL,'
                   '  emaj_tuple     VARCHAR(3)  NOT NULL,'
                   '  emaj_gid       BIGINT      NOT NULL DEFAULT nextval(''emaj.emaj_global_seq''),'
                   '  emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
                   '  emaj_txid      BIGINT      DEFAULT txid_current(),'
                   '  emaj_user      VARCHAR(32) DEFAULT session_user,'
                   '  CONSTRAINT %s PRIMARY KEY (emaj_gid, emaj_tuple) %s'
                   '  ) %s',
                    v_logTableName, v_fullTableName, v_logIdxName, v_idxTblSpace, v_dataTblSpace);
-- Get the attnum of the emaj_verb column.
    SELECT attnum INTO STRICT v_attnum
      FROM pg_catalog.pg_attribute
           JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = v_logSchema
        AND relname = v_baseLogTableName
        AND attname = 'emaj_verb';
-- Adjust the log table structure with the alter_log_table parameter, if set.
    SELECT param_value_text INTO v_alter_log_table_param
      FROM emaj.emaj_param
      WHERE param_key = ('alter_log_table');
    IF v_alter_log_table_param IS NOT NULL AND v_alter_log_table_param <> '' THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_alter_log_table_param);
    END IF;
-- Set the index associated to the primary key as cluster index (It may be useful for CLUSTER command).
    EXECUTE format('ALTER TABLE ONLY %s CLUSTER ON %s',
                   v_logTableName, v_logIdxName);
-- Remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables.
    SELECT string_agg(action, ',') INTO v_stmt
      FROM
        (SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
           FROM pg_catalog.pg_attribute
                JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = v_logSchema
             AND relname = v_baseLogTableName
             AND attnum > 0
             AND attnum < v_attnum
             AND NOT attisdropped
             AND attnotnull
        ) AS t;
    IF v_stmt IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %s %s',
                     v_logTableName, v_stmt);
    END IF;
-- Create the sequence associated to the log table.
    EXECUTE format('CREATE SEQUENCE %s',
                   v_sequenceName);
-- Create the log function.
-- The new row is logged for each INSERT, the old row is logged for each DELETE and the old and new rows are logged for each UPDATE.
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || v_logFnctName || '() RETURNS TRIGGER AS $logfnct$'
         || 'BEGIN'
-- The sequence associated to the log table is incremented at the beginning of the function ...
         || '  PERFORM NEXTVAL(' || quote_literal(v_sequenceName) || ');'
-- ... and the global id sequence is incremented by the first/only INSERT into the log table.
         || '  IF (TG_OP = ''DELETE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''DEL'', ''OLD'';'
         || '    RETURN OLD;'
         || '  ELSIF (TG_OP = ''UPDATE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''UPD'', ''OLD'';'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''UPD'', ''NEW'', lastval();'
         || '    RETURN NEW;'
         || '  ELSIF (TG_OP = ''INSERT'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''INS'', ''NEW'';'
         || '    RETURN NEW;'
         || '  END IF;'
         || '  RETURN NULL;'
         || 'END;'
         || '$logfnct$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;';
-- Create the log and truncate triggers.
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                   p_schema, p_tbl);
    EXECUTE format('CREATE TRIGGER emaj_log_trg'
                   ' AFTER INSERT OR UPDATE OR DELETE ON %I.%I'
                   '  FOR EACH ROW EXECUTE PROCEDURE %s()',
                   p_schema, p_tbl, v_logFnctName);
    EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                   p_schema, p_tbl);
    EXECUTE format('CREATE TRIGGER emaj_trunc_trg'
                   '  BEFORE TRUNCATE ON %I.%I'
                   '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._truncate_trigger_fnct()',
                   p_schema, p_tbl);
    IF p_groupIsLogging THEN
-- If the group is in logging state, set the triggers as ALWAYS triggers, so that they can fire at rollback time.
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg, ENABLE ALWAYS TRIGGER emaj_log_trg',
                     p_schema, p_tbl);
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg, ENABLE ALWAYS TRIGGER emaj_trunc_trg',
                     p_schema, p_tbl);
    ELSE
-- If the group is idle, deactivate the triggers (they will be enabled at emaj_start_group time).
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                     p_schema, p_tbl);
      EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg',
                     p_schema, p_tbl);
    END IF;
-- Set emaj_adm as owner of log objects.
    EXECUTE format('ALTER TABLE %s OWNER TO emaj_adm',
                   v_logTableName);
    EXECUTE format('ALTER SEQUENCE %s OWNER TO emaj_adm',
                   v_sequenceName);
    EXECUTE format('ALTER FUNCTION %s () OWNER TO emaj_adm',
                   v_logFnctName);
-- Grant appropriate rights to the emaj_viewer role.
    EXECUTE format('GRANT SELECT ON TABLE %s TO emaj_viewer',
                   v_logTableName);
    EXECUTE format('GRANT SELECT ON SEQUENCE %s TO emaj_viewer',
                   v_sequenceName);
-- Build the PK columns names array and some pieces of SQL statements that will be needed at table rollback and gen_sql times.
-- They are left NULL if the table has no pkey.
    SELECT * FROM emaj._build_sql_tbl(v_fullTableName)
      INTO v_pkCols, v_genExprCols, v_rlbkColList, v_genColList, v_genValList, v_genSetList, v_genPkConditions, v_nbGenAlwaysIdentCol;
-- Register the table into emaj_relation.
    INSERT INTO emaj.emaj_relation
               (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_priority,
                rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind, rel_log_table,
                rel_log_index, rel_log_sequence, rel_log_function, rel_ignored_triggers, rel_pk_cols, rel_gen_expr_cols,
                rel_emaj_verb_attnum, rel_has_always_ident_col, rel_sql_rlbk_columns,
                rel_sql_gen_ins_col, rel_sql_gen_ins_val, rel_sql_gen_upd_set, rel_sql_gen_pk_conditions)
        VALUES (p_schema, p_tbl, int8range(p_timeId, NULL, '[)'), p_groupName, p_priority,
                v_logSchema, p_logDatTsp, p_logIdxTsp, 'r', v_baseLogTableName,
                v_baseLogIdxName, v_baseSequenceName, v_baseLogFnctName, p_ignoredTriggers, v_pkCols, v_genExprCols,
                v_attnum, v_nbGenAlwaysIdentCol > 0, v_rlbkColList,
                v_genColList, v_genValList, v_genSetList, v_genPkConditions);
-- Check if the table has application (neither internal - ie. created for fk - nor previously created by emaj) triggers not already
-- declared as 'to be ignored at rollback time'.
    SELECT string_agg(tgname, ', ' ORDER BY tgname) INTO v_triggerList
      FROM
        (SELECT tgname
           FROM pg_catalog.pg_trigger
           WHERE tgrelid = v_fullTableName::regclass
             AND tgconstraint = 0
             AND tgname NOT LIKE E'emaj\\_%\\_trg'
             AND NOT tgname = ANY(coalesce(p_ignoredTriggers, '{}'))
        ) AS t;
-- If yes, issue a warning.
-- If a trigger updates another table in the same table group or outside, it could generate problem at rollback time.
    IF v_triggerList IS NOT NULL THEN
      RAISE WARNING '_create_tbl: The table "%" has triggers that will be automatically disabled during E-Maj rollback operations (%).'
                    ' Use the emaj_modify_table() function to change this behaviour.', v_fullTableName, v_triggerList;
    END IF;
--
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._build_sql_tbl(p_fullTableName TEXT, OUT p_pkCols TEXT[], OUT p_genExprCols TEXT[],
                                               OUT p_rlbkColList TEXT, OUT p_genColList TEXT, OUT p_genValList TEXT,
                                               OUT p_genSetList TEXT, OUT p_genPkConditions TEXT, OUT p_nbGenAlwaysIdentCol INT)
LANGUAGE plpgsql AS
$_build_sql_tbl$
-- This function builds, for one application table:
--   - the PK columns names array
--   - all pieces of SQL that will be recorded into the emaj_relation table.
-- They will later be used at rollback or SQL script generation time.
-- All SQL pieces are left NULL or empty if the table has no pkey, neither rollback nor sql script generation operations being possible
--   in this case.
-- The Insert columns list remains empty if it is not needed to have a specific list (i.e. when the application table does not contain
--   any generated column).
-- Input: the full application table name
-- Output: PK columns names array, 5 pieces of SQL, and the number of columns declared GENERATED ALWAYS AS IDENTITY
  DECLARE
    v_unquotedType           CONSTANT TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                                     'int2','int4','int8','serial','bigserial',
                                                     'real','double precision','float','float4','float8','oid'];
    r_col                    RECORD;
  BEGIN
-- Build the pkey columns array and the "equality on the primary key" conditions for the UPDATE and DELETE statements of the
--   sql generation function.
    SELECT array_agg(attname),
           string_agg(
             CASE WHEN format_type = ANY(v_unquotedType) THEN
               quote_ident(replace(attname,'''','''''')) || ' = '' || o.' || quote_ident(attname) || ' || '''
                  ELSE
               quote_ident(replace(attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(attname) || ') || '''
             END, ' AND ')
      INTO p_pkCols, p_genPkConditions
      FROM
        (SELECT attname, regexp_replace(format_type(atttypid,atttypmod),E'\\(.*$','') AS format_type
           FROM pg_catalog.pg_attribute
                JOIN  pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
           WHERE attnum = ANY (indkey)
             AND indrelid = p_fullTableName::regclass
             AND indisprimary
             AND attnum > 0
             AND attisdropped = FALSE
           ORDER BY attnum
        ) AS t;
-- Build the generated columns array.
    SELECT array_agg(attname ORDER BY attnum)
      INTO p_genExprCols
      FROM pg_catalog.pg_attribute
      WHERE attrelid = p_fullTableName::regclass
        AND attnum > 0
        AND attisdropped = FALSE
        AND attgenerated <> '';
-- Retrieve from pg_attribute simple columns list and indicators.
-- If the table has no pkey, keep all the sql pieces to NULL (rollback or sql script generation operations being impossible).
    IF p_pkCols IS NOT NULL THEN
      SELECT string_agg('tbl.' || quote_ident(attname), ',') FILTER (WHERE attgenerated = ''),
--               the columns list for rollback, excluding the GENERATED ALWAYS AS (expression) columns
             string_agg(quote_ident(attname), ', ') FILTER (WHERE attgenerated = ''),
--               the INSERT columns list for sql generation, excluding the GENERATED ALWAYS AS (expression) columns
             count(*) FILTER (WHERE attidentity = 'a')
--               the number of GENERATED ALWAYS AS IDENTITY columns
        INTO p_rlbkColList, p_genColList, p_nbGenAlwaysIdentCol
        FROM (
          SELECT attname, attidentity, attgenerated
            FROM pg_catalog.pg_attribute
            WHERE attrelid = p_fullTableName::regclass
              AND attnum > 0 AND NOT attisdropped
          ORDER BY attnum) AS t;
      IF p_genExprCols IS NULL THEN
-- If the table doesn't contain any generated as expression columns, there is no need for the columns list in the INSERT clause.
        p_genColList = '';
      END IF;
-- Retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements.
-- The logic is too complex to be build with aggregate functions. So loop on all columns.
      p_genValList = '';
      p_genSetList = '';
      FOR r_col IN EXECUTE format(
        ' SELECT attname, format_type(atttypid,atttypmod) AS format_type, attidentity, attgenerated'
        ' FROM pg_catalog.pg_attribute'
        ' WHERE attrelid = %s::regclass'
        '   AND attnum > 0 AND NOT attisdropped'
        ' ORDER BY attnum',
        quote_literal(p_fullTableName))
      LOOP
-- Test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric
-- data types)
        IF regexp_replace(r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- Literal for this column can remain as is.
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            p_genValList = p_genValList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::TEXT,''NULL'') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            p_genSetList = p_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.'
                                        || quote_ident(r_col.attname) || ' ::TEXT,''NULL'') || '', ';
          END IF;
        ELSE
-- Literal for this column must be quoted.
          IF r_col.attgenerated = '' THEN                                     -- GENERATED ALWAYS AS (expression) columns are not inserted
            p_genValList = p_genValList || ''' || quote_nullable(o.' || quote_ident(r_col.attname) || ') || '', ';
          END IF;
          IF r_col.attidentity <> 'a' AND r_col.attgenerated = '' THEN        -- GENERATED ALWAYS columns are not updated
            p_genSetList = p_genSetList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_nullable(n.'
                                        || quote_ident(r_col.attname) || ') || '', ';
          END IF;
        END IF;
      END LOOP;
-- Suppress the final separators.
      p_genValList = substring(p_genValList FROM 1 FOR char_length(p_genValList) - 2);
      p_genSetList = substring(p_genSetList FROM 1 FOR char_length(p_genSetList) - 2);
    END IF;
--
    RETURN;
  END;
$_build_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._add_tbl(p_schema TEXT, p_table TEXT, p_group TEXT, p_priority INT, p_logDatTsp TEXT, p_logIdxTsp TEXT,
                                         p_ignoredTriggers TEXT[], p_groupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_add_tbl$
-- The function adds a table to a group. It is called during an alter group or a dynamic assignment operation.
-- If the group is in idle state, it simply calls the _create_tbl() function.
-- Otherwise, it calls the _create_tbl() function, activates the log trigger and
--    sets a restart value for the log sequence if a previous range exists for the relation.
-- Required inputs: the schema and table to add
--                  the group name
--                  the table properties: priority, log data and index tablespace, triggers to ignore at rollback time
--                  the group's logging state
--                  the time stamp id of the operation
--                  the main calling function
  DECLARE
    v_logSchema              TEXT;
    v_logSequence            TEXT;
    v_nextVal                BIGINT;
  BEGIN
-- Create the table.
    PERFORM emaj._create_tbl(p_schema, p_table, p_group, p_priority, p_logDatTsp, p_logIdxTsp, p_ignoredTriggers,
                             p_timeId, p_groupIsLogging);
-- If the group is in logging state, perform additional tasks,
    IF p_groupIsLogging THEN
-- ... get the log schema and sequence for the new relation
      SELECT rel_log_schema, rel_log_sequence INTO v_logSchema, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- ... get the last log sequence value, if any, for this relation (recorded in emaj_relation at a previous REMOVE_TBL operation)
      SELECT max(rel_log_seq_last_value) + 1 INTO v_nextVal
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND rel_log_seq_last_value IS NOT NULL;
-- ... set the new log sequence next_val, if needed
      IF v_nextVal IS NOT NULL AND v_nextVal > 1 THEN
        EXECUTE format('ALTER SEQUENCE %I.%I RESTART %s',
                       v_logSchema, v_logSequence, v_nextVal);
      END IF;
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
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_table, 'ADD_TABLE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TABLE ADDED', quote_ident(p_schema) || '.' || quote_ident(p_table),
              'To the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
--
    RETURN;
  END;
$_add_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_priority_tbl(p_schema TEXT, p_table TEXT, p_currentPriority INT, p_newPriority INT,
                                                     p_timeId BIGINT, p_group TEXT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_change_priority_tbl$
-- This function changes the priority for an application table.
-- Input: the table identity, the old and new priorities, the operation time id and the calling function.
  BEGIN
-- Update the emaj_relation row for the table
    UPDATE emaj.emaj_relation
      SET rel_priority = p_newPriority
      FROM emaj.emaj_group
      WHERE rel_group = group_name
        AND rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
-- Insert an entry into the emaj_relation_change table.
      INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group,
                                             rlchg_priority, rlchg_new_priority)
        VALUES (p_timeId, p_schema, p_table, 'CHANGE_PRIORITY', p_group, p_currentPriority, p_newPriority);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'PRIORITY CHANGED',
              quote_ident(p_schema) || '.' || quote_ident(p_table),
              coalesce(p_currentPriority::text, 'NULL') || ' => ' || coalesce(p_newPriority::text, 'NULL'));
--
    RETURN;
  END;
$_change_priority_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_log_data_tsp_tbl(p_schema TEXT, p_table TEXT, p_logSchema TEXT, p_currentLogTable TEXT,
                                                         p_currentLogDatTsp TEXT, p_newLogDatTsp TEXT,
                                                         p_timeId BIGINT, p_group TEXT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_change_log_data_tsp_tbl$
-- This function changes the log data tablespace for an application table.
-- Input: the existing emaj_relation characteristics for the table, the new log data tablespace, the operation time id and the
--        calling function.
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- Build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = p_newLogDatTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- Process the log data tablespace change.
    EXECUTE format('ALTER TABLE %I.%I SET TABLESPACE %I',
                   p_logSchema, p_currentLogTable, v_newTsp);
-- Update the table attributes into emaj_relation.
    UPDATE emaj.emaj_relation
      SET rel_log_dat_tsp = p_newLogDatTsp
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
-- Insert an entry into the emaj_relation_change table.
      INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group,
                                             rlchg_log_data_tsp, rlchg_new_log_data_tsp)
        VALUES (p_timeId, p_schema, p_table, 'CHANGE_LOG_DATA_TABLESPACE', p_group, p_currentLogDatTsp, p_newLogDatTsp);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'LOG DATA TABLESPACE CHANGED',
              quote_ident(p_schema) || '.' || quote_ident(p_table),
              coalesce(p_currentLogDatTsp, 'Default tablespace') || ' => ' || coalesce(p_newLogDatTsp, 'Default tablespace'));
--
    RETURN;
  END;
$_change_log_data_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_log_index_tsp_tbl(p_schema TEXT, p_table TEXT, p_logSchema TEXT, p_currentLogIndex TEXT,
                                                          p_currentLogIdxTsp TEXT, p_newLogIdxTsp TEXT,
                                                          p_timeId BIGINT, p_group TEXT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_change_log_index_tsp_tbl$
-- This function changes the log index tablespace for an application table.
-- Input: the existing emaj_relation characteristics for the table, the new log index tablespace, the operation time id and the
--        calling function.
  DECLARE
    v_newTsp                 TEXT;
  BEGIN
-- Build the new data tablespace name. If needed, get the name of the current default tablespace.
    v_newTsp = p_newLogIdxTsp;
    IF v_newTsp IS NULL OR v_newTsp = '' THEN
      v_newTsp = emaj._get_default_tablespace();
    END IF;
-- Process the log index tablespace change.
    EXECUTE format('ALTER INDEX %I.%I SET TABLESPACE %I',
                   p_logSchema, p_currentLogIndex, v_newTsp);
-- Update the table attributes into emaj_relation.
    UPDATE emaj.emaj_relation
      SET rel_log_idx_tsp = p_newLogIdxTsp
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
-- Insert an entry into the emaj_relation_change table.
      INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group,
                                             rlchg_log_index_tsp, rlchg_new_log_index_tsp)
        VALUES (p_timeId, p_schema, p_table, 'CHANGE_LOG_INDEX_TABLESPACE', p_group, p_currentLogIdxTsp, p_newLogIdxTsp);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'LOG INDEX TABLESPACE CHANGED',
              quote_ident(p_schema) || '.' || quote_ident(p_table),
              coalesce(p_currentLogIdxTsp, 'Default tablespace') || ' => ' || coalesce(p_newLogIdxTsp, 'Default tablespace'));
--
    RETURN;
  END;
$_change_log_index_tsp_tbl$;

CREATE OR REPLACE FUNCTION emaj._change_ignored_triggers_tbl(p_schema TEXT, p_table TEXT, p_currentIgnoredTriggers TEXT[],
                                                             p_newIgnoredTriggers TEXT[], p_timeId BIGINT, p_group TEXT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_change_ignored_triggers_tbl$
-- This function changes the set of application triggers to ignore at rollback time.
-- Input: the table identity, the old and new arrays of triggers to ignore, the operation time id and the calling function.
  BEGIN
-- Update the emaj_relation row for the table.
    UPDATE emaj.emaj_relation
      SET rel_ignored_triggers = p_newIgnoredTriggers
      FROM emaj.emaj_group
      WHERE rel_group = group_name
        AND rel_schema = p_schema
        AND rel_tblseq = p_table
        AND upper_inf(rel_time_range);
-- Insert an entry into the emaj_relation_change table.
      INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group,
                                             rlchg_ignored_triggers, rlchg_new_ignored_triggers)
        VALUES (p_timeId, p_schema, p_table, 'CHANGE_IGNORED_TRIGGERS', p_group, p_currentIgnoredTriggers, p_newIgnoredTriggers);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TRIGGERS TO IGNORE CHANGED',
              quote_ident(p_schema) || '.' || quote_ident(p_table),
              coalesce(p_currentIgnoredTriggers::text, 'none') || ' => ' || coalesce(p_newIgnoredTriggers::text, 'none'));
--
    RETURN;
  END;
$_change_ignored_triggers_tbl$;

CREATE OR REPLACE FUNCTION emaj._remove_tbl(p_schema TEXT, p_table TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                            p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_remove_tbl$
-- The function removes a table from a group. It is called during an alter group or a dynamic removal operation.
-- If the group is in idle state, it simply calls the _drop_tbl() function.
-- Otherwise, only triggers, log function and log sequence are dropped now. The other components will be dropped later (at reset_group
-- time for instance).
-- Required inputs: schema and sequence to remove, related group name and logging state,
--                  time stamp id of the operation, main calling function.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_logSchema              TEXT;
    v_currentLogTable        TEXT;
    v_currentLogIndex        TEXT;
    v_logFunction            TEXT;
    v_logSequence            TEXT;
    v_logSequenceLastValue   BIGINT;
    v_namesSuffix            TEXT;
  BEGIN
    IF NOT p_groupIsLogging THEN
-- If the group is in idle state, drop the table immediately.
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*, p_timeId)
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
    ELSE
-- The group is in logging state.
-- Get the current relation characteristics.
      SELECT rel_log_schema, rel_log_table, rel_log_index, rel_log_function, rel_log_sequence
        INTO v_logSchema, v_currentLogTable, v_currentLogIndex, v_logFunction, v_logSequence
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- Get the current log sequence characteristics.
      SELECT tbl_log_seq_last_val INTO STRICT v_logSequenceLastValue
        FROM emaj.emaj_table
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table
          AND tbl_time_id = p_timeId;
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
-- Drop the log and truncate triggers.
-- (check the application table exists before dropping its triggers to avoid an error fires with postgres version <= 9.3)
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              WHERE nspname = p_schema
                AND relname = p_table
                AND relkind = 'r'
           ) THEN
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                       p_schema, p_table);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                       p_schema, p_table);
      END IF;
-- Drop the log function and the log sequence.
-- (but we keep the sequence related data in the emaj_table and the emaj_seq_hole tables)
      EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                     v_logSchema, v_logFunction);
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     v_logSchema, v_logSequence);
-- Register the end of the relation time frame, the last value of the log sequence, the log table and index names change.
-- Reflect the changes into the emaj_relation rows:
--   - for all timeranges pointing to this log table and index
--     (do not reset the rel_log_sequence value: it will be needed later for _drop_tbl() for the emaj_sequence cleanup)
      UPDATE emaj.emaj_relation
        SET rel_log_table = v_currentLogTable || v_namesSuffix , rel_log_index = v_currentLogIndex || v_namesSuffix,
            rel_log_function = NULL, rel_sql_rlbk_columns = NULL,
            rel_log_seq_last_value = v_logSequenceLastValue
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND rel_log_table = v_currentLogTable;
--   - and close the last timerange.
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range), p_timeId, '[)')
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_table, 'REMOVE_TABLE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'TABLE REMOVED', quote_ident(p_schema) || '.' || quote_ident(p_table),
              'From the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
--
    RETURN;
  END;
$_remove_tbl$;

CREATE OR REPLACE FUNCTION emaj._move_tbl(p_schema TEXT, p_table TEXT, p_oldGroup TEXT, p_oldGroupIsLogging BOOLEAN, p_newGroup TEXT,
                                          p_newGroupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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

CREATE OR REPLACE FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_drop_tbl$
-- The function deletes a timerange for a table. This centralizes the deletion of all what has been created by _create_tbl() function.
-- Required inputs: row from emaj_relation corresponding to the appplication table to proccess, time id.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  BEGIN
-- If the table is currently linked to a group, drop the log trigger, function and sequence.
    IF upper_inf(r_rel.rel_time_range) THEN
-- Check the table exists before dropping its triggers.
      IF EXISTS
           (SELECT 0
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              WHERE nspname = r_rel.rel_schema
                AND relname = r_rel.rel_tblseq
                AND relkind = 'r'
           ) THEN
-- Drop the log and truncate triggers on the application table.
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_log_trg ON %I.%I',
                       r_rel.rel_schema, r_rel.rel_tblseq);
        EXECUTE format('DROP TRIGGER IF EXISTS emaj_trunc_trg ON %I.%I',
                       r_rel.rel_schema, r_rel.rel_tblseq);
      END IF;
-- Drop the log function.
      IF r_rel.rel_log_function IS NOT NULL THEN
        EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE',
                       r_rel.rel_log_schema, r_rel.rel_log_function);
      END IF;
-- Drop the sequence associated to the log table.
      EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I',
                     r_rel.rel_log_schema, r_rel.rel_log_sequence);
    END IF;
-- Drop the log table if it is not referenced on other timeranges (for potentially other groups).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_relation
            WHERE rel_log_schema = r_rel.rel_log_schema
              AND rel_log_table = r_rel.rel_log_table
              AND rel_time_range <> r_rel.rel_time_range
         ) THEN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                     r_rel.rel_log_schema, r_rel.rel_log_table);
    END IF;
-- Process log sequence information if the sequence is not referenced in other timerange (for potentially other groups).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_relation
            WHERE rel_log_schema = r_rel.rel_log_schema
              AND rel_log_sequence = r_rel.rel_log_sequence
              AND rel_time_range <> r_rel.rel_time_range
         ) THEN
-- Delete rows related to the log sequence from emaj_table
-- (it may delete rows for other already processed time_ranges for the same table).
      DELETE FROM emaj.emaj_table
        WHERE tbl_schema = r_rel.rel_schema
          AND tbl_name = r_rel.rel_tblseq;
-- Delete rows related to the table from emaj_seq_hole table
-- (it may delete holes for timeranges that do not belong to the group, if a table has been moved to another group,
--  but is safe enough for rollbacks).
      DELETE FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = r_rel.rel_schema
          AND sqhl_table = r_rel.rel_tblseq;
    END IF;
-- Keep a trace of the table group ownership history and finaly delete the table reference from the emaj_relation table.
    WITH deleted AS (
      DELETE FROM emaj.emaj_relation
        WHERE rel_schema = r_rel.rel_schema
          AND rel_tblseq = r_rel.rel_tblseq
          AND rel_time_range = r_rel.rel_time_range
        RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq,
             CASE WHEN upper_inf(rel_time_range) THEN int8range(lower(rel_time_range), p_timeId, '[)') ELSE rel_time_range END,
             rel_group, rel_kind
        FROM deleted;
--
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj._repair_tbl(p_schema TEXT, p_table TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                            p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_repair_tbl$
-- The function repairs a table detected as corrupted, i.e. with any trouble detected by the emaj_verify_all() and similar functions.
-- Inputs: the schema and table names to repair
--         the group that currently owns the table, and its state
--         the time_id of the operation
--         the calling function name
  BEGIN
    IF p_groupIsLogging THEN
      RAISE EXCEPTION '_repair_tbl: Cannot repair the table %.%. Its group % is in LOGGING state. Remove first the table from its group.',
        p_schema, p_table, p_group;
    ELSE
-- Remove the table from its group.
      PERFORM emaj._drop_tbl(emaj.emaj_relation.*, p_timeId)
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_table
          AND upper_inf(rel_time_range);
-- And recreate it.
      PERFORM emaj._create_tbl(p_schema, p_table, p_group, tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp,
                               tmp_ignored_triggers, p_timeId, p_groupIsLogging)
        FROM tmp_app_table
        WHERE tmp_group = p_group
          AND tmp_schema = p_schema
          AND tmp_tbl_name = p_table;
-- Insert an entry into the emaj_relation_change table.
      INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
        VALUES (p_timeId, p_schema, p_table, 'REPAIR_TABLE', p_group);
-- Insert an entry into the emaj_hist table.
      INSERT INTO emaj.emaj_hist(hist_function, hist_event, hist_object, hist_wording)
        VALUES (p_function, 'TABLE REPAIRED', quote_ident(p_schema) || '.' || quote_ident(p_table), 'In group ' || p_group);
    END IF;
--
    RETURN;
  END;
$_repair_tbl$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequence(p_schema TEXT, p_sequence TEXT, p_group TEXT, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequence$
-- The function assigns a sequence into a tables group.
-- Inputs: schema name, sequence name, assignment group name, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group, ie. 1
  BEGIN
    RETURN emaj._assign_sequences(p_schema, ARRAY[p_sequence], p_group, p_mark , FALSE, FALSE);
  END;
$emaj_assign_sequence$;
COMMENT ON FUNCTION emaj.emaj_assign_sequence(TEXT,TEXT,TEXT,TEXT) IS
$$Assign a sequence into a tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_assign_sequences(p_schema TEXT, p_sequences TEXT[], p_group TEXT, p_mark TEXT DEFAULT 'ASSIGN_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_assign_sequences$
-- The function assigns several sequences at once into a tables group.
-- Inputs: schema, array of sequence names, assignment group name,
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively assigned to the tables group
  BEGIN
    RETURN emaj._assign_sequences(p_schema, p_sequences, p_group, p_mark, TRUE, FALSE);
  END;
$emaj_assign_sequences$;
COMMENT ON FUNCTION emaj.emaj_assign_sequences(TEXT,TEXT[],TEXT,TEXT) IS
$$Assign several sequences into a tables group.$$;

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
-- Process empty filters as NULL.
    SELECT CASE WHEN p_sequencesIncludeFilter = '' THEN NULL ELSE p_sequencesIncludeFilter END,
           CASE WHEN p_sequencesExcludeFilter = '' THEN NULL ELSE p_sequencesExcludeFilter END
      INTO p_sequencesIncludeFilter, p_sequencesExcludeFilter;
-- Build the list of sequences names satisfying the pattern.
    SELECT array_agg(relname) INTO v_sequences
      FROM
        (SELECT relname
           FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
           WHERE nspname = p_schema
             AND relname ~ p_sequencesIncludeFilter
             AND (p_sequencesExcludeFilter IS NULL OR relname !~ p_sequencesExcludeFilter)
             AND relkind = 'S'
           ORDER BY relname
        ) AS t;
-- OK, call the _assign_sequences() function for execution.
    RETURN emaj._assign_sequences(p_schema, v_sequences, p_group, p_mark, TRUE, TRUE);
  END;
$emaj_assign_sequences$;
COMMENT ON FUNCTION emaj.emaj_assign_sequences(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Assign sequences on name patterns into a tables group.$$;

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
        PERFORM emaj._set_mark_groups(ARRAY[p_group], v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_remove_sequence(p_schema TEXT, p_sequence TEXT, p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_sequence$
-- The function removes a sequence from its tables group.
-- Inputs: schema name, sequence name, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively removed to the tables group, id. 1
  BEGIN
    RETURN emaj._remove_sequences(p_schema, ARRAY[p_sequence], p_mark, FALSE, FALSE);
  END;
$emaj_remove_sequence$;
COMMENT ON FUNCTION emaj.emaj_remove_sequence(TEXT,TEXT,TEXT) IS
$$Remove a sequence from its tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_sequences(p_schema TEXT, p_sequences TEXT[], p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_sequences$
-- The function removes several sequences at once from their tables group.
-- Inputs: schema, array of sequence names, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively removed from the tables group
  BEGIN
    RETURN emaj._remove_sequences(p_schema, p_sequences, p_mark, TRUE, FALSE);
  END;
$emaj_remove_sequences$;
COMMENT ON FUNCTION emaj.emaj_remove_sequences(TEXT,TEXT[],TEXT) IS
$$Remove several sequences from their tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_remove_sequences(p_schema TEXT, p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                      p_mark TEXT DEFAULT 'REMOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_remove_sequences$
-- The function removes sequences on name patterns from their tables group.
-- Inputs: schema, 2 patterns to filter sequence names (one to include and another to exclude),
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively removed from the tables group
  DECLARE
    v_sequences              TEXT[];
  BEGIN
    v_sequences = emaj._build_tblseqs_array_from_regexp(p_schema, 'S', p_sequencesIncludeFilter, p_sequencesExcludeFilter, FALSE);
-- Call the _remove_sequences() function for execution.
    RETURN emaj._remove_sequences(p_schema, v_sequences, p_mark, TRUE, TRUE);
  END;
$emaj_remove_sequences$;
COMMENT ON FUNCTION emaj.emaj_remove_sequences(TEXT,TEXT,TEXT,TEXT) IS
$$Remove several sequences on name patterns from their tables group.$$;

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
      p_sequences = emaj._check_tblseqs_array(p_schema, p_sequences, 'S', FALSE);
    END IF;
-- Get and lock the tables groups and logging groups holding these sequences.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_sequences, NULL);
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
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, FALSE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_move_sequence(p_schema TEXT, p_sequence TEXT, p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_sequence$
-- The function moves a sequence from its tables group to another tables group.
-- Inputs: schema name, sequence name, new group name, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively moved to the new tables group, ie. 1
  BEGIN
    RETURN emaj._move_sequences(p_schema, ARRAY[p_sequence], p_newGroup, p_mark, FALSE, FALSE);
  END;
$emaj_move_sequence$;
COMMENT ON FUNCTION emaj.emaj_move_sequence(TEXT,TEXT,TEXT,TEXT) IS
$$Move a sequence from its tables group to another tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_move_sequences(p_schema TEXT, p_sequences TEXT[], p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_sequences$
-- The function moves several sequences at once from their tables group to another tables group.
-- Inputs: schema, array of sequence names, new group name, mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively moved to the new tables group
  BEGIN
    RETURN emaj._move_sequences(p_schema, p_sequences, p_newGroup, p_mark, TRUE, FALSE);
  END;
$emaj_move_sequences$;
COMMENT ON FUNCTION emaj.emaj_move_sequences(TEXT,TEXT[],TEXT,TEXT) IS
$$Move several sequences from their tables group to another tables group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_move_sequences(p_schema TEXT, p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                 p_newGroup TEXT, p_mark TEXT DEFAULT 'MOVE_%')
RETURNS INTEGER LANGUAGE plpgsql AS
$emaj_move_sequences$
-- The function moves sequences on name patterns from their tables group to another tables group.
-- Inputs: schema, 2 patterns to filter sequence names (one to include and another to exclude), new group name,
--         mark name to set when logging groups (optional)
-- Outputs: number of sequences effectively moved to the new tables group
  DECLARE
    v_sequences              TEXT[];
  BEGIN
    v_sequences = emaj._build_tblseqs_array_from_regexp(p_schema, 'S', p_sequencesIncludeFilter, p_sequencesExcludeFilter, TRUE);
-- Call the _move_sequences() function for execution.
    RETURN emaj._move_sequences(p_schema, v_sequences, p_newGroup, p_mark, TRUE, TRUE);
  END;
$emaj_move_sequences$;
COMMENT ON FUNCTION emaj.emaj_move_sequences(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Move several sequences on name patterns from their tables group to another tables group.$$;

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
      p_sequences = emaj._check_tblseqs_array(p_schema, p_sequences, 'S', TRUE);
    END IF;
-- Remove sequences that already belong to the new group.
    SELECT array_agg(rel_tblseq ORDER BY rel_tblseq) FILTER (WHERE rel_group <> p_newGroup),
           string_agg(quote_ident(rel_tblseq), ', ' ORDER BY rel_tblseq) FILTER (WHERE rel_group = p_newGroup)
      INTO p_sequences, v_list
      FROM emaj.emaj_relation
      WHERE rel_schema = p_schema
        AND rel_tblseq = ANY(p_sequences)
        AND upper_inf(rel_time_range);
-- Warn only if the sequences list has been supplied by the user.
    IF v_list IS NOT NULL AND NOT p_arrayFromRegex THEN
      RAISE WARNING '_move_sequences: In schema "%", some sequences (%) already belong to the tables group "%".',
                    p_schema, v_list, p_newGroup;
    END IF;
-- Get and lock the tables groups and logging groups holding these sequences, and count the number of AUDIT_ONLY groups.
    SELECT p_groups, p_loggingGroups INTO v_groups, v_loggingGroups
      FROM emaj._get_lock_tblseqs_groups(p_schema, p_sequences, p_newGroup);
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
        PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj._create_seq(p_schema TEXT, p_seq TEXT, p_groupName TEXT, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_create_seq$
-- The function records a sequence into a tables group
-- Required inputs: the application sequence to process, the group to add it into, the priority attribute, the time id of the operation.
  BEGIN
-- Record the sequence in the emaj_relation table.
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind)
      VALUES (p_schema, p_seq, int8range(p_timeId, NULL, '[)'), p_groupName, 'S');
    RETURN;
  END;
$_create_seq$;

CREATE OR REPLACE FUNCTION emaj._add_seq(p_schema TEXT, p_sequence TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                         p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_add_seq$
-- The function adds a sequence to a group. It is called during an alter group or a dynamic assignment operation.
-- If the group is in idle state, it simply calls the _create_seq() function.
-- Otherwise, it calls the _create_seql() function, and records the current state of the sequence.
-- Required inputs: schema and sequence to add, group name, priority, the group's logging state,
--                  the time stamp id of the operation, main calling function.
-- The function is defined as SECURITY DEFINER so that emaj_adm roles can use it even without SELECT right on the sequence.
  BEGIN
-- Create the sequence.
    PERFORM emaj._create_seq(p_schema, p_sequence, p_group, p_timeId);
-- If the group is in logging state, perform additional tasks.
    IF p_groupIsLogging THEN
-- Record the new sequence state in the emaj_sequence table for the current alter_group mark.
      EXECUTE format('INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,'
                     '            sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)'
                     '  SELECT nspname, relname, %s, sq.last_value, seqstart,'
                     '         seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '    FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '    WHERE nspname = %L AND relname = %L',
                     p_timeId, p_schema, p_sequence, p_schema, p_sequence);
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_sequence, 'ADD_SEQUENCE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'SEQUENCE ADDED', quote_ident(p_schema) || '.' || quote_ident(p_sequence),
              'To the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
--
    RETURN;
  END;
$_add_seq$;

CREATE OR REPLACE FUNCTION emaj._remove_seq(p_schema TEXT, p_sequence TEXT, p_group TEXT, p_groupIsLogging BOOLEAN,
                                            p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_remove_seq$
-- The function removes a sequence from a group. It is called during an alter group or a dynamic removal operation.
-- Required inputs: schema and sequence to remove, related group name and logging state,
--                  time stamp id of the operation, main calling function.
  BEGIN
    IF NOT p_groupIsLogging THEN
-- If the group is in idle state, drop the sequence immediately.
      PERFORM emaj._drop_seq(emaj.emaj_relation.*, p_timeId)
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND upper_inf(rel_time_range);
    ELSE
-- If the group is in logging state, just register the end of the relation time frame.
      UPDATE emaj.emaj_relation
        SET rel_time_range = int8range(lower(rel_time_range),p_timeId, '[)')
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND upper_inf(rel_time_range);
    END IF;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group)
      VALUES (p_timeId, p_schema, p_sequence, 'REMOVE_SEQUENCE', p_group);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'SEQUENCE REMOVED', quote_ident(p_schema) || '.' || quote_ident(p_sequence),
              'From the ' || CASE WHEN p_groupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_group);
    RETURN;
  END;
$_remove_seq$;

CREATE OR REPLACE FUNCTION emaj._move_seq(p_schema TEXT, p_sequence TEXT, p_oldGroup TEXT, p_oldGroupIsLogging BOOLEAN, p_newGroup TEXT,
                                          p_newGroupIsLogging BOOLEAN, p_timeId BIGINT, p_function TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_move_seq$
-- The function change the group ownership of a sequence. It is called during an alter group or a dynamic assignment operation.
-- Required inputs: schema and sequence to move, old and new group names and their logging state,
--                  time stamp id of the operation, main calling function.
  BEGIN
-- Register the end of the previous relation time frame and create a new relation time frame with the new group.
    UPDATE emaj.emaj_relation
      SET rel_time_range = int8range(lower(rel_time_range),p_timeId,'[)')
      WHERE rel_schema = p_schema
        AND rel_tblseq = p_sequence
        AND upper_inf(rel_time_range);
    INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind)
      SELECT rel_schema, rel_tblseq, int8range(p_timeId, NULL, '[)'), p_newGroup, rel_kind
        FROM emaj.emaj_relation
        WHERE rel_schema = p_schema
          AND rel_tblseq = p_sequence
          AND upper(rel_time_range) = p_timeId;
-- Insert an entry into the emaj_relation_change table.
    INSERT INTO emaj.emaj_relation_change (rlchg_time_id, rlchg_schema, rlchg_tblseq, rlchg_change_kind, rlchg_group, rlchg_new_group)
      VALUES (p_timeId, p_schema, p_sequence, 'MOVE_SEQUENCE', p_oldGroup, p_newGroup);
-- Insert an entry into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (p_function, 'SEQUENCE MOVED', quote_ident(p_schema) || '.' || quote_ident(p_sequence),
              'From the ' || CASE WHEN p_oldGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_oldGroup ||
              ' to the ' || CASE WHEN p_newGroupIsLogging THEN 'logging ' ELSE 'idle ' END || 'group ' || p_newGroup);
--
    RETURN;
  END;
$_move_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence timerange.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess.
  BEGIN
-- Delete rows from emaj_sequence, but only when dealing with the last timerange of the sequence.
    IF NOT EXISTS
        (SELECT 0
           FROM emaj.emaj_relation
           WHERE rel_schema = r_rel.rel_schema
             AND rel_tblseq = r_rel.rel_tblseq
             AND rel_time_range <> r_rel.rel_time_range
        ) THEN
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = r_rel.rel_schema
          AND sequ_name = r_rel.rel_tblseq;
    END IF;
-- Keep a trace of the sequence group ownership history and finaly delete the sequence timerange from the emaj_relation table.
    WITH deleted AS
      (DELETE FROM emaj.emaj_relation
         WHERE rel_schema = r_rel.rel_schema
           AND rel_tblseq = r_rel.rel_tblseq
           AND rel_time_range = r_rel.rel_time_range
         RETURNING rel_schema, rel_tblseq, rel_time_range, rel_group, rel_kind
      )
    INSERT INTO emaj.emaj_rel_hist
             (relh_schema, relh_tblseq, relh_time_range, relh_group, relh_kind)
      SELECT rel_schema, rel_tblseq,
             CASE WHEN upper_inf(rel_time_range) THEN int8range(lower(rel_time_range), p_timeId, '[)') ELSE rel_time_range END,
             rel_group, rel_kind
        FROM deleted;
--
    RETURN;
  END;
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, p_minGlobalSeq BIGINT, p_maxGlobalSeq BIGINT, p_nbSession INT,
                                          p_isLoggedRlbk BOOLEAN, p_isReplRoleReplica BOOLEAN)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence.
-- The function is called by emaj._rlbk_session_exec().
-- Input: row from emaj_relation corresponding to the appplication table to proccess
--        global sequence (non inclusive) lower and (inclusive) upper limits covering the rollback time frame
--        number of sessions
--        a boolean indicating whether the rollback is logged
--        a boolean indicating whether the rollback is to be performed with a session_replication_role set to replica
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_pkColsList             TEXT;
    v_pkCondition            TEXT;
    v_nbPk                   BIGINT;
  BEGIN
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName,
              'All log rows where emaj_gid > ' || p_minGlobalSeq || ' and <= ' || p_maxGlobalSeq
              || CASE WHEN p_isReplRoleReplica THEN ', in session_replication_role = replica mode' ELSE '' END);
-- Build pieces of code
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
    SELECT string_agg(quote_ident(attname), ','),
           string_agg('tbl.' || quote_ident(attname) || ' = keys.' || quote_ident(attname), ' AND ')
      INTO v_pkColsList, v_pkCondition
      FROM unnest(r_rel.rel_pk_cols) AS attname;
-- Set the session_replication_role if needed.
    IF p_isReplRoleReplica THEN
      SET session_replication_role = 'replica';
    END IF;
-- Create the temporary table containing all primary key values with their earliest emaj_gid.
    IF p_nbSession = 1 THEN
      v_tableType = 'TEMP';
      v_tmpTable = 'emaj_tmp_' || pg_backend_pid();
    ELSE
-- With multi session parallel rollbacks, the table cannot be a TEMP table because it would not be usable in 2PC
-- but it may be an UNLOGGED table.
      v_tableType = 'UNLOGGED';
      v_tmpTable = 'emaj.emaj_tmp_' || pg_backend_pid();
    END IF;
    EXECUTE format('CREATE %s TABLE %s AS '
                   '  SELECT %s, min(emaj_gid) as emaj_gid FROM %s'
                   '    WHERE emaj_gid > %s AND emaj_gid <= %s'
                   '    GROUP BY %s',
                   v_tableType, v_tmpTable, v_pkColsList, v_logTableName,
                   p_minGlobalSeq, p_maxGlobalSeq, v_pkColsList);
    GET DIAGNOSTICS v_nbPk = ROW_COUNT;
-- Delete all rows from the application table corresponding to each touched primary key.
-- This deletes rows inserted or updated during the rolled back period.
    EXECUTE format('DELETE FROM ONLY %s tbl USING %s keys WHERE %s',
                   v_fullTableName, v_tmpTable, v_pkCondition);
-- For logged rollbacks, if the number of pkey to process is greater than 1.000, ANALYZE the log table to take into account
--   the impact of just inserted rows, avoiding a potentialy bad plan for the next INSERT statement.
    IF p_isLoggedRlbk AND v_nbPk > 1000 THEN
      EXECUTE format('ANALYZE %s',
                     v_logTableName);
    END IF;
-- Insert into the application table rows that were deleted or updated during the rolled back period.
    EXECUTE format('INSERT INTO %s (%s) %s'
                   '  SELECT %s FROM %s tbl, %s keys '
                   '    WHERE %s AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD'''
                   '      AND tbl.emaj_gid > %s AND tbl.emaj_gid <= %s',
                   v_fullTableName, replace(r_rel.rel_sql_rlbk_columns, 'tbl.',''),
                   CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
                   r_rel.rel_sql_rlbk_columns, v_logTableName, v_tmpTable,
                   v_pkCondition,
                   p_minGlobalSeq, p_maxGlobalSeq);
-- Drop the now useless temporary table.
    EXECUTE format('DROP TABLE %s',
                   v_tmpTable);
-- Reset the session_replication_role if changed at the beginning of the function.
    IF p_isReplRoleReplica THEN
      RESET session_replication_role;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
--
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

CREATE OR REPLACE FUNCTION emaj._delete_log_tbl(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT, p_lastGlobalSeq BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_delete_log_tbl$
-- This function deletes the part of a log table corresponding to updates that have been rolled back.
-- The function is only called by emaj._rlbk_session_exec(), for unlogged rollbacks.
-- It deletes sequences records corresponding to marks that are not visible anymore after the rollback.
-- It also registers the hole in sequence numbers generated by the deleted log rows.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        begin and end time stamp ids to define the time range identifying the hole to create in the log sequence
--        global sequence value limit for rollback
-- Output: deleted rows
  DECLARE
    v_nbRows                 BIGINT;
  BEGIN
-- Delete obsolete log rows
    EXECUTE format('DELETE FROM %I.%I WHERE emaj_gid > %s',
                   r_rel.rel_log_schema, r_rel.rel_log_table, p_lastGlobalSeq);
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- Record the sequence holes generated by the delete operation.
-- This is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group() function
--   (and indirectly by emaj_estimate_rollback_group() and emaj_estimate_rollback_groups()).
-- First delete, if exist, sequence holes that have disappeared with the rollback.
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = r_rel.rel_schema
        AND sqhl_table = r_rel.rel_tblseq
        AND sqhl_begin_time_id >= p_beginTimeId
        AND sqhl_begin_time_id < p_endTimeId;
-- Then insert the new log sequence hole.
    INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)
      SELECT r_rel.rel_schema, r_rel.rel_tblseq, p_beginTimeId, p_endTimeId,
             emaj._get_log_sequence_last_value(r_rel.rel_log_schema, r_rel.rel_log_sequence) - tbl_log_seq_last_val
        FROM emaj.emaj_table
        WHERE tbl_schema = r_rel.rel_schema AND tbl_name = r_rel.rel_tblseq AND tbl_time_id = p_beginTimeId;
--
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, p_timeId BIGINT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark.
-- Input: the emaj_relation row related to the application sequence to process, time id of the mark to rollback to.
-- Ouput: 0 if no change have to be applied, otherwise 1.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it is not the owner of the application sequence.
  DECLARE
    v_stmt                   TEXT;
    v_fullSeqName            TEXT;
    mark_seq_rec             emaj.emaj_sequence%ROWTYPE;
    curr_seq_rec             emaj.emaj_sequence%ROWTYPE;
  BEGIN
-- Read sequence's characteristics at mark time.
    SELECT *
      INTO mark_seq_rec
      FROM emaj.emaj_sequence
      WHERE sequ_schema = r_rel.rel_schema
        AND sequ_name = r_rel.rel_tblseq
        AND sequ_time_id = p_timeId;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_rlbk_seq: No mark at time id "%" can be found for the sequence "%.%".',
        p_timeId, r_rel.rel_schema, r_rel.rel_tblseq;
    END IF;
-- Read the current sequence's characteristics.
    EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                   '  FROM %I.%I sq,'
                   '       pg_catalog.pg_sequence s'
                   '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                   '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                   '  WHERE nspname = %L AND relname = %L',
                   r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
      INTO STRICT curr_seq_rec;
-- Build the ALTER SEQUENCE statement, depending on the differences between the current sequence state and its characteristics
-- at the requested mark time.
    SELECT emaj._build_alter_seq(curr_seq_rec, mark_seq_rec) INTO v_stmt;
-- If there is no change to apply, return with 0.
    IF v_stmt = '' THEN
      RETURN 0;
    END IF;
-- Otherwise, execute the statement, report the event into the history and return 1.
    v_fullSeqName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    EXECUTE format('ALTER SEQUENCE %s %s',
                   v_fullSeqName, v_stmt);
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
--
    RETURN 1;
  END;
$_rlbk_seq$;

CREATE OR REPLACE FUNCTION emaj._build_alter_seq(ref_seq_rec emaj.emaj_sequence, trg_seq_rec emaj.emaj_sequence)
RETURNS TEXT LANGUAGE plpgsql AS
$_build_alter_seq$
-- This function builds an ALTER SEQUENCE clause including only the sequence characteristics that have changed between a reference
-- and a target.
-- The function is called by _rlbk_seq() and _gen_sql_groups().
-- Input: 2 emaj_sequence records representing the reference and the target sequence characteristics
-- Output: the alter sequence clause with all modified characteristics
  DECLARE
    v_stmt                   TEXT;
  BEGIN
    v_stmt = '';
-- Build the ALTER SEQUENCE clause, depending on the differences between the reference and target values.
    IF ref_seq_rec.sequ_last_val <> trg_seq_rec.sequ_last_val OR
       ref_seq_rec.sequ_is_called <> trg_seq_rec.sequ_is_called THEN
      IF trg_seq_rec.sequ_is_called THEN
        v_stmt = v_stmt || ' RESTART ' || trg_seq_rec.sequ_last_val + trg_seq_rec.sequ_increment;
      ELSE
        v_stmt = v_stmt || ' RESTART ' || trg_seq_rec.sequ_last_val;
      END IF;
    END IF;
    IF ref_seq_rec.sequ_start_val <> trg_seq_rec.sequ_start_val THEN
      v_stmt = v_stmt || ' START ' || trg_seq_rec.sequ_start_val;
    END IF;
    IF ref_seq_rec.sequ_increment <> trg_seq_rec.sequ_increment THEN
      v_stmt = v_stmt || ' INCREMENT ' || trg_seq_rec.sequ_increment;
    END IF;
    IF ref_seq_rec.sequ_min_val <> trg_seq_rec.sequ_min_val THEN
      v_stmt = v_stmt || ' MINVALUE ' || trg_seq_rec.sequ_min_val;
    END IF;
    IF ref_seq_rec.sequ_max_val <> trg_seq_rec.sequ_max_val THEN
      v_stmt = v_stmt || ' MAXVALUE ' || trg_seq_rec.sequ_max_val;
    END IF;
    IF ref_seq_rec.sequ_cache_val <> trg_seq_rec.sequ_cache_val THEN
      v_stmt = v_stmt || ' CACHE ' || trg_seq_rec.sequ_cache_val;
    END IF;
    IF ref_seq_rec.sequ_is_cycled <> trg_seq_rec.sequ_is_cycled THEN
      IF trg_seq_rec.sequ_is_cycled = 'f' THEN
        v_stmt = v_stmt || ' NO';
      END IF;
      v_stmt = v_stmt || ' CYCLE ';
    END IF;
--
    RETURN v_stmt;
  END;
$_build_alter_seq$;

CREATE OR REPLACE FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_log_stat_tbl$
-- This function returns the number of log rows for a single table between 2 time stamps or between a time stamp and the current state.
-- It is called by various functions, when building log statistics, but also when setting or deleting a mark, rollbacking a group
--   or dumping changes.
-- These statistics are computed using the log sequence associated to each application table and holes in sequences recorded into
--   emaj_seq_hole.
-- Input: emaj_relation row corresponding to the appplication table to proccess, the time stamp ids defining the time range to examine
--        (a end time stamp id set to NULL indicates the current state)
-- Output: number of log rows between both marks for the table
  BEGIN
    IF p_endTimeId IS NULL THEN
-- Compute log rows between a mark and the current state.
      RETURN
           -- the current last value of the log sequence
          (SELECT emaj._get_log_sequence_last_value(r_rel.rel_log_schema, r_rel.rel_log_sequence))
           -- the log sequence last value at begin time id
        - (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_beginTimeId)
           -- sum of hole from the begin time to now
        - (SELECT coalesce(sum(sqhl_hole_size),0)
             FROM emaj.emaj_seq_hole
             WHERE sqhl_schema = r_rel.rel_schema
               AND sqhl_table = r_rel.rel_tblseq
               AND sqhl_begin_time_id >= p_beginTimeId);
    ELSE
-- Compute log rows between 2 marks.
      RETURN
           -- the log sequence last value at end time id
          (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_endTimeId)
           -- the log sequence last value at begin time id
        - (SELECT tbl_log_seq_last_val
             FROM emaj.emaj_table
             WHERE tbl_schema = r_rel.rel_schema
               AND tbl_name = r_rel.rel_tblseq
               AND tbl_time_id = p_beginTimeId)
           -- sum of hole between begin time and end time
        - (SELECT coalesce(sum(sqhl_hole_size),0)
             FROM emaj.emaj_seq_hole
             WHERE sqhl_schema = r_rel.rel_schema
               AND sqhl_table = r_rel.rel_tblseq
               AND sqhl_begin_time_id >= p_beginTimeId
               AND sqhl_end_time_id <= p_endTimeId);
    END IF;
  END;
$_log_stat_tbl$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_gen_sql_tbl$
-- This function generates elementary SQL statements representing all updates performed on a table between 2 marks
-- or beetween a mark and the current state.
-- These statements are stored into a temporary table created by the _gen_sql_groups() calling function.
-- Input: row from emaj_relation corresponding to the appplication table to proccess,
--        the global sequence value at requested start and end marks
-- Output: number of generated SQL statements
  DECLARE
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_rqInsert               TEXT;
    v_rqUpdate               TEXT;
    v_rqDelete               TEXT;
    v_rqTruncate             TEXT;
    v_conditions             TEXT;
    v_lastEmajGidRel         BIGINT;
    v_nbSQL                  BIGINT;
  BEGIN
-- Build schema specified table name and log table name.
    v_fullTableName = quote_ident(r_rel.rel_schema) || '.' || quote_ident(r_rel.rel_tblseq);
    v_logTableName = quote_ident(r_rel.rel_log_schema) || '.' || quote_ident(r_rel.rel_log_table);
-- Prepare sql skeletons for each statement type, using the pieces of sql recorded in the emaj_relation row at table assignment time.
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''')
              || CASE WHEN r_rel.rel_sql_gen_ins_col <> '' THEN ' (' || r_rel.rel_sql_gen_ins_col || ')' ELSE '' END
              || CASE WHEN r_rel.rel_has_always_ident_col THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END
              || ' VALUES (' || r_rel.rel_sql_gen_ins_val || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''')
              || ' SET ' || r_rel.rel_sql_gen_upd_set || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''')
              || ' WHERE ' || r_rel.rel_sql_gen_pk_conditions || ';''';
    v_rqTruncate = '''TRUNCATE ONLY ' || replace(v_fullTableName,'''','''''') || ' CASCADE;''';
-- Build the restriction conditions on emaj_gid, depending on supplied marks range and the relation time range upper bound.
    v_conditions = 'o.emaj_gid > ' || p_firstEmajGid;
--   Get the EmajGid of the relation time range upper bound, if any.
    IF NOT upper_inf(r_rel.rel_time_range) THEN
      SELECT time_last_emaj_gid INTO v_lastEmajGidRel
        FROM emaj.emaj_time_stamp
        WHERE time_id = upper(r_rel.rel_time_range);
    END IF;
--   If the relation time range upper bound is before the requested end mark, restrict the EmajGid upper limit.
    IF v_lastEmajGidRel IS NOT NULL AND
       (p_lastEmajGid IS NULL OR (p_lastEmajGid IS NOT NULL AND v_lastEmajGidRel < p_lastEmajGid)) THEN
      p_lastEmajGid = v_lastEmajGidRel;
    END IF;
--   Complete the restriction conditions.
    IF p_lastEmajGid IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || p_lastEmajGid;
    END IF;
-- Now scan the log table to process all statement types at once.
    EXECUTE format('INSERT INTO emaj_temp_script '
                   'SELECT o.emaj_gid, 0, o.emaj_txid,'
                   '  CASE'
                   '    WHEN o.emaj_verb = ''INS'' THEN %s'
                   '    WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN %s'
                   '    WHEN o.emaj_verb = ''DEL'' THEN %s'
                   '    WHEN o.emaj_verb = ''TRU'' THEN %s'
                   '  END'
                   '  FROM %s o'
                   '       LEFT OUTER JOIN %s n ON n.emaj_gid = o.emaj_gid'
                   '                          AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'')'
                   '  WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
                   '    AND NOT (o.emaj_verb = ''TRU'' AND o.emaj_tuple <> '''')'
                   '    AND %s',
                   v_rqInsert, v_rqUpdate, v_rqDelete, v_rqTruncate, v_logTableName, v_logTableName, v_conditions);
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
--
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

CREATE OR REPLACE FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                   OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_sequence_stat_seq$
-- This function compares the state of a single sequence between 2 time stamps or between a time stamp and the current state.
-- It is called by the _sequence_stat_group() function.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time stamp ids defining the time range to examine (a end time stamp id set to NULL indicates the current state).
-- Output: number of sequence increments between both time stamps for the sequence
--         a boolean indicating whether any structure property has been modified between both time stamps.
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
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
      EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '  FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '  WHERE nspname = %L AND relname = %L',
                     r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
        INTO STRICT r_endSeq;
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
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_gen_sql_seq$
-- This function generates a SQL statement to set the final characteristics of a sequence.
-- The statement is stored into a temporary table created by the _gen_sql_groups() calling function.
-- If the sequence has not been changed between both marks, no statement is generated.
-- Input: row from emaj_relation corresponding to the appplication sequence to proccess,
--        the time id at requested start and end marks,
--        the number of already processed sequences
-- Output: number of generated SQL statements (0 or 1)
-- The function is defined as SECURITY DEFINER so that emaj_adm and emaj_viewer roles can use it even without SELECT right on the sequence.
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
      EXECUTE format('SELECT nspname, relname, 0, sq.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '  FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '  WHERE nspname = %L AND relname = %L',
                     r_rel.rel_schema, r_rel.rel_tblseq, r_rel.rel_schema, r_rel.rel_tblseq)
        INTO STRICT trg_seq_rec;
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

CREATE OR REPLACE FUNCTION emaj._get_log_sequence_last_value(p_schema TEXT, p_sequence TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_get_log_sequence_last_value$
-- The function returns the last value state of a single log sequence.
-- If the sequence has not been called, it returns the previous value, the increment being always 1.
-- It first calls the undocumented but very efficient pg_sequence_last_value(oid) function.
-- If this pg_sequence_last_value() function returns NULL, meaning the is_called attribute is FALSE which is not a frequent case,
--   select the sequence itself.
-- Input: log schema and log sequence name,
-- Output: last_value
  DECLARE
    v_lastValue                BIGINT;
  BEGIN
    SELECT pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
    IF v_lastValue IS NULL THEN
-- The is_called attribute seems to be false, so reach the sequence itself.
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END as last_value FROM %I.%I',
                     p_schema, p_sequence)
        INTO STRICT v_lastValue;
    END IF;
    RETURN v_lastValue;
  END;
$_get_log_sequence_last_value$;

CREATE OR REPLACE FUNCTION emaj._get_app_sequence_last_value(p_schema TEXT, p_sequence TEXT)
RETURNS BIGINT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_get_app_sequence_last_value$
-- The function returns the last value state of a single application sequence.
-- If the sequence has not been called, it returns the previous value defined as (last_value - increment).
-- It first calls the undocumented but very efficient pg_sequence_last_value(oid) function.
-- If this pg_sequence_last_value() function returns NULL, meaning the is_called attribute is FALSE which is not a frequent case,
--   select the sequence itself.
-- Input: schema and sequence name
-- Output: last_value
-- The function is defined as SECURITY DEFINER so that any emaj role can use it even if he is not the application sequence owner.
  DECLARE
    v_lastValue                BIGINT;
  BEGIN
    SELECT pg_sequence_last_value((quote_ident(p_schema) || '.' || quote_ident(p_sequence))::regclass) INTO v_lastValue;
    IF v_lastValue IS NULL THEN
-- The is_called attribute seems to be false, so reach the sequence itself.
      EXECUTE format('SELECT CASE WHEN is_called THEN last_value ELSE last_value -'
                     '         (SELECT seqincrement'
                     '            FROM pg_catalog.pg_sequence s'
                     '                 JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '                 JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '            WHERE nspname = %L AND relname = %L'
                     '         ) END as last_value FROM %I.%I',
                     p_schema, p_sequence, p_schema, p_sequence)
        INTO STRICT v_lastValue;
    END IF;
    RETURN v_lastValue;
  END;
$_get_app_sequence_last_value$;

--------------------------------------------
--                                        --
--       Functions to manage groups       --
--                                        --
--------------------------------------------

CREATE OR REPLACE FUNCTION emaj._verify_groups(p_groups TEXT[], p_onErrorStop BOOLEAN)
RETURNS SETOF emaj._verify_groups_type LANGUAGE plpgsql AS
$_verify_groups$
-- The function verifies the consistency of a tables groups array.
-- Input: - tables groups array,
--        - a boolean indicating whether the function has to raise an exception in case of detected unconsistency.
-- If onErrorStop boolean is false, it returns a set of _verify_groups_type records, one row per detected unconsistency, including
-- the faulting schema and table or sequence names and a detailed message.
-- If no error is detected, no row is returned.
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_hint                   CONSTANT TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected.
-- Look for groups unconsistency.
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist.
-- Check that all application relations referenced in the emaj_relation table still exist.
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq, r.rel_group,
             'In tables group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist anymore.' AS msg
        FROM
          (  SELECT rel_schema, rel_tblseq, rel_kind
               FROM emaj.emaj_relation
               WHERE rel_group = ANY (p_groups)
                 AND upper_inf(rel_time_range)
           EXCEPT                                -- all relations known by postgres
             SELECT nspname, relname, relkind::TEXT
               FROM pg_catalog.pg_class
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
               WHERE relkind IN ('r','S')
          ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema
          AND t.rel_tblseq = r.rel_tblseq
          AND upper_inf(r.rel_time_range)
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (1): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log table for all tables referenced in the emaj_relation table still exist.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_log_schema
                     AND relname = rel_log_table
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (2): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the log function for each table referenced in the emaj_relation table still exists.
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log function "' || rel_log_schema || '"."' || rel_log_function ||
             '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r' AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_proc
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                   WHERE nspname = rel_log_schema
                     AND proname = rel_log_function
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (3): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that log and truncate triggers for all tables referenced in the emaj_relation table still exist.
--   Start with the log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_log_trg'
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (4): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   Then the truncate trigger.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_trunc_trg'
                )
      ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (5): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have a structure consistent with the application tables they reference
-- (same columns and same formats). It only returns one row per faulting table.
    FOR r_object IN
      WITH cte_app_tables_columns AS                  -- application table's columns
        (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
           FROM emaj.emaj_relation
                JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
           WHERE attnum > 0
             AND attisdropped = FALSE
             AND rel_group = ANY (p_groups)
             AND rel_kind = 'r'
             AND upper_inf(rel_time_range)
        ),
           cte_log_tables_columns AS                  -- log table's columns
        (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
           FROM emaj.emaj_relation
                JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
            WHERE attnum > 0
              AND NOT attisdropped
              AND attnum < rel_emaj_verb_attnum
              AND rel_group = ANY (p_groups)
              AND rel_kind = 'r'
              AND upper_inf(rel_time_range))
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the structure of the application table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
             rel_log_schema || '"."' || rel_log_table || '").' AS msg
        FROM
          (
            (                                        -- application table's columns
               SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                 FROM cte_app_tables_columns
             EXCEPT                                   -- minus log table's columns
               SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                 FROM cte_log_tables_columns
            )
          UNION
            (                                         -- log table's columns
               SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                 FROM cte_log_tables_columns
             EXCEPT                                    -- minus application table's columns
               SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                 FROM cte_app_tables_columns
            )
          ) AS t
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (6): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all tables have their primary key if they belong to a rollbackable group.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key anymore.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND group_is_rollbackable
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND contype = 'p'
                )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (7): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- For rollbackable groups, check that no table has been altered as UNLOGGED or dropped and recreated as TEMP table after the tables
-- groups creation.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE rel_group = ANY (p_groups)
          AND rel_kind = 'r'
          AND upper_inf(rel_time_range)
          AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (8): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the primary key structure of all tables belonging to rollbackable groups is unchanged.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In the rollbackable tables group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  array_to_string(rel_pk_cols, ',') AS registered_pk_columns,
                  string_agg(attname, ',' ORDER BY attnum) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_index ON (indrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_index.indrelid)
             WHERE rel_group = ANY (p_groups)
               AND rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND attnum = ANY (indkey)
               AND indisprimary
               AND attnum > 0
               AND NOT attisdropped
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_pk_columns <> current_pk_columns
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (9): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that the "GENERATED AS expression" columns list of all tables have not changed.
-- (The expression of virtual generated columns be changed, without generating any trouble)
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the "GENERATED AS expression" columns list of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_gen_columns || ' => ' || current_gen_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  coalesce(array_to_string(rel_gen_expr_cols, ','), '<none>') AS registered_gen_columns,
                  coalesce(string_agg(attname, ',' ORDER BY attnum), '<none>') AS current_gen_columns
             FROM emaj.emaj_relation
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
             WHERE rel_group = ANY (p_groups)
               AND rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND attgenerated <> ''
               AND attnum > 0
               AND NOT attisdropped
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_gen_columns <> current_gen_columns
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (10): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check the array of triggers to ignore at rollback time only contains existing triggers.
    FOR r_object IN
      SELECT rel_schema, rel_tblseq, rel_group,
             'Error: In group "' || rel_group || '", the trigger "' || trg_name || '" for table "'
          || rel_schema || '"."' || rel_tblseq || '" is missing. '
          || 'Use the emaj_modify_table() function to adjust the list of application triggers that should not be'
          || ' automatically disabled at rollback time.' AS msg
        FROM
          (SELECT rel_group, rel_schema, rel_tblseq, unnest(rel_ignored_triggers) AS trg_name
             FROM emaj.emaj_relation
             WHERE upper_inf(rel_time_range)
               AND rel_ignored_triggers IS NOT NULL
          ) AS t
        WHERE NOT EXISTS
                 (SELECT NULL
                    FROM pg_catalog.pg_trigger
                         JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                    WHERE nspname = rel_schema
                      AND relname = rel_tblseq
                      AND tgname = trg_name
                 )
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (11): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- Check that all log tables have the 6 required technical columns. It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq, rel_group,
             'In tables group "' || rel_group || '", the log table "' ||
             rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
             string_agg(attname,', ') || ').' AS msg
        FROM
          (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
               FROM emaj.emaj_relation,
                   (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
               WHERE rel_group = ANY (p_groups)
                 AND rel_kind = 'r'
                 AND upper_inf(rel_time_range)
                 AND EXISTS
                       (SELECT NULL
                          FROM pg_catalog.pg_class
                               JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                          WHERE nspname = rel_log_schema
                            AND relname = rel_log_table
                       )
           EXCEPT
             SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
               FROM emaj.emaj_relation
                    JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                    JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
               WHERE rel_group = ANY (p_groups)
                 AND rel_kind = 'r'
                 AND upper_inf(rel_time_range)
                 AND attnum > 0
                 AND attisdropped = FALSE
                 AND attname IN ('emaj_verb', 'emaj_tuple', 'emaj_gid', 'emaj_changed', 'emaj_txid', 'emaj_user')
          ) AS t2
        GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
        ORDER BY 1,2,3
    LOOP
      IF p_onErrorStop THEN RAISE EXCEPTION '_verify_groups (12): % %', r_object.msg, v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj._check_fk_groups(p_groupNames TEXT[])
RETURNS VOID LANGUAGE plpgsql AS
$_check_fk_groups$
-- This function checks foreign key constraints for tables of a groups array.
-- Tables from audit_only groups are ignored in this check because they will never be rolled back.
-- Input: group names array
  DECLARE
    v_isEmajExtension        BOOLEAN;
    r_fk                     RECORD;
  BEGIN
-- Issue a warning if a table of the groups has a foreign key that references a table outside the groups.
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class t ON (t.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace = n.oid AND n.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid  = t.oid)
             JOIN pg_catalog.pg_class tf ON (tf.oid = c.confrelid)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND tf.relkind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table currently outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = nf.nspname
                     AND rel_tblseq = tf.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groupNames)
                )
        ORDER BY 1,2,3
    LOOP
      RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" references the table "%.%" that is outside the groups (%).',
        r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,array_to_string(p_groupNames,',');
    END LOOP;
-- Issue a warning if a table of the groups is referenced by a table outside the groups.
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class tf ON (tf.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace AND nf.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.confrelid  = tf.oid)
             JOIN pg_catalog.pg_class t ON (t.oid = c.conrelid)
             JOIN pg_catalog.pg_namespace n ON (n.oid = t.relnamespace)
        WHERE contype = 'f'                                         -- FK constraints only
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND t.relkind = 'r'                                       -- only constraints referenced by true tables, ie. excluding
                                                                    --   partitionned tables
          AND NOT EXISTS                                            -- referenced table outside the groups
                (SELECT 0
                   FROM emaj.emaj_relation
                   WHERE rel_schema = n.nspname
                     AND rel_tblseq = t.relname
                     AND upper_inf(rel_time_range)
                     AND rel_group = ANY (p_groupNames)
                )
        ORDER BY 1,2,3
    LOOP
      RAISE WARNING '_check_fk_groups: The table "%.%" is referenced by the foreign key "%" on the table "%.%" that is outside'
                    ' the groups (%).', r_fk.rel_schema, r_fk.rel_tblseq, r_fk.conname, r_fk.nspname, r_fk.relname,
                    array_to_string(p_groupNames,',');
    END LOOP;
-- Issue a warning for rollbackable groups if a FK on a partition is actualy set on the partitionned table.
-- The warning message depends on the FK characteristics.
    v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
    FOR r_fk IN
      SELECT rel_schema, rel_tblseq, c.conname, confupdtype, confdeltype, condeferrable
        FROM emaj.emaj_relation r
             JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
             JOIN pg_catalog.pg_class t ON (t.relname = r.rel_tblseq)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rel_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid)
        WHERE contype = 'f'                                         -- FK constraints only
          AND coninhcount > 0                                       -- inherited FK
          AND upper_inf(r.rel_time_range)
          AND r.rel_group = ANY (p_groupNames)                      -- only tables currently belonging to the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND r.rel_kind = 'r'                                      -- only constraints referencing true tables, ie. excluding
                                                                    --   partitionned tables
        ORDER BY 1,2,3
    LOOP
      IF r_fk.confupdtype = 'a' AND r_fk.confdeltype = 'a' AND NOT r_fk.condeferrable THEN
        -- Advise DEFERRABLE FK if there is no ON UPDATE|DELETE clause.
        RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table. It should'
                      ' be set as DEFERRABLE to avoid E-Maj rollback failure.',
                      r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
      ELSIF r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
        -- Warn about potential rollback failure with FK having ON UPDATE|DELETE clause.
        IF v_isEmajExtension THEN
          RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table. An E-Maj'
                        ' rollback targeting a mark set before the latest table assignement could fail.',
                        r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
        ELSE
          RAISE WARNING '_check_fk_groups: The foreign key "%" on the table "%.%" is inherited from a partitionned table and has'
                        ' ON DELETE and/or ON UPDATE clause. This will generate E-Maj rollback failures if this FK needs to be'
                        ' temporarily dropped and recreated.',
                        r_fk.conname, r_fk.rel_schema, r_fk.rel_tblseq;
        END IF;
      END IF;
    END LOOP;
--
    RETURN;
  END;
$_check_fk_groups$;

CREATE OR REPLACE FUNCTION emaj._lock_groups(p_groupNames TEXT[], p_lockMode TEXT, p_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_lock_groups$
-- This function locks all tables of a groups array.
-- The lock mode is provided by the calling function.
-- It only locks existing tables. It is calling function's responsability to handle cases when application tables are missing.
-- Input: array of group names, lock mode, flag indicating whether the function is called to processed several groups.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on tables.
  DECLARE
    v_nbRetry                SMALLINT = 0;
    v_nbTbl                  INT;
    v_ok                     BOOLEAN = FALSE;
    v_fullTableName          TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN p_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END,'BEGIN', array_to_string(p_groupNames,','));
-- Acquire lock on all tables.
-- In case of deadlock, retry up to 5 times.
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- Process each table currently belonging to the groups.
        v_nbTbl = 0;
        FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq
            FROM emaj.emaj_relation
                 JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                 JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
            WHERE rel_group = ANY (p_groupNames)
              AND rel_kind = 'r'
              AND upper_inf(rel_time_range)
            ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- Lock the table.
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE format('LOCK TABLE %s IN %s MODE',
                         v_fullTableName, p_lockMode);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- Ok, all tables are locked.
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_lock_groups: A deadlock has been trapped while locking tables of group "%".', p_groupNames;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_lock_groups: Too many (5) deadlocks encountered while locking tables of group "%".',p_groupNames;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END, 'END',
              array_to_string(p_groupNames,','), v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
--
    RETURN;
  END;
$_lock_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(p_groupName TEXT, p_isRollbackable BOOLEAN DEFAULT TRUE, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates a group, for the moment empty.
-- Input: group name,
--        boolean indicating whether the group is rollbackable or not (true by default),
--        optional comment.
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
    INSERT INTO emaj.emaj_group (group_name, group_is_rollbackable, group_is_logging, group_is_rlbk_protected,
                                 group_nb_table, group_nb_sequence, group_comment)
      VALUES (p_groupName, p_isRollbackable, FALSE, NOT p_isRollbackable, 0, 0, p_comment);
    INSERT INTO emaj.emaj_group_hist (grph_group, grph_time_range, grph_is_rollbackable, grph_log_sessions)
      VALUES (p_groupName, int8range(v_timeId, NULL, '[]'), p_isRollbackable, 0);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (v_function, 'END', p_groupName);
    RETURN 1;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT,BOOLEAN,TEXT) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_group(p_groupName TEXT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_group$
-- This function sets or modifies a comment on a group by updating the group_comment of the emaj_group table.
-- Input: group name, comment
--   To reset an existing comment for a group, the supplied comment can be NULL.
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Update the group_comment column from emaj_group table.
    UPDATE emaj.emaj_group SET group_comment = p_comment WHERE group_name = p_groupName;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_GROUP', p_groupName);
--
    RETURN;
  END;
$emaj_comment_group$;
COMMENT ON FUNCTION emaj.emaj_comment_group(TEXT,TEXT) IS
$$Sets a comment on an E-Maj group.$$;

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
-- It also drops log schemas that are not useful anymore.
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

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_groups TEXT[] DEFAULT NULL)
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_groups_configuration$
-- This function returns a JSON formatted structure representing some or all configured tables groups
-- The function can be called by clients like emaj_web.
-- This is just a wrapper of the internal _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the tables groups content in JSON format
  BEGIN
    RETURN emaj._export_groups_conf(p_groups);
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT[]) IS
$$Generates a json structure describing configured tables groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_groups_configuration$
-- This function stores some or all configured tables groups configuration into a file on the server.
-- The JSON structure is built by the _export_groups_conf() function.
-- Input: an optional array of goup's names, NULL means all tables groups
-- Output: the number of tables groups recorded in the file.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_groupsJson             JSON;
  BEGIN
-- Get the json structure.
    SELECT emaj._export_groups_conf(p_groups) INTO v_groupsJson;
-- Store the structure into the provided file name.
    CREATE TEMP TABLE t (groups TEXT);
    INSERT INTO t
      SELECT line
        FROM regexp_split_to_table(v_groupsJson::TEXT, '\n') AS line;
    EXECUTE format ('COPY t TO %L',
                    p_location);
    DROP TABLE t;
-- Return the number of recorded tables groups.
    RETURN json_array_length(v_groupsJson->'tables_groups');
  END;
$emaj_export_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_groups_configuration(TEXT, TEXT[]) IS
$$Generates and stores in a file a json structure describing configured tables groups.$$;

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
                           emaj.emaj_get_version() || ', at ' || statement_timestamp() || E'",\n';
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

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_json JSON, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_groups_configuration$
-- This function import a supplied JSON formatted structure representing tables groups to create or update.
-- This structure can have been generated by the emaj_export_groups_configuration() functions and may have been adapted by the user.
-- It calls the _import_groups_conf() function to process the tables groups.
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--        - an optional mark name to set for tables groups in logging state (IMPORT_% by default)
-- Output: the number of created or altered tables groups
  BEGIN
-- Just process the tables groups.
    RETURN emaj._import_groups_conf(p_json, p_groups, p_allowGroupsUpdate, NULL, p_mark);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(JSON,TEXT[],BOOLEAN, TEXT) IS
$$Import a json structure describing tables groups to create or alter.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_groups_configuration(p_location TEXT, p_groups TEXT[] DEFAULT NULL,
                                                                 p_allowGroupsUpdate BOOLEAN DEFAULT FALSE, p_mark TEXT DEFAULT 'IMPORT_%')
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
    RETURN emaj._import_groups_conf(v_json, p_groups, p_allowGroupsUpdate, p_location, p_mark);
  END;
$emaj_import_groups_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_groups_configuration(TEXT,TEXT[],BOOLEAN, TEXT) IS
$$Create or alter tables groups configuration from a JSON formatted file.$$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf(p_json JSON, p_groups TEXT[], p_allowGroupsUpdate BOOLEAN,
                                                    p_location TEXT, p_mark TEXT)
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
        FROM emaj._import_groups_conf_prepare(p_json, p_groups, p_allowGroupsUpdate, p_location)
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3
    LOOP
      RAISE WARNING '_import_groups_conf (2): %', r_msg.rpt_message;
    END LOOP;
    IF FOUND THEN
      RAISE EXCEPTION '_import_groups_conf: One or several errors have been detected in the JSON groups configuration.';
    END IF;
-- OK
    RETURN emaj._import_groups_conf_exec(p_json, p_groups, p_mark);
 END;
$_import_groups_conf$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_prepare(p_groupsJson JSON, p_groups TEXT[],
                                                    p_allowGroupsUpdate BOOLEAN, p_location TEXT)
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_prepare$
-- This function prepares the effective tables groups configuration import.
-- It is called by _import_groups_conf() and by emaj_web
-- At the end of the function, the tmp_app_table table is updated with the new configuration of groups
--   and a temporary table is created to prepare the application triggers management
-- Input: - the tables groups configuration structure in JSON format
--        - an optional array of group names to process (a NULL value process all tables groups described in the JSON structure)
--        - an optional boolean indicating whether tables groups to import may already exist (FALSE by default)
--            (if TRUE, existing groups are altered, even if they are in logging state)
--        - the input file name, if any, to record in the emaj_hist table
-- Output: diagnostic records
  DECLARE
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
    DROP TABLE IF EXISTS tmp_app_table, tmp_app_sequence;
-- Create the temporary table that will hold the application tables configured in groups.
    CREATE TEMP TABLE tmp_app_table (
      tmp_group            TEXT NOT NULL,
      tmp_schema           TEXT NOT NULL,
      tmp_tbl_name         TEXT NOT NULL,
      tmp_priority         INTEGER,
      tmp_log_dat_tsp      TEXT,
      tmp_log_idx_tsp      TEXT,
      tmp_ignored_triggers TEXT[],
      PRIMARY KEY (tmp_schema, tmp_tbl_name)
      );
-- Create the temporary table that will hold the application sequences configured in groups.
    CREATE TEMP TABLE tmp_app_sequence (
      tmp_schema           TEXT NOT NULL,
      tmp_seq_name         TEXT NOT NULL,
      tmp_group            TEXT,
      PRIMARY KEY (tmp_schema, tmp_seq_name)
      );
-- In a second pass over the JSON structure, populate the tmp_app_table and tmp_app_sequence temporary tables.
    v_rollbackableGroups = '{}';
    FOR r_group IN
      SELECT value AS groupJson
        FROM json_array_elements(p_groupsJson)
        WHERE value ->> 'group' = ANY (p_groups)
    LOOP
-- Build the array of rollbackable groups.
      IF coalesce((r_group.groupJson ->> 'is_rollbackable')::BOOLEAN, TRUE) THEN
        v_rollbackableGroups = array_append(v_rollbackableGroups, r_group.groupJson ->> 'group');
      END IF;
-- Insert tables into tmp_app_table.
      FOR r_table IN
        SELECT value AS tableJson
          FROM json_array_elements(r_group.groupJson -> 'tables')
      LOOP
--   Prepare the array of trigger names for the table,
        SELECT array_agg("value" ORDER BY "value") INTO v_ignoredTriggers
          FROM json_array_elements_text(r_table.tableJson -> 'ignored_triggers') AS t;
--   ... and insert
        INSERT INTO tmp_app_table(tmp_group, tmp_schema, tmp_tbl_name,
                                  tmp_priority, tmp_log_dat_tsp, tmp_log_idx_tsp, tmp_ignored_triggers)
          VALUES (r_group.groupJson ->> 'group', r_table.tableJson ->> 'schema', r_table.tableJson ->> 'table',
                  (r_table.tableJson ->> 'priority')::INT, r_table.tableJson ->> 'log_data_tablespace',
                  r_table.tableJson ->> 'log_index_tablespace', v_ignoredTriggers);
      END LOOP;
-- Insert sequences into tmp_app_table.
      FOR r_sequence IN
        SELECT value AS sequenceJson
          FROM json_array_elements(r_group.groupJson -> 'sequences')
      LOOP
        INSERT INTO tmp_app_sequence(tmp_schema, tmp_seq_name, tmp_group)
          VALUES (r_sequence.sequenceJson ->> 'schema', r_sequence.sequenceJson ->> 'sequence', r_group.groupJson ->> 'group');
      END LOOP;
    END LOOP;
-- Check the just imported tmp_app_table content is ok for the groups.
    RETURN QUERY
      SELECT *
        FROM emaj._import_groups_conf_check(p_groups)
        WHERE ((rpt_text_var_1 = ANY (p_groups) AND rpt_severity = 1)
            OR (rpt_text_var_1 = ANY (v_rollbackableGroups) AND rpt_severity = 2))
        ORDER BY rpt_msg_type, rpt_text_var_1, rpt_text_var_2, rpt_text_var_3;
--
    RETURN;
  END;
$_import_groups_conf_prepare$;

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_check(p_groupNames TEXT[])
RETURNS SETOF emaj._report_message_type LANGUAGE plpgsql AS
$_import_groups_conf_check$
-- This function verifies that the content of tables group as defined into the tmp_app_table table is correct.
-- Any detected issue is reported as a message row. The caller defines what to do with them, depending on the tables group type.
-- It is called by the _import_groups_conf_prepare() function.
-- This function checks that the referenced application tables and sequences:
--  - exist,
--  - are not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group,
-- It also checks that:
--  - tables are not TEMPORARY
--  - for rollbackable groups, tables are not UNLOGGED or WITH OIDS
--  - for rollbackable groups, all tables have a PRIMARY KEY
--  - for tables, configured tablespaces exist
-- Input: name array of the tables groups to check
-- Output: _report_message_type records representing diagnostic messages
--         the rpt_severity is set to 1 if the error blocks any type group creation or alter,
--                                 or 2 if the error only blocks ROLLBACKABLE groups creation
  BEGIN
-- Check that all application tables listed for the group really exist.
    RETURN QUERY
      SELECT 1, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s does not exist.',
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
             format('in the group %s, the table %s.%s is a partitionned table (only elementary partitions are supported by E-Maj).',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'p';
-- Check no application schema listed for the group in the tmp_app_table table is an E-Maj schema.
    RETURN QUERY
      SELECT 3, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table or sequence %s.%s belongs to an E-Maj schema.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN emaj.emaj_schema ON (sch_name = tmp_schema);
-- Check that no table of the checked groups already belongs to other created groups.
    RETURN QUERY
      SELECT 4, 1, tmp_group, tmp_schema, tmp_tbl_name, rel_group, NULL::INT,
             format('in the group %s, the table %s.%s already belongs to the group %s.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(rel_group))
        FROM tmp_app_table
             JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_tbl_name AND upper_inf(rel_time_range))
        WHERE NOT rel_group = ANY (p_groupNames);
-- Check no table is a TEMP table.
    RETURN QUERY
      SELECT 5, 1, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s is a TEMPORARY table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r' AND relpersistence = 't';
-- Check that the log data tablespaces for tables exist.
    RETURN QUERY
      SELECT 12, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_dat_tsp, NULL::INT,
             format('in the group %s, for the table %s.%s, the data log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_dat_tsp))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND tmp_log_dat_tsp IS NOT NULL
          AND NOT EXISTS
                (SELECT 1
                   FROM pg_catalog.pg_tablespace
                   WHERE spcname = tmp_log_dat_tsp
                );
-- Check that the log index tablespaces for tables exist.
    RETURN QUERY
      SELECT 13, 1, tmp_group, tmp_schema, tmp_tbl_name, tmp_log_idx_tsp, NULL::INT,
             format('in the group %s, for the table %s.%s, the index log tablespace %s does not exist.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name), quote_ident(tmp_log_idx_tsp))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND tmp_log_idx_tsp IS NOT NULL
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
          ) AS t
        WHERE tmp_trigger IN ('emaj_trunc_trg', 'emaj_log_trg');
-- Check that no table is an unlogged table (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 20, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s is an UNLOGGED table.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_tbl_name))
        FROM tmp_app_table
             JOIN pg_catalog.pg_class ON (relname = tmp_tbl_name)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = tmp_schema)
        WHERE relkind = 'r'
          AND relpersistence = 'u';
-- Check every table has a primary key (blocking rollbackable groups only).
    RETURN QUERY
      SELECT 22, 2, tmp_group, tmp_schema, tmp_tbl_name, NULL::TEXT, NULL::INT,
             format('in the group %s, the table %s.%s has no PRIMARY KEY.',
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
             format('in the group %s, the sequence %s.%s does not exist.',
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
    RETURN QUERY
      SELECT 32, 1, tmp_group, tmp_schema, tmp_seq_name, rel_group, NULL::INT,
             format('in the group %s, the sequence %s.%s already belongs to the group %s.',
                    tmp_group, quote_ident(tmp_schema), quote_ident(tmp_seq_name), quote_ident(rel_group))
        FROM tmp_app_sequence
             JOIN emaj.emaj_relation ON (rel_schema = tmp_schema AND rel_tblseq = tmp_seq_name AND upper_inf(rel_time_range))
        WHERE NOT rel_group = ANY (p_groupNames);
--
    RETURN;
  END;
$_import_groups_conf_check$;

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
      PERFORM emaj._set_mark_groups(v_loggingGroups, v_markName, NULL, TRUE, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj._import_groups_conf_alter(p_groupNames TEXT[], p_mark TEXT, p_timeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_import_groups_conf_alter$
-- This function effectively alters the tables groups to import.
-- It uses the content of tmp_app_table and tmp_app_sequence temporary tables and calls the appropriate elementary functions
-- It is called by the _import_groups_conf_exec() function.
-- Input: group names array,
--        the mark name to set on groups in logging state
--        the timestamp id
  DECLARE
    v_eventTriggers          TEXT[];
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
-- Reset the concerned groups in IDLE state, before changing tablespaces.
    PERFORM emaj._reset_groups(array_agg(group_name ORDER BY group_name))
      FROM emaj.emaj_group
      WHERE group_name = ANY (p_groupNames)
        AND NOT group_is_logging;
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

CREATE OR REPLACE FUNCTION emaj.emaj_start_group(p_groupName TEXT, p_mark TEXT DEFAULT 'START_%', p_resetLog BOOLEAN DEFAULT TRUE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_group$
-- This function activates the log triggers of all the tables for a group and set a first mark.
-- It may reset log tables.
-- Input: group name,
--        name of the mark to set
--          '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--          if omitted or if null or '', the mark is set to 'START_%', % representing the current timestamp
--        boolean indicating whether the log tables of the group must be reset, true by default.
-- Output: number of processed tables and sequences
  BEGIN
-- Call the common _start_groups function.
    RETURN emaj._start_groups(array[p_groupName], p_mark, FALSE, p_resetLog);
  END;
$emaj_start_group$;
COMMENT ON FUNCTION emaj.emaj_start_group(TEXT,TEXT,BOOLEAN) IS
$$Starts an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_start_groups(p_groupNames TEXT[], p_mark TEXT DEFAULT 'START_%', p_resetLog BOOLEAN DEFAULT TRUE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_groups$
-- This function activates the log triggers of all the tables for a groups array and set a first mark.
-- Input: array of group names,
--        name of the mark to set (if omitted, START_<current timestamp>)
--          '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--          if omitted or if null or '', the mark is set to 'START_%', % representing the current timestamp
--        boolean indicating whether the log tables of the group must be reset, true by default.
-- Output: total number of processed tables and sequences
  BEGIN
-- Call the common _start_groups function.
    RETURN emaj._start_groups(p_groupNames, p_mark, TRUE, p_resetLog);
  END;
$emaj_start_groups$;
COMMENT ON FUNCTION emaj.emaj_start_groups(TEXT[],TEXT, BOOLEAN) IS
$$Starts several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._start_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark.
-- It also delete oldest rows in emaj_hist table.
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function,
--        boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
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
        SELECT rel_kind, rel_schema, rel_tblseq
          FROM emaj.emaj_relation
          WHERE upper_inf(rel_time_range)
            AND rel_group = ANY (p_groupNames)
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, enable the emaj log and truncate triggers.
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_log_trg',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq);
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_trunc_trg',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq);
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
      PERFORM emaj._set_mark_groups(p_groupNames, v_markName, NULL, p_multiGroup, TRUE, NULL, v_timeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (v_function, 'END', array_to_string(p_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
--
    RETURN v_nbTblSeq;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_group(p_groupName TEXT, p_mark TEXT DEFAULT 'STOP_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_group$
-- This function de-activates the log triggers of all the tables for a group.
-- Execute several emaj_stop_group functions for the same group doesn't produce any error.
-- Input: group name
--        name of the mark to set (if omitted, STOP_<current timestamp>)
--          '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--          if omitted or if null or '', the mark is set to 'STOP_%', % representing the current timestamp
-- Output: number of processed tables and sequences
  BEGIN
    RETURN emaj._stop_groups(array[p_groupName], p_mark, FALSE, FALSE);
  END;
$emaj_stop_group$;
COMMENT ON FUNCTION emaj.emaj_stop_group(TEXT,TEXT) IS
$$Stops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_groups(p_groupNames TEXT[], p_mark TEXT DEFAULT 'STOP_%')
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_groups$
-- This function de-activates the log triggers of all the tables for a groups array.
-- Groups already not in LOGGING state are simply not processed.
-- Input: array of group names, stop mark name to set (by default, STOP_<current timestamp>)
-- Output: number of processed tables and sequences
  BEGIN
    RETURN emaj._stop_groups(p_groupNames, p_mark, TRUE, FALSE);
  END;
$emaj_stop_groups$;
COMMENT ON FUNCTION emaj.emaj_stop_groups(TEXT[], TEXT) IS
$$Stops several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_force_stop_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_force_stop_group$
-- This function forces a tables group stop.
-- The differences with the standart emaj_stop_group() function are:
--   - it silently ignores errors when an application table or one of its triggers is missing,
--   - no stop mark is set (to avoid error)
-- Input: group name
-- Output: number of processed tables and sequences
  BEGIN
    RETURN emaj._stop_groups(array[p_groupName], NULL, FALSE, TRUE);
  END;
$emaj_force_stop_group$;
COMMENT ON FUNCTION emaj.emaj_force_stop_group(TEXT) IS
$$Forces an E-Maj group stop.$$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(p_groupNames TEXT[], p_mark TEXT, p_multiGroup BOOLEAN, p_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_function               TEXT;
    v_groupList              TEXT;
    v_count                  INT;
    v_timeId                 BIGINT;
    v_nbTblSeq               INT = 0;
    v_markName               TEXT;
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
          RAISE WARNING '_stop_groups: The schema "%" does not exist anymore.', r_schema.rel_schema;
        ELSE
          RAISE EXCEPTION '_stop_groups: The schema "%" does not exist anymore.', r_schema.rel_schema;
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
              RAISE WARNING '_stop_groups: The table "%.%" does not exist anymore.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            ELSE
              RAISE EXCEPTION '_stop_groups: The table "%.%" does not exist anymore.', r_tblsq.rel_schema, r_tblsq.rel_tblseq;
            END IF;
          ELSE
-- ... and disable the emaj log and truncate triggers.
-- Errors are captured so that emaj_force_stop_group() can be silently executed.
            BEGIN
              EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                             r_tblsq.rel_schema, r_tblsq.rel_tblseq);
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The log trigger "emaj_log_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
            BEGIN
              EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_trunc_trg',
                             r_tblsq.rel_schema, r_tblsq.rel_tblseq);
            EXCEPTION
              WHEN undefined_object THEN
                IF p_isForced THEN
                  RAISE WARNING '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                ELSE
                  RAISE EXCEPTION '_stop_groups: The truncate trigger "emaj_trunc_trg" on table "%.%" does not exist anymore.',
                    r_tblsq.rel_schema, r_tblsq.rel_tblseq;
                END IF;
            END;
          END IF;
        END IF;
        v_nbTblSeq = v_nbTblSeq + 1;
      END LOOP;
      IF NOT p_isForced THEN
-- If the function is not called by emaj_force_stop_group(), set the stop mark for each group,
        PERFORM emaj._set_mark_groups(p_groupNames, v_markName, NULL, p_multiGroup, TRUE, NULL, v_timeId);
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

CREATE OR REPLACE FUNCTION emaj.emaj_protect_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_group$
-- This function sets a protection on a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name
-- Output: 1 if successful, 0 if the group was already in protected state
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkLogging := TRUE, p_checkRollbackable := TRUE);
-- OK, set the protection.
    UPDATE emaj.emaj_group
      SET group_is_rlbk_protected = TRUE
      WHERE group_name = p_groupName
        AND NOT group_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_GROUP', p_groupName, 'Status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_protect_group$;
COMMENT ON FUNCTION emaj.emaj_protect_group(TEXT) IS
$$Sets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_group$
-- This function unsets a protection on a group against accidental rollback.
-- Input: group name
-- Output: 1 if successful, 0 if the group was not already in protected state
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- OK, unset the protection.
    UPDATE emaj.emaj_group
      SET group_is_rlbk_protected = FALSE
      WHERE group_name = p_groupName
        AND group_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_GROUP', p_groupName, 'Status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_unprotect_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_group(TEXT) IS
$$Unsets a protection against a rollback on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_group(p_groupName TEXT, p_mark TEXT DEFAULT NULL, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group.
-- Input: group name, mark to set, optional comment.
-- Output: number of processed tables and sequences
-- '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
-- if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp,
  DECLARE
    v_markName               TEXT;
    v_nbRel                  INT;
  BEGIN
-- Insert a BEGIN event into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'BEGIN', p_groupName, v_markName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkLogging := TRUE);
-- Check if the emaj group is OK.
    PERFORM 0
      FROM emaj._verify_groups(array[p_groupName], TRUE);
-- Check and process the supplied mark name.
    SELECT emaj._check_new_mark(array[p_groupName], p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
    PERFORM emaj._lock_groups(array[p_groupName],'ROW EXCLUSIVE',FALSE);
-- Effectively set the mark using the internal _set_mark_groups() function.
    SELECT emaj._set_mark_groups(array[p_groupName], v_markName, p_comment, FALSE, FALSE) INTO v_nbRel;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'END', p_groupName, v_markName);
--
    RETURN v_nbRel;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_groups(p_groupNames TEXT[], p_mark TEXT DEFAULT NULL, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time.
-- Input: array of group names, mark to set, optional comment.
-- Output: number of processed tables and sequences
-- '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
-- if omitted or if null or '', the mark is set to 'MARK_%', % representing the current timestamp
  DECLARE
    v_markName               TEXT;
    v_nbTblseq               INT = 0;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'BEGIN', array_to_string(p_groupNames,','), p_mark);
-- Check the group names.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := TRUE, p_lockGroups := TRUE, p_checkLogging := TRUE)
      INTO p_groupNames;
-- Process the groups.
    IF p_groupNames IS NOT NULL THEN
-- Check that no group is damaged.
      PERFORM 0
        FROM emaj._verify_groups(p_groupNames, TRUE);
-- Check and process the supplied mark name.
      SELECT emaj._check_new_mark(p_groupNames, p_mark) INTO v_markName;
-- OK, lock all tables to get a stable point.
-- Use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or
-- vacuum operation.
      PERFORM emaj._lock_groups(p_groupNames,'ROW EXCLUSIVE',TRUE);
-- Effectively set the mark using the internal _set_mark_groups() function.
      SELECT emaj._set_mark_groups(p_groupNames, v_markName, p_comment, TRUE, FALSE) INTO v_nbTblseq;
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(p_groupNames,','), p_mark);
--
    RETURN v_nbTblseq;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT,TEXT) IS
$$Sets a mark on several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(p_groupNames TEXT[], p_mark TEXT, p_comment TEXT, p_multiGroup BOOLEAN,
                                                 p_eventToRecord BOOLEAN, p_loggedRlbkTargetMark TEXT DEFAULT NULL,
                                                 p_timeId BIGINT DEFAULT NULL, p_dblinkSchema TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- The function is defined as SECURITY DEFINER so that emaj_adm roles can use it even without SELECT right on the sequence.
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
      EXECUTE format('INSERT INTO emaj.emaj_sequence (sequ_schema, sequ_name, sequ_time_id, sequ_last_val, sequ_start_val,'
                     '            sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called)'
                     '  SELECT nspname, relname, %s, sq.last_value, seqstart,'
                     '         seqincrement, seqmax, seqmin, seqcache, seqcycle, sq.is_called'
                     '    FROM %I.%I sq,'
                     '       pg_catalog.pg_sequence s'
                     '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                     '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                     '    WHERE nspname = %L AND relname = %L',
                     p_timeId, r_seq.rel_schema, r_seq.rel_tblseq, r_seq.rel_schema, r_seq.rel_tblseq);
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

CREATE OR REPLACE FUNCTION emaj.emaj_comment_mark_group(p_groupName TEXT, p_mark TEXT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_mark_group$
-- This function sets or modifies a comment on a mark by updating the mark_comment of the emaj_mark table.
-- Input: group name, mark to comment, comment
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
--   To reset an existing comment for a mark, the supplied comment can be NULL.
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- OK, update the mark_comment from emaj_mark table.
    UPDATE emaj.emaj_mark
      SET mark_comment = p_comment
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('COMMENT_MARK_GROUP', p_groupName, 'Mark ' || p_mark);
--
    RETURN;
  END;
$emaj_comment_mark_group$;
COMMENT ON FUNCTION emaj.emaj_comment_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a comment on a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_datetime TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given date and time.
-- It may return unpredictable result in case of system date or time change.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, date and time
-- Output: mark name, or NULL if there is no mark before the given date and time
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Find the requested mark.
    RETURN mark_name
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND time_clock_timestamp < p_datetime
      ORDER BY time_clock_timestamp DESC
      LIMIT 1;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TIMESTAMPTZ) IS
$$Returns the latest mark name preceeding a point in time.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given mark for a group.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, mark name
--   The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark.
-- Output: mark name, or NULL if there is no mark before the given mark
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := FALSE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Find the requested mark.
    RETURN emaj._get_previous_mark_group(p_groupName, p_mark);
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TEXT) IS
$$Returns the latest mark name preceeding a given mark for a group.$$;

CREATE OR REPLACE FUNCTION emaj._get_previous_mark_group(p_groupName TEXT, p_realMark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given mark for a group.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, mark name
--   The mark name has already been checked and resolved if the keyword 'EMAJ_LAST_MARK' has been used by the user.
-- Output: mark name, or NULL if there is no mark before the given mark
  BEGIN
-- Find the requested mark and return.
    RETURN mark_name
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id <
              (SELECT mark_time_id
                 FROM emaj.emaj_mark
                 WHERE mark_group = p_groupName
                   AND mark_name = p_realMark
              )
      ORDER BY mark_time_id DESC
      LIMIT 1;
  END;
$_get_previous_mark_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function.
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence.
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained.
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_markTimeId             BIGINT;
    v_previousMarkTimeId     BIGINT;
    v_previousMarkName       TEXT;
    v_previousMarkGlobalSeq  BIGINT;
    v_nextMarkTimeId         BIGINT;
    v_nextMarkName           TEXT;
    v_nextMarkGlobalSeq      BIGINT;
    v_timeIdNewMin           BIGINT;
    v_markNewMin             TEXT;
    v_count                  INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Count the number of marks in the group.
    SELECT count(*) INTO v_count
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName;
-- Check there are at least 2 marks for the group.
    IF v_count < 2 THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: "%" is the only mark of the group. It cannot be deleted.', p_mark;
    END IF;
-- OK, now get the time stamp id of the mark to delete,
    SELECT mark_time_id INTO v_markTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- ... and the timestamp of the future first mark,
    SELECT mark_time_id, mark_name INTO v_timeIdNewMin, v_markNewMin
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name <> p_mark
      ORDER BY mark_time_id
      LIMIT 1;
-- ... and the name, the time id and the last global sequence value of the previous mark,
    SELECT emaj._get_previous_mark_group(p_groupName, p_mark) INTO v_previousMarkName;
    SELECT mark_time_id, time_last_emaj_gid INTO v_previousMarkTimeId, v_previousMarkGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = v_previousMarkName;
-- ... and the name, the time id and the last global sequence value of the next mark,
    SELECT mark_name INTO v_nextMarkName
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id >
              (SELECT mark_time_id
                 FROM emaj.emaj_mark
                 WHERE mark_group = p_groupName
                   AND mark_name = p_mark
              )
      ORDER BY mark_time_id ASC
      LIMIT 1;
    SELECT mark_time_id, time_last_emaj_gid INTO v_nextMarkTimeId, v_nextMarkGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = v_nextMarkName;
-- Effectively delete the mark for the group.
    IF v_previousMarkTimeId IS NULL THEN
-- If the mark to delete is the first one, process its deletion with _delete_before_mark_group(), as the first rows of log tables become
-- useless.
      PERFORM emaj._delete_before_mark_group(p_groupName, v_markNewMin);
    ELSE
-- Otherwise, the mark to delete is an intermediate mark for the group.
-- Process the mark deletion with _delete_intermediate_mark_group().
      PERFORM emaj._delete_intermediate_mark_group(p_groupName, p_mark, v_markTimeId);
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'END', p_groupName, p_mark);
--
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_before_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_delete_before_mark_group$
-- This function deletes all marks set before a given mark.
-- Then, any rollback on the deleted marks will not be possible.
-- It deletes rows corresponding to the marks to delete from emaj_mark, emaj_sequence, emaj_seq_hole.
-- It also deletes rows from all concerned log tables.
-- Input: group name, name of the new first mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark name.
-- Output: number of deleted marks
--   or NULL if the provided mark name is NULL
  DECLARE
    v_nbMark                 INT;
  BEGIN
-- Insert a BEGIN event into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Return NULL if the mark name is NULL.
    IF p_mark IS NULL THEN
      RETURN NULL;
    END IF;
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Effectively delete all marks before the supplied mark.
    SELECT emaj._delete_before_mark_group(p_groupName, p_mark) INTO v_nbMark;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', p_groupName,  v_nbMark || ' marks deleted ; ' || p_mark || ' is now the initial mark' );
--
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

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

CREATE OR REPLACE FUNCTION emaj._delete_intermediate_mark_group(p_groupName TEXT, p_markName TEXT, p_markTimeId BIGINT)
RETURNS VOID LANGUAGE plpgsql AS
$_delete_intermediate_mark_group$
-- This function effectively deletes an intermediate mark for a group.
-- It is called by the emaj_delete_mark_group() function.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained.
-- Input: group name, mark name, mark id and mark time stamp id of the mark to delete
  DECLARE
    v_lsesTimeRange          INT8RANGE;
    v_previousMark           TEXT;
    v_nextMark               TEXT;
    v_previousMarkTimeId     BIGINT;
    v_nextMarkTimeId         BIGINT;
  BEGIN
-- Get the log session time range that contains the mark time id.
    SELECT lses_time_range
      INTO STRICT v_lsesTimeRange
      FROM emaj.emaj_log_session
      WHERE lses_group = p_groupName
        AND p_markTimeId <@ lses_time_range;
-- Delete the sequences related to the mark to delete, if it is not a log session boundary.
    IF p_markTimeId <> lower(v_lsesTimeRange) AND p_markTimeId <> upper(v_lsesTimeRange) - 1 THEN
-- Delete data related to the application sequences (those attached to the group at the set mark time, but excluding the relation time
-- range bounds).
      DELETE FROM emaj.emaj_sequence
        USING emaj.emaj_relation
        WHERE sequ_schema = rel_schema
          AND sequ_name = rel_tblseq
          AND rel_time_range @> sequ_time_id
          AND rel_group = p_groupName
          AND rel_kind = 'S'
          AND sequ_time_id = p_markTimeId
          AND lower(rel_time_range) <> sequ_time_id;
-- Delete data related to the log sequences for tables (those attached to the group at the set mark time, but excluding the relation time
-- range bounds).
      DELETE FROM emaj.emaj_table
        USING emaj.emaj_relation
        WHERE tbl_schema = rel_schema
          AND tbl_name = rel_tblseq
          AND rel_time_range @> tbl_time_id
          AND rel_group = p_groupName
          AND rel_kind = 'r'
          AND tbl_time_id = p_markTimeId
          AND lower(rel_time_range) <> tbl_time_id;
    END IF;
-- Physically delete the mark from emaj_mark.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = p_markName;
-- Adjust the mark_log_rows_before_next column of the previous mark.
-- Get the name of the mark immediately preceeding the mark to delete.
    SELECT mark_name, mark_time_id INTO v_previousMark, v_previousMarkTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id < p_markTimeId
      ORDER BY mark_time_id DESC
      LIMIT 1;
-- Get the name of the first mark succeeding the mark to delete.
    SELECT mark_name, mark_time_id INTO v_nextMark, v_nextMarkTimeId
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id > p_markTimeId
      ORDER BY mark_time_id
      LIMIT 1;
    IF NOT FOUND THEN
-- No next mark, so update the previous mark with NULL.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next = NULL
        WHERE mark_group = p_groupName
          AND mark_name = v_previousMark;
    ELSE
-- Update the previous mark by computing the sum of _log_stat_tbl() call's result for all relations that belonged
-- to the group at the time when the mark before the deleted mark had been set.
      UPDATE emaj.emaj_mark
        SET mark_log_rows_before_next =
          (SELECT sum(emaj._log_stat_tbl(emaj_relation, v_previousMarkTimeId, v_nextMarkTimeId))
             FROM emaj.emaj_relation
             WHERE rel_group = p_groupName
               AND rel_kind = 'r'
               AND rel_time_range @> v_previousMarkTimeId
          )
        WHERE mark_group = p_groupName
          AND mark_name = v_previousMark;
    END IF;
-- Reset the mark_logged_rlbk_target_mark column to null for other marks of the group that may have the deleted mark
-- as target mark from a previous logged rollback operation.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = p_groupName
        AND mark_logged_rlbk_target_mark = p_markName;
--
    RETURN;
  END;
$_delete_intermediate_mark_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_rename_mark_group(p_groupName TEXT, p_mark TEXT, p_newName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be in LOGGING or not.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly.
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', p_groupName, p_mark);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- Check the new mark name.
    SELECT emaj._check_new_mark(ARRAY[p_groupName], p_newName) INTO p_newName;
-- OK, update the emaj_mark table.
    UPDATE emaj.emaj_mark
      SET mark_name = p_newName
      WHERE mark_group = p_groupName
        AND mark_name = p_mark;
-- Also rename mark names recorded in the mark_logged_rlbk_target_mark column, if needed.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = p_newName
      WHERE mark_group = p_groupName
        AND mark_logged_rlbk_target_mark = p_mark;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'END', p_groupName, p_mark || ' renamed ' || p_newName);
--
    RETURN;
  END;
$emaj_rename_mark_group$;
COMMENT ON FUNCTION emaj.emaj_rename_mark_group(TEXT,TEXT,TEXT) IS
$$Renames a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_protect_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_protect_mark_group$
-- This function sets a protection on a mark for a group against accidental rollback.
-- However this doesn't block rollback simulations performed with the emaj_estimate_rollback_group() function.
-- Input: group name, mark to protect
-- Output: 1 if successful, 0 if the mark was already in protected state
-- The group must be ROLLBACKABLE and in LOGGING state.
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark, p_checkActive := TRUE) INTO p_mark;
-- OK, set the protection, if not already set, and return 1, or 0 if the mark was already protected.
    UPDATE emaj.emaj_mark
      SET mark_is_rlbk_protected = TRUE
      WHERE mark_group = p_groupName
        AND mark_name = p_mark
        AND NOT mark_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('PROTECT_MARK_GROUP', p_groupName, 'Mark ' || p_mark || ' ; status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_protect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_protect_mark_group(TEXT,TEXT) IS
$$Sets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_unprotect_mark_group(p_groupName TEXT, p_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_unprotect_mark_group$
-- This function unsets a protection on a mark for a group against accidental rollback.
-- Input: group name, mark to unprotect
-- Output: 1 if successful, 0 if the mark was already in unprotected state
-- The group must be ROLLBACKABLE and in LOGGING state.
  DECLARE
    v_status                 INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE,
                                    p_checkRollbackable := TRUE);
-- Check the mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_mark) INTO p_mark;
-- OK, unset the protection, and return 1, or 0 if the mark was already unprotected.
    UPDATE emaj.emaj_mark
      SET mark_is_rlbk_protected = FALSE
      WHERE mark_group = p_groupName
        AND mark_name = p_mark
        AND mark_is_rlbk_protected;
    GET DIAGNOSTICS v_status = ROW_COUNT;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('UNPROTECT_MARK_GROUP', p_groupName, 'Mark ' || p_mark || ' ; status ' || v_status);
--
    RETURN v_status;
  END;
$emaj_unprotect_mark_group$;
COMMENT ON FUNCTION emaj.emaj_unprotect_mark_group(TEXT,TEXT) IS
$$Unsets a protection against a rollback on a mark of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(p_groupName TEXT, p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                    p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
--        operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just rollback the group, with boolean: isLoggedRlbk = false, multiGroup = false.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(array[p_groupName], p_mark, FALSE, FALSE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT,BOOLEAN,TEXT) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                     p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
-- operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just rollback the groups, with boolean: isLoggedRlbk = false, multiGroup = true.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(p_groupNames, p_mark, FALSE, TRUE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT,BOOLEAN,TEXT) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(p_groupName TEXT, p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                           p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter group
--        operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just "logged-rollback" the group, with boolean: isLoggedRlbk = true, multiGroup = false.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(array[p_groupName], p_mark, TRUE, FALSE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT,BOOLEAN,TEXT) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN DEFAULT FALSE,
                                                            p_comment TEXT DEFAULT NULL, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automatically set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark to rollback to, boolean indicating whether the rollback may return to a mark set before an alter
--          group operation, optional comment
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  BEGIN
-- Just "logged-rollback" the groups, with boolean: isLoggedRlbk = true, multiGroup = true.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_groups(p_groupNames, p_mark, TRUE, TRUE, coalesce(p_isAlterGroupAllowed, FALSE), p_comment);
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT,BOOLEAN,TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_groups(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN, p_multiGroup BOOLEAN,
                                             p_isAlterGroupAllowed BOOLEAN, p_comment TEXT,
                                             OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_groups$
-- The function rollbacks all tables and sequences of a groups array up to a mark in the history.
-- It is called by emaj_rollback_group(), emaj_rollback_groups(), emaj_logged_rollback_group() and emaj_logged_rollback_group().
-- It effectively manages the rollback operation for each table or sequence.
-- Its activity is split into smaller functions that are also called by the parallel restore php function.
-- Input: group name,
--        mark to rollback to,
--        a boolean indicating whether the rollback is a logged rollback, a boolean indicating whether the function is a multi_group
--          function
--        a boolean saying whether the rollback may return to a mark set before an alter group operation
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
    v_rlbkId                 INT;
  BEGIN
-- Check the group names (the groups lock and the state checks are delayed for the later - needed for rollbacks generated by the web
-- application).
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If the group names array is null, immediately return.
    IF p_groupNames IS NULL THEN
       rlbk_severity = 'Notice'; rlbk_message = 0;
       RETURN NEXT;
      RETURN;
    END IF;
-- Check supplied parameter and prepare the rollback operation.
    SELECT emaj._rlbk_init(p_groupNames, p_mark, p_isLoggedRlbk, 1, p_multiGroup, p_isAlterGroupAllowed, p_comment) INTO v_rlbkId;
-- Lock all tables.
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
-- Set a rollback start mark if logged rollback.
    PERFORM emaj._rlbk_start_mark(v_rlbkId, p_multiGroup);
-- Execute the rollback planning.
    PERFORM emaj._rlbk_session_exec(v_rlbkId, 1);
-- Process sequences, complete the rollback operation and return the execution report.
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_end(v_rlbkId, p_multiGroup);
  END;
$_rlbk_groups$;

CREATE OR REPLACE FUNCTION emaj._rlbk_async(p_rlbkId INT, p_multiGroup BOOLEAN, OUT rlbk_severity TEXT, OUT rlbk_message TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_rlbk_async$
-- The function calls the main rollback functions following the initialisation phase.
-- It is only called by the Emaj_web client, in an asynchronous way, so that the rollback can be then monitored by the client.
-- Input: rollback identifier, and a boolean saying if the rollback is a logged rollback
-- Output: a set of records building the execution report, with a severity level (N-otice or W-arning) and a text message
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dbLinkCnxStatus        INT;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_is_dblink_used INTO v_isDblinkUsed
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- If dblink is used (which should always be true), try to open the first session connection (no error is issued if it is already opened).
    IF v_isDblinkUsed THEN
      SELECT p_status INTO v_dbLinkCnxStatus
        FROM emaj._dblink_open_cnx('rlbk#1', current_role);
      IF v_dbLinkCnxStatus < 0 THEN
        RAISE EXCEPTION '_rlbk_async: Error while opening the dblink session #1 (Status of the dblink connection attempt = %'
                        ' - see E-Maj documentation).',
          v_dbLinkCnxStatus;
      END IF;
    ELSE
      RAISE EXCEPTION '_rlbk_async: The function is called but dblink cannot be used. This is an error from the client side.';
    END IF;
-- Simply chain the internal functions.
    PERFORM emaj._rlbk_session_lock(p_rlbkId, 1);
    PERFORM emaj._rlbk_start_mark(p_rlbkId, p_multiGroup);
    PERFORM emaj._rlbk_session_exec(p_rlbkId, 1);
    RETURN QUERY
      SELECT *
        FROM emaj._rlbk_end(p_rlbkId, p_multiGroup);
  END;
$_rlbk_async$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN, p_nbSession INT, p_multiGroup BOOLEAN,
                                           p_isAlterGroupAllowed BOOLEAN, p_comment TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps (or NULL if there are some NULL input).
-- This function may be directly called by the Emaj_web client.
  DECLARE
    v_startTs                TIMESTAMPTZ;
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
    v_startTs = clock_timestamp();
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
        FROM emaj._dblink_open_cnx('rlbk#1', current_role);
-- For parallel rollback (i.e. when nb sessions > 1), the dblink connection must be ok.
      IF p_nbSession > 1 AND NOT v_isDblinkUsed THEN
        RAISE EXCEPTION '_rlbk_init: Cannot use several sessions without dblink connection capability. (Status of the dblink'
                        ' connection attempt = % - see E-Maj documentation)',
          v_dbLinkCnxStatus;
      END IF;
-- Create the row representing the rollback event in the emaj_rlbk table and get the rollback id back.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed, ' ||
               'rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, ' ||
               'rlbk_eff_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
               'rlbk_dblink_schema, rlbk_is_dblink_used, rlbk_start_datetime) ' ||
               'VALUES (' || quote_literal(p_groupNames) || ',' || quote_literal(v_markName) || ',' ||
               v_markTimeId || ',' || p_isLoggedRlbk || ',' || quote_nullable(p_isAlterGroupAllowed) || ',' ||
               quote_nullable(p_comment) || ',' || p_nbSession || ',' || v_nbTblInGroups || ',' || v_nbSeqInGroups || ',' ||
               CASE WHEN v_nbSeqInGroups = 0 THEN '0' ELSE 'NULL' END || ',''PLANNING'',' || v_histId || ',' ||
               quote_nullable(v_dbLinkSchema) || ',' || v_isDblinkUsed || ',' || quote_literal(v_startTs) || ') RETURNING rlbk_id';
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
               ', rlbk_status = ''LOCKING'', rlbk_end_planning_datetime = ''' || clock_timestamp() || '''' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema);
    END IF;
--
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

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

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(p_rlbkId INT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called to perform a rollback operation. It is also called to simulate a rollback operation and get its duration estimate.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viewer role can write into rollback tables, when estimating the rollback
--   duration, without having specific privileges on them to do it.
  DECLARE
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_nbSequence             INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_markTimeId             BIGINT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_fixed_table_rlbk       INTERVAL;
    v_effNbTable             INT;
    v_isEmajExtension        BOOLEAN;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimDurationRlbkSeq   INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    v_fkList                 TEXT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- Get the rollback characteristics for the emaj_rlbk event.
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session, rlbk_nb_sequence,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK'::emaj._rlbk_step_enum ELSE 'CTRL-DBLINK'::emaj._rlbk_step_enum END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_nbSequence,
           v_ctrlStepName
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
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::INTERVAL),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::INTERVAL)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk, v_fixed_table_rlbk;
-- Process the sequences, if any in the tables groups.
    IF v_nbSequence > 0 THEN
-- Compute the cost for each RLBK_SEQUENCES step and keep it for later.
      SELECT p_estimateMethod, p_estimatedDuration INTO v_estimMethod, v_estimDurationRlbkSeq
        FROM emaj._estimate_rlbk_step_duration('RLBK_SEQUENCES', NULL, NULL, NULL, v_nbSequence, v_fixed_step_rlbk, v_fixed_table_rlbk);
-- Insert a RLBK_SEQUENCES step into emaj_rlbk_plan.
-- Assign it the first session, so that it will be executed by the same session as the start mark set when the rollback is logged.
      INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_session, rlbp_batch_number,
                                       rlbp_estimated_quantity, rlbp_estimated_duration, rlbp_estimate_method)
        VALUES (p_rlbkId, 'RLBK_SEQUENCES', '', '', '', 1, 1,
                v_nbSequence, v_estimDurationRlbkSeq, v_estimMethod);
    END IF;
-- Insert into emaj_rlbk_plan a RLBK_TABLE step per table to effectively rollback.
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
-- If nothing has to be rolled back, return quickly
    IF v_nbSequence = 0 AND v_effNbTable = 0 THEN
      RETURN 0;
    END IF;
-- Insert into emaj_rlbk_plan a LOCK_TABLE step per table currently belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_is_repl_role_replica)
      SELECT p_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, '', FALSE
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_group = ANY(v_groupNames)
          AND rel_kind = 'r';
-- For tables to effectively rollback, add related steps (for FK, triggers, E-Maj logs) and adjust step properties.
    IF v_effNbTable > 0 THEN
-- Set the rlbp_is_repl_role_replica flag to TRUE for tables having all foreign keys linking tables:
--   1) in the rolled back groups and 2) with the same rollback target mark.
-- This only concerns emaj installed as an extension because one needs to be sure that the _rlbk_tbl() function is executed with a
-- superuser role (this is needed to set the session_replication_role to 'replica').
      v_isEmajExtension = EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj');
      IF v_isEmajExtension THEN
        WITH fkeys AS (
            -- the foreign keys belonging to tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, nf.nspname, tf.relname,
                   (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames) AND
                    rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)))
                     AS are_both_tables_in_groups_with_the_same_target_mark
                     -- (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames)) AS are_both_tables_in_groups,
                     -- rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)) AS have_both_tables_the_same_target_mark
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
          UNION ALL
            -- the foreign keys referencing tables to rollback
            SELECT rlbp_schema, rlbp_table, c.conname, n.nspname, t.relname,
                   (rel_group IS NOT NULL AND rel_group = ANY (v_groupNames) AND
                    rlbp_target_time_id = greatest(v_markTimeId, lower(rel_time_range)))
                     AS are_both_tables_in_groups_with_the_same_target_mark
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
                 count(*) AS nb_fk,
                 count(*) FILTER (WHERE are_both_tables_in_groups_with_the_same_target_mark) AS nb_fk_ok
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
            AND nb_fk = nb_fk_ok                                           -- all fkeys are linking tables 1) in the rolled back groups
                                                                           -- and 2) with the same rollback target mark
        ;
      END IF;
--
-- Group tables into batchs to process all tables linked by foreign keys as a batch.
--
-- Start at 2, 1 being allocated to the RLBK_SEQUENCES step, if exists.
      v_batchNumber = 2;
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
          PERFORM emaj._rlbk_set_batch_number(p_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table,
                                              r_tbl.rlbp_is_repl_role_replica);
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
          SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, c.condeferrable,
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
          SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, c.condeferrable,
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
-- Non deferrable fkeys and fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped.
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            p_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimDropFkDuration, v_estimDropFkMethod
            );
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_batch_number, rlbp_object_def,
            rlbp_estimated_quantity
            ) VALUES (
            p_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, pg_get_constraintdef(r_fk.conoid),
            r_fk.reltuples
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
-- Raise an exception if DROP_FK steps concerns inherited FK (i.e. FK set on a partitionned table)
      SELECT string_agg(rlbp_schema || '.' || rlbp_table || '.' || rlbp_object, ', ')
        INTO v_fkList
        FROM emaj.emaj_rlbk_plan r
             JOIN pg_catalog.pg_class t ON (t.relname = r.rlbp_table)
             JOIN pg_catalog.pg_namespace n ON (t.relnamespace  = n.oid AND n.nspname = r.rlbp_schema)
             JOIN pg_catalog.pg_constraint c ON (c.conrelid = t.oid AND c.conname = r.rlbp_object)
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step = 'DROP_FK'
          AND coninhcount > 0;
      IF v_fkList IS NOT NULL THEN
        RAISE EXCEPTION '_rlbk_planning: Some foreign keys (%) would need to be temporarily dropped during the operation. '
                        'But this would fail because they are inherited from a partitionned table.', v_fkList;
      END IF;
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
-- Allocate batches to sessions to spread the load on sessions as best as possible.
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys.
--
      IF v_nbSession = 1 THEN
-- In single session rollback, assign all steps to session 1 at once.
        UPDATE emaj.emaj_rlbk_plan
          SET rlbp_session = 1
          WHERE rlbp_rlbk_id = p_rlbkId;
      ELSE
-- Initialisation (for session 1, the RLBK_SEQUENCES step may have been already assigned).
        v_sessionLoad [1] = coalesce(v_estimDurationRlbkSeq, '0 SECONDS'::INTERVAL);
        FOR v_session IN 2 .. v_nbSession LOOP
          v_sessionLoad [v_session] = '0 SECONDS'::INTERVAL;
        END LOOP;
-- Allocate tables batch to sessions, starting with the heaviest to rollback batch.
        FOR r_batch IN
          SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
            FROM emaj.emaj_rlbk_plan
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_batch_number IS NOT NULL
              AND rlbp_session IS NULL
            GROUP BY rlbp_batch_number
            ORDER BY sum(rlbp_estimated_duration) DESC
        LOOP
-- Compute the least loaded session.
          v_minSession = 1; v_minDuration = v_sessionLoad [1];
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
      END IF;
    END IF;
-- Assign all not yet assigned 'LOCK_TABLE' steps to session 1.
    UPDATE emaj.emaj_rlbk_plan
      SET rlbp_session = 1
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_session IS NULL;
--
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate.
--
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

CREATE OR REPLACE FUNCTION emaj._rlbk_set_batch_number(p_rlbkId INT, p_batchNumber INT, p_schema TEXT, p_table TEXT,
                                                       p_isReplRoleReplica BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_set_batch_number$
-- This function updates the emaj_rlbk_plan table to set the batch_number for one table.
-- It also looks for other tables to rollback that are linked to this table by foreign keys to force them to be allocated to the same
--   batch number.
-- If the rollback operation for the table is performed with a session_replication_role set to replica, there is no need to force
--   referenced and referencing tables to be in the same batch.
-- The function is called by _rlbk_planning().
-- As those linked tables can also be linked to other tables by other foreign keys, the function has to be recursiley called.
  DECLARE
    v_fullTableName          TEXT;
    r_tbl                    RECORD;
  BEGIN
-- Set the batch number to this application table (there is a 'LOCK_TABLE' step and potentialy a 'RLBK_TABLE' step).
    UPDATE emaj.emaj_rlbk_plan
      SET rlbp_batch_number = p_batchNumber
      WHERE rlbp_rlbk_id = p_rlbkId
        AND rlbp_schema = p_schema
        AND rlbp_table = p_table;
-- If the rollback is not performed with session_replication_role set to replica, look for all other application tables linked by foreign
-- key relationships.
    IF NOT p_isReplRoleReplica THEN
      v_fullTableName = quote_ident(p_schema) || '.' || quote_ident(p_table);
      FOR r_tbl IN
        SELECT rlbp_schema, rlbp_table, rlbp_is_repl_role_replica
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = p_rlbkId
            AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_batch_number IS NULL            -- not yet allocated
            AND (rlbp_schema, rlbp_table) IN         -- list of (schema,table) linked to the original table by fkeys
              (  SELECT nspname, relname
                   FROM pg_catalog.pg_constraint
                        JOIN pg_catalog.pg_class t ON (t.oid = conrelid)
                        JOIN pg_catalog.pg_namespace n ON (relnamespace = n.oid)
                   WHERE contype = 'f'
                     AND confrelid = v_fullTableName::regclass
               UNION ALL
                 SELECT nspname, relname
                   FROM pg_catalog.pg_constraint
                        JOIN pg_catalog.pg_class t ON (t.oid = confrelid)
                        JOIN pg_catalog.pg_namespace n ON (relnamespace = n.oid)
                   WHERE contype = 'f'
                     AND conrelid = v_fullTableName::regclass
              )
      LOOP
-- Recursive call to allocate these linked tables to the same batch_number.
        PERFORM emaj._rlbk_set_batch_number(p_rlbkId, p_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table, r_tbl.rlbp_is_repl_role_replica);
      END LOOP;
    END IF;
--
    RETURN;
  END;
$_rlbk_set_batch_number$;

CREATE OR REPLACE FUNCTION emaj._estimate_rlbk_step_duration(p_step emaj._rlbk_step_enum, p_schema TEXT, p_table TEXT,
                                                             p_object TEXT, p_estimatedQuantity BIGINT,
                                                             p_defaultFixedCost INTERVAL, p_defaultVariableCost INTERVAL,
                                                             OUT p_estimateMethod INT, OUT p_estimatedDuration INTERVAL)
LANGUAGE plpgsql AS
$_estimate_rlbk_step_duration$
-- This function reads the rollback statistics in order to compute the duration estimate for elementary steps.
-- The function is called by _rlbk_planning().
-- The cost model depends on the step.
-- Input: step name, the schema, table and object names when it is relevant, the expected volume for the step,
--        the default fixed cost and the default variable cost from the emaj parameters
-- Output: the estimate method (1, 2 or 3), the duration estimate
  BEGIN
-- Initialize the output data.
    p_estimatedDuration = NULL;
-- Compute the duration estimate depending on the step.
    CASE
      WHEN p_step IN ('RLBK_TABLE', 'DELETE_LOG') THEN
-- For RLBK_TBL and DELETE_LOG, the estimate takes into account the estimated number of log rows to revert.
-- First look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude).
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 1
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step
            AND rlbt_quantity > 0
            AND rlbt_schema = p_schema
            AND rlbt_table = p_table
            AND rlbt_quantity / p_estimatedQuantity < 10
            AND p_estimatedQuantity / rlbt_quantity < 10;
        IF p_estimatedDuration IS NULL THEN
-- If there is no previous rollback operation with similar volume, take statistics for the table with all available volumes.
          SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
            INTO p_estimatedDuration, p_estimateMethod
            FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = p_step
              AND rlbt_quantity > 0
              AND rlbt_schema = p_schema
              AND rlbt_table = p_table;
          IF p_estimatedDuration IS NULL THEN
-- No statistics found for the step, so use supplied E-Maj parameters.
            p_estimatedDuration = p_defaultVariableCost * p_estimatedQuantity + p_defaultFixedCost;
            p_estimateMethod = 3;
          END IF;
        END IF;
--
      WHEN p_step = 'ADD_FK' THEN
        IF p_estimatedQuantity = 0 THEN
-- Empty table (or table not yet analyzed).
          p_estimatedDuration = p_defaultFixedCost;
          p_estimateMethod = 3;
        ELSE
-- Non empty table and statistics (with at least one row) are available.
          SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 1
            INTO p_estimatedDuration, p_estimateMethod
            FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = p_step
              AND rlbt_quantity > 0
              AND rlbt_schema = p_schema
              AND rlbt_table = p_table
              AND rlbt_object = p_object;
          IF p_estimatedDuration IS NULL THEN
-- Non empty table, but no statistic with at least one row is available => take the last duration for this fkey, if any.
            SELECT rlbt_duration, 2
              INTO p_estimatedDuration, p_estimateMethod
              FROM emaj.emaj_rlbk_stat
              WHERE rlbt_step = p_step
                AND rlbt_schema = p_schema
                AND rlbt_table = p_table
                AND rlbt_object = p_object
                AND rlbt_rlbk_id =
                      (SELECT max(rlbt_rlbk_id)
                         FROM emaj.emaj_rlbk_stat
                         WHERE rlbt_step = p_step
                           AND rlbt_schema = p_schema
                           AND rlbt_table = p_table
                           AND rlbt_object = p_object
                      );
            IF p_estimatedDuration IS NULL THEN
-- Definitely no statistics available, compute with the supplied default parameters.
              p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
              p_estimateMethod = 3;
            END IF;
          END IF;
        END IF;
--
      WHEN p_step = 'SET_FK_IMM' THEN
-- If fkey checks statistics are available for this fkey, compute an average cost.
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step
            AND rlbt_quantity > 0
            AND rlbt_schema = p_schema
            AND rlbt_table = p_table
            AND rlbt_object = p_object;
        IF p_estimatedDuration IS NULL THEN
-- No statistics are available for this fkey, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
--
      WHEN p_step = 'RLBK_SEQUENCES' THEN
-- If sequences rollback statistics are available, compute an average cost.
        SELECT sum(rlbt_duration) * (p_estimatedQuantity::float / sum(rlbt_quantity)), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step;
        IF p_estimatedDuration IS NULL THEN
-- No statistics are available for sequences rollbacks, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_estimatedQuantity * p_defaultVariableCost + p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
--
      ELSE
-- For other steps, there is no volume to consider.
-- Read statistics, if any, and compute an average cost.
        SELECT sum(rlbt_duration) / sum(rlbt_quantity), 2
          INTO p_estimatedDuration, p_estimateMethod
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = p_step;
        IF p_estimatedDuration IS NULL THEN
-- No statistics found for the step, so use the supplied E-Maj parameters.
          p_estimatedDuration = p_defaultFixedCost;
          p_estimateMethod = 3;
        END IF;
    END CASE;
--
    RETURN;
  END;
$_estimate_rlbk_step_duration$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(p_rlbkId INT, p_session INT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_lock$
-- It opens the session if needed, creates the session row in the emaj_rlbk_session table
-- and then locks all the application tables for the session.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he has not been granted privileges on tables.
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dblinkSchema           TEXT;
    v_dbLinkCnxStatus        INT;
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_nbRetry                SMALLINT = 0;
    v_ok                     BOOLEAN = FALSE;
    v_nbTbl                  INT;
    r_tbl                    RECORD;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema, rlbk_groups
      INTO v_isDblinkUsed, v_dblinkSchema, v_groupNames
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- For dblink session > 1, open the connection (the session 1 is already opened).
    IF p_session > 1 THEN
      SELECT p_status INTO v_dbLinkCnxStatus
        FROM emaj._dblink_open_cnx('rlbk#' || p_session, current_role);
      IF v_dbLinkCnxStatus < 0 THEN
        RAISE EXCEPTION '_rlbk_session_lock: Error while opening the dblink session #% (Status of the dblink connection attempt = %'
                        ' - see E-Maj documentation).',
          p_session, v_dbLinkCnxStatus;
      END IF;
-- ... and create the session row the emaj_rlbk_session table.
      v_stmt = 'INSERT INTO emaj.emaj_rlbk_session (rlbs_rlbk_id, rlbs_session, rlbs_txid, rlbs_start_datetime) ' ||
               'VALUES (' || p_rlbkId || ',' || p_session || ',' || txid_current() || ',' ||
                quote_literal(clock_timestamp()) || ') RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
    END IF;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'BEGIN', array_to_string(v_groupNames,','), 'Rollback session #' || p_session);
--
-- Acquire locks on tables.
--
-- In case of deadlock, retry up to 5 times.
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- Scan all tables of the session, in priority ascending order.
        FOR r_tbl IN
          SELECT quote_ident(rlbp_schema) || '.' || quote_ident(rlbp_table) AS fullName,
                 EXISTS
                   (SELECT 1
                      FROM emaj.emaj_rlbk_plan rlbp2
                      WHERE rlbp2.rlbp_rlbk_id = p_rlbkId
                        AND rlbp2.rlbp_session = p_session
                        AND rlbp2.rlbp_schema = rlbp1.rlbp_schema
                        AND rlbp2.rlbp_table = rlbp1.rlbp_table
                        AND rlbp2.rlbp_step = 'DIS_LOG_TRG'
                   ) AS disLogTrg
            FROM emaj.emaj_rlbk_plan rlbp1
                 JOIN emaj.emaj_relation ON (rel_schema = rlbp_schema AND rel_tblseq = rlbp_table AND upper_inf(rel_time_range))
            WHERE rlbp_rlbk_id = p_rlbkId
              AND rlbp_step = 'LOCK_TABLE'
              AND rlbp_session = p_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- Lock each table.
-- The locking level is EXCLUSIVE mode.
-- This blocks all concurrent update capabilities of all tables of the groups (including tables with no logged update to rollback),
-- in order to ensure a stable state of the group at the end of the rollback operation).
-- But these tables can be accessed by SELECT statements during the E-Maj rollback.
          EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE',
                         r_tbl.fullName);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- OK, all tables locked.
        v_ok = TRUE;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_session_lock: A deadlock has been trapped while locking tables for groups "%".',
            array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      PERFORM emaj._rlbk_error(p_rlbkId, '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables',
                               'rlbk#' || p_session);
      RAISE EXCEPTION '_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables for groups "%".',
        array_to_string(v_groupNames,',');
    END IF;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('LOCK_GROUP', 'END', array_to_string(v_groupNames,','),
              'Rollback session #' || p_session || ': ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_session_lock() for session ' || p_session || ': ' || SQLERRM, 'rlbk#' || p_session);
      RAISE;
  END;
$_rlbk_session_lock$;

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
    v_markComment            TEXT;
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
-- If the rollback is "logged", set a mark named with the pattern: 'RLBK_<rollback_id>_START'.
      v_markName = 'RLBK_' || p_rlbkId::text || '_START';
      v_markComment = 'Automatically set at rollback to mark ' || v_mark || ' start';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_markComment, p_multiGroup, TRUE, NULL, v_timeId, v_dblinkSchema);
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

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(p_rlbkId INT, p_session INT)
RETURNS VOID LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can perform the action on any application table.
  DECLARE
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_rlbkTimeId             BIGINT;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_nbSequence             INT;
    v_dblinkSchema           TEXT;
    v_isDblinkUsed           BOOLEAN;
    v_maxGlobalSeq           BIGINT;
    v_rlbkMarkTimeId         BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_effNbSequence          INT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- Get the rollback characteristics from the emaj_rlbk table.
    SELECT rlbk_groups, rlbk_mark, rlbk_time_id, rlbk_is_logged, rlbk_nb_session, rlbk_nb_sequence,
           rlbk_dblink_schema, rlbk_is_dblink_used, time_last_emaj_gid
      INTO v_groupNames, v_mark, v_rlbkTimeId, v_isLoggedRlbk, v_nbSession, v_nbSequence,
           v_dblinkSchema, v_isDblinkUsed, v_maxGlobalSeq
      FROM emaj.emaj_rlbk
           JOIN emaj.emaj_time_stamp ON (time_id = rlbk_time_id)
      WHERE rlbk_id = p_rlbkId;
-- Fetch the mark_time_id, the last global sequence at set_mark time for the first group of the groups array.
-- They all share the same values.
    SELECT mark_time_id, time_last_emaj_gid INTO v_rlbkMarkTimeId, v_lastGlobalSeq
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = v_groupNames[1]
        AND mark_name = v_mark;
-- Scan emaj_rlbp_plan to get all steps to process that have been assigned to this session, in batch_number and step order.
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_object, rlbp_object_def, rlbp_app_trg_type,
             rlbp_is_repl_role_replica, rlbp_target_time_id
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = p_rlbkId
          AND rlbp_step NOT IN ('LOCK_TABLE','CTRL-DBLINK','CTRL+DBLINK')
          AND rlbp_session = p_session
        ORDER BY rlbp_batch_number, rlbp_step, rlbp_table, rlbp_object
    LOOP
-- Update the emaj_rlbk_plan table to set the step start time.
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = clock_timestamp() ' ||
               ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
-- Process the step depending on its type.
      CASE r_step.rlbp_step
        WHEN 'RLBK_SEQUENCES' THEN
-- Rollback all sequences at once
-- If the sequence has been added to its group after the target rollback mark, rollback up to the corresponding alter_group time.
          SELECT sum(emaj._rlbk_seq(t.*, greatest(v_rlbkMarkTimeId, lower(t.rel_time_range)))) INTO v_effNbSequence
            FROM
              (SELECT *
                 FROM emaj.emaj_relation
                 WHERE upper_inf(rel_time_range)
                   AND rel_group = ANY (v_groupNames)
                   AND rel_kind = 'S'
                 ORDER BY rel_schema, rel_tblseq
              ) as t;
-- Record into emaj_rlbk the number of effectively rolled back sequences
          v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_sequence = ' || coalesce(v_effNbSequence, 0) ||
                   ' WHERE rlbk_id = ' || p_rlbkId || ' RETURNING 1';
          PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
        WHEN 'DIS_APP_TRG' THEN
-- Disable an application trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'SET_ALWAYS_APP_TRG' THEN
-- Set an application trigger as an ALWAYS trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I, ENABLE ALWAYS TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object);
        WHEN 'DIS_LOG_TRG' THEN
-- Disable a log trigger.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
        WHEN 'DROP_FK' THEN
-- Delete a foreign key.
          EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object);
        WHEN 'SET_FK_DEF' THEN
-- Set a foreign key deferred.
          EXECUTE format('SET CONSTRAINTS %I.%I DEFERRED',
                             r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'RLBK_TABLE' THEN
-- Process a table rollback.
-- For tables added to the group after the rollback target mark, get the last sequence value specific to each table.
          SELECT emaj._rlbk_tbl(emaj_relation.*,
                                CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                     ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp WHERE time_id = r_step.rlbp_target_time_id)
                                END,
                                v_maxGlobalSeq, v_nbSession, v_isLoggedRlbk, r_step.rlbp_is_repl_role_replica) INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema
              AND rel_tblseq = r_step.rlbp_table
              AND upper_inf(rel_time_range);
        WHEN 'DELETE_LOG' THEN
-- Process the deletion of log rows.
-- For tables added to the group after the rollback target mark, get the last sequence value specific to each table.
          SELECT emaj._delete_log_tbl(emaj_relation.*, r_step.rlbp_target_time_id, v_rlbkTimeId,
                                      CASE WHEN v_rlbkMarkTimeId = r_step.rlbp_target_time_id THEN v_lastGlobalSeq      -- common case
                                           ELSE (SELECT time_last_emaj_gid FROM emaj.emaj_time_stamp
                                                   WHERE time_id = r_step.rlbp_target_time_id) END)
            INTO v_nbRows
            FROM emaj.emaj_relation
            WHERE rel_schema = r_step.rlbp_schema
              AND rel_tblseq = r_step.rlbp_table
              AND upper_inf(rel_time_range);
        WHEN 'SET_FK_IMM' THEN
-- Set a foreign key immediate.
          EXECUTE format('SET CONSTRAINTS %I.%I IMMEDIATE',
                             r_step.rlbp_schema, r_step.rlbp_object);
        WHEN 'ADD_FK' THEN
-- Re-create a foreign key.
          EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object_def);
        WHEN 'ENA_APP_TRG' THEN
-- Enable an application trigger.
          EXECUTE format('ALTER TABLE %I.%I ENABLE %s TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_app_trg_type, r_step.rlbp_object);
        WHEN 'SET_LOCAL_APP_TRG' THEN
-- Reset an application trigger to its common type.
          EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I, ENABLE TRIGGER %I',
                         r_step.rlbp_schema, r_step.rlbp_table, r_step.rlbp_object, r_step.rlbp_object);
        WHEN 'ENA_LOG_TRG' THEN
-- Enable a log trigger.
          EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER emaj_log_trg',
                         r_step.rlbp_schema, r_step.rlbp_table);
      END CASE;
-- Update the emaj_rlbk_plan table to set the step duration as well as the quantity when it is relevant.
-- The computed duration does not include the time needed to update the emaj_rlbk_plan table,
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_duration = ' || quote_literal(clock_timestamp()) || ' - rlbp_start_datetime';
      IF r_step.rlbp_step = 'RLBK_TABLE' OR r_step.rlbp_step = 'DELETE_LOG' THEN
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbRows;
      END IF;
      IF r_step.rlbp_step = 'RLBK_SEQUENCES' THEN
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbSequence;
      END IF;
      v_stmt = v_stmt ||
               ' WHERE rlbp_rlbk_id = ' || p_rlbkId || ' AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_object = ' || quote_literal(r_step.rlbp_object) || ' RETURNING 1';
      PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
    END LOOP;
-- Update the emaj_rlbk_session table to set the timestamp representing the end of work for the session.
    v_stmt = 'UPDATE emaj.emaj_rlbk_session SET rlbs_end_datetime = clock_timestamp()' ||
             ' WHERE rlbs_rlbk_id = ' || p_rlbkId || ' AND rlbs_session = ' || p_session ||
             ' RETURNING 1';
    PERFORM emaj._dblink_sql_exec('rlbk#' || p_session, v_stmt, v_dblinkSchema);
-- Close the dblink connection, if any, for session > 1.
    IF v_isDblinkUsed AND p_session > 1 THEN
      PERFORM emaj._dblink_close_cnx('rlbk#' || p_session, v_dblinkSchema);
    END IF;
--
    RETURN;
-- Trap and record exception during the rollback operation.
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
      RAISE;
    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
      PERFORM emaj._rlbk_error(p_rlbkId, 'In _rlbk_session_exec() for session ' || p_session || ': ' || SQLERRM, 'rlbk#' || p_session);
      RAISE;
  END;
$_rlbk_session_exec$;

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
    v_timeId                 BIGINT;
    v_markComment            TEXT;
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
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_SEQUENCE' OR (rlchg_change_kind = 'MOVE_SEQUENCE' AND NOT new_group_is_rolledback) THEN
                    'The sequence ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since ' ||
                    to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'ADD_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has only been rolled back to its latest group attachment ('
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
                  WHEN rlchg_change_kind = 'REMOVE_TABLE' OR (rlchg_change_kind = 'MOVE_TABLE' AND NOT new_group_is_rolledback) THEN
                    'The table ' || quote_ident(rlchg_schema) || '.' || quote_ident(rlchg_tblseq) ||
                    ' has been left unchanged (not in group anymore since '
                    || to_char(time_clock_timestamp, 'YYYY/MM/DD HH:MI:SS TZ') || ')'
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
    IF v_isLoggedRlbk THEN
-- If the rollback is "logged", set a mark named with the pattern: 'RLBK_<rollback_id>_DONE'.
      v_markName = 'RLBK_' || p_rlbkId::text || '_DONE';
      v_markComment = 'Automatically set at rollback to mark ' || v_mark || ' end';
--   Get a timestamp via dblink, if it is usable,
      v_stmt = 'SELECT emaj._set_time_stamp(''' || v_function || ''', ''M'')';
      SELECT emaj._dblink_sql_exec('rlbk#1', v_stmt, v_dblinkSchema) INTO v_timeId;
--   ... and effectively set the mark.
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_markComment, p_multiGroup, TRUE, v_mark, v_timeId, v_dblinkSchema);
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

CREATE OR REPLACE FUNCTION emaj._rlbk_error(p_rlbkId INT, p_msg TEXT, p_cnxName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_error$
-- This function records a rollback error into the emaj_rlbk table, but only if a dblink connection is open.
-- Input: rollback identifier, message to record and dblink connection name.
-- If the rollback operation is already in aborted state, one keeps the emaj_rlbk data unchanged.
  DECLARE
    v_isDblinkUsed           BOOLEAN;
    v_dblinkSchema           TEXT;
    v_stmt                   TEXT;
  BEGIN
-- Get the dblink usage characteristics for the current rollback.
    SELECT rlbk_is_dblink_used, rlbk_dblink_schema INTO v_isDblinkUsed, v_dblinkSchema
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
-- If a dblink connection is open, update the emaj_rlbk table.
    IF v_isDblinkUsed THEN
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''ABORTED'', rlbk_messages = ARRAY[' || quote_literal(p_msg) ||
                '], rlbk_end_datetime =  clock_timestamp() ' ||
               'WHERE rlbk_id = ' || p_rlbkId || ' AND rlbk_status <> ''ABORTED'' RETURNING 1';
      PERFORM emaj._dblink_sql_exec(p_cnxName, v_stmt, v_dblinkSchema);
    END IF;
--
    RETURN;
  END;
$_rlbk_error$;

CREATE OR REPLACE FUNCTION emaj.emaj_cleanup_rollback_state()
RETURNS INT LANGUAGE plpgsql AS
$emaj_cleanup_rollback_state$
-- This function sets the status of not yet "COMMITTED" or "ABORTED" rollback events.
-- To perform its tasks, it just calls the _cleanup_rollback_state() function.
-- Input: no parameter
-- Output: number of updated rollback events
  BEGIN
    RETURN emaj._cleanup_rollback_state();
  END;
$emaj_cleanup_rollback_state$;
COMMENT ON FUNCTION emaj.emaj_cleanup_rollback_state() IS
$$Sets the status of pending E-Maj rollback events.$$;

CREATE OR REPLACE FUNCTION emaj._cleanup_rollback_state()
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_cleanup_rollback_state$
-- This function effectively cleans the rollback states up. It is called by the emaj_cleanup_rollback_state()
-- and by other emaj functions.
-- The rollbacks whose transaction(s) is/are active are left as is.
-- Among the others, those which are also visible in the emaj_hist table are set "COMMITTED",
--   while those which are not visible in the emaj_hist table are set "ABORTED".
-- Input: no parameter
-- Output: number of updated rollback events
-- The function is defined as SECURITY DEFINER so that emaj_viewer role can update emaj_rlbk and emaj_hist tables.
  DECLARE
    v_nbRlbk                 INT = 0;
    v_nbInProgressTx         INT;
    v_newStatus              emaj._rlbk_status_enum;
    r_rlbk                   RECORD;
  BEGIN
-- Scan all pending rollback events, counting their in-progress sessions.
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session
        FROM emaj.emaj_rlbk
             JOIN emaj.emaj_rlbk_session ON (rlbs_rlbk_id = rlbk_id AND rlbs_session = 1)
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED')  -- only pending rollback events
          AND rlbs_txid <> txid_current()                                       -- exclude the current tx, to not process the current
                                                                                --   rollback when start/stop marks are created
        ORDER BY rlbk_id
    LOOP
-- Get the number of unterminated sessions.
      SELECT count(*) INTO v_nbInProgressTx
        FROM emaj.emaj_rlbk_session
        WHERE rlbs_rlbk_id = r_rlbk.rlbk_id
          AND NOT txid_visible_in_snapshot(rlbs_txid, txid_current_snapshot());
-- Only process rollback id with no in-progress session.
      IF v_nbInProgressTx = 0 THEN
-- Try to lock the current rlbk_id, but skip it if it is not immediately possible to avoid deadlocks in rare cases.
        IF EXISTS
             (SELECT 0
                FROM emaj.emaj_rlbk
                WHERE rlbk_id = r_rlbk.rlbk_id
                FOR UPDATE SKIP LOCKED
             ) THEN
-- Look at the emaj_hist to find the trace of the rollback begin event.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_hist
                  WHERE hist_id = r_rlbk.rlbk_begin_hist_id
               ) THEN
-- If the emaj_hist rollback_begin event is visible, the rollback transaction has been committed.
-- So set the rollback event in emaj_rlbk as "COMMITTED".
          v_newStatus = 'COMMITTED';
          ELSE
-- Otherwise, set the rollback event in emaj_rlbk as "ABORTED"
            v_newStatus = 'ABORTED';
          END IF;
          UPDATE emaj.emaj_rlbk
            SET rlbk_status = v_newStatus
            WHERE rlbk_id = r_rlbk.rlbk_id;
          INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
            VALUES ('CLEANUP_RLBK_STATE', 'Rollback id ' || r_rlbk.rlbk_id, 'set to ' || v_newStatus);
          v_nbRlbk = v_nbRlbk + 1;
        END IF;
      END IF;
    END LOOP;
--
    RETURN v_nbRlbk;
  END;
$_cleanup_rollback_state$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_rollback(p_rlbkId INT, p_comment TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_comment_rollback$
-- This function sets or modifies a comment on a rollback by updating the rlbk_comment of the emaj_rlbk table.
-- Input: rollback identifier, comment
--   To reset an existing comment for a rollback, set the supplied comment to NULL.
  BEGIN
-- Check the rollback id.
    PERFORM 0
      FROM emaj.emaj_rlbk
      WHERE rlbk_id = p_rlbkId;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_rollback: The rollback identifier % does not exist.',
        p_rlbkId;
    END IF;
-- Update the rlbk_comment column from the emaj_rlbk table.
    UPDATE emaj.emaj_rlbk SET rlbk_comment = p_comment WHERE rlbk_id = p_rlbkId;
-- Insert the event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_ROLLBACK', p_rlbkId);
--
    RETURN;
  END;
$emaj_comment_rollback$;
COMMENT ON FUNCTION emaj.emaj_comment_rollback(INT, TEXT) IS
$$Sets a comment on an E-Maj Rollback.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_consolidate_rollback_group(p_groupName TEXT, p_endRlbkMark TEXT)
RETURNS BIGINT LANGUAGE plpgsql AS
$emaj_consolidate_rollback_group$
-- This function "consolidates" a rollback for a group. It transforms an already completed logged rollback into an unlogged rollback.
-- All marks and update logs between a mark used as reference by an unlogged rollback operation and the final mark set by this rollback
-- are suppressed.
-- The group may be in any state (logging or idle).
-- Input: group name, name of the final mark set by the rollback operation to consolidate
-- Output: number of sequences and tables effectively processed
  DECLARE
    v_firstMark              TEXT;
    v_lastMark               TEXT;
    v_nbMark                 INT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE);
-- Check the supplied end rollback mark name.
    SELECT emaj._check_mark_name(p_groupNames := ARRAY[p_groupName], p_mark := p_endRlbkMark) INTO v_lastMark;
-- Check that no group is damaged.
    PERFORM 0
      FROM emaj._verify_groups(ARRAY[p_groupName], TRUE);
-- Check the supplied mark is known as an end rollback mark.
    SELECT mark_logged_rlbk_target_mark INTO v_firstMark
      FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_name = v_lastMark;
    IF v_firstMark IS NULL THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The mark "%" for the group "%" is not an end rollback mark.',
        v_lastMark, p_groupName;
    END IF;
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'BEGIN', p_groupName, 'Erase all between marks ' || v_firstMark || ' and ' || v_lastMark);
-- Check the first mark really exists (it should, because deleting or renaming a mark must update the mark_logged_rlbk_mark_name column).
    IF NOT EXISTS
         (SELECT 0
            FROM emaj.emaj_mark
            WHERE mark_group = p_groupName
              AND mark_name = v_firstMark
         ) THEN
      RAISE EXCEPTION 'emaj_consolidate_rollback_group: The rollback target mark "%" for the group "%" has not been found.',
        v_firstMark, p_groupName;
    END IF;
-- Perform the consolidation operation.
    SELECT * INTO v_nbMark, v_nbTbl
      FROM emaj._delete_between_marks_group(p_groupName, v_firstMark, v_lastMark);
-- Get the number of sequences belonging to the group.
    SELECT group_nb_sequence INTO v_nbSeq
      FROM emaj.emaj_group
      WHERE group_name = p_groupName;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CONSOLIDATE_RLBK_GROUP', 'END', p_groupName,
              v_nbTbl || ' tables and ' || v_nbSeq || ' sequences processed ; ' || v_nbMark || ' marks deleted');
--
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_consolidate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_consolidate_rollback_group(TEXT,TEXT) IS
$$Consolidate a rollback for a group.$$;

CREATE OR REPLACE FUNCTION emaj._delete_between_marks_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                            OUT p_nbMark INT, OUT p_nbTbl INT)
RETURNS RECORD LANGUAGE plpgsql AS
$_delete_between_marks_group$
-- This function deletes all logs and intermediate marks set between two given marks.
-- The function is called by the emaj_consolidate_rollback_group() function.
-- It deletes rows corresponding to the marks to delete from emaj_mark and emaj_sequence.
-- It deletes rows from emaj_relation corresponding to old versions that become unreacheable.
-- It deletes rows from all concerned log tables.
-- It also manages sequence holes in emaj_seq_hole.
-- Input: group name, name of both marks that defines the range to delete.
-- Output: number of deleted marks, number of tables effectively processed (for which at least one log row has been deleted)
  DECLARE
    v_firstMarkGlobalSeq     BIGINT;
    v_firstMarkTimeId        BIGINT;
    v_lastMarkGlobalSeq      BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_nbUpd                  BIGINT;
    r_rel                    RECORD;
  BEGIN
-- Retrieve the timestamp and the emaj_gid value and the time stamp id of the first mark.
    SELECT time_last_emaj_gid, mark_time_id INTO v_firstMarkGlobalSeq, v_firstMarkTimeId
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = p_firstMark;
-- Retrieve the timestamp and the emaj_gid value and the time stamp id of the last mark.
    SELECT time_last_emaj_gid, mark_time_id INTO v_lastMarkGlobalSeq, v_lastMarkTimeId
      FROM emaj.emaj_mark
           JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
      WHERE mark_group = p_groupName
        AND mark_name = p_lastMark;
-- Delete rows from all log tables (no need to try to delete if v_firstMarkGlobalSeq and v_lastMarkGlobalSeq are equal).
    p_nbTbl = 0;
    IF v_firstMarkGlobalSeq < v_lastMarkGlobalSeq THEN
-- Loop on all tables that belonged to the group at the end of the period.
      FOR r_rel IN
        SELECT quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_table) AS log_table_name
          FROM emaj.emaj_relation
          WHERE rel_group = p_groupName
            AND rel_kind = 'r'
            AND rel_time_range @> v_lastMarkTimeId
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- Delete log rows.
        EXECUTE format('DELETE FROM %s WHERE emaj_gid > $1 AND emaj_gid <= $2',
                       r_rel.log_table_name)
          USING v_firstMarkGlobalSeq, v_lastMarkGlobalSeq;
        GET DIAGNOSTICS v_nbUpd = ROW_COUNT;
        IF v_nbUpd > 0 THEN
           p_nbTbl = p_nbTbl + 1;
        END IF;
      END LOOP;
    END IF;
-- Process emaj_seq_hole content.
-- Delete all existing holes (if any) between both marks for tables that belonged to the group at the end of the period.
    DELETE FROM emaj.emaj_seq_hole
      USING emaj.emaj_relation
      WHERE rel_group = p_groupName
        AND rel_kind = 'r'
        AND rel_time_range @> v_lastMarkTimeId
        AND rel_schema = sqhl_schema
        AND rel_tblseq = sqhl_table
        AND sqhl_begin_time_id >= v_firstMarkTimeId
        AND sqhl_begin_time_id < v_lastMarkTimeId;
-- Create holes representing the deleted logs.
    INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_begin_time_id, sqhl_end_time_id, sqhl_hole_size)
      SELECT rel_schema, rel_tblseq, greatest(v_firstMarkTimeId, lower(rel_time_range)), v_lastMarkTimeId,
             (SELECT tbl_log_seq_last_val
                FROM emaj.emaj_table
                WHERE tbl_schema = rel_schema
                  AND tbl_name = rel_tblseq
                  AND tbl_time_id = v_lastMarkTimeId
             )
             -
             (SELECT tbl_log_seq_last_val
                FROM emaj.emaj_table
                WHERE tbl_schema = rel_schema
                  AND tbl_name = rel_tblseq
                  AND tbl_time_id = greatest(v_firstMarkTimeId, lower(rel_time_range))
             )
        FROM emaj.emaj_relation
        WHERE rel_group = p_groupName
          AND rel_kind = 'r'
          AND rel_time_range @> v_lastMarkTimeId
          AND (SELECT tbl_log_seq_last_val
                 FROM emaj.emaj_table
                 WHERE tbl_schema = rel_schema
                   AND tbl_name = rel_tblseq
                   AND tbl_time_id = v_lastMarkTimeId
              )
              -
              (SELECT tbl_log_seq_last_val
                 FROM emaj.emaj_table
                 WHERE tbl_schema = rel_schema
                   AND tbl_name = rel_tblseq
                   AND tbl_time_id = greatest(v_firstMarkTimeId, lower(rel_time_range))
              ) > 0;
-- Now the sequences related to the mark to delete can be suppressed:
--   Delete first application sequences related data for the group (excluding the time range bounds).
    DELETE FROM emaj.emaj_sequence
      USING emaj.emaj_relation
      WHERE sequ_schema = rel_schema
        AND sequ_name = rel_tblseq
        AND rel_time_range @> v_lastMarkTimeId
        AND rel_group = p_groupName
        AND rel_kind = 'S'
        AND sequ_time_id > v_firstMarkTimeId
        AND sequ_time_id < v_lastMarkTimeId
        AND lower(rel_time_range) <> sequ_time_id;
--   Delete then tables related data for the group (excluding the time range bounds).
    DELETE FROM emaj.emaj_table
      USING emaj.emaj_relation
      WHERE tbl_schema = rel_schema
        AND tbl_name = rel_tblseq
        AND rel_time_range @> v_lastMarkTimeId
        AND rel_group = p_groupName
        AND rel_kind = 'r'
        AND tbl_time_id > v_firstMarkTimeId
        AND tbl_time_id < v_lastMarkTimeId
        AND tbl_time_id <@ rel_time_range
        AND tbl_time_id <> lower(rel_time_range);
-- In emaj_mark, reset the mark_logged_rlbk_target_mark column to null for marks of the group that will remain
--    and that may have one of the deleted marks as target mark from a previous logged rollback operation.
    UPDATE emaj.emaj_mark
      SET mark_logged_rlbk_target_mark = NULL
      WHERE mark_group = p_groupName
        AND mark_time_id >= v_lastMarkTimeId
        AND mark_logged_rlbk_target_mark IN
              (SELECT mark_name
                 FROM emaj.emaj_mark
                 WHERE mark_group = p_groupName
                   AND mark_time_id > v_firstMarkTimeId
                   AND mark_time_id < v_lastMarkTimeId
              );
-- Set the mark_log_rows_before_next of the first mark to 0.
    UPDATE emaj.emaj_mark
      SET mark_log_rows_before_next = 0
      WHERE mark_group = p_groupName
        AND mark_name = p_firstMark;
-- And finaly delete all intermediate marks.
    DELETE FROM emaj.emaj_mark
      WHERE mark_group = p_groupName
        AND mark_time_id > v_firstMarkTimeId
        AND mark_time_id < v_lastMarkTimeId;
    GET DIAGNOSTICS p_nbMark = ROW_COUNT;
--
    RETURN;
  END;
$_delete_between_marks_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_consolidable_rollbacks()
RETURNS SETOF emaj.emaj_consolidable_rollback_type LANGUAGE plpgsql AS
$emaj_get_consolidable_rollbacks$
-- This function returns the list of logged rollback operations that can be consolidated, defined as a marks range for a group.
-- It doesn't need input parameter.
-- It returns a set of emaj_consolidable_rollback_type records, sorted by ascending rollback time.
-- The cons_group and cons_end_rlbk_mark_name returned columns can be used as input parameters for the emaj_consolidate_rollback_group()
-- function.
  BEGIN
-- Search and return all marks range corresponding to any logged rollback operation.
    RETURN QUERY
      SELECT m1.mark_group AS cons_group,
             m2.mark_name AS cons_target_rlbk_mark_name, m2.mark_time_id AS cons_target_rlbk_mark_time_id,
             m1.mark_name AS cons_end_rlbk_mark_name, m1.mark_time_id AS cons_end_rlbk_mark_time_id,
             cast(coalesce(
                  (SELECT sum(emaj._log_stat_tbl(emaj_relation,
                                                 greatest(m2.mark_time_id, lower(rel_time_range)),
                                                 m1.mark_time_id))
                     FROM emaj.emaj_relation
                           -- for tables belonging to the group at the rollback time
                     WHERE rel_group = m1.mark_group
                       AND rel_kind = 'r'
                       AND rel_time_range @> m1.mark_time_id
                  ), 0) AS BIGINT) AS cons_rows,
             cast(
                   (SELECT count(*)
                      FROM emaj.emaj_mark m3
                      WHERE m3.mark_group = m1.mark_group
                        AND m3.mark_time_id > m2.mark_time_id
                        AND m3.mark_time_id < m1.mark_time_id
                   ) AS INT
                 ) AS cons_marks
        FROM emaj.emaj_mark m1
          JOIN emaj.emaj_mark m2 ON (m2.mark_name = m1.mark_logged_rlbk_target_mark AND m2.mark_group = m1.mark_group)
          WHERE m1.mark_logged_rlbk_target_mark IS NOT NULL
          ORDER BY m1.mark_time_id;
  END;
$emaj_get_consolidable_rollbacks$;
COMMENT ON FUNCTION emaj.emaj_get_consolidable_rollbacks() IS
$$Returns the list of logged rollback operations that can be consolidated.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_reset_group(p_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves.
-- It calls the emaj_rst_group function to do the job.
-- Input: group name
-- Output: number of processed tables
  DECLARE
    v_nbRel                  INT = 0;
    v_eventTriggers          TEXT[];
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('RESET_GROUP', 'BEGIN', p_groupName);
-- Check the group name.
    PERFORM emaj._check_group_names(p_groupNames := ARRAY[p_groupName], p_mayBeNull := FALSE, p_lockGroups := TRUE, p_checkIdle := TRUE);
-- Perform the reset operation.
    SELECT emaj._reset_groups(ARRAY[p_groupName]) INTO v_nbRel;
-- Drop the log schemas that would have been emptied by the _reset_groups() call.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
    PERFORM emaj._drop_log_schemas('RESET_GROUP', FALSE);
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RESET_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics about logged data changes between 2 marks or between a mark and the current state for a single group.
-- It is used to quickly get simple statistics of changes logged between 2 marks.
-- Input: group name, the 2 mark names defining a range
-- Output: set of stat rows by table (including tables without any data change)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_last_mark,
             stat_last_mark_datetime, stat_rows
        FROM emaj._log_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged data changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_groups$
-- This function returns statistics about logged data changes between 2 marks or between a mark and the current state for a groups array.
-- It is used to quickly get simple statistics of changes logged between 2 marks.
-- Input: group names array, the 2 mark names defining a range
-- Output: set of stat rows by table (including tables without any data change)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_last_mark,
             stat_last_mark_datetime, stat_rows
        FROM emaj._log_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns global statistics about logged data changes between 2 marks for several groups.$$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_group$
-- This function returns statistics on logged data changes between 2 marks as viewed through the log tables for one tables group.
-- It provides much precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group name, the 2 marks names defining a range
-- Output: set of stat rows by table, user and SQL type
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_last_mark, stat_last_mark_datetime,
             stat_role, stat_verb, stat_rows
        FROM emaj._detailed_log_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged data changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_detailed_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_detailed_log_stat_type LANGUAGE plpgsql AS
$emaj_detailed_log_stat_groups$
-- This function returns statistics on logged data changes between 2 marks as viewed through the log tables for several tables groups.
-- It provides much precise information than emaj_log_stat_group but it needs to scan log tables in order to provide these data.
-- So the response time may be much longer.
-- Input: group names array, the 2 marks names defining a range
-- Output: set of stat rows by table, user and SQL type
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_table, stat_first_mark, stat_first_mark_datetime, stat_last_mark, stat_last_mark_datetime,
             stat_role, stat_verb, stat_rows
        FROM emaj._detailed_log_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_detailed_log_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns detailed statistics about logged data changes between 2 marks for several groups.$$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_sequence_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_type LANGUAGE plpgsql AS
$emaj_sequence_stat_group$
-- This function returns statistics on sequences changes recorded between 2 marks or between a mark and the current state
--   for a single group.
-- Input: group name, both mark names defining a range
-- Output: set of stats by sequence (including unchanged sequences)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_mark_datetime, stat_last_mark,
             stat_last_mark_datetime, stat_increments, stat_has_structure_changed
        FROM emaj._sequence_stat_groups(ARRAY[p_groupName], FALSE, p_firstMark, p_lastMark);
  END;
$emaj_sequence_stat_group$;
COMMENT ON FUNCTION emaj.emaj_sequence_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about recorded sequences changes between 2 marks for a single group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_sequence_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT)
RETURNS SETOF emaj.emaj_sequence_stat_type LANGUAGE plpgsql AS
$emaj_sequence_stat_groups$
-- This function returns statistics on sequences changes recorded between 2 marks or between a mark and the current state
--   for a groups array.
-- Input: group names array, both mark names defining a range
-- Output: set of stats by sequence (including unchanged sequences)
  BEGIN
    RETURN QUERY
      SELECT stat_group, stat_schema, stat_sequence, stat_first_mark, stat_first_mark_datetime, stat_last_mark,
             stat_last_mark_datetime, stat_increments, stat_has_structure_changed
        FROM emaj._sequence_stat_groups(p_groupNames, TRUE, p_firstMark, p_lastMark);
  END;
$emaj_sequence_stat_groups$;
COMMENT ON FUNCTION emaj.emaj_sequence_stat_groups(TEXT[],TEXT,TEXT) IS
$$Returns global statistics about recorded sequences changes between 2 marks for several groups.$$;

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

CREATE OR REPLACE FUNCTION emaj._get_sequences_last_value(p_groupsIncludeFilter TEXT, p_groupsExcludeFilter TEXT,
                                                          p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                          p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                          OUT p_key TEXT, OUT p_value TEXT)
RETURNS SETOF RECORD LANGUAGE plpgsql AS
$_get_sequences_last_value$
-- The function is used by the emajStat client and Emaj_web to monitor the recorded tables and/or sequences changes.
-- It returns in textual format:
--    - the last_value of selected log and application sequences,
--    - the last value of 2 emaj technical sequences to detect tables groups changes or marks set,
--    - the current timestamp as EPOCH
-- The function traps the execution error that may happen when table groups structure are changing.
-- In this case, it just returns a key set to 'error' with the SQLSTATE (typically XX000) as value.
-- This error trapping is the main reason for this function exists. Otherwise, the client could just execute the main query.
-- Input: include and exclude regexps to filter tables groups, tables and sequences.
-- Output: a set of (key, value) records.
  DECLARE
    v_stmt                   TEXT;
  BEGIN
-- Set the default value for NULL filters (to include all and exclude nothing).
    p_groupsIncludeFilter = coalesce(p_groupsIncludeFilter, '.*');
    p_groupsExcludeFilter = coalesce(p_groupsExcludeFilter, '');
    p_tablesIncludeFilter = coalesce(p_tablesIncludeFilter, '.*');
    p_tablesExcludeFilter = coalesce(p_tablesExcludeFilter, '');
    p_sequencesIncludeFilter = coalesce(p_sequencesIncludeFilter, '.*');
    p_sequencesExcludeFilter = coalesce(p_sequencesExcludeFilter, '');
-- Build the statement to execute.
    v_stmt = $$
      WITH filtered_group AS (
        SELECT group_name
          FROM emaj.emaj_group
          WHERE group_name ~ $1
            AND ($2 = '' OR group_name !~ $2)
        )
        SELECT 'current_epoch', extract('EPOCH' FROM statement_timestamp())::TEXT
      UNION ALL
        SELECT 'emaj.emaj_time_stamp_time_id_seq', last_value::TEXT FROM emaj.emaj_time_stamp_time_id_seq
      UNION ALL
        SELECT 'emaj.emaj_global_seq', last_value::TEXT FROM emaj.emaj_global_seq
    $$;
    IF p_tablesExcludeFilter != '.*' THEN
      v_stmt = v_stmt || $$
      UNION ALL
        SELECT rel_schema || '.' || rel_tblseq, emaj._get_log_sequence_last_value(rel_log_schema, rel_log_sequence)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'r'
           AND (rel_schema || '.' || rel_tblseq) ~ $3
           AND ($4 = '' OR (rel_schema || '.' || rel_tblseq) !~ $4)
      $$;
    END IF;
    IF p_sequencesExcludeFilter != '.*' THEN
      v_stmt = v_stmt || $$
      UNION ALL
        SELECT rel_schema || '.' || rel_tblseq, emaj._get_app_sequence_last_value(rel_schema, rel_tblseq)::TEXT AS seq_current
          FROM emaj.emaj_relation
            JOIN filtered_group ON (group_name = rel_group)
         WHERE upper_inf(rel_time_range)
           AND rel_kind = 'S'
           AND (rel_schema || '.' || rel_tblseq) ~ $5
           AND ($6 = '' OR (rel_schema || '.' || rel_tblseq) !~ $6)
      $$;
    END IF;
    BEGIN
      RETURN QUERY EXECUTE v_stmt
        USING p_groupsIncludeFilter, p_groupsExcludeFilter,
              p_tablesIncludeFilter, p_tablesExcludeFilter,
              p_sequencesIncludeFilter, p_sequencesExcludeFilter;
    EXCEPTION WHEN OTHERS THEN
-- If an error occurs, just return an error key with the SQLSTATE.
      RETURN QUERY SELECT 'error', SQLSTATE;
    END;
--
    RETURN;
  END;
$_get_sequences_last_value$;

CREATE OR REPLACE FUNCTION emaj.emaj_estimate_rollback_group(p_groupName TEXT, p_mark TEXT, p_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql AS
$emaj_estimate_rollback_group$
-- This function computes an approximate duration of a rollback to a predefined mark for a group.
-- It uses the _estimate_rollback_group() function to effectively compute this estimate.
-- Input: group name, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval
  BEGIN
    RETURN emaj._estimate_rollback_groups(ARRAY[p_groupName], FALSE, p_mark, p_isLoggedRlbk);
  END;
$emaj_estimate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_group(TEXT,TEXT,BOOLEAN) IS
$$Estimates the duration of a potential rollback for a tables group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_estimate_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql AS
$emaj_estimate_rollback_groups$
-- This function computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It uses the _estimate_rollback_group() function to effectively compute this estimate.
-- Input: a group names array, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval
  BEGIN
    RETURN emaj._estimate_rollback_groups(p_groupNames, TRUE, p_mark, p_isLoggedRlbk);
  END;
$emaj_estimate_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Estimates the duration of a potential rollback for a set of tables groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_mark TEXT, p_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary.
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: group names array, a boolean indicating whether the groups array may contain several groups,
--        the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
  BEGIN
-- Check the group names (the groups state checks are delayed for later).
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If the group names array is null, immediately return NULL.
    IF p_groupNames IS NULL THEN
      RETURN NULL;
    END IF;
-- Check supplied group names and mark parameters with the isAlterGroupAllowed and isRollbackSimulation flags set to true.
    SELECT emaj._rlbk_check(p_groupNames, p_mark, TRUE, TRUE) INTO v_markName;
-- Compute the number of tables and sequences contained in groups to rollback.
    SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTbl, v_nbSeq
      FROM emaj.emaj_group
      WHERE group_name = ANY(p_groupNames);
-- Compute a random negative rollback-id (not to interfere with ids of real rollbacks).
    SELECT (random() * -2147483648)::INT INTO v_rlbkId;
--
-- Simulate a rollback planning.
--
    BEGIN
-- Insert a row into the emaj_rlbk table for this simulated rollback operation.
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_time_id, rlbk_is_logged, rlbk_is_alter_group_allowed,
                                  rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence)
        SELECT v_rlbkId, p_groupNames, v_markName, mark_time_id, p_isLoggedRlbk, FALSE, 1, v_nbTbl, v_nbSeq
          FROM emaj.emaj_mark
          WHERE mark_group = p_groupNames[1]
            AND mark_name = v_markName;
-- Call the _rlbk_planning function to build the simulated plan with the duration estimate for elementary steps.
      PERFORM emaj._rlbk_planning(v_rlbkId);
-- Compute the sum of the duration estimates of all elementary steps (except LOCK_TABLE).
      SELECT coalesce(sum(rlbp_estimated_duration), '0 SECONDS'::INTERVAL) INTO v_estimDuration
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId
          AND rlbp_step <> 'LOCK_TABLE';
-- Cancel the effect of the rollback planning.
      RAISE EXCEPTION '';
    EXCEPTION
      WHEN RAISE_EXCEPTION THEN                 -- catch the raised exception and continue
    END;
-- Get the "fixed_table_rollback_duration" parameter from the emaj_param table.
    SELECT coalesce(
             (SELECT param_value_interval
                FROM emaj.emaj_param
                WHERE param_key = 'fixed_table_rollback_duration'
             ),'1 millisecond'::INTERVAL) INTO v_fixed_table_rlbk;
-- Compute the final estimated duration, by adding the minimum cost for LOCK_TABLE steps.
    v_estimDuration = v_estimDuration + (v_nbTbl * v_fixed_table_rlbk);
--
    RETURN v_estimDuration;
  END;
$_estimate_rollback_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$emaj_rollback_activity$
-- This function returns the list of rollback operations currently in execution, with information about their progress.
-- It returns a set of emaj_rollback_activity_type records.
  BEGIN
-- Cleanup the freshly completed rollback operations, if any.
    PERFORM emaj._cleanup_rollback_state();
-- And retrieve information regarding the rollback operations that are always in execution.
    RETURN QUERY
      SELECT *
        FROM emaj._rollback_activity();
  END;
$emaj_rollback_activity$;
COMMENT ON FUNCTION emaj.emaj_rollback_activity() IS
$$Returns the list of rollback operations currently in execution, with information about their progress.$$;

CREATE OR REPLACE FUNCTION emaj._rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$_rollback_activity$
-- This function effectively builds the list of rollback operations currently in execution.
-- It is called by the emaj_rollback_activity() function.
-- This is a separate function to help in testing the feature (avoiding the effects of _cleanup_rollback_state()).
-- The number of parallel rollback sessions is not taken into account here,
--   as it is difficult to estimate the benefit brought by several parallel sessions.
  DECLARE
    v_now                    TIMESTAMPTZ;        -- The clock timestamp at the function entry
    v_ipsDuration            INTERVAL;           -- In Progress Steps Duration
    v_nyssDuration           INTERVAL;           -- Not Yes Started Steps Duration
    v_nbNyss                 INT;                -- Number of Net Yes Started Steps
    v_ctrlDuration           INTERVAL;
    v_currentTotalEstimate   INTERVAL;
    r_rlbk                   emaj.emaj_rollback_activity_type;
  BEGIN
    v_now = clock_timestamp();
-- Retrieve all not completed rollback operations (ie in 'PLANNING', 'LOCKING' or 'EXECUTING' state).
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_groups, rlbk_mark, tm.time_clock_timestamp, rlbk_is_logged, rlbk_is_alter_group_allowed,
             rlbk_comment, rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_eff_nb_sequence,
             rlbk_status, rlbk_start_datetime,
             rlbk_end_planning_datetime - rlbk_start_datetime AS rlbk_planning_duration,
             rlbk_end_locking_datetime - rlbk_end_planning_datetime AS rlbk_locking_duration,
             v_now - rlbk_start_datetime AS "elapse", NULL, 0
        FROM emaj.emaj_rlbk
             JOIN emaj.emaj_time_stamp tm ON (tm.time_id = rlbk_mark_time_id)
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING')
        ORDER BY rlbk_id
    LOOP
-- Compute the estimated remaining duration for rollback operations in 'PLANNING' state, the remaining duration is NULL.
      IF r_rlbk.rlbk_status IN ('LOCKING', 'EXECUTING') THEN
-- Estimated duration of remaining work of in progress steps.
        SELECT coalesce(
               sum(CASE WHEN rlbp_start_datetime + rlbp_estimated_duration - v_now > '0'::INTERVAL
                        THEN rlbp_start_datetime + rlbp_estimated_duration - v_now
                        ELSE '0'::INTERVAL END),'0'::INTERVAL) INTO v_ipsDuration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_start_datetime IS NOT NULL
            AND rlbp_duration IS NULL;
-- Estimated duration and number of not yet started steps.
        SELECT coalesce(sum(rlbp_estimated_duration),'0'::INTERVAL), count(*) INTO v_nyssDuration, v_nbNyss
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_start_datetime IS NULL
            AND rlbp_step NOT IN ('CTRL-DBLINK','CTRL+DBLINK');
-- Estimated duration of inter-step duration for not yet started steps.
        SELECT coalesce(sum(rlbp_estimated_duration) * v_nbNyss / sum(rlbp_estimated_quantity),'0'::INTERVAL)
          INTO v_ctrlDuration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
            AND rlbp_step IN ('CTRL-DBLINK','CTRL+DBLINK');
-- Update the global remaining duration estimate.
        r_rlbk.rlbk_remaining = v_ipsDuration + v_nyssDuration + v_ctrlDuration;
      END IF;
-- Compute the completion pct for rollback operations in 'PLANNING' or 'LOCKING' state, the completion_pct = 0.
      IF r_rlbk.rlbk_status = 'EXECUTING' THEN
-- First compute the new total duration estimate, using the estimate of the remaining work,
        SELECT v_now - rlbk_start_datetime + r_rlbk.rlbk_remaining INTO v_currentTotalEstimate
          FROM emaj.emaj_rlbk
          WHERE rlbk_id = r_rlbk.rlbk_id;
-- ... and then the completion pct.
        IF v_currentTotalEstimate <> '0'::INTERVAL THEN
          SELECT 100 - (extract(epoch FROM r_rlbk.rlbk_remaining) * 100
                      / extract(epoch FROM v_currentTotalEstimate))::SMALLINT
            INTO r_rlbk.rlbk_completion_pct;
        END IF;
      END IF;
      RETURN NEXT r_rlbk;
    END LOOP;
--
    RETURN;
  END;
$_rollback_activity$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                                p_optionsList TEXT, p_tblseqs TEXT[])
RETURNS TEXT LANGUAGE plpgsql AS
$emaj_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- The SQL statements are stored a temporary table named emaj_temp_sql.
-- Some options may be set to customize the SQL generation (here in alphabetic order):
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - ORDER_BY=PK|TIME defines the data sort criteria in the output for tables (default depends on the consolidation level)
--   - SEQUENCES_ONLY filters only sequences
--   - PSQL_COPY_DIR generates a psql \copy meta-command for each statement, using the directory name given by the option
--   - PSQL_COPY_OPTIONS defines the options to use for the psql \copy meta-command
--   - SQL_FORMAT=RAW|PRETTY defines how the generated SQL will be formatted
--   - TABLES_ONLY filters only tables
-- Complex options such as lists or directory names must be set between ().
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations).
-- Output: Message with the number of generated SQL statements.
  DECLARE
    v_nbStmt                 INT;
  BEGIN
-- Call the _gen_sql_dump_changes_group() function to proccess options and build the SQL statements.
    SELECT p_nbStmt
      INTO v_nbStmt
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, TRUE);
-- Just add an index on the temporary table to help its use by the client.
    CREATE INDEX ON emaj_temp_sql(sql_stmt_number, sql_line_number);
-- Return a formatted message.
    RETURN format('%s SQL statements are available in the "emaj_temp_sql" temporary table', v_nbStmt);
  END;
$emaj_gen_sql_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[]) IS
$$Generate SQL statements to dump recorded changes between two marks for application tables and sequences of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT,
                                                                p_optionsList TEXT, p_tblseqs TEXT[], p_scriptLocation TEXT)
RETURNS TEXT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_gen_sql_dump_changes_group$
-- This function returns SQL statements that read log tables and sequences states to show the data changes recorded between 2 marks for
--   a group.
-- The SQL statements are stored into a flat file with a COPY TO statement, using a location provided by the caller.
-- Some options may be set to customize the SQL generation (here in alphabetic order):
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - ORDER_BY=PK|TIME defines the data sort criteria in the output for tables (default depends on the consolidation level)
--   - SEQUENCES_ONLY filters only sequences
--   - PSQL_COPY_DIR generates a psql \copy meta-command for each statement, using the directory name given by the option
--   - PSQL_COPY_OPTIONS defines the options to use for the psql \copy meta-command
--   - SQL_FORMAT=RAW|PRETTY defines how the generated SQL will be formatted
--   - TABLES_ONLY filters only tables
-- Complex options such as lists or directory names must be set between ().
-- It's users responsability to create the directory containing the output file before the function call (with proper permissions
--   allowing the cluster to write into).
-- The SQL statements are generated by the _gen_sql_dump_changes_group() function.
-- Input: group name, 2 mark names defining a range (The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark),
--        options (a comma separated options list),
--        array of schema qualified table and sequence names to process (NULL to process all relations),
--        the absolute pathname of the file that will hold the result (NULL to get the result from a temporary table).
-- Output: Message with the number of generated SQL statements.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_nbStmt                 INT;
    v_isPsqlCopy             BOOLEAN;
  BEGIN
-- Insert the BEGIN event into the history, but only if an external will be produced.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_DUMP_CHANGES_GROUP', 'BEGIN', p_groupName,
       'From mark ' || coalesce(p_firstMark, '') || ' to ' || coalesce(p_lastMark, '') ||
       ' towards ' || p_scriptLocation);
-- Check the script location is not null.
    IF p_scriptLocation IS NULL THEN
      RAISE EXCEPTION 'emaj_gen_sql_dump_changes_group: The output script location parameter cannot be NULL.';
    END IF;
-- Call the _gen_sql_dump_changes_group() function to proccess options and build the SQL statements.
    SELECT p_nbStmt, p_isPsqlCopy
      FROM emaj._gen_sql_dump_changes_group(p_groupName, p_firstMark, p_lastMark, p_optionsList, p_tblseqs, TRUE)
      INTO v_nbStmt, v_isPsqlCopy;
-- Process the emaj_temp_sql temporary table.
-- An output file is supplied. So write the SQL script into the external file and drop the temporary table.
    IF v_isPsqlCopy THEN
-- If there are psql \copy meta-commands, remove the doubled antislash characters.
      BEGIN
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number)' ||
                      ' TO PROGRAM ''sed "s/\\\\\\\\/\\\\/g" >%s'' ',
                      p_scriptLocation);
      EXCEPTION
        WHEN OTHERS THEN
--   If it fails (typically because the sed command is not available), write the script as is, and warn the user about the doubled
--     antislashes he has to remove.
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number) TO %L',
                      p_scriptLocation);
          RAISE WARNING 'emaj_gen_sql_dump_changes_group: the shell sed command does not seem to exist.'
                        ' Generated doubled antislash characters will need to be removed manually.';
      END;
    ELSE
-- There is no psql meta-command so no antislashes to remove.
      EXECUTE format ('COPY (SELECT sql_text FROM emaj_temp_sql ORDER BY sql_stmt_number, sql_line_number) TO %L',
                      p_scriptLocation);
    END IF;
    DROP TABLE IF EXISTS emaj_temp_sql;
-- Insert a END event into the history if a file has been generated.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_DUMP_CHANGES_GROUP', 'END', p_groupName, v_nbStmt || ' generated SQL statements');
-- Return a formatted message.
    RETURN format('%s SQL statements have been written into the "%s" file', v_nbStmt, p_scriptLocation);
  END;
$emaj_gen_sql_dump_changes_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(TEXT,TEXT,TEXT,TEXT,TEXT[],TEXT) IS
$$Generate SQL statements into a file to dump recorded changes between two marks for application tables and sequences of an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_optionsList TEXT,
                                                        p_tblseqs TEXT[], p_dir TEXT)
RETURNS TEXT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_dump_changes_group$
-- This function reads log tables and sequences states to export into files the data changes recorded between 2 marks for a group.
-- The function performs COPY TO statements, using the options provided by the caller.
-- Some options may be set to customize the changes dump:
--   - COLS_ORDER=LOG_TABLE|PK defines the columns order in the output for tables (default depends on the consolidation level)
--   - CONSOLIDATION=NONE|PARTIAL|FULL allows to get a consolidated view of changes for each PK during the mark range
--   - COPY_OPTIONS=(options) sets the options to use for COPY TO statements
--   - EMAJ_COLUMNS=ALL|MIN|(columns list) restricts the emaj columns recorded into the output (default depends on the consolidation level)
--   - NO_EMPTY_FILES ... removes empty files
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
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_copyOptions            TEXT;
    v_noEmptyFiles           BOOLEAN;
    v_nbFile                 INT = 1;
    v_pathName               TEXT;
    v_copyResult             INT;
    v_nbRows                 INT;
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
    EXECUTE format ('COPY (SELECT '''') TO %L %s',
                    p_dir || '/_INFO', coalesce(v_copyOptions, ''));
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
          EXECUTE format ('COPY (%s) TO %L %s',
                          r_sql.sql_text, v_pathName, coalesce(v_copyOptions, ''));
          v_copyResult = 1;
-- If the output file is empty, remove it, if requested.
          IF v_noEmptyFiles THEN
            GET DIAGNOSTICS v_nbRows = ROW_COUNT;
            IF v_nbRows = 0 THEN
-- The file is removed by calling a rm shell command through a COPY TO PROGRAM statement.
              EXECUTE format ('COPY (SELECT NULL) TO PROGRAM ''rm %s''',
                              v_pathName);
              v_copyResult = 0;
            END IF;
          END IF;
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
    EXECUTE format ('COPY %s TO %L',
                    v_stmt, p_dir || '/_INFO');
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

CREATE OR REPLACE FUNCTION emaj._gen_sql_dump_changes_seq(p_schema TEXT, p_sequence TEXT, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT,
                                                          p_consolidationLevel TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_gen_sql_dump_changes_seq$
-- This function builds a SQL statement that gets sequence state at 2 marks.
-- Input: the schema and sequence names,
--        the emaj sequence range corresponding to the selected marks range,
--        the requested consolidation level.
-- Output: the formatted SQL statement.
-- When CONSOLIDATION=NONE or PARTIAL, the SQL statements return 1 row of type OLD with the initial sequence characteristics and 1 row
--   of type NEW with the final sequence characteristics, even if the sequence has not been changed.
-- When CONSOLIDATION=FULL, no row is returned if the sequence has not been changed between mark bounds.
  DECLARE
    v_template               TEXT;
    v_stmt                   TEXT;
  BEGIN
-- Build the statement, depending on the requested consolidation level.
    CASE WHEN p_consolidationLevel IN ('NONE', 'PARTIAL') THEN
        v_template =
          E'SELECT ''OLD'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '  WHERE sequ_schema = %L\n'
           '    AND sequ_name = %L\n'
           '    AND sequ_time_id = %s\n'
           ' UNION ALL \n'
           'SELECT ''NEW'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '  WHERE sequ_schema = %L\n'
           '    AND sequ_name = %L\n'
           '    AND sequ_time_id = %s\n'
           ' ORDER BY emaj_tuple DESC\n';
        v_stmt = format(v_template,
                        p_schema, p_sequence, p_firstEmajGid,
                        p_schema, p_sequence, p_lastEmajGid);
      WHEN p_consolidationLevel = 'FULL' THEN
        v_template =
          E'WITH seq_begin AS (\n'
           '  SELECT ''OLD'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '    WHERE sequ_schema = %L\n'
           '      AND sequ_name = %L\n'
           '      AND sequ_time_id = %s\n'
           '  ), seq_end AS (\n'
           '  SELECT ''NEW'' AS emaj_tuple, * FROM emaj.emaj_sequence\n'
           '    WHERE sequ_schema = %L\n'
           '      AND sequ_name = %L\n'
           '      AND sequ_time_id = %s\n'
           '  ), seq_agg AS (\n'
           '  SELECT ''<>'' FROM seq_begin b, seq_end e\n'     -- 0 row if the sequence has the same characteristics at both mark times
           '    WHERE (CASE WHEN b.sequ_is_called THEN b.sequ_last_val ELSE b.sequ_last_val - b.sequ_increment END <>\n'
           '           CASE WHEN e.sequ_is_called THEN e.sequ_last_val ELSE e.sequ_last_val - e.sequ_increment END) OR\n'
           '           b.sequ_start_val <> e.sequ_start_val OR b.sequ_increment <> e.sequ_increment OR\n'
           '           b.sequ_max_val <> e.sequ_max_val OR b.sequ_min_val <> e.sequ_min_val OR\n'
           '           b.sequ_cache_val <> e.sequ_cache_val OR b.sequ_is_cycled <> e.sequ_is_cycled\n'
           '  )\n'
           'SELECT t.* FROM ( SELECT * FROM seq_begin UNION ALL SELECT * FROM seq_end ) AS t, seq_agg\n'
           '  ORDER BY emaj_tuple DESC\n';
        v_stmt = format(v_template,
                        p_schema, p_sequence, p_firstEmajGid,
                        p_schema, p_sequence, p_lastEmajGid);
    END CASE;
    IF v_stmt IS NULL THEN
      RAISE EXCEPTION '_gen_sql_dump_changes_seq: Internal error - the generated statement is NULL.';
    END IF;
    RETURN v_stmt;
  END;
$_gen_sql_dump_changes_seq$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(p_groupName TEXT, p_dir TEXT, p_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
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
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_nbRel                  INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    v_relOid                 OID;
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
      CASE r_tblsq.rel_kind
        WHEN 'r' THEN
-- It is a table.
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          SELECT pg_class.oid INTO v_relOid
            FROM pg_class
                 JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
            WHERE nspname = r_tblsq.rel_schema
              AND relname = r_tblsq.rel_tblseq;
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
                     AND indrelid = v_relOid
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
                   WHERE attrelid = v_relOid
                     AND attnum > 0
                     AND attisdropped = FALSE
                ) AS t;
          END IF;
--   Dump the table
          v_stmt = format('(SELECT * FROM %s ORDER BY %s)', v_fullTableName, v_colList);
          EXECUTE format ('COPY %s TO %L %s',
                          v_stmt, v_pathName, coalesce(p_copyOptions, ''));
        WHEN 'S' THEN
-- It is a sequence.
          v_stmt = format('(SELECT relname, rel.last_value, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, rel.is_called'
                          '  FROM %I.%I rel,'
                          '       pg_catalog.pg_sequence s'
                          '       JOIN pg_class c ON (c.oid = s.seqrelid)'
                          '       JOIN pg_namespace n ON (n.oid = c.relnamespace)'
                          '  WHERE nspname = %L AND relname = %L)',
                         r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_schema, r_tblsq.rel_tblseq);
--    Dump the sequence properties.
          EXECUTE format ('COPY %s TO %L %s',
                          v_stmt, v_pathName, coalesce(p_copyOptions, ''));
      END CASE;
      v_nbRel = v_nbRel + 1;
    END LOOP;
-- Create the _INFO file to keep general information about the snap operation.
    v_stmt = '(SELECT ' || quote_literal('E-Maj snap of tables group ' || p_groupName || ' at ' || statement_timestamp()) || ')';
    EXECUTE format ('COPY %s TO %L',
                    v_stmt, p_dir || '/_INFO');
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', p_groupName, v_nbRel || ' tables/sequences processed');
--
    RETURN v_nbRel;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_location TEXT,
                                                   p_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SET standard_conforming_strings = ON AS
$emaj_gen_sql_group$
-- This function generates a SQL script representing all updates performed on a tables group between 2 marks.
-- or beetween a mark and the current state. The result is stored into an external file.
-- It calls the _gen_sql_groups() function to effetively process the request.
-- Input: - tables group
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- Call the _gen_sql_groups() function that effectively processes the request.
    RETURN emaj._gen_sql_groups(array[p_groupName], FALSE, p_firstMark, p_lastMark, p_location, p_tblseqs, current_user);
  END;
$emaj_gen_sql_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_group(TEXT,TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a tables group between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT, p_location TEXT,
                                                    p_tblseqs TEXT[] DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql
SET standard_conforming_strings = ON AS
$emaj_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a set of tables groups between 2 marks
-- or beetween a mark and the current state. The result is stored into an external file.
-- It calls the _gen_sql_groups() function to effetively process the request.
-- Input: - tables groups array
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - array of schema qualified table and sequence names to only process those tables and sequences (NULL by default)
-- Output: number of generated SQL statements (non counting comments and transaction management)
  BEGIN
-- Call the _gen_sql_groups() function that effectively processes the request.
    RETURN emaj._gen_sql_groups(p_groupNames, TRUE, p_firstMark, p_lastMark, p_location, p_tblseqs, current_user);
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(TEXT[],TEXT,TEXT,TEXT,TEXT[]) IS
$$Generates a sql script replaying all updates performed on a tables groups set between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT,
                                                p_location TEXT, p_tblseqs TEXT[], p_currentUser TEXT)
RETURNS BIGINT LANGUAGE plpgsql
SET DateStyle = 'ISO, YMD' SET standard_conforming_strings = ON
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a tables groups array between 2 marks
-- or beetween a mark and the current state. The result is stored into an external file.
-- The function can process groups that are in LOGGING state or not.
-- The sql statements are placed between a BEGIN TRANSACTION and a COMMIT statements.
-- The output file can be reused as input file to a psql command to replay the updates scenario. Just '\\'
-- character strings (double antislash), if any, must be replaced by '\' (single antislash) before feeding
-- the psql command.
-- Input: - tables groups array
--        - start mark
--        - end mark, NULL representing the current state, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--          (may be NULL if the caller reads the temporary table that will hold the script after the function execution)
--        - optional array of schema qualified table and sequence names to only process those tables and sequences
--        - the current user of the calling function
-- Output: number of generated SQL statements (non counting comments and transaction management)
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_firstMarkTimeId        BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastMarkTimeId         BIGINT;
    v_lastEmajGid            BIGINT;
    v_firstMarkTs            TIMESTAMPTZ;
    v_lastMarkTs             TIMESTAMPTZ;
    v_tblseqErr              TEXT;
    v_count                  INT;
    v_nbSQL                  BIGINT;
    v_nbSeq                  INT;
    v_cumNbSQL               BIGINT = 0;
    v_endComment             TEXT;
    v_dateStyle              TEXT;
    r_rel                    emaj.emaj_relation%ROWTYPE;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'BEGIN', array_to_string(p_groupNames,','),
       'From mark ' || coalesce(p_firstMark, '') ||
       CASE WHEN p_lastMark IS NULL OR p_lastMark = '' THEN ' to current state' ELSE ' to mark ' || p_lastMark END ||
       CASE WHEN p_tblseqs IS NOT NULL THEN ' with tables/sequences filtering' ELSE '' END );
-- Check the group name.
    SELECT emaj._check_group_names(p_groupNames := p_groupNames, p_mayBeNull := p_multiGroup, p_lockGroups := FALSE)
      INTO p_groupNames;
-- If there is at least 1 group to process, go on.
    IF p_groupNames IS NOT NULL THEN
-- Check the marks range and get some data about both marks.
      SELECT *
        INTO p_firstMark, p_lastMark, v_firstMarkTimeId, v_lastMarkTimeId, v_firstMarkTs, v_lastMarkTs, v_firstEmajGid, v_lastEmajGid
        FROM emaj._check_marks_range(p_groupNames := p_groupNames, p_firstMark := p_firstMark, p_lastMark := p_lastMark);
-- If table/sequence names are supplied, check them.
      IF p_tblseqs IS NOT NULL THEN
        SELECT emaj._check_tblseqs_filter(p_tblseqs, p_groupNames, v_firstMarkTimeId, v_lastMarkTimeId, TRUE)
          INTO p_tblseqs;
      END IF;
-- Check that all tables had pk at start mark time, by verifying the emaj_relation.rel_sql_gen_pk_conditions column.
      SELECT string_agg(rel_schema || '.' || rel_tblseq, ', ' ORDER BY rel_schema, rel_tblseq), count(*)
        INTO v_tblseqErr, v_count
        FROM
          (SELECT *
             FROM emaj.emaj_relation
             WHERE rel_group = ANY (p_groupNames)
               AND rel_kind = 'r'                                                                  -- tables belonging to the groups
               AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
               AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))        -- filtered or not by the user
               AND rel_sql_gen_pk_conditions IS NULL                                               -- no pk at assignment time
          ) as t;
      IF v_tblseqErr IS NOT NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: % tables/sequences (%) had no pkey at % mark time.',
          v_count, v_tblseqErr, p_firstMark;
      END IF;
-- Create a temporary table to hold the generated script.
      DROP TABLE IF EXISTS emaj_temp_script CASCADE;
      CREATE TEMP TABLE emaj_temp_script (
        scr_emaj_gid           BIGINT,              -- the emaj_gid of the corresponding log row,
                                                    --   0 for initial technical statements,
                                                    --   NULL for final technical statements
        scr_subid              INT,                 -- used to distinguish several generated sql per log row
        scr_emaj_txid          BIGINT,              -- for future use, to insert commit statement at each txid change
        scr_sql                TEXT                 -- the generated sql text
      );
-- Test the supplied output file to avoid to discover a bad file name after having spent a lot of time to build the script.
      IF p_location IS NOT NULL THEN
        EXECUTE format ('COPY (SELECT 0) TO %L',
                        p_location);
      END IF;
-- End of checks.
-- Insert initial comments, some session parameters setting:
--    - the standard_conforming_strings option to properly handle special characters,
--    - the DateStyle mode used at export time,
-- and a transaction start.
      IF v_lastMarkTimeId IS NOT NULL THEN
        v_endComment = ' and mark ' || p_lastMark;
      ELSE
        v_endComment = ' and the current state';
      END IF;
      SELECT setting INTO v_dateStyle
        FROM pg_settings
        WHERE name = 'DateStyle';
      INSERT INTO emaj_temp_script VALUES
        (0, 1, 0, '-- SQL script generated by E-Maj at ' || statement_timestamp()),
        (0, 2, 0, '--    for tables group(s): ' || array_to_string(p_groupNames,',')),
        (0, 3, 0, '--    processing logs between mark ' || p_firstMark || v_endComment);
      IF p_tblseqs IS NOT NULL THEN
        INSERT INTO emaj_temp_script VALUES
          (0, 4, 0, '--    only for the following tables/sequences: ' || array_to_string(p_tblseqs,','));
      END IF;
      INSERT INTO emaj_temp_script VALUES
        (0, 10, 0, 'SET standard_conforming_strings = OFF;'),
        (0, 11, 0, 'SET escape_string_warning = OFF;'),
        (0, 12, 0, 'SET datestyle = ' || quote_literal(v_dateStyle) || ';'),
        (0, 20, 0, 'BEGIN TRANSACTION;');
-- Process tables.
      FOR r_rel IN
        SELECT *
          FROM emaj.emaj_relation
          WHERE rel_group = ANY (p_groupNames)
            AND rel_kind = 'r'                                                                  -- tables belonging to the groups
            AND rel_time_range @> v_firstMarkTimeId                                             --   at the first mark time
            AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))        -- filtered or not by the user
            AND emaj._log_stat_tbl(emaj_relation, v_firstMarkTimeId,                            -- only tables having updates to process
                                  least(v_lastMarkTimeId, upper(rel_time_range))) > 0
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- For each application table referenced in the emaj_relation table, process the related log table, by calling the _gen_sql_tbl() function.
        SELECT emaj._gen_sql_tbl(r_rel, v_firstEmajGid, v_lastEmajGid) INTO v_nbSQL;
        v_cumNbSQL = v_cumNbSQL + v_nbSQL;
      END LOOP;
-- Process sequences.
      v_nbSeq = 0;
      FOR r_rel IN
        SELECT *
          FROM emaj.emaj_relation
          WHERE rel_group = ANY (p_groupNames)
            AND rel_kind = 'S'
            AND rel_time_range @> v_firstMarkTimeId                                -- sequences belonging to the groups at the start mark
            AND (p_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (p_tblseqs))         -- filtered or not by the user
          ORDER BY rel_schema DESC, rel_tblseq DESC
      LOOP
-- Process each sequence and increment the sequence counter.
        v_nbSeq = v_nbSeq + emaj._gen_sql_seq(r_rel, v_firstMarkTimeId, v_lastMarkTimeId, v_nbSeq);
      END LOOP;
-- Add command to commit the transaction and reset the modified session parameters.
      INSERT INTO emaj_temp_script VALUES
        (NULL, 1, txid_current(), 'COMMIT;'),
        (NULL, 10, txid_current(), 'RESET standard_conforming_strings;'),
        (NULL, 11, txid_current(), 'RESET escape_string_warning;'),
        (NULL, 11, txid_current(), 'RESET datestyle;');
-- If an output file is supplied, write the SQL script on the external file and drop the temporary table.
      IF p_location IS NOT NULL THEN
        EXECUTE format ('COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid) TO %L',
                        p_location);
        DROP TABLE IF EXISTS emaj_temp_script;
      ELSE
-- Otherwise create a view to ease the generation script export..;
        CREATE TEMPORARY VIEW emaj_sql_script AS
          SELECT scr_sql
            FROM emaj_temp_script
            ORDER BY scr_emaj_gid NULLS LAST, scr_subid;
-- ... and grant SELECT privileges on the temporary view and the underneath temporary table to the current user.
        EXECUTE format('GRANT SELECT ON emaj_sql_script, emaj_temp_script TO %I',
                       p_currentUser);
      END IF;
-- Return the number of sql verbs generated into the output file.
      v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    END IF;
-- Insert end in the history and return.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN p_multiGroup THEN 'GEN_SQL_GROUPS' ELSE 'GEN_SQL_GROUP' END, 'END',
              array_to_string(p_groupNames,','), v_cumNbSQL || ' generated statements' ||
                CASE WHEN p_location IS NOT NULL THEN ' - script exported into ' || p_location ELSE ' - script not exported' END );
--
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

------------------------------------
--                                --
-- Global purpose functions       --
--                                --
------------------------------------
CREATE OR REPLACE FUNCTION emaj.emaj_get_version()
RETURNS TEXT LANGUAGE SQL STABLE AS
$$
-- This function returns the current emaj extension version.
SELECT verh_version FROM emaj.emaj_version_hist WHERE upper_inf(verh_time_range);
$$;
COMMENT ON FUNCTION emaj.emaj_get_version() IS
$$Returns the current emaj version.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_purge_histories(p_retentionDelay INTERVAL)
RETURNS VOID LANGUAGE plpgsql AS
$emaj_purge_histories$
-- This function purges the emaj histories
-- The function is called by an emaj_adm user, typically an external scheduler.
-- It calls the _purge_histories() function.
-- Input: retention delay
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('PURGE_HISTORIES', 'BEGIN', 'Retention delay ' || p_retentionDelay);
-- Effectively perform the purge.
    PERFORM emaj._purge_histories(p_retentionDelay);
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES ('PURGE_HISTORIES', 'END');
--
    RETURN;
  END;
$emaj_purge_histories$;
COMMENT ON FUNCTION emaj.emaj_purge_histories(INTERVAL) IS
$$Purges obsolete data from emaj history tables.$$;

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

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration()
RETURNS JSON LANGUAGE plpgsql AS
$emaj_export_parameters_configuration$
-- This function returns a JSON formatted structure representing all the parameters registered in the emaj_param table.
-- The function can be called by clients like emaj_web.
-- This is just a wrapper of the internal _export_param_conf() function.
-- Output: the parameters content in JSON format
  BEGIN
    RETURN emaj._export_param_conf();
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration() IS
$$Generates a json structure describing the E-Maj parameters.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_export_parameters_configuration(p_location TEXT)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_export_parameters_configuration$
-- This function stores the parameters configuration into a file on the server.
-- The JSON structure is built by the _export_param_conf() function.
-- Output: the number of parameters of the recorded JSON structure.
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_paramsJson             JSON;
  BEGIN
-- Get the json structure.
    SELECT emaj._export_param_conf() INTO v_paramsJson;
-- Store the structure into the provided file name.
    CREATE TEMP TABLE t (params TEXT);
    INSERT INTO t
      SELECT line
        FROM regexp_split_to_table(v_paramsJson::TEXT, '\n') AS line;
    EXECUTE format ('COPY t TO %L',
                    p_location);
    DROP TABLE t;
-- Return the number of recorded parameters.
    RETURN json_array_length(v_paramsJson->'parameters');
  END;
$emaj_export_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_export_parameters_configuration(TEXT) IS
$$Generates and stores in a file a json structure describing the E-Maj parameters.$$;

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
                           emaj.emaj_get_version() || ', at ' || statement_timestamp() || E'",\n' ||
               E'  "_help": "Known parameter keys: dblink_user_password, history_retention (default = 1 year), alter_log_table, '
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

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(p_paramsJson JSON, p_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql AS
$emaj_import_parameters_configuration$
-- This function import a supplied JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- The function can be called by clients like emaj_web.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - the parameter configuration structure in JSON format
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
  DECLARE
    v_nbParam                INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN');
-- Load the parameters.
    SELECT emaj._import_param_conf(p_paramsJson, p_deleteCurrentConf) INTO v_nbParam;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
--
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(JSON,BOOLEAN) IS
$$Import a json structure describing E-Maj parameters to load.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_import_parameters_configuration(p_location TEXT, p_deleteCurrentConf BOOLEAN DEFAULT FALSE)
RETURNS INT LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$emaj_import_parameters_configuration$
-- This function imports a file containing a JSON formatted structure representing E-Maj parameters to load.
-- This structure can have been generated by the emaj_export_parameters_configuration() functions and may have been adapted by the user.
-- It calls the _import_param_conf() function to perform the emaj_param table changes.
-- Input: - input file location
--        - an optional boolean indicating whether the current parameters configuration must be deleted before loading the new parameters
--          (by default, the parameter keys not referenced in the input json structure are kept unchanged)
-- Output: the number of inserted or updated parameter keys
-- The function is defined as SECURITY DEFINER so that emaj roles can perform the COPY statement.
  DECLARE
    v_paramsText             TEXT;
    v_paramsJson             JSON;
    v_nbParam                INT;
  BEGIN
-- Insert a BEGIN event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'BEGIN', 'Input file: ' || quote_literal(p_location));
-- Read the input file and put its content into a temporary table.
    CREATE TEMP TABLE t (params TEXT);
    EXECUTE format ('COPY t FROM %L',
                    p_location);
-- Aggregate the lines into a single text variable.
    SELECT string_agg(params, E'\n') INTO v_paramsText
      FROM t;
    DROP TABLE t;
-- Verify that the file content is a valid json structure.
    BEGIN
      v_paramsJson = v_paramsText::JSON;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'emaj_import_parameters_configuration: The file content is not a valid JSON content.';
    END;
-- Load the parameters.
    SELECT emaj._import_param_conf(v_paramsJson, p_deleteCurrentConf) INTO v_nbParam;
-- Insert a END event into the history.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('IMPORT_PARAMETERS', 'END', v_nbParam || ' parameters imported');
--
    RETURN v_nbParam;
  END;
$emaj_import_parameters_configuration$;
COMMENT ON FUNCTION emaj.emaj_import_parameters_configuration(TEXT,BOOLEAN) IS
$$Import E-Maj parameters from a JSON formatted file.$$;

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

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of error or warning messages for discovered discrepancies.
-- If no error is detected, no row is returned.
  BEGIN
--
-- Errors detection.
--
-- Check that all application schemas referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: The application schema "' || rel_schema || '" does not exist anymore.' AS msg
        FROM
          (  SELECT DISTINCT rel_schema
               FROM emaj.emaj_relation
               WHERE upper_inf(rel_time_range)
           EXCEPT
              SELECT nspname
                FROM pg_catalog.pg_namespace
          ) AS t
        ORDER BY msg;
-- Check that all application relations referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist anymore.' AS msg
        FROM                                          -- all expected application relations
          (  SELECT rel_schema, rel_tblseq, rel_kind
               FROM emaj.emaj_relation
               WHERE upper_inf(rel_time_range)
           EXCEPT                                    -- minus relations known by postgres
             SELECT nspname, relname, relkind::TEXT
               FROM pg_catalog.pg_class
                    JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
               WHERE relkind IN ('r','S')
          ) AS t
          JOIN emaj.emaj_relation r ON (t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq AND upper_inf(r.rel_time_range))
        ORDER BY t.rel_schema, t.rel_tblseq, 1;
-- Check that the log table for all tables referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_log_table || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_log_schema
                     AND relname = rel_log_table
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that the log sequence for all tables referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log sequence "' ||
               rel_log_schema || '"."' || rel_log_sequence || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                 WHERE nspname = rel_log_schema AND relname = rel_log_sequence
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the log function for each table referenced in the emaj_relation table still exist.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log function "' ||
               rel_log_schema || '"."' || rel_log_function || '" is not found.'
             AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_proc
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                 WHERE nspname = rel_log_schema
                   AND proname = rel_log_function
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check log and truncate triggers for all tables referenced in the emaj_relation table still exist.
-- Start with log triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the log trigger "emaj_log_trg" on table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_log_trg'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Then truncate triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the truncate trigger "emaj_trunc_trg" on table "' ||
             rel_schema || '"."' || rel_tblseq || '" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_trigger
                        JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND tgname = 'emaj_trunc_trg'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that all log tables have a structure consistent with the application tables they reference
-- (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM
        (WITH cte_app_tables_columns AS                -- application table's columns
           (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation
                   JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                   JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
              WHERE attnum > 0
                AND attisdropped = FALSE
                AND upper_inf(rel_time_range)
                AND rel_kind = 'r'
           ),
              cte_log_tables_columns AS                 -- log table's columns
           (SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation
                   JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                   JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
              WHERE attnum > 0
                AND attisdropped = FALSE
                AND attnum < rel_emaj_verb_attnum
                AND upper_inf(rel_time_range)
                AND rel_kind = 'r'
           )
        SELECT DISTINCT rel_schema, rel_tblseq,
               'Error: In tables group "' || rel_group || '", the structure of the application table "' ||
                 rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
               rel_log_schema || '"."' || rel_log_table || '").' AS msg
          FROM
            (                                              -- application table's columns
              (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_app_tables_columns
               EXCEPT                                      -- minus log table's columns
                 SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_log_tables_columns
              )
            UNION                                          -- log table's columns
              (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_log_tables_columns
               EXCEPT                                      --  minus application table's columns
                 SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname, atttypid, attlen, atttypmod
                   FROM cte_app_tables_columns
              )
            ) AS t
                           -- do not issue a row if the log or application table does not exist,
                           -- these cases have been already detected
        WHERE (rel_log_schema, rel_log_table) IN
              (SELECT nspname, relname
                 FROM pg_catalog.pg_class
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              )
          AND (rel_schema, rel_tblseq) IN
              (SELECT nspname, relname
                 FROM pg_catalog.pg_class
                      JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
              )
        ORDER BY 1,2,3
        ) AS t;
-- Check that all tables of rollbackable groups have their primary key.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has no primary key anymore.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND group_is_rollbackable
          AND NOT EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = pg_class.oid)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                     AND contype = 'p'
                )
                       -- do not issue a row if the application table does not exist,
                       -- this case has been already detected
          AND EXISTS
                (SELECT NULL
                   FROM pg_catalog.pg_class
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   WHERE nspname = rel_schema
                     AND relname = rel_tblseq
                )
        ORDER BY rel_schema, rel_tblseq, 1;
-- For rollbackable groups, check that no table has been altered as UNLOGGED or dropped and recreated as TEMP table after
-- tables groups creation.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the table "' ||
             rel_schema || '"."' || rel_tblseq || '" is UNLOGGED or TEMP.' AS msg
        FROM emaj.emaj_relation
             JOIN emaj.emaj_group ON (group_name = rel_group)
             JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
             JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
        WHERE upper_inf(rel_time_range)
          AND rel_kind = 'r'
          AND group_is_rollbackable
          AND relpersistence <> 'p'
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the primary key structure of all tables belonging to rollbackable groups is unchanged.
    RETURN QUERY
      SELECT 'Error: In the rollbackable group "' || rel_group || '", the primary key of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_pk_columns || ' => ' || current_pk_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  array_to_string(rel_pk_cols, ',') AS registered_pk_columns,
                  string_agg(attname, ',' ORDER BY attnum) AS current_pk_columns
             FROM emaj.emaj_relation
                  JOIN emaj.emaj_group ON (group_name = rel_group)
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_index ON (indrelid = pg_class.oid)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_index.indrelid)
             WHERE rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND group_is_rollbackable
               AND attnum = ANY (indkey)
               AND indisprimary
               AND attnum > 0
               AND attisdropped = FALSE
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_pk_columns <> current_pk_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check that the "GENERATED AS expression" columns list of all tables have not changed.
-- (The expression of virtual generated columns be changed, without generating any trouble)
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the "GENERATED AS expression" columns list of the table "' ||
             rel_schema || '"."' || rel_tblseq || '" has changed (' ||
             registered_gen_columns || ' => ' || current_gen_columns || ').' AS msg
        FROM
          (SELECT rel_schema, rel_tblseq, rel_group,
                  coalesce(array_to_string(rel_gen_expr_cols, ','), '<none>') AS registered_gen_columns,
                  coalesce(string_agg(attname, ',' ORDER BY attnum), '<none>') AS current_gen_columns
             FROM emaj.emaj_relation
                  JOIN pg_catalog.pg_class ON (relname = rel_tblseq)
                  JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_schema)
                  JOIN pg_catalog.pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
             WHERE rel_kind = 'r'
               AND upper_inf(rel_time_range)
               AND attgenerated <> ''
               AND attnum > 0
               AND NOT attisdropped
             GROUP BY 1,2,3,4
          ) AS t
        WHERE registered_gen_columns <> current_gen_columns
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check the array of triggers to ignore at rollback time only contains existing triggers.
    RETURN QUERY
      SELECT 'Error: In tables group "' || rel_group || '", the trigger "' || trg_name || '" for table "'
          || rel_schema || '"."' || rel_tblseq || '" is missing. '
          || 'Use the emaj_modify_table() function to adjust the list of application triggers that should not be'
          || ' automatically disabled at rollback time.'
             AS msg
        FROM
          (SELECT rel_group, rel_schema, rel_tblseq, unnest(rel_ignored_triggers) AS trg_name
             FROM emaj.emaj_relation
             WHERE upper_inf(rel_time_range)
               AND rel_ignored_triggers IS NOT NULL
          ) AS t
        WHERE NOT EXISTS
                 (SELECT NULL
                    FROM pg_catalog.pg_trigger
                         JOIN pg_catalog.pg_class ON (pg_class.oid = tgrelid)
                         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                    WHERE nspname = rel_schema
                      AND relname = rel_tblseq
                      AND tgname = trg_name
                 )
        ORDER BY rel_schema, rel_tblseq, 1;
-- Check all log tables have the 6 required technical columns.
    RETURN QUERY
      SELECT msg FROM
        (SELECT DISTINCT rel_schema, rel_tblseq,
                'Error: In tables group "' || rel_group || '", the log table "' ||
                rel_log_schema || '"."' || rel_log_table || '" miss some technical columns (' ||
                string_agg(attname,', ') || ').' AS msg
           FROM
             (  SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                  FROM emaj.emaj_relation,
                       (VALUES ('emaj_verb'), ('emaj_tuple'), ('emaj_gid'), ('emaj_changed'), ('emaj_txid'), ('emaj_user')) AS t(attname)
                  WHERE rel_kind = 'r'
                    AND upper_inf(rel_time_range)
                    AND EXISTS
                          (SELECT NULL
                             FROM pg_catalog.pg_namespace
                                  JOIN pg_catalog.pg_class ON (relnamespace = pg_namespace.oid)
                             WHERE nspname = rel_log_schema
                               AND relname = rel_log_table
                          )
              EXCEPT
                SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table, attname
                  FROM emaj.emaj_relation
                       JOIN pg_catalog.pg_class ON (relname = rel_log_table)
                       JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace AND nspname = rel_log_schema)
                       JOIN pg_catalog.pg_attribute ON (attrelid = pg_class.oid)
                  WHERE attnum > 0
                    AND attisdropped = FALSE
                    AND attname IN ('emaj_verb', 'emaj_tuple', 'emaj_gid', 'emaj_changed', 'emaj_txid', 'emaj_user')
                    AND rel_kind = 'r'
                    AND upper_inf(rel_time_range)
             ) AS t2
           GROUP BY rel_group, rel_schema, rel_tblseq, rel_log_schema, rel_log_table
           ORDER BY 1,2,3
         ) AS t;
--
-- Warnings detection.
--
-- Detect all sequences associated to a serial or a "generated as identity" column have their related table in the same group.
    RETURN QUERY
      SELECT msg FROM
        (WITH serial_dependencies AS
           (SELECT rs.rel_group AS seq_group, rs.rel_schema AS seq_schema, rs.rel_tblseq AS seq_name,
                   rt.rel_group AS tbl_group, nt.nspname AS tbl_schema, ct.relname AS tbl_name
              FROM emaj.emaj_relation rs
                   JOIN pg_catalog.pg_class cs ON (cs.relname = rel_tblseq)
                   JOIN pg_catalog.pg_namespace ns ON (ns.oid = cs.relnamespace AND ns.nspname = rel_schema)
                   JOIN pg_catalog.pg_depend ON (pg_depend.objid = cs.oid)
                   JOIN pg_catalog.pg_class ct ON (ct.oid = pg_depend.refobjid)
                   JOIN pg_catalog.pg_namespace nt ON (nt.oid = ct.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation rt ON (rt.rel_schema = nt.nspname AND rt.rel_tblseq = ct.relname
                                                             AND (rt.rel_time_range IS NULL OR upper_inf(rt.rel_time_range)))
              WHERE rs.rel_kind = 'S'
                AND upper_inf(rs.rel_time_range)
                AND pg_depend.classid = pg_depend.refclassid             -- the classid et refclassid must be 'pg_class'
                AND pg_depend.classid =
                      (SELECT oid
                         FROM pg_catalog.pg_class
                         WHERE relname = 'pg_class'
                      )
           )
           SELECT DISTINCT seq_schema, seq_name,
                  'Warning: In tables group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
                  '" is linked to the table "' || tbl_schema || '"."' || tbl_name ||
                  '" but this table does not belong to any tables group.' AS msg
             FROM serial_dependencies
             WHERE tbl_group IS NULL
         UNION ALL
           SELECT DISTINCT seq_schema, seq_name,
                  'Warning: In tables group "' || seq_group || '", the sequence "' || seq_schema || '"."' || seq_name ||
                  '" is linked to the table "' || tbl_schema || '"."' || tbl_name ||
                  '" but this table belongs to another tables group (' || tbl_group || ').' AS msg
             FROM serial_dependencies
             WHERE tbl_group <> seq_group
           ORDER BY 1,2,3
        ) AS t;
-- Detect tables linked by a foreign key but not belonging to the same tables group and inherited FK that cannot be dropped and
--   recreated at rollback time.
    RETURN QUERY
      SELECT msg FROM
        (WITH fk_dependencies AS             -- all foreign keys that link 2 tables at least one of both belongs to a tables group
           (SELECT n.nspname AS tbl_schema, t.relname AS tbl_name,
                   c.conname, c. coninhcount, c.condeferrable, c.confdeltype, c.confupdtype,
                   nf.nspname AS reftbl_schema, tf.relname AS reftbl_name,
                   r.rel_group AS tbl_group, g.group_is_rollbackable AS tbl_group_is_rollbackable,
                   rf.rel_group AS reftbl_group, gf.group_is_rollbackable AS reftbl_group_is_rollbackable
              FROM pg_catalog.pg_constraint c
                   JOIN pg_catalog.pg_class t      ON (t.oid = c.conrelid)
                   JOIN pg_catalog.pg_namespace n  ON (n.oid = t.relnamespace)
                   JOIN pg_catalog.pg_class tf     ON (tf.oid = c.confrelid)
                   JOIN pg_catalog.pg_namespace nf ON (nf.oid = tf.relnamespace)
                   LEFT OUTER JOIN emaj.emaj_relation r ON (r.rel_schema = n.nspname AND r.rel_tblseq = t.relname
                                                       AND upper_inf(r.rel_time_range))
                   LEFT OUTER JOIN emaj.emaj_group g ON (g.group_name = r.rel_group)
                   LEFT OUTER JOIN emaj.emaj_relation rf ON (rf.rel_schema = nf.nspname AND rf.rel_tblseq = tf.relname
                                                       AND upper_inf(rf.rel_time_range))
                   LEFT OUTER JOIN emaj.emaj_group gf ON (gf.group_name = rf.rel_group)
              WHERE contype = 'f'                                         -- FK constraints only
                AND t.relkind = 'r' AND tf.relkind = 'r'                  -- excluding partitionned tables
                AND (r.rel_group IS NOT NULL OR rf.rel_group IS NOT NULL) -- at least the table or the referenced table belongs to
                                                                          -- a tables group
           )
-- Referenced table not in a group.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on "' || tbl_schema || '"."' || tbl_name || '" references the table "' ||
                  reftbl_schema || '"."' || reftbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND tbl_group_is_rollbackable
               AND reftbl_group IS NULL
         UNION ALL
-- Referencing table not in a group.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || reftbl_group || '", the table "' || reftbl_schema || '"."' || reftbl_name ||
                  '" is referenced by the foreign key "' || conname || '" on "' ||
                  tbl_schema || '"."' || tbl_name || '" that does not belong to any group.' AS msg
             FROM fk_dependencies
             WHERE reftbl_group IS NOT NULL
               AND reftbl_group_is_rollbackable
               AND tbl_group IS NULL
         UNION ALL
-- Both tables in different groups.
           SELECT tbl_schema, tbl_name,
                  'Warning: In tables group "' || tbl_group || '", the foreign key "' || conname ||
                  '" on "' || tbl_schema || '"."' || tbl_name || '" references the table "' ||
                  reftbl_schema || '"."' || reftbl_name || '" that belongs to another group ("' ||
                  reftbl_group || '")' AS msg
             FROM fk_dependencies
             WHERE tbl_group IS NOT NULL
               AND reftbl_group IS NOT NULL
               AND tbl_group <> reftbl_group
               AND (tbl_group_is_rollbackable OR reftbl_group_is_rollbackable)
-- Inherited FK that cannot be dropped/recreated.
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: The foreign key "' || conname || '" on "' || tbl_schema || '"."' || tbl_name || '" is inherited'
                  ' from a partitionned table and is not deferrable. This could block E-Maj rollbacks.' AS msg
             FROM fk_dependencies
             WHERE coninhcount > 0
               AND NOT condeferrable
               AND ((tbl_group IS NOT NULL AND tbl_group_is_rollbackable) OR
                    (reftbl_group IS NOT NULL AND reftbl_group_is_rollbackable))
         UNION ALL
           SELECT tbl_schema, tbl_name,
                  'Warning: The foreign key "' || conname || '" on "' || tbl_schema || '"."' || tbl_name || '" has ON DELETE'
                  ' / ON UPDATE clauses and is inherited from a partitionned table. This could block E-Maj rollbacks.' AS msg
             FROM fk_dependencies
             WHERE coninhcount > 0
               AND (confdeltype <> 'a' OR confupdtype <> 'a')
               AND ((tbl_group IS NOT NULL AND tbl_group_is_rollbackable) OR
                    (reftbl_group IS NOT NULL AND reftbl_group_is_rollbackable))
           ORDER BY 1,2,3
        ) AS t;
--
    RETURN;
  END;
$_verify_all_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_schemas()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_schemas$
-- The function verifies that all E-Maj schemas only contains E-Maj objects.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  BEGIN
-- Verify that the expected E-Maj schemas still exist.
    RETURN QUERY
      SELECT DISTINCT 'Error: The E-Maj schema "' || sch_name || '" does not exist anymore.' AS msg
        FROM emaj.emaj_schema
        WHERE NOT EXISTS
               (SELECT NULL
                  FROM pg_catalog.pg_namespace
                  WHERE nspname = sch_name
               )
        ORDER BY msg;
-- Detect all objects that are not directly linked to a known table groups in all E-Maj schemas, by scanning the catalog
-- (pg_class, pg_proc, pg_type, pg_conversion, pg_operator, pg_opclass).
    RETURN QUERY
      SELECT msg FROM
-- Look for unexpected tables.
        (  SELECT nspname, 1, 'Error: In schema "' || nspname ||
                 '", the table "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'r'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal tables
                AND NOT EXISTS                                                   -- exclude emaj log tables
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_table = relname
                     )
         UNION ALL
-- Look for unexpected sequences.
           SELECT nspname, 2, 'Error: In schema "' || nspname ||
                  '", the sequence "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'S'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal sequences
                AND NOT EXISTS                                                   -- exclude emaj log table sequences
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_sequence = relname
                     )
         UNION ALL
-- Look for unexpected functions.
           SELECT nspname, 3, 'Error: In schema "' || nspname ||
                  '", the function "' || nspname || '"."' || proname  || '" is not linked to any created tables group.' AS msg
              FROM pg_catalog.pg_proc
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = pronamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE (nspname <> 'emaj' OR (proname NOT LIKE E'emaj\\_%' AND proname NOT LIKE E'\\_%'))
                                                                                 -- exclude emaj internal functions
                AND NOT EXISTS                                                   -- exclude emaj log functions
                     (SELECT 0
                        FROM emaj.emaj_relation
                        WHERE rel_log_schema = nspname
                          AND rel_log_function = proname
                     )
         UNION ALL
-- Look for unexpected composite types.
           SELECT nspname, 4, 'Error: In schema "' || nspname ||
                  '", the type "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'c'
                AND (nspname <> 'emaj' OR (relname NOT LIKE E'emaj\\_%' AND relname NOT LIKE E'\\_%'))
                                                                                 -- exclude emaj internal types
         UNION ALL
-- Look for unexpected views.
           SELECT nspname, 5, 'Error: In schema "' || nspname ||
                  '", the view "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'v'
                AND (nspname <> 'emaj' OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal views
         UNION ALL
-- Look for unexpected foreign tables.
           SELECT nspname, 6, 'Error: In schema "' || nspname ||
                  '", the foreign table "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_class
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE relkind = 'f'
         UNION ALL
-- Look for unexpected domains.
           SELECT nspname, 7, 'Error: In schema "' || nspname ||
                  '", the domain "' || nspname || '"."' || typname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_type
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = typnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
              WHERE typisdefined
                AND typtype = 'd'
         UNION ALL
-- Look for unexpected conversions.
         SELECT nspname, 8, 'Error: In schema "' || nspname ||
                '", the conversion "' || nspname || '"."' || conname || '" is not an E-Maj component.' AS msg
            FROM pg_catalog.pg_conversion
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = connamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
           UNION ALL
-- Look for unexpected operators.
           SELECT nspname, 9, 'Error: In schema "' || nspname ||
                  '", the operator "' || nspname || '"."' || oprname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_operator
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = oprnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
         UNION ALL
-- Look for unexpected operator classes.
           SELECT nspname, 10, 'Error: In schema "' || nspname ||
                  '", the operator class "' || nspname || '"."' || opcname || '" is not an E-Maj component.' AS msg
              FROM pg_catalog.pg_opclass
                   JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = opcnamespace)
                   JOIN emaj.emaj_schema ON (sch_name = nspname)
           ORDER BY 1, 2, 3
        ) AS t;
--
    RETURN;
  END;
$_verify_all_schemas$;

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in the emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_errorFound             BOOLEAN = FALSE;
    v_status                 INT;
    v_schema                 TEXT;
    r_object                 RECORD;
  BEGIN
-- Global checks.
-- Detect if the current postgres version is at least 11.
    IF emaj._pg_version_num() < 120000 THEN
      RETURN NEXT 'Error: The current postgres version (' || version()
               || ') is not compatible with this E-Maj version. It should be at least 12';
      v_errorFound = TRUE;
    END IF;
-- Check all E-Maj schemas.
    FOR r_object IN
      SELECT msg
        FROM emaj._verify_all_schemas() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- Check all groups components.
    FOR r_object IN
      SELECT msg
        FROM emaj._verify_all_groups() msg
    LOOP
      RETURN NEXT r_object.msg;
      IF r_object.msg LIKE 'Error%' THEN
        v_errorFound = TRUE;
      END IF;
    END LOOP;
-- Report a warning if dblink connections are not operational
    IF has_function_privilege('emaj._dblink_open_cnx(text,text)', 'execute') THEN
      SELECT p_status, p_schema INTO v_status, v_schema
        FROM emaj._dblink_open_cnx('emaj_verify_all', current_role);
      CASE v_status
        WHEN 0, 1 THEN
          PERFORM emaj._dblink_close_cnx('emaj_verify_all', v_schema);
        WHEN -1 THEN
          RETURN NEXT 'Warning: The dblink extension is not installed.';
        WHEN -3 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the current role is not granted to execute dblink_connect_u().';
        WHEN -4 THEN
          RETURN NEXT 'Warning: While testing the dblink connection, the transaction isolation level is not READ COMMITTED.';
        WHEN -5 THEN
          RETURN NEXT 'Warning: The ''dblink_user_password'' parameter value is not set in the emaj_param table.';
        WHEN -6 THEN
          RETURN NEXT 'Warning: The dblink connection test failed. The ''dblink_user_password'' parameter value is probably incorrect.';
        WHEN -7 THEN
          RETURN NEXT 'Warning: The role set in the ''dblink_user_password'' parameter has not emaj_adm rights.';
        ELSE
          RETURN NEXT format('Warning: The dblink connection test failed for an unknown reason (status = %s).',
                             v_status::TEXT);
      END CASE;
    ELSE
      RETURN NEXT 'Warning: The dblink connection has not been tested (the current role is not granted emaj_adm).';
    END If;
-- Report a warning if the max_prepared_transaction GUC setting is not appropriate for parallel rollbacks
    IF current_setting('max_prepared_transactions')::INT <= 1 THEN
      RETURN NEXT format('Warning: The max_prepared_transactions parameter value (%s) on this cluster is too low to launch parallel '
                         'rollback.',
                         current_setting('max_prepared_transactions'));
    END IF;
-- Report a warning if the emaj_protection_trg event triggers is missing.
-- The other event triggers are protected by the emaj extension they belong to.
    PERFORM 0
      FROM pg_catalog.pg_event_trigger
      WHERE evtname = 'emaj_protection_trg';
    IF NOT FOUND THEN
      RETURN NEXT 'Warning: The "emaj_protection_trg" event triggers is missing. It can be recreated using the '
                  'emaj_enable_protection_by_event_triggers() function.';
    END IF;
-- Report a warning if some E-Maj event triggers exist but are not enabled.
    IF EXISTS
         (SELECT 0
            FROM pg_catalog.pg_event_trigger
              WHERE evtname LIKE 'emaj%'
                AND evtenabled = 'D'
         ) THEN
      RETURN NEXT 'Warning: Some E-Maj event triggers are disabled. You may enable them using the '
                  'emaj_enable_protection_by_event_triggers() function.';
    END IF;
-- Final message if no error has been yet detected.
    IF NOT v_errorFound THEN
      RETURN NEXT 'No error detected';
    END IF;
--
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

------------------------------------------
--                                      --
-- event trigger related functions      --
--                                      --
------------------------------------------

CREATE OR REPLACE FUNCTION public._emaj_protection_event_trigger_fnct()
 RETURNS EVENT_TRIGGER LANGUAGE plpgsql AS
$_emaj_protection_event_trigger_fnct$
-- This function is called by the emaj_protection_trg event trigger.
-- The function only blocks any attempt to drop the emaj schema or the emaj extension.
-- It is located into the public schema to be able to detect the emaj schema removal attempt.
-- It is also unlinked from the emaj extension to be able to detect the emaj extension removal attempt.
-- Another pair of function and event trigger handles all other drop attempts.
  DECLARE
    r_dropped                RECORD;
  BEGIN
-- Scan all dropped objects.
    FOR r_dropped IN
      SELECT object_type, object_name
        FROM pg_event_trigger_dropped_objects()
    LOOP
      IF r_dropped.object_type = 'schema' AND r_dropped.object_name = 'emaj' THEN
-- Detecting an attempt to drop the emaj object.
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "emaj".'
                        ' Please execute the emaj.emaj_drop_extension() function if you really want to remove all E-Maj components.';
      END IF;
      IF r_dropped.object_type = 'extension' AND r_dropped.object_name = 'emaj' THEN
-- Detecting an attempt to drop the emaj extension.
        RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the emaj extension.'
                        ' Please execute the emaj.emaj_drop_extension() function if you really want to remove all E-Maj components.';
      END IF;
    END LOOP;
  END;
$_emaj_protection_event_trigger_fnct$;
COMMENT ON FUNCTION public._emaj_protection_event_trigger_fnct() IS
$$E-Maj extension: support of the emaj_protection_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj._event_trigger_sql_drop_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_sql_drop_fnct$
-- This function is called by the emaj_sql_drop_trg event trigger.
-- The function blocks any ddl operation that leads to a drop of
--   - an application table or a sequence registered into an active (not stopped) E-Maj group, or a schema containing such tables/sequence
--   - an E-Maj schema, a log table, a log sequence, a log function or a log trigger
-- The drop of emaj schema or extension is managed by another event trigger.
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when dropping their objects.
  DECLARE
    v_groupName              TEXT;
    v_tableName              TEXT;
    r_dropped                RECORD;
  BEGIN
-- Scan all dropped objects.
    FOR r_dropped IN
      SELECT object_type, schema_name, object_name, object_identity, original
        FROM pg_event_trigger_dropped_objects()
    LOOP
      CASE
        WHEN r_dropped.object_type = 'schema' THEN
-- The object is a schema.
-- Look at the emaj_relation table to verify that the schema being dropped does not belong to any active (not stopped) group.
          SELECT string_agg(DISTINCT rel_group, ', ' ORDER BY rel_group) INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF v_groupName IS NOT NULL THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application schema "%". But it belongs to the active tables'
                            ' groups "%".', r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_schema table to verify that the schema being dropped is not an E-Maj schema containing log tables.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_schema
                  WHERE sch_name = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the schema "%". But dropping an E-Maj schema is not allowed.',
              r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'table' THEN
-- The object is a table.
-- Look at the emaj_relation table to verify that the table being dropped does not currently belong to any active (not stopped) group.
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application table "%.%". But it belongs to the active tables'
                            ' group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_relation table to verify that the table being dropped is not a log table.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_relation
                  WHERE rel_log_schema = r_dropped.schema_name
                    AND rel_log_table = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log table "%.%". But dropping an E-Maj log table is not allowed.',
                            r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'sequence' THEN
-- The object is a sequence.
-- Look at the emaj_relation table to verify that the sequence being dropped does not currently belong to any active (not stopped) group.
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = r_dropped.object_name
              AND upper_inf(rel_time_range)
              AND group_is_logging;
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the application sequence "%.%". But it belongs to the active'
                            ' tables group "%".', r_dropped.schema_name, r_dropped.object_name, v_groupName;
          END IF;
-- Look at the emaj_relation table to verify that the sequence being dropped is not a log sequence.
          IF EXISTS
               (SELECT 0
                  FROM emaj.emaj_relation
                  WHERE rel_log_schema = r_dropped.schema_name
                    AND rel_log_sequence = r_dropped.object_name
               ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log sequence "%.%". But dropping an E-Maj sequence is not'
                           ' allowed.', r_dropped.schema_name, r_dropped.object_name;
          END IF;
        WHEN r_dropped.object_type = 'function' THEN
-- The object is a function.
-- Look at the emaj_relation table to verify that the function being dropped is not a log function.
          IF EXISTS
            (SELECT 0
               FROM emaj.emaj_relation
               WHERE r_dropped.object_identity = quote_ident(rel_log_schema) || '.' || quote_ident(rel_log_function) || '()'
            ) THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the log function "%". But dropping an E-Maj log function is not'
                            ' allowed.', r_dropped.object_identity;
          END IF;
-- Verify that the function is not public._emaj_protection_event_trigger_fnct() (which is intentionaly not linked to the emaj extension)
          IF r_dropped.object_identity = 'public._emaj_protection_event_trigger_fnct()' THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the public._emaj_protection_event_trigger_fnct() function. '
                            'But dropping this E-Maj technical function is not allowed.';
          END IF;
        WHEN r_dropped.object_type = 'trigger' THEN
-- The object is a trigger.
-- Look at the trigger name pattern to identify emaj trigger.
-- Do not raise an exception if the triggers drop is derived from a drop of a table or a function.
          IF r_dropped.original AND
             (r_dropped.object_identity LIKE 'emaj_log_trg%' OR r_dropped.object_identity LIKE 'emaj_trunc_trg%') THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the "%" E-Maj trigger. But dropping an E-Maj trigger is not allowed.',
              r_dropped.object_identity;
          END IF;
        WHEN r_dropped.object_type = 'table constraint' AND tg_tag = 'ALTER TABLE' THEN
-- The object is a table constraint. It may be primary key or another constraint.
-- Verify that if the targeted table belongs to a rollbackable group, its primary key still exists.
-- The table name can be found in the last part of the object_identity variable,
-- after the '.' that separates the schema and table names, and possibly between double quotes
          v_tableName = substring(r_dropped.object_identity from '\.(?:"?)(.*)(?:"?)');
          SELECT rel_group INTO v_groupName
            FROM emaj.emaj_relation
                 JOIN emaj.emaj_group ON (group_name = rel_group)
            WHERE rel_schema = r_dropped.schema_name
              AND rel_tblseq = v_tableName
              AND upper_inf(rel_time_range)
              AND group_is_rollbackable
              AND NOT EXISTS
                (SELECT 0
                   FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = c.relnamespace)
                        JOIN pg_catalog.pg_constraint ON (connamespace = pg_namespace.oid AND conrelid = c.oid)
                             WHERE contype = 'p'
                               AND nspname = rel_schema
                               AND c.relname = rel_tblseq
                );
          IF FOUND THEN
            RAISE EXCEPTION 'E-Maj event trigger: Attempting to drop the primary key for the application table "%.%". But it belongs to'
                            ' the rollbackable tables group "%".', r_dropped.schema_name, v_tableName, v_groupName;
          END IF;
        ELSE
          CONTINUE;
      END CASE;
    END LOOP;
  END;
$_event_trigger_sql_drop_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_sql_drop_fnct() IS
$$E-Maj extension: support of the emaj_sql_drop_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj._event_trigger_table_rewrite_fnct()
RETURNS EVENT_TRIGGER LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_event_trigger_table_rewrite_fnct$
-- This function is called by the emaj_table_rewrite_trg event trigger.
-- The function blocks any ddl operation that leads to a table rewrite for:
--   - an application table registered into an active (not stopped) E-Maj group,
--   - an E-Maj log table.
-- The function is declared SECURITY DEFINER so that non emaj roles can access the emaj internal tables when altering their tables.
  DECLARE
    v_tableSchema            TEXT;
    v_tableName              TEXT;
    v_groupName              TEXT;
  BEGIN
-- Get the schema and table names of the altered table.
    SELECT nspname, relname INTO v_tableSchema, v_tableName
      FROM pg_catalog.pg_class
           JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE pg_class.oid = pg_event_trigger_table_rewrite_oid();
-- Look at the emaj_relation table to verify that the table being rewritten does not belong to any active (not stopped) group.
    SELECT rel_group INTO v_groupName
      FROM emaj.emaj_relation
           JOIN emaj.emaj_group ON (group_name = rel_group)
      WHERE rel_schema = v_tableSchema
        AND rel_tblseq = v_tableName
        AND upper_inf(rel_time_range)
        AND group_is_logging;
    IF FOUND THEN
-- The table is an application table that belongs to a group, so raise an exception.
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the application table "%.%" structure. But the table belongs to the'
                      ' active tables group "%".', v_tableSchema, v_tableName , v_groupName;
    END IF;
-- Look at the emaj_relation table to verify that the table being rewritten is not a known log table.
    SELECT rel_group INTO v_groupName
      FROM emaj.emaj_relation
      WHERE rel_log_schema = v_tableSchema
        AND rel_log_table = v_tableName;
    IF FOUND THEN
-- The table is an E-Maj log table, so raise an exception.
      RAISE EXCEPTION 'E-Maj event trigger: Attempting to change the log table "%.%" structure. But the table belongs to the tables'
                      ' group "%".', v_tableSchema, v_tableName , v_groupName;
    END IF;
  END;
$_event_trigger_table_rewrite_fnct$;
COMMENT ON FUNCTION emaj._event_trigger_table_rewrite_fnct() IS
$$E-Maj extension: support of the emaj_table_rewrite_trg event trigger.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_disable_protection_by_event_triggers()
RETURNS INT LANGUAGE plpgsql AS
$emaj_disable_protection_by_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively disabled event triggers.
  DECLARE
    v_eventTriggers          TEXT[];
  BEGIN
-- Call the _disable_event_triggers() function and get the disabled event trigger names array.
    SELECT emaj._disable_event_triggers() INTO v_eventTriggers;
-- Keep a trace into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('DISABLE_PROTECTION', 'EVENT TRIGGERS DISABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- Return the number of disabled event triggers.
    RETURN coalesce(array_length(v_eventTriggers,1),0);
  END;
$emaj_disable_protection_by_event_triggers$;
COMMENT ON FUNCTION emaj.emaj_disable_protection_by_event_triggers() IS
$$Disables the protection of E-Maj components by event triggers.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_enable_protection_by_event_triggers()
RETURNS INT LANGUAGE plpgsql AS
$emaj_enable_protection_by_event_triggers$
-- This function enables all known E-Maj event triggers that are in disabled state.
-- It may be used by an emaj_adm role.
-- Output: number of effectively enabled event triggers.
  DECLARE
    v_eventTriggers          TEXT[];
  BEGIN
-- Build the event trigger names array from the pg_event_trigger table.
    SELECT coalesce(array_agg(evtname  ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger
      WHERE evtname LIKE 'emaj%'
        AND evtenabled = 'D';
-- Call the _enable_event_triggers() function.
    PERFORM emaj._enable_event_triggers(v_eventTriggers);
-- Keep a trace into the emaj_hist table.
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
      VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS ENABLED',
              CASE WHEN v_eventTriggers <> ARRAY[]::TEXT[] THEN array_to_string(v_eventTriggers, ', ') ELSE '<none>' END);
-- Return the number of enabled event triggers.
    RETURN coalesce(array_length(v_eventTriggers,1),0);
  END;
$emaj_enable_protection_by_event_triggers$;
COMMENT ON FUNCTION emaj.emaj_enable_protection_by_event_triggers() IS
$$Enables the protection of E-Maj components by event triggers.$$;

CREATE OR REPLACE FUNCTION emaj._disable_event_triggers()
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_disable_event_triggers$
-- This function disables all known E-Maj event triggers that are in enabled state.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_disable_event_triggers_protection() function.
-- Output: array of effectively disabled event trigger names. It can be reused as input when calling _enable_event_triggers().
-- The function is declared as SECURITY DEFINER because only superusers can alter an event trigger
  DECLARE
    v_eventTrigger           TEXT;
    v_eventTriggers          TEXT[] = ARRAY[]::TEXT[];
  BEGIN
-- Build the event trigger names array from the pg_event_trigger table.
-- A single operation like _alter_groups() may call the function several times. But this is not an issue as only enabled triggers are
-- disabled.
    SELECT coalesce(array_agg(evtname ORDER BY evtname),ARRAY[]::TEXT[]) INTO v_eventTriggers
      FROM pg_catalog.pg_event_trigger
      WHERE evtname LIKE 'emaj%'
        AND evtenabled <> 'D';
-- Disable each event trigger.
    FOREACH v_eventTrigger IN ARRAY v_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I DISABLE',
                     v_eventTrigger);
    END LOOP;
--
    RETURN v_eventTriggers;
  END;
$_disable_event_triggers$;

CREATE OR REPLACE FUNCTION emaj._enable_event_triggers(p_eventTriggers TEXT[])
RETURNS TEXT[] LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_enable_event_triggers$
-- This function enables all event triggers supplied as parameter.
-- It also recreates the emaj_protection_trg event trigger if it does not exist. This event trigger is the only component
-- that is not linked to the emaj extension and cannot be protected by another event trigger.
-- The function is called by functions that alter or drop E-Maj components, such as
--   _drop_group(), _alter_groups(), _delete_before_mark_group() and _reset_groups().
-- It is also called by the user emaj_enable_event_triggers_protection() function.
-- Input: array of event trigger names to enable.
-- Output: same array.
-- The function is declared as SECURITY DEFINER because only superusers can alter an event trigger
  DECLARE
    v_eventTrigger           TEXT;
  BEGIN
-- If the emaj_protection_trg event trigger does not exist, recreate it and report.
    PERFORM 0
      FROM pg_catalog.pg_event_trigger
      WHERE evtname = 'emaj_protection_trg';
    IF NOT FOUND THEN
      CREATE EVENT TRIGGER emaj_protection_trg
        ON sql_drop
        WHEN TAG IN ('DROP EXTENSION','DROP SCHEMA')
        EXECUTE PROCEDURE public._emaj_protection_event_trigger_fnct();
--      COMMENT ON EVENT TRIGGER emaj_protection_trg IS
--      $$Blocks the removal of the emaj extension or schema.$$;
      RAISE WARNING '_enable_event_triggers: the emaj_protection_trg event trigger has been recreated.';
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_wording)
        VALUES ('ENABLE_PROTECTION', 'EVENT TRIGGERS RECREATED', 'emaj_protection_trg');
    END If;
    FOREACH v_eventTrigger IN ARRAY p_eventTriggers
    LOOP
      EXECUTE format('ALTER EVENT TRIGGER %I ENABLE',
                     v_eventTrigger);
    END LOOP;
--
  RETURN p_eventTriggers;
  END;
$_enable_event_triggers$;

------------------------------------------
--                                      --
-- Extension drop function              --
--                                      --
------------------------------------------
CREATE OR REPLACE FUNCTION emaj.emaj_drop_extension()
RETURNS VOID LANGUAGE plpgsql AS
$emaj_drop_extension$
-- This function drops emaj from the current database, with both installation kinds,
-- - either as EXTENSION (i.e. with a CREATE EXTENSION SQL statement),
-- - or with the alternate psql script.
  DECLARE
    v_nbObject              INTEGER;
    v_roleToDrop            BOOLEAN;
    v_dbList                TEXT;
    v_granteeRoleList       TEXT;
    v_granteeClassList      TEXT;
    v_granteeFunctionList   TEXT;
    v_tspList               TEXT;
    r_object                RECORD;
  BEGIN
-- First perform some checks to verify that the conditions to execute the function are met.
--
-- Check emaj schema is present.
    PERFORM 1 FROM pg_namespace WHERE nspname = 'emaj';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_drop_extension: The schema ''emaj'' doesn''t exist';
    END IF;
--
-- For extensions, check the current role is superuser.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'emaj') THEN
       PERFORM 1 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
       IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be a superuser';
       END IF;
    ELSE
-- Otherwise, check the current role is the owner of the emaj schema, i.e. the role who installed emaj.
      PERFORM 1 FROM pg_catalog.pg_roles, pg_catalog.pg_namespace
        WHERE nspowner = pg_roles.oid AND nspname = 'emaj' AND rolname = current_user;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_drop_extension: The role executing this script must be the owner of the emaj schema';
      END IF;
    END IF;
--
-- Check that no E-Maj schema contain any non E-Maj object.
    v_nbObject = 0;
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_schemas() msg
        WHERE msg NOT LIKE 'Error: The E-Maj schema % does not exist anymore.'
      LOOP
-- An E-Maj schema contains objects that do not belong to the extension.
      RAISE WARNING 'emaj_drop_extension - schema consistency checks: %',r_object.msg;
      v_nbObject = v_nbObject + 1;
    END LOOP;
    IF v_nbObject > 0 THEN
      RAISE EXCEPTION 'emaj_drop_extension: There are % unexpected objects in E-Maj schemas. Drop them before reexecuting the uninstall'
                      ' function.', v_nbObject;
    END IF;
--
-- OK, perform the removal actions.
--
-- Disable event triggers that would block the DROP EXTENSION command.
    PERFORM emaj.emaj_disable_protection_by_event_triggers();
--
-- If the emaj_demo_cleanup function exists (created by the emaj_demo.sql script), execute it.
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_demo_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_demo_cleanup();
    END IF;
-- If the emaj_parallel_rollback_test_cleanup function exists (created by the emaj_prepare_parallel_rollback_test.sql script), execute it.
    PERFORM 0 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
      WHERE pronamespace = pg_namespace.oid AND nspname = 'emaj' AND proname = 'emaj_parallel_rollback_test_cleanup';
    IF FOUND THEN
      PERFORM emaj.emaj_parallel_rollback_test_cleanup();
    END IF;
--
-- Drop all created groups, bypassing potential errors, to remove all components not directly linked to the EXTENSION.
    PERFORM emaj.emaj_force_drop_group(group_name) FROM emaj.emaj_group;
--
-- Drop the emaj extension, if it is an EXTENSION.
    DROP EXTENSION IF EXISTS emaj CASCADE;
--
-- Drop the primary schema.
    DROP SCHEMA IF EXISTS emaj CASCADE;
--
-- Drop the event trigger that protects the extension against unattempted drop and its function (they are external to the extension).
    DROP FUNCTION IF EXISTS public._emaj_protection_event_trigger_fnct() CASCADE;
--
-- Revoke also the grant given to emaj_adm on the dblink_connect_u function at install time.
    FOR r_object IN
      SELECT nspname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
        WHERE pronamespace = pg_namespace.oid AND proname = 'dblink_connect_u' AND pronargs = 2
      LOOP
        BEGIN
          EXECUTE 'REVOKE ALL ON FUNCTION ' || r_object.nspname || '.dblink_connect_u(text,text) FROM emaj_adm';
        EXCEPTION
          WHEN insufficient_privilege THEN
            RAISE WARNING 'emaj_drop_extension: Trying to REVOKE grants on function dblink_connect_u() raises an exception. Continue...';
        END;
    END LOOP;
--
-- Check if emaj roles can be dropped.
    v_roleToDrop = true;
--
-- Are emaj_roles also used in other databases of the cluster ?
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_viewer' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
    v_dbList = NULL;
    SELECT string_agg(datname,', ') INTO v_dbList FROM (
      SELECT DISTINCT datname FROM pg_catalog.pg_shdepend shd, pg_catalog.pg_database db, pg_catalog.pg_roles r
        WHERE db.oid = dbid AND r.oid = refobjid AND rolname = 'emaj_adm' AND datname <> current_database()
      ) AS t;
    IF v_dbList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: emaj_adm role is also referenced in some other databases (%)',v_dbList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to other roles ?
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList
      FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_viewer';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_viewer role.',
                    v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeRoleList = NULL;
    SELECT string_agg(q.rolname,', ') INTO v_granteeRoleList
      FROM pg_catalog.pg_auth_members m, pg_catalog.pg_roles r, pg_catalog.pg_roles q
      WHERE m.roleid = r.oid AND m.member = q.oid AND r.rolname = 'emaj_adm';
    IF v_granteeRoleList IS NOT NULL THEN
      RAISE WARNING 'emaj_drop_extension: There are remaining roles (%) who have been granted emaj_adm role.',
            v_granteeRoleList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to relations (tables, views, sequences) (other than just dropped emaj ones) ?
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role has some remaining grants on tables, views or sequences (%).',
                    v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeClassList = NULL;
    SELECT string_agg(nspname || '.' || relname, ', ') INTO v_granteeClassList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_class
      WHERE pg_namespace.oid = relnamespace AND array_to_string (relacl,';') LIKE '%emaj_adm=%';
    IF v_granteeClassList IS NOT NULL THEN
      IF length(v_granteeClassList) > 200 THEN
        v_granteeClassList = substr(v_granteeClassList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_adm role has some remaining grants on tables, views or sequences (%).',
                    v_granteeClassList;
      v_roleToDrop = false;
    END IF;
--
-- Are emaj roles granted to functions (other than just dropped emaj ones) ?
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_viewer=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeFunctionList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_viewer role has some remaining grants on functions (%).',
                    v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
    v_granteeFunctionList = NULL;
    SELECT string_agg(nspname || '.' || proname || '()', ', ') INTO v_granteeFunctionList
      FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
      WHERE pg_namespace.oid = pronamespace AND array_to_string (proacl,';') LIKE '%emaj_adm=%';
    IF v_granteeFunctionList IS NOT NULL THEN
      IF length(v_granteeFunctionList) > 200 THEN
        v_granteeClassList = substr(v_granteeFunctionList,1,200) || '...';
      END IF;
      RAISE WARNING 'emaj_drop_extension: emaj_adm role has some remaining grants on functions (%).',
                    v_granteeFunctionList;
      v_roleToDrop = false;
    END IF;
--
-- If emaj roles can be dropped, drop them.
    IF v_roleToDrop THEN
-- Revoke the remaining grants set on tablespaces
      SELECT string_agg(spcname, ', ') INTO v_tspList
        FROM pg_catalog.pg_tablespace
        WHERE array_to_string (spcacl,';') LIKE '%emaj_viewer=%' OR array_to_string (spcacl,';') LIKE '%emaj_adm=%';
      IF v_tspList IS NOT NULL THEN
        EXECUTE 'REVOKE ALL ON TABLESPACE ' || v_tspList || ' FROM emaj_viewer, emaj_adm';
      END IF;
-- ... and drop both emaj_viewer and emaj_adm roles.
      DROP ROLE emaj_viewer, emaj_adm;
      RAISE WARNING 'emaj_drop_extension: emaj_adm and emaj_viewer roles have been dropped.';
    ELSE
      RAISE WARNING 'emaj_drop_extension: For these reasons, emaj roles are not dropped by this script.';
    END IF;
--
    RETURN;
  END;
$emaj_drop_extension$;
COMMENT ON FUNCTION emaj.emaj_drop_extension() IS
$$Uninstalls the E-Maj components from the current database.$$;
--
------------------------------------------
--                                      --
-- event triggers                       --
--                                      --
------------------------------------------
--
-- Event triggers creation depends on postgres version:
-- - sql_drop event trigger needs postgres 9.3+
-- - table_rewrite trigger needs postgres 9.5+
-- Now that the oldest supported postgres version is 9.5, all installations should have both event triggers.
-- If E-Maj has been installed with older postgres versions, and this version has then been upgraded, the
-- set_event_triggers_protection.sql script can be used to add the missing components.

-- sql_drop event triggers

CREATE EVENT TRIGGER emaj_protection_trg
  ON sql_drop
  WHEN TAG IN ('DROP EXTENSION','DROP SCHEMA')
  EXECUTE PROCEDURE public._emaj_protection_event_trigger_fnct();
--COMMENT ON EVENT TRIGGER emaj_protection_trg IS
--$$Blocks the removal of the emaj extension or schema.$$;

-- remove both event trigger components from the extension, so that they can fire the "DROP EXTENSION emaj"
--ALTER EXTENSION emaj DROP FUNCTION public._emaj_protection_event_trigger_fnct();
--ALTER EXTENSION emaj DROP EVENT TRIGGER emaj_protection_trg;

CREATE EVENT TRIGGER emaj_sql_drop_trg
  ON sql_drop
  WHEN TAG IN ('DROP FUNCTION','DROP SCHEMA','DROP SEQUENCE','DROP TABLE','ALTER TABLE','DROP TRIGGER')
  EXECUTE PROCEDURE emaj._event_trigger_sql_drop_fnct();
--COMMENT ON EVENT TRIGGER emaj_sql_drop_trg IS
--$$Controls the removal of E-Maj components.$$;

-- table_rewrite event trigger

CREATE EVENT TRIGGER emaj_table_rewrite_trg
  ON table_rewrite
  EXECUTE PROCEDURE emaj._event_trigger_table_rewrite_fnct();
--COMMENT ON EVENT TRIGGER emaj_table_rewrite_trg IS
--$$Controls some changes in E-Maj tables structure.$$;

------------------------------------
--                                --
-- Rights on emaj components      --
--                                --
------------------------------------

-- Global rights on functions.
--

-- Revoke all rights on all created functions from PUBLIC.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA emaj FROM PUBLIC;

-- Rights given to emaj_adm.
--
-- emaj_adm can execute all emaj functions and access all emaj tables without any restrictions.

GRANT ALL ON SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL TABLES IN SCHEMA emaj TO emaj_adm;
GRANT ALL ON ALL SEQUENCES IN SCHEMA emaj TO emaj_adm;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA emaj TO emaj_adm;

-- Rights given to emaj_viewer.
--
-- emaj_viewer can only
-- ... view the emaj objects, i.e. the content of emaj and log tables,
--     except the emaj_param table that emaj_viewer should only see through the emaj_visible_param view
--     that hides the password used by the configured dblink user,

GRANT USAGE ON SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA emaj TO emaj_viewer;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA emaj TO emaj_viewer;

REVOKE SELECT ON TABLE emaj.emaj_param FROM emaj_viewer;

-- ... and execute a subset of emaj functions for which rights are explicitely granted.
GRANT EXECUTE ON FUNCTION emaj._pg_version_num() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_group_names(p_groupNames TEXT[], p_mayBeNull BOOLEAN, p_lockGroups BOOLEAN,
                                                  p_checkIdle BOOLEAN, p_checkLogging BOOLEAN,
                                                  p_checkRollbackable BOOLEAN, p_checkUnprotected BOOLEAN)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_mark_name(p_groupNames TEXT[], p_mark TEXT, p_checkActive BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_marks_range(p_groupNames TEXT[], INOUT p_firstMark TEXT, INOUT p_lastMark TEXT,
                          p_finiteUpperBound BOOLEAN, p_checkLogSession BOOLEAN,
                          OUT p_firstMarkTimeId BIGINT, OUT p_lastMarkTimeId BIGINT,
                          OUT p_firstMarkTs TIMESTAMPTZ, OUT p_lastMarkTs TIMESTAMPTZ,
                          OUT p_firstMarkEmajGid BIGINT, OUT p_lastMarkEmajGid BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_current_log_table(p_app_schema TEXT, p_app_table TEXT,
                          OUT log_schema TEXT, OUT log_table TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, p_firstMarkTimeId BIGINT, p_lastMarkTimeId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._sequence_stat_seq(r_rel emaj.emaj_relation, p_beginTimeId BIGINT, p_endTimeId BIGINT,
                                                  OUT p_increments BIGINT, OUT p_hasStructureChanged BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_log_sequence_last_value(p_schema TEXT, p_sequence TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_app_sequence_last_value(p_schema TEXT, p_sequence TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_groups(p_groupNames TEXT[], p_onErrorStop boolean) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_datetime TIMESTAMPTZ) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(p_groupName TEXT, p_mark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_previous_mark_group(p_groupName TEXT, p_mark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_check(p_groupNames TEXT[], p_mark TEXT, p_isAlterGroupAllowed BOOLEAN, isRollbackSimulation BOOLEAN)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_planning(p_rlbkId INT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_set_batch_number(p_rlbkId INT, p_batchNumber INT, p_schema TEXT, p_table TEXT,
                          p_isReplRoleReplica BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_cleanup_rollback_state() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._cleanup_rollback_state() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._detailed_log_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_sequence_stat_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_sequence_stat_groups(p_groupNames TEXT[], p_firstMark TEXT, p_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._sequence_stat_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_firstMark TEXT, p_lastMark TEXT)
                          TO emaj_viewer;

GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_group(p_groupName TEXT, p_mark TEXT, p_isLoggedRlbk BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_groups(p_groupNames TEXT[], p_mark TEXT, p_isLoggedRlbk BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._estimate_rollback_groups(p_groupNames TEXT[], p_multiGroup BOOLEAN, p_mark TEXT, p_isLoggedRlbk BOOLEAN)
                          TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_version() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_sequences_last_value(p_groupsIncludeFilter TEXT, p_groupsExcludeFilter TEXT,
                                                         p_tablesIncludeFilter TEXT, p_tablesExcludeFilter TEXT,
                                                         p_sequencesIncludeFilter TEXT, p_sequencesExcludeFilter TEXT,
                                                         OUT p_key TEXT, OUT p_value TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_consolidable_rollbacks() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, p_lastMark TEXT, p_optionsList TEXT,
                                                               p_tblseqs TEXT[]) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_group(p_groupName TEXT, p_firstMark TEXT, INOUT p_lastMark TEXT,
                                                           p_optionsList TEXT, p_tblseqs TEXT[], p_genSqlOnly BOOLEAN,
                                                           OUT p_nbStmt INT, OUT p_copyOptions TEXT, OUT p_noEmptyFiles BOOLEAN,
                                                           OUT p_isPsqlCopy BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_tbl(p_logSchema TEXT, p_logTable TEXT, p_emajVerbAttnum INT, p_pkCols TEXT[],
                                                         p_firstEmajGid BIGINT, p_lastEmajGid BIGINT, p_consolidationLevel TEXT,
                                                         p_emajColumnsList TEXT, p_colsOrder TEXT, p_orderBy TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_dump_changes_seq(p_schema TEXT, p_sequence TEXT, p_firstEmajGid BIGINT, p_lastEmajGid BIGINT,
                                                         p_consolidationLevel TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_all_groups() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_all_schemas() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_viewer;

----------------------------------------
--                                    --
-- specific operations for extension  --
--                                    --
----------------------------------------
-- Register emaj tables content as candidate for pg_dump.
--SELECT pg_catalog.pg_extension_config_dump('emaj_version_hist','WHERE NOT upper_inf(verh_time_range)');
--SELECT pg_catalog.pg_extension_config_dump('emaj_param','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_hist','WHERE hist_id > 1');
--SELECT pg_catalog.pg_extension_config_dump('emaj_time_stamp','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_group','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_group_hist','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_schema','WHERE sch_name <> ''emaj''');
--SELECT pg_catalog.pg_extension_config_dump('emaj_relation','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_rel_hist','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_log_session','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_mark','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_sequence','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_table','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_seq_hole','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_relation_change','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_session','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_plan','');
--SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_stat','');

-- Register emaj sequences values as candidate for pg_dump.
--SELECT pg_catalog.pg_extension_config_dump('emaj_global_seq','');
--SELECT pg_catalog.pg_extension_config_dump('emaj.emaj_hist_hist_id_seq','');
--SELECT pg_catalog.pg_extension_config_dump('emaj.emaj_time_stamp_time_id_seq','');
--SELECT pg_catalog.pg_extension_config_dump('emaj.emaj_rlbk_rlbk_id_seq','');

-- Insert the emaj schema into the emaj_schema table.
INSERT INTO emaj.emaj_schema (sch_name) VALUES ('emaj');
-- Insert the INIT event into the operations history.
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <devel>', 'Initialisation completed');
-- Update the emaj_version_hist row to record the installation duration and shift the time range lower bound to the current time.
WITH start_time_data AS (
  SELECT clock_timestamp() - lower(verh_time_range) AS duration
    FROM emaj.emaj_version_hist
    LIMIT 1
  )
  UPDATE emaj.emaj_version_hist
    SET verh_time_range = TSTZRANGE(clock_timestamp(), null, '[]'), verh_install_duration = duration
    FROM start_time_data;

-- Final checks and messages.
DO LANGUAGE plpgsql
$do$
  BEGIN
    RAISE NOTICE 'E-Maj installation: E-Maj successfully installed.';
-- Check that the role is superuser.
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE WARNING 'E-Maj installation: The current user (%) is not a superuser. This may lead to permission issues when using E-Maj.', current_user;
    END IF;
-- Check the max_prepared_transactions GUC value and report a warning if its value is too low for parallel rollback.
    IF current_setting('max_prepared_transactions')::INT <= 1 THEN
      RAISE WARNING 'E-Maj installation: As the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel'
                    ' rollback is possible.', current_setting('max_prepared_transactions');
    END IF;
--
    RETURN;
  END;
$do$;
COMMIT;
