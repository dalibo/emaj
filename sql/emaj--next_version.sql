--
-- E-Maj : logs and rollbacks table updates : Version <NEXT_VERSION>
--
-- This software is distributed under the GNU General Public License.
--
-- This script is automatically called by a "CREATE EXTENSION emaj;" statement in postgres 9.1+.
--
-- This script must be executed by a role having SUPERUSER privileges.
-- Before its execution:
-- 	-> the concerned cluster may contain a tablespace named "tspemaj" to hold E-Maj files,
--     for instance previously created by
--	   CREATE TABLESPACE tspemaj LOCATION '/.../tspemaj',
--	-> the plpgsql language must have been created in the concerned database,
-- It is also advisable to have the dblink contrib/extension installed in order to take benefit of the E-Maj rollback monitoring capabilities.

COMMENT ON SCHEMA emaj IS
$$Contains all E-Maj related objects.$$;

-- create emaj roles
-- and set tspemaj as current_tablespace if exists
DO LANGUAGE plpgsql
$tmp$
  DECLARE
    v_pgVersion              TEXT = substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)');
    v_stmt                   TEXT;
    r_fnct                   RECORD;
  BEGIN
-- create emaj roles (NOLOGIN), if they do not exist
-- does 'emaj_adm' already exist ?
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = 'emaj_adm';
-- if no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_adm;
      COMMENT ON ROLE emaj_adm IS
        $$This role may be granted to other roles in charge of E-Maj administration.$$;
    END IF;
-- give EXECUTE right to emaj_adm on all dblink_connect_u functions.
--   They are revoked by default, but are needed to monitor the rollback operations progress.
--   Note dblink may have been installed in several schemas and we don't know at installation time
--   what will be the search_path of future users.
    FOR r_fnct IN
      SELECT nspname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
        WHERE pg_proc.pronamespace = pg_namespace.oid
          AND proname = 'dblink_connect_u' AND pronargs = 2
      LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' ||
                quote_ident(r_fnct.nspname) || '.dblink_connect_u(text,text) TO emaj_adm';
    END LOOP;
-- does 'emaj_viewer' already exist ?
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = 'emaj_viewer';
-- if no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_viewer;
      COMMENT ON ROLE emaj_viewer IS
        $$This role may be granted to other roles allowed to view E-Maj objects content.$$;
    END IF;
-- if tspemaj tablespace exists,
--   use it as default_tablespace for emaj tables creation
--   and grant the create rights on it to emaj_adm
    PERFORM 0 FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
    IF FOUND THEN
      SET LOCAL default_tablespace TO tspemaj;
      GRANT CREATE ON TABLESPACE tspemaj TO emaj_adm;
    END IF;
--
    RETURN;
  END;
$tmp$;

------------------------------------------------
--                                            --
-- emaj enum types, sequences and tables      --
--                                            --
------------------------------------------------

-- enum of the possible values for the rollback status columns
CREATE TYPE emaj._rlbk_status_enum AS ENUM (
  'PLANNING',                -- the emaj rollback is in the initial planning phase
  'LOCKING',                 -- the emaj rollback is acquiring locks on tables
  'EXECUTING',               -- the emaj rollback is in the main executing phase
  'COMPLETED',               -- the emaj rollback is completed but the status of its transaction is not yet known
  'COMMITTED',               -- the emaj rollback transaction is known as committed
  'ABORTED'                  -- the emaj rollback transaction is known as aborted
  );
-- enum of the possible values for the rollback steps
CREATE TYPE emaj._rlbk_step_enum AS ENUM (
  'LOCK_TABLE',              -- set a lock on a table
  'DIS_LOG_TRG',             -- disable a log trigger
  'DROP_FK',                 -- drop a foreign key
  'SET_FK_DEF',              -- set a foreign key deferred
  'RLBK_TABLE',              -- rollback a table
  'DELETE_LOG',              -- delete rows from a log table
  'SET_FK_IMM',              -- set a foreign key immediate
  'ADD_FK',                  -- recreate a foreign key
  'ENA_LOG_TRG',             -- enable a log trigger
  'CTRL+DBLINK',             -- pseudo step representing the periods between 2 steps execution, when dblink is used
  'CTRL-DBLINK'              -- pseudo step representing the periods between 2 steps execution, when dblink is not used
  );

-- the emaj_global_seq sequence provides a unique identifier for all rows inserted into all emaj log tables of the database.
-- It is used to order all these rows in insertion time order for rollback as well as other purposes.
-- (So this order is not based on system time that can be unsafe).
-- The sequence is created with the following  (default) characteristics:
-- - increment = 1
-- - no cache (to keep the delivered nextval value in time order)
-- - no cycle (would the end of the sequence be reached, no new log row would be accepted)
CREATE SEQUENCE emaj.emaj_global_seq;
COMMENT ON SEQUENCE emaj.emaj_global_seq IS
$$Global sequence to identifiy all rows of emaj log tables.$$;

-- table containing Emaj parameters
CREATE TABLE emaj.emaj_param (
  param_key                  TEXT        NOT NULL,       -- parameter key
  param_value_text           TEXT,                       -- value if type is text, otherwise NULL
  param_value_int            BIGINT,                     -- value if type is bigint, otherwise NULL
  param_value_boolean        BOOLEAN,                    -- value if type is boolean, otherwise NULL
  param_value_interval       INTERVAL,                   -- value if type is interval, otherwise NULL
  PRIMARY KEY (param_key)
  );
COMMENT ON TABLE emaj.emaj_param IS
$$Contains E-Maj parameters.$$;

-- table containing the history of all E-Maj events
CREATE TABLE emaj.emaj_hist (
  hist_id                    BIGSERIAL   NOT NULL,       -- internal id
  hist_datetime              TIMESTAMPTZ NOT NULL
                             DEFAULT clock_timestamp(),  -- insertion time
  hist_function              TEXT        NOT NULL,       -- main E-Maj function generating the event
  hist_event                 TEXT,                       -- type of event (often BEGIN or END)
  hist_object                TEXT,                       -- object supporting the event (often the group name)
  hist_wording               TEXT,                       -- additional comment
  hist_user                  TEXT
                             DEFAULT session_user,       -- the user who call the E-Maj function
  hist_txid                  BIGINT
                             DEFAULT txid_current(),     -- and its tx_id
  PRIMARY KEY (hist_id)
  );
COMMENT ON TABLE emaj.emaj_hist IS
$$Contains E-Maj events history.$$;

-- table containing the definition of groups' content. Filled and maintained by the user, it is used by emaj_create_group function.
CREATE TABLE emaj.emaj_group_def (
  grpdef_group               TEXT        NOT NULL,       -- name of the group containing this table or sequence
  grpdef_schema              TEXT        NOT NULL,       -- schema name of this table or sequence
  grpdef_tblseq              TEXT        NOT NULL,       -- table or sequence name
  grpdef_priority            INTEGER,                    -- priority level (tables are processed in ascending
                                                         --   order, with NULL last)
  grpdef_log_schema_suffix   TEXT,                       -- schema suffix for the log table, functions and sequence
                                                         --   (NULL for 'emaj' schema)
  grpdef_log_dat_tsp         TEXT,                       -- tablespace for the log table (NULL to use default value)
  grpdef_log_idx_tsp         TEXT,                       -- tablespace for the log index (NULL to use default value)
  PRIMARY KEY (grpdef_group, grpdef_schema, grpdef_tblseq)
-- the group name is included in the pkey so that a table/sequence can be temporarily assigned to several groups
  );
COMMENT ON TABLE emaj.emaj_group_def IS
$$Contains E-Maj groups definition, supplied by the E-Maj administrator.$$;

-- table containing the defined groups
--     rows are created at emaj_create_group time and deleted at emaj_drop_group time
CREATE TABLE emaj.emaj_group (
  group_name                 TEXT        NOT NULL,
  group_is_logging           BOOLEAN     NOT NULL,       -- are log triggers activated ?
                                                         -- true between emaj_start_group(s) and emaj_stop_group(s)
                                                         -- flase in other cases
  group_nb_table             INT,                        -- number of tables at emaj_create_group time
  group_nb_sequence          INT,                        -- number of sequences at emaj_create_group time
  group_is_rollbackable      BOOLEAN,                    -- false for 'AUDIT_ONLY' and true for 'ROLLBACKABLE' groups
  group_creation_datetime    TIMESTAMPTZ NOT NULL        -- start time of the transaction that created the group
                             DEFAULT transaction_timestamp(),
  group_last_alter_datetime  TIMESTAMPTZ,                -- date and time of the last emaj_alter_group() exec,
                                                         -- set to NULL at emaj_create_group() time
  group_pg_version           TEXT        NOT NULL        -- postgres version at emaj_create_group() time
                             DEFAULT substring (version() from E'PostgreSQL\\s([.,0-9,A-Z,a-z]*)'),
  group_comment              TEXT,                       -- optional user comment
  PRIMARY KEY (group_name)
  );
COMMENT ON TABLE emaj.emaj_group IS
$$Contains created E-Maj groups.$$;

-- table containing the relations (tables and sequences) of created tables groups
CREATE TABLE emaj.emaj_relation (
  rel_schema                 TEXT        NOT NULL,       -- schema name containing the relation
  rel_tblseq                 TEXT        NOT NULL,       -- table or sequence name
  rel_group                  TEXT        NOT NULL,       -- name of the group that owns the relation
  rel_priority               INTEGER,                    -- priority level of processing inside the group
  rel_log_schema             TEXT,                       -- schema for the log table, functions and sequence
  rel_log_dat_tsp            TEXT,                       -- tablespace for the log table (NULL for sequences)
  rel_log_idx_tsp            TEXT,                       -- tablespace for the log index (NULL for sequences)
  rel_kind                   TEXT,                       -- similar to the relkind column of pg_class table
                                                         --   ('r' = table, 'S' = sequence)
  PRIMARY KEY (rel_schema, rel_tblseq),
  FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
  );
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;
-- index on emaj_relation used to speedup most functions working on groups with large E-Maj configuration
CREATE INDEX emaj_relation_idx1 ON emaj.emaj_relation (rel_group, rel_kind);
-- index on emaj_relation used to speedup _verify_schema() with large E-Maj configuration
CREATE INDEX emaj_relation_idx2 ON emaj.emaj_relation (rel_log_schema);

-- table containing the marks
CREATE TABLE emaj.emaj_mark (
  mark_group                 TEXT        NOT NULL,       -- group for which the mark has been set
  mark_name                  TEXT        NOT NULL,       -- mark name
  mark_id                    BIGSERIAL   NOT NULL,       -- serial id used to order rows (not to rely on timestamps
                                                         -- that are not safe if system time changes)
  mark_datetime              TIMESTAMPTZ NOT NULL,       -- precise timestamp of the mark creation, used as a reference
                                                         --   for other tables like emaj_sequence and all log tables
  mark_global_seq            BIGINT      NOT NULL,       -- emaj_global_seq last value at mark set (used to rollback)
  mark_is_deleted            BOOLEAN     NOT NULL,       -- boolean to indicate if the mark is deleted
  mark_comment               TEXT,                       -- optional user comment
  mark_txid                  BIGINT                      -- id of the tx that has set the mark
                             DEFAULT txid_current(),
  mark_last_sequence_id      BIGINT,                     -- last sequ_id for the group at the end of the _set_mark_groups operation
  mark_last_seq_hole_id      BIGINT,                     -- last sqhl_id for the group at _set_mark_groups time
  mark_log_rows_before_next  BIGINT,                     -- number of log rows recorded for the group between the mark
                                                         -- and the next one (NULL if last mark)
                                                         -- used to speedup marks list display in phpPgAdmin plugin
  PRIMARY KEY (mark_group, mark_name),
  FOREIGN KEY (mark_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
  );
COMMENT ON TABLE emaj.emaj_mark IS
$$Contains marks set on E-Maj tables groups.$$;

-- table containing the sequences characteristics log
-- (to record at mark time the state of application sequences and sequences used by log tables)
CREATE TABLE emaj.emaj_sequence (
  sequ_id                    BIGSERIAL   NOT NULL,       -- serial id used to delete oldest or newest rows (not to rely
                                                         -- on timestamps that are not safe if system time changes)
  sequ_schema                TEXT        NOT NULL,       -- application or 'emaj' schema that owns the sequence
  sequ_name                  TEXT        NOT NULL,       -- application or emaj sequence name
  sequ_datetime              TIMESTAMPTZ NOT NULL,       -- timestamp the sequence characteristics have been recorded
                                                         --   the same timestamp as referenced in emaj_mark table
  sequ_mark                  TEXT        NOT NULL,       -- name of the mark associated to the insertion timestamp
  sequ_last_val              BIGINT      NOT NULL,       -- sequence last value
  sequ_start_val             BIGINT      NOT NULL,       -- sequence start value, (0 with postgres 8.2)
  sequ_increment             BIGINT      NOT NULL,       -- sequence increment
  sequ_max_val               BIGINT      NOT NULL,       -- sequence max value
  sequ_min_val               BIGINT      NOT NULL,       -- sequence min value
  sequ_cache_val             BIGINT      NOT NULL,       -- sequence cache value
  sequ_is_cycled             BOOLEAN     NOT NULL,       -- sequence flag 'is cycled ?'
  sequ_is_called             BOOLEAN     NOT NULL,       -- sequence flag 'is called ?'
  PRIMARY KEY (sequ_schema, sequ_name, sequ_datetime)
  );
COMMENT ON TABLE emaj.emaj_sequence IS
$$Contains values of sequences at E-Maj set_mark times.$$;

-- table containing the holes in sequences log
-- these holes are due to rollback operations that do not adjust log sequences
-- the hole size = difference of sequence's current last_value and last value at the rollback mark
CREATE TABLE emaj.emaj_seq_hole (
  sqhl_id                    BIGSERIAL   NOT NULL,       -- serial id used to delete oldest or newest rows (not to rely
                                                         -- on timestamps that are not safe if system time changes)
  sqhl_schema                TEXT        NOT NULL,       -- schema that owns the table
  sqhl_table                 TEXT        NOT NULL,       -- application table for which a sequence hole is recorded
                                                         --   in the associated log table
  sqhl_datetime              TIMESTAMPTZ NOT NULL        -- timestamp of the rollback operation that generated the hole
                             DEFAULT transaction_timestamp(),
  sqhl_hole_size             BIGINT      NOT NULL,       -- hole size computed as the difference of 2 sequence last-values
  PRIMARY KEY (sqhl_schema, sqhl_table, sqhl_datetime)
  );
COMMENT ON TABLE emaj.emaj_seq_hole IS
$$Contains description of holes in sequence values for E-Maj log tables.$$;

-- table containing rollback events
CREATE TABLE emaj.emaj_rlbk (
  rlbk_id                    SERIAL      NOT NULL,       -- rollback id
  rlbk_groups                TEXT[]      NOT NULL,       -- groups array to rollback
  rlbk_mark                  TEXT        NOT NULL,       -- mark to rollback to
  rlbk_mark_datetime         TIMESTAMPTZ ,               -- timestamp of the mark as recorded into emaj_mark
  rlbk_is_logged             BOOLEAN     NOT NULL,       -- rollback type: true = logged rollback
  rlbk_nb_session            INT         NOT NULL,       -- number of requested rollback sessions
  rlbk_nb_table              INT         ,               -- total number of tables in groups
  rlbk_nb_sequence           INT         ,               -- number of sequences to rollback
  rlbk_eff_nb_table          INT         ,               -- number of tables with rows to rollback
  rlbk_status                emaj._rlbk_status_enum,     -- rollback status
  rlbk_begin_hist_id         BIGINT      ,               -- hist_id of the rollback BEGIN event in the emaj_hist
                                                         --   used to know if the rollback has been committed or not
  rlbk_is_dblink_used        BOOLEAN     ,               -- boolean indicating whether dblink connection are used
  rlbk_start_datetime        TIMESTAMPTZ ,               -- clock timestamp of the rollback BEGIN event in emaj_hist
  rlbk_end_datetime          TIMESTAMPTZ,                -- clock time the rollback has been completed,
                                                         --   NULL if rollback is in progress or aborted
  rlbk_msg                   TEXT,                       -- result message
  PRIMARY KEY (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk IS
$$Contains description of rollback events.$$;
-- partial index on emaj_rlbk targeting in progress rollbacks (not yet committed or marked as aborted)
CREATE INDEX emaj_rlbk_idx1 ON emaj.emaj_rlbk (rlbk_status)
    WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED');
-- index to help rollback tables purges
CREATE INDEX emaj_rlbk_idx2 ON emaj.emaj_rlbk (rlbk_start_datetime);

-- table containing rollback events sessions
CREATE TABLE emaj.emaj_rlbk_session (
  rlbs_rlbk_id               INT         NOT NULL,       -- rollback id
  rlbs_session               INT         NOT NULL,       -- session number (from 1 to rlbk_nb_session)
  rlbs_txid                  BIGINT      NOT NULL,       -- id of the tx that executes this rollback session
  rlbs_start_datetime        TIMESTAMPTZ NOT NULL,       -- rollback session start timestamp
  rlbs_end_datetime          TIMESTAMPTZ,                -- clock time the rollback session has been completed,
                                                         --   NULL if rollback is in progress
  PRIMARY KEY (rlbs_rlbk_id, rlbs_session),
  FOREIGN KEY (rlbs_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_session IS
$$Contains description of rollback events sessions.$$;

-- table containing the elementary steps of rollback operations
CREATE TABLE emaj.emaj_rlbk_plan (
  rlbp_rlbk_id               INT         NOT NULL,       -- rollback id
  rlbp_step                  emaj._rlbk_step_enum
                                         NOT NULL,       -- kind of elementary step in the rollback processing
  rlbp_schema                TEXT        NOT NULL,       -- schema object of the step
  rlbp_table                 TEXT        NOT NULL,       -- table name
  rlbp_fkey                  TEXT        NOT NULL,       -- foreign key name for step on foreign key, or ''
  rlbp_batch_number          INT,                        -- identifies a set of tables linked by foreign keys
  rlbp_session               INT,                        -- session number the step is affected to
  rlbp_fkey_def              TEXT,                       -- foreign key definition used to recreate it, or NULL
  rlbp_estimated_quantity    BIGINT,                     -- for RLBK_TABLE, estimated number of updates to rollback
                                                         -- for DELETE_LOG, estimated number of rows to delete
                                                         -- for fkeys, estimated number of keys to check
  rlbp_estimated_duration    INTERVAL,                   -- estimated elapse time for the step processing
  rlbp_estimate_method       INT,                        -- method used to compute the estimated duration
                                                         --  1: use rollback stats with volume in same order of magnitude
                                                         --  2: use all previous rollback stats
                                                         --  3: use only parameters (from emaj_param or default values)
  rlbp_start_datetime        TIMESTAMPTZ,                -- clock start time of the step, NULL is not yet started
  rlbp_quantity              BIGINT,                     -- for RLBK_TABLE, number of effectively rolled back updates
                                                         -- for DELETE_LOG, number of effectively deleted log rows
                                                         -- null for fkeys
  rlbp_duration              INTERVAL,                   -- real elapse time of the step, NULL is not yet completed
  PRIMARY KEY (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey),
  FOREIGN KEY (rlbp_rlbk_id) REFERENCES emaj.emaj_rlbk (rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_plan IS
$$Contains description of elementary steps for rollback operations.$$;

-- table containing statistics about previously executed rollback operations
-- and used to estimate rollback durations
-- depending on the step, it contains 1 row per elementary step (like 'RLBK_TABLE' or 'DELETE_LOG'),
-- or 1 row per type of step for 1 rollback operation (like 'DROP_FK', or 'DIS_LOG_TRG')
CREATE TABLE emaj.emaj_rlbk_stat (
  rlbt_step                  emaj._rlbk_step_enum
                                         NOT NULL,       -- kind of elementary step in the rollback processing
  rlbt_schema                TEXT        NOT NULL,       -- schema object of the step
  rlbt_table                 TEXT        NOT NULL,       -- table name
  rlbt_fkey                  TEXT        NOT NULL,       -- foreign key name for step on foreign key, or ''
  rlbt_rlbk_id               INT         NOT NULL,       -- rollback id
  rlbt_rlbk_datetime         TIMESTAMPTZ NOT NULL,       -- timestamp of the rollback that has generated the statistic
  rlbt_quantity              BIGINT      NOT NULL,       -- depending on the step, either estimated quantity processed
                                                         --   by the elementary step or number of executed steps
  rlbt_duration              INTERVAL    NOT NULL,       -- duration or sum of durations of the elementary step(s)
  PRIMARY KEY (rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey, rlbt_rlbk_id)
  );
COMMENT ON TABLE emaj.emaj_rlbk_stat IS
$$Contains statistics about previous E-Maj rollback durations.$$;

------------------------------------
--                                --
-- emaj composite types           --
--                                --
------------------------------------

CREATE TYPE emaj.emaj_log_stat_type AS (
  stat_group                 TEXT,                       -- group name owning the schema.table
  stat_schema                TEXT,                       -- schema name
  stat_table                 TEXT,                       -- table name
  stat_rows                  BIGINT                      -- estimated number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_log_stat_type IS
$$Represents the structure of rows returned by the emaj_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_detailed_log_stat_type AS (
  stat_group                 TEXT,                       -- group name owning the schema.table
  stat_schema                TEXT,                       -- schema name
  stat_table                 TEXT,                       -- table name
  stat_role                  VARCHAR(32),                -- user having generated update events
  stat_verb                  VARCHAR(6),                 -- type of SQL statement (INSERT/UPDATE/DELETE)
  stat_rows                  BIGINT                      -- real number of update events recorded for this table
  );
COMMENT ON TYPE emaj.emaj_detailed_log_stat_type IS
$$Represents the structure of rows returned by the emaj_detailed_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_rollback_activity_type AS (
  rlbk_id                    INT,                        -- rollback id
  rlbk_groups                TEXT[],                     -- groups array to rollback
  rlbk_mark                  TEXT,                       -- mark to rollback to
  rlbk_mark_datetime         TIMESTAMPTZ,                -- timestamp of the mark as recorded into emaj_mark
  rlbk_is_logged             BOOLEAN,                    -- rollback type: true = logged rollback
  rlbk_nb_session            INT,                        -- number of requested sessions
  rlbk_nb_table              INT,                        -- total number of tables in groups
  rlbk_nb_sequence           INT,                        -- number of sequences to rollback
  rlbk_eff_nb_table          INT,                        -- number of tables with rows to rollback
  rlbk_status                emaj._rlbk_status_enum,     -- rollback status
  rlbk_start_datetime        TIMESTAMPTZ,                -- clock timestamp of the rollback BEGIN event in emaj_hist
  rlbk_elapse                INTERVAL,                   -- elapse time since the begining of the execution
  rlbk_remaining             INTERVAL,                   -- estimated remaining time to complete the rollback
  rlbk_completion_pct        SMALLINT                    -- estimated percentage of the rollback operation
  );
COMMENT ON TYPE emaj.emaj_rollback_activity_type IS
$$Represents the structure of rows returned by the emaj_rollback_activity() function.$$;

CREATE TYPE emaj._verify_groups_type AS (                -- this type is not used by functions called by users
  ver_schema                 TEXT,
  ver_tblseq                 TEXT,
  ver_msg                    TEXT
  );
COMMENT ON TYPE emaj._verify_groups_type IS
$$Represents the structure of rows returned by the internal _verify_groups() function.$$;

------------------------------------
--                                --
-- Parameters                     --
--                                --
------------------------------------
INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('emaj_version','<NEXT_VERSION>');

-- Other parameters are optional. They may be set by E-Maj administrators if needed.

-- The dblink_user_password parameter defines the role and its associated password, if any, to establish a dblink
-- connection for the monitoring of rollback operations.
--   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('dblink_user_password','user=<user> password=<password>');

-- The history_retention parameter defines the time interval when a row remains in the emaj history and rollback tables - default is 1 year
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','1 year'::interval);

-- 6 parameters are used by the emaj_estimate_rollback_group(s) and the rollback functions as default values to compute the approximate duration of a rollback operation.
-- The avg_row_rollback_duration parameter defines the average duration needed to rollback a row.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','100 microsecond'::interval);
-- The avg_row_delete_log_duration parameter defines the average duration needed to delete log rows.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','10 microsecond'::interval);
-- The avg_fkey_check_duration parameter defines the average duration needed to check a foreign key.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_fkey_check_duration','20 microsecond'::interval);
-- The fixed_step_rollback_duration parameter defines the fixed cost for any elementary rollback step
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_step_rollback_duration','2.5 millisecond'::interval);
-- The fixed_table_rollback_duration parameter defines the fixed rollback cost for any table or sequence belonging to a group
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','1 millisecond'::interval);
-- The fixed_dblink_rollback_duration parameter defines the fixed cost of dblink use for any rollback step
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_dblink_rollback_duration','4 millisecond'::interval);

-- view readable by emaj_viewer role. It hides the 'dblink_user_password' parameter's value
CREATE VIEW emaj.emaj_visible_param AS
  SELECT param_key,
         CASE WHEN param_key = 'dblink_user_password' THEN '<masked data>' 
                                                      ELSE param_value_text END AS param_value_text,
         param_value_int, param_value_boolean, param_value_interval
  FROM emaj.emaj_param;

------------------------------------
--                                --
-- Low level Functions            --
--                                --
------------------------------------
CREATE OR REPLACE FUNCTION emaj._pg_version()
RETURNS TEXT LANGUAGE sql IMMUTABLE AS
$$
-- This function returns as a string the 2 major parts of the current postgresql version (x.y)
SELECT substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)');
$$;

CREATE OR REPLACE FUNCTION emaj._get_mark_name(TEXT, TEXT)
RETURNS TEXT LANGUAGE sql AS
$$
-- This function returns a mark name if exists for a group, processing the EMAJ_LAST_MARK keyword.
-- input: group name and mark name
-- output: mark name or NULL
SELECT case
         when $2 = 'EMAJ_LAST_MARK' then
              (SELECT mark_name FROM emaj.emaj_mark WHERE mark_group = $1 ORDER BY mark_id DESC LIMIT 1)
         else (SELECT mark_name FROM emaj.emaj_mark WHERE mark_group = $1 AND mark_name = $2)
       end
$$;

CREATE OR REPLACE FUNCTION emaj._get_mark_datetime(TEXT, TEXT)
RETURNS TIMESTAMPTZ LANGUAGE sql AS
$$
-- This function returns the creation timestamp of a mark if exists for a group,
--   processing the EMAJ_LAST_MARK keyword.
-- input: group name and mark name
-- output: mark date-time or NULL
SELECT case
         when $2 = 'EMAJ_LAST_MARK' then
              (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = $1 ORDER BY mark_id DESC LIMIT 1)
         else (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = $1 AND mark_name = $2)
       end
$$;

CREATE OR REPLACE FUNCTION emaj._build_log_seq_name(TEXT, TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS
$$
-- This function returns the log sequence name associated to an application schema and table.
-- input: application schema name and table name
-- output: log sequence name
SELECT $1 || '_' || $2 || '_log_seq'
$$;

CREATE OR REPLACE FUNCTION emaj._dblink_open_cnx(v_cnxName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$_dblink_open_cnx$
-- This function tries to open a named dblink connection.
-- It uses as target: the current cluster (port), the current database and a role defined in the emaj_param table.
-- This role must be defined with a row having:
--   - param_key = 'dblink_user_password',
--   - param_value_text = 'user=<user> password=<password>' with the rules that apply to usual libPQ connect strings
-- The password can be omited if the connection doesn't require it.
-- The dblink_connect_u is used to open the connection so that emaj_adm but non superuser roles can access
--    cluster even when no password is required to log on.
-- Input:  connection name
-- Output: integer status return.
--           1 successful connection
--           0 already opened connection
--          -1 dblink is not installed
--          -2 dblink functions are not visible for the session
--          -3 dblink functions are not accessible by the role
--          -4 the transaction isolation level is not READ COMMITTED
--          -5 no 'dblink_user_password' parameter is defined in the emaj_param table
--          -6 error at dblink_connect() call
  DECLARE
    v_UserPassword           TEXT;
    v_connectString          TEXT;
    v_status                 INT;
  BEGIN
    IF (SELECT count(*) FROM pg_catalog.pg_proc WHERE proname = 'dblink_connect_u') = 0 THEN
      v_status = -1;                      -- dblink is not installed
    ELSIF (SELECT count(*) FROM pg_catalog.pg_proc
             WHERE proname = 'dblink_connect_u' AND pg_function_is_visible(oid)) = 0 THEN
      v_status = -2;                      -- dblink is not visible in the search_path
    ELSIF NOT has_function_privilege('dblink_connect_u(text, text)', 'execute') THEN
      v_status = -3;                      -- current role has not the execute rights on dblink functions
    ELSIF substring(v_cnxName from 1 for 5) = 'rlbk#' AND
          current_setting('transaction_isolation') <> 'read committed' THEN
      v_status = -4;                      -- 'rlbk#*' connection (used for rollbacks) must only come from a
                                          --   READ COMMITTED transaction
    ELSIF v_cnxName = ANY (dblink_get_connections()) THEN
-- dblink is usable, so search the requested connection name in dblink connections list
      v_status = 0;                       -- the requested connection is already open
    ELSE
-- so, get the 'dblink_user_password' parameter if exists, from emaj_param
      SELECT param_value_text INTO v_UserPassword FROM emaj.emaj_param WHERE param_key = 'dblink_user_password';
      IF NOT FOUND THEN
        v_status = -5;                    -- no 'dblink_user_password' parameter is defined in the emaj_param table
      ELSE
-- ... build the connect string
        v_connectString = 'host=localhost port=' || current_setting('port') ||
                          ' dbname=' || current_database() || ' ' || v_userPassword;
-- ... and try to connect
        BEGIN
          PERFORM dblink_connect_u(v_cnxName,v_connectString);
          v_status = 1;                 -- the connection is successful
        EXCEPTION
          WHEN OTHERS THEN
            v_status = -6;              -- the connection attempt failed
        END;
      END IF;
    END IF;
-- for rlbk#* connection, record the dblink connection attempt in the emaj_hist table
    IF substring(v_cnxName from 1 for 5) = 'rlbk#' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('DBLINK','OPEN CNX',v_cnxName,'Status = ' || v_status);
    END IF;
    RETURN v_status;
  END;
$_dblink_open_cnx$;

CREATE OR REPLACE FUNCTION emaj._dblink_is_cnx_opened(v_cnxName TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql AS
$_dblink_is_cnx_opened$
-- This function examines if a named dblink connection is opened.
-- Input:  connection name
-- Output: boolean indicating whether the dblink connection is opened.
  DECLARE
  BEGIN
-- test if dblink is installed and usable by the current user
    IF (SELECT count(*) FROM pg_catalog.pg_proc WHERE proname = 'dblink_connect' AND pg_function_is_visible(oid)) > 0
      AND has_function_privilege('dblink_connect(text, text)', 'execute') THEN
-- dblink is usable, so search connection name in opened dblink connections
      IF v_cnxName = ANY (dblink_get_connections()) THEN
        RETURN true;
      END IF;
    END IF;
    RETURN false;
  END;
$_dblink_is_cnx_opened$;

CREATE OR REPLACE FUNCTION emaj._dblink_close_cnx(v_cnxName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_dblink_close_cnx$
-- This function closes a named dblink connection.
-- Input:  connection name
  DECLARE
  BEGIN
    IF emaj._dblink_is_cnx_opened(v_cnxName) THEN
-- the emaj connection exists, so disconnect
      PERFORM dblink_disconnect(v_cnxName);
-- for emaj* connection, record the dblink disconnection in the emaj_hist table
      IF substring(v_cnxName from 1 for 5) = 'rlbk#' THEN
        INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
          VALUES ('DBLINK','CLOSE CNX',v_cnxName);
      END IF;
    END IF;
    RETURN;
  END;
$_dblink_close_cnx$;

CREATE OR REPLACE FUNCTION emaj._purge_hist()
RETURNS VOID LANGUAGE plpgsql AS
$_purge_hist$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter, but
--   not deleting event traces after the oldest active mark.
-- It also purges oldest rows from emaj_rlbk, emaj_rlbk_session and emaj_rlbk_plan tables, using the same rules.
-- It is called at start group time and when oldest marks are deleted.
  DECLARE
    v_datetimeLimit          TIMESTAMPTZ;
    v_nbPurgedHist           BIGINT;
    v_maxRlbkId              BIGINT;
    v_nbPurgedRlbk           BIGINT;
    v_wording                TEXT = '';
  BEGIN
-- compute the timestamp limit
   SELECT MIN(datetime) FROM
     (                                           -- compute the oldest non deleted mark for all groups
       (SELECT MIN(mark_datetime) FROM emaj.emaj_mark WHERE NOT mark_is_deleted)
     UNION ALL                                   -- compute the current timestamp of now minus the history_retention
       (SELECT current_timestamp -
          coalesce((SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'),'1 YEAR'))
     ) AS t(datetime) INTO v_datetimeLimit;
-- delete oldest rows from emaj_hist
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < v_datetimeLimit;
    GET DIAGNOSTICS v_nbPurgedHist = ROW_COUNT;
    IF v_nbPurgedHist > 0 THEN
      v_wording = v_nbPurgedHist || ' emaj_hist rows deleted';
    END IF;
-- get the greatest rollback identifier to purge
    SELECT MAX(rlbk_id) INTO v_maxRlbkId
      FROM emaj.emaj_rlbk WHERE rlbk_start_datetime < v_datetimeLimit;
-- and purge rollback tables
    IF v_maxRlbkId IS NOT NULL THEN
      DELETE FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id <= v_maxRlbkId;
      DELETE FROM emaj.emaj_rlbk_session WHERE rlbs_rlbk_id <= v_maxRlbkId;
      DELETE FROM emaj.emaj_rlbk WHERE rlbk_id <= v_maxRlbkId;
      GET DIAGNOSTICS v_nbPurgedRlbk = ROW_COUNT;
      v_wording = v_wording || ' ; ' || v_nbPurgedRlbk || ' rollback events deleted';
    END IF;
-- record the purge into the history if there are significant data
    IF v_wording <> '' THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_wording)
        VALUES ('PURGE_HISTORY', v_wording);
    END IF;
    RETURN;
  END;
$_purge_hist$;

CREATE OR REPLACE FUNCTION emaj._check_names_array(v_names TEXT[], v_type TEXT)
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_names_array$
-- This function build a array of names similar to the supplied array, except that NULL
-- values, empty string and duplicate names are suppressed. Issue a warning if the result array is NULL.
-- The function is used to validate group names array or table and sequence names array.
-- Input: names array
--        type of element, used to format warning messages
-- Output: validated names array
  DECLARE
    v_outputNames            TEXT[];
    v_i                      INT;
  BEGIN
    IF array_upper(v_names,1) >= 1 THEN
-- if there are elements, build the result array
      FOR v_i IN 1 .. array_upper(v_names,1) LOOP
-- look for not NULL & not empty name
        IF v_names[v_i] IS NULL OR v_names[v_i] = '' THEN
          RAISE WARNING '_check_names_array: a % name is NULL or empty.', v_type;
-- look for duplicate name
        ELSEIF v_outputNames IS NOT NULL AND v_names[v_i] = ANY (v_outputNames) THEN
          RAISE WARNING '_check_names_array: duplicate % name %.', v_type, v_names[v_i];
        ELSE
-- OK, keep the name
          v_outputNames = array_append (v_outputNames, v_names[v_i]);
        END IF;
      END LOOP;
    END IF;
-- check for NULL result
    IF v_outputNames IS NULL THEN
      RAISE WARNING '_check_names_array: No % name to process.', v_type;
    END IF;
    RETURN v_outputNames;
  END;
$_check_names_array$;

CREATE OR REPLACE FUNCTION emaj._check_group_content(v_groupName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_check_group_content$
-- This function verifies that the content of tables group as defined into the emaj_group_def table is correct.
-- It is called by emaj_create_group() and emaj_alter_group() functions.
-- It checks that the referenced application tables and sequences,
--  - exist,
--  - is not located into an E-Maj schema (to protect against an E-Maj recursive use),
--  - do not already belong to another tables group.
-- Input: the name of the tables group to check
  DECLARE
    v_msg                    TEXT = '';
    r_tblsq                  RECORD;
  BEGIN
-- check that all application tables and sequences listed for the group really exist
    FOR r_tblsq IN
      SELECT grpdef_schema || '.' || grpdef_tblseq AS full_name
        FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName
      EXCEPT
      SELECT nspname || '.' || relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
      ORDER BY 1
      LOOP
      IF v_msg <> '' THEN
        v_msg = v_msg || ', ';
      END IF;
      v_msg = v_msg || r_tblsq.full_name;
    END LOOP;
    IF v_msg <> '' THEN
      RAISE EXCEPTION '_check_group_content: one or several tables or sequences do not exist (%).', v_msg;
    END IF;
-- check no application schema listed for the group in the emaj_group_def table is an E-Maj schema
    FOR r_tblsq IN
      SELECT grpdef_schema || '.' || grpdef_tblseq AS full_name
        FROM emaj.emaj_group_def
        WHERE grpdef_group = v_groupName
          AND grpdef_schema IN (
                SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
                UNION
                SELECT 'emaj')
        ORDER BY 1
      LOOP
      IF v_msg <> '' THEN
        v_msg = v_msg || ', ';
      END IF;
      v_msg = v_msg || r_tblsq.full_name;
    END LOOP;
    IF v_msg <> '' THEN
      RAISE EXCEPTION '_check_group_content: one or several tables or sequences belong to an E-Maj schema (%).', v_msg;
    END IF;
-- check that no table or sequence of the new group already belongs to another created group
    FOR r_tblsq IN
        SELECT grpdef_schema || '.' || grpdef_tblseq || ' in ' || rel_group AS full_name
          FROM emaj.emaj_group_def, emaj.emaj_relation
          WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq
            AND grpdef_group = v_groupName AND rel_group <> v_groupName
        ORDER BY 1
      LOOP
      IF v_msg <> '' THEN
        v_msg = v_msg || ', ';
      END IF;
      v_msg = v_msg || r_tblsq.full_name;
    END LOOP;
    IF v_msg <> '' THEN
      RAISE EXCEPTION '_check_group_content: one or several tables already belong to another group (%).', v_msg;
    END IF;
    RETURN;
  END;
$_check_group_content$;

CREATE OR REPLACE FUNCTION emaj._check_new_mark(v_mark TEXT, v_groupNames TEXT[])
RETURNS TEXT LANGUAGE plpgsql AS
$_check_new_mark$
-- This function verifies that a new mark name supplied the user is valid.
-- It processes the possible NULL mark value and the replacement of % wild characters.
-- It also checks that the mark name do not already exist for any group.
-- Input: name of the mark to set, array of group names
--        The array of group names may be NULL to avoid the check against groups
-- Output: internal name of the mark
  DECLARE
    v_i                      INT;
    v_markName               TEXT = v_mark;
  BEGIN
-- check the mark name is not 'EMAJ_LAST_MARK'
    IF v_mark = 'EMAJ_LAST_MARK' THEN
       RAISE EXCEPTION '_check_new_mark: % is not an allowed name for a new mark.', v_mark;
    END IF;
-- process null or empty supplied mark name
    IF v_markName = '' OR v_markName IS NULL THEN
      v_markName = 'MARK_%';
    END IF;
-- process % wild characters in mark name
    v_markName = replace(v_markName, '%', to_char(current_timestamp, 'HH24.MI.SS.MS'));
-- if requested, check the existence of the mark in groups
    IF v_groupNames IS NOT NULL THEN
-- for each group of the array,
      FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
-- ... if a mark with the same name already exists for the group, stop
        PERFORM 0 FROM emaj.emaj_mark
          WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
        IF FOUND THEN
           RAISE EXCEPTION '_check_new_mark: Group % already contains a mark named %.', v_groupNames[v_i], v_markName;
        END IF;
      END LOOP;
    END IF;
    RETURN v_markName;
  END;
$_check_new_mark$;

CREATE OR REPLACE FUNCTION emaj._forbid_truncate_fnct() RETURNS TRIGGER AS
$_forbid_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of a rollbackable group
-- in logging mode.
-- It can only be called with postgresql in a version greater or equal 8.4
  BEGIN
    IF (TG_OP = 'TRUNCATE') THEN
      RAISE EXCEPTION 'emaj._forbid_truncate_fnct: TRUNCATE is not allowed while updates on this table (%.%) are currently protected by E-Maj. Consider stopping the group before issuing a TRUNCATE.', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    RETURN NULL;
  END;
$_forbid_truncate_fnct$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION emaj._log_truncate_fnct() RETURNS TRIGGER AS
$_log_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of an audit_only group
-- in logging mode.
-- It can only be called with postgresql in a version greater or equal 8.4
  DECLARE
    v_logSchema              TEXT;
    v_logTableName           TEXT;
  BEGIN
    IF (TG_OP = 'TRUNCATE') THEN
      SELECT rel_log_schema INTO v_logSchema FROM emaj.emaj_relation
        WHERE rel_schema = TG_TABLE_SCHEMA AND rel_tblseq = TG_TABLE_NAME;
      v_logTableName = quote_ident(v_logSchema) || '.' || quote_ident(TG_TABLE_SCHEMA || '_' || TG_TABLE_NAME || '_log');
      EXECUTE 'INSERT INTO ' || v_logTableName || ' (emaj_verb) VALUES (''TRU'')';
    END IF;
    RETURN NULL;
  END;
$_log_truncate_fnct$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION emaj._create_log_schema(v_logSchemaName TEXT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_create_log_schema$
-- The function creates a log schema and gives the appropriate rights to emaj users
-- Input: log schema name
-- The function is created as SECURITY DEFINER so that secondary schemas can be owned by superuser
  DECLARE
  BEGIN
-- check that the schema doesn't already exist
    PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchemaName;
    IF FOUND THEN
      RAISE EXCEPTION '_create_log_schema: schema % should not exist. Drop it manually, or modify emaj_group_def table''s content.',v_logSchemaName;
    END IF;
-- create the schema and give the appropriate rights
    EXECUTE 'CREATE SCHEMA ' || quote_ident(v_logSchemaName);
    EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(v_logSchemaName) || ' TO emaj_adm';
    EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(v_logSchemaName) || ' TO emaj_viewer';
    RETURN;
  END;
$_create_log_schema$;

CREATE OR REPLACE FUNCTION emaj._drop_log_schema(v_logSchemaName TEXT, v_isForced BOOLEAN)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_log_schema$
-- The function drops a log schema
-- Input: log schema name, boolean telling whether the schema to drop may contain residual objects
-- The function is created as SECURITY DEFINER so that secondary schemas can be dropped in any case
  DECLARE
  BEGIN
-- check that the schema doesn't already exist
    PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = v_logSchemaName;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_log_schema: schema % doesn''t exist.',v_logSchemaName;
    END IF;
    IF v_isForced THEN
-- drop cascade when called by emaj_force_xxx_group()
      EXECUTE 'DROP SCHEMA ' || quote_ident(v_logSchemaName) || ' CASCADE';
    ELSE
-- otherwise, drop restrict with a trap on the potential error
      BEGIN
        EXECUTE 'DROP SCHEMA ' || quote_ident(v_logSchemaName);
        EXCEPTION
-- trap the 2BP01 exception to generate a more understandable error message
          WHEN DEPENDENT_OBJECTS_STILL_EXIST THEN         -- SQLSTATE '2BP01'
            RAISE EXCEPTION '_drop_log_schema: cannot drop schema %. It probably owns unattended objects. Use the emaj_verify_all() function to get details', quote_ident(v_logSchemaName);
      END;
    END IF;
    RETURN;
  END;
$_drop_log_schema$;

---------------------------------------------------
--                                               --
-- Elementary functions for tables and sequences --
--                                               --
---------------------------------------------------

CREATE OR REPLACE FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_logDatTsp TEXT, v_logIdxTsp TEXT, v_isRollbackable BOOLEAN)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: schema name (mandatory even for the 'public' schema), table name, schema holding log objects, data and index tablespaces for the log table, boolean indicating whether the table belongs to a rollbackable group
-- Are created in the log schema:
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_fullTableName          TEXT;
    v_dataTblSpace           TEXT;
    v_idxTblSpace            TEXT;
    v_logTableName           TEXT;
    v_logIdxName             TEXT;
    v_logFnctName            TEXT;
    v_logTriggerName         TEXT;
    v_truncTriggerName       TEXT;
    v_sequenceName           TEXT;
    v_relPersistence         CHAR(1);
    v_relIsTemp              BOOLEAN;
    v_relhaspkey             BOOLEAN;
    v_stmt                   TEXT = '';
    v_attname                TEXT;
    v_triggerList            TEXT = '';
    r_column                 RECORD;
    r_trigger                RECORD;
  BEGIN
-- check the table is neither a temporary nor an unlogged table
    IF v_pgVersion >= '9.1' THEN
      SELECT relpersistence INTO v_relPersistence FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = v_schemaName AND relname = v_tableName;
      IF v_relPersistence = 't' THEN
        RAISE EXCEPTION '_create_tbl: table % is a temporary table.', v_tableName;
      ELSIF v_relPersistence = 'u' THEN
        RAISE EXCEPTION '_create_tbl: table % is an unlogged table.', v_tableName;
      END IF;
    ELSIF v_pgVersion >= '8.4' THEN
      SELECT relistemp INTO v_relIsTemp FROM pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = v_schemaName AND relname = v_tableName;
      IF v_relIsTemp THEN
        RAISE EXCEPTION '_create_tbl: table % is a temporary table.', v_tableName;
      END IF;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', v_tableName;
    END IF;
-- check the table has a primary key
    SELECT true INTO v_relhaspkey FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
      WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid
        AND contype = 'p' AND nspname = v_schemaName AND relname = v_tableName;
    IF NOT FOUND THEN
      v_relhaspkey = false;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', v_tableName;
    END IF;
-- prepare TABLESPACE clauses for data and index
    IF v_logDatTsp IS NOT NULL THEN
      v_dataTblSpace = 'TABLESPACE ' || quote_ident(v_logDatTsp);
    ELSE
      v_dataTblSpace = '';
    END IF;
    IF v_logIdxTsp IS NOT NULL THEN
      v_idxTblSpace = 'TABLESPACE ' || quote_ident(v_logIdxTsp);
    ELSE
      v_idxTblSpace = '';
    END IF;
-- build the different name for table, trigger, functions,...
    v_fullTableName    = quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logIdxName       = quote_ident(v_schemaName || '_' || v_tableName || '_log_idx');
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_logTriggerName   = quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName = quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');
    v_sequenceName     = quote_ident(v_logSchema) || '.' || quote_ident(emaj._build_log_seq_name(v_schemaName, v_tableName));
-- creation of the log table: the log table looks like the application table, with some additional technical columns
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName;
    EXECUTE 'CREATE TABLE ' || v_logTableName
         || ' ( LIKE ' || v_fullTableName || ') ' || v_dataTblSpace;
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_verb      VARCHAR(3),'
         || ' ADD COLUMN emaj_tuple     VARCHAR(3),'
         || ' ADD COLUMN emaj_gid       BIGINT      NOT NULL   DEFAULT nextval(''emaj.emaj_global_seq''),'
         || ' ADD COLUMN emaj_changed   TIMESTAMPTZ DEFAULT clock_timestamp(),'
         || ' ADD COLUMN emaj_txid      BIGINT      DEFAULT txid_current(),'
         || ' ADD COLUMN emaj_user      VARCHAR(32) DEFAULT session_user,'
         || ' ADD COLUMN emaj_user_ip   INET        DEFAULT inet_client_addr(),'
         || ' ADD COLUMN emaj_user_port INT         DEFAULT inet_client_port()';
-- creation of the index on the log table
    EXECUTE 'CREATE UNIQUE INDEX ' || v_logIdxName || ' ON '
         ||  v_logTableName || ' (emaj_gid, emaj_tuple) ' || v_idxTblSpace;
-- set the index associated to the primary key as cluster index. It may be useful for CLUSTER command.
    EXECUTE 'ALTER TABLE ONLY ' || v_logTableName || ' CLUSTER ON ' || v_logIdxName;
-- remove the NOT NULL constraints of application columns.
--   They are useless and blocking to store truncate event for tables belonging to audit_only tables
    FOR r_column IN
      SELECT ' ALTER COLUMN ' || quote_ident(attname) || ' DROP NOT NULL' AS action
        FROM pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE relnamespace = pg_namespace.oid AND attrelid = pg_class.oid
          AND nspname = v_logSchema AND relname = v_schemaName || '_' || v_tableName || '_log'
          AND attnum > 0 AND attnotnull AND attisdropped = false AND attname NOT LIKE E'emaj\\_%'
    LOOP
      IF v_stmt = '' THEN
        v_stmt = v_stmt || r_column.action;
      ELSE
        v_stmt = v_stmt || ',' || r_column.action;
      END IF;
    END LOOP;
    IF v_stmt <> '' THEN
      EXECUTE 'ALTER TABLE ' || v_logTableName || v_stmt;
    END IF;
-- create the sequence associated to the log table
    EXECUTE 'CREATE SEQUENCE ' || v_sequenceName;
-- creation of the log fonction that will be mapped to the log trigger later
-- The new row is logged for each INSERT, the old row is logged for each DELETE
-- and the old and the new rows are logged for each UPDATE.
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || v_logFnctName || '() RETURNS trigger AS $logfnct$'
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
         || '$logfnct$ LANGUAGE plpgsql SECURITY DEFINER;';
-- creation of the log trigger on the application table, using the previously created log function
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    EXECUTE 'DROP TRIGGER IF EXISTS ' || v_logTriggerName || ' ON ' || v_fullTableName;
    EXECUTE 'CREATE TRIGGER ' || v_logTriggerName
         || ' AFTER INSERT OR UPDATE OR DELETE ON ' || v_fullTableName
         || '  FOR EACH ROW EXECUTE PROCEDURE ' || v_logFnctName || '()';
    EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
-- creation of the trigger that manage any TRUNCATE on the application table
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    IF v_pgVersion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
      IF v_isRollbackable THEN
-- For rollbackable groups, use the common _forbid_truncate_fnct() function that blocks the operation
        EXECUTE 'CREATE TRIGGER ' || v_truncTriggerName
             || ' BEFORE TRUNCATE ON ' || v_fullTableName
             || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()';
      ELSE
-- For audit_only groups, use the common _log_truncate_fnct() function that records the operation into the log table
        EXECUTE 'CREATE TRIGGER ' || v_truncTriggerName
             || ' BEFORE TRUNCATE ON ' || v_fullTableName
             || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._log_truncate_fnct()';
      END IF;
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
    END IF;
--
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger
    FOR r_trigger IN
      SELECT tgname FROM pg_catalog.pg_trigger
        WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'%emaj\\_%\\_trg'
    LOOP
      IF v_triggerList = '' THEN
        v_triggerList = v_triggerList || r_trigger.tgname;
      ELSE
        v_triggerList = v_triggerList || ', ' || r_trigger.tgname;
      END IF;
    END LOOP;
-- if yes, issue a warning (if a trigger updates another table in the same table group or outside) it could generate problem at rollback time)
    IF v_triggerList <> '' THEN
      RAISE WARNING '_create_tbl: table % has triggers (%). Verify the compatibility with emaj rollback operations (in particular if triggers update one or several other tables). Triggers may have to be manualy disabled before rollback.', v_fullTableName, v_triggerList;
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE OR REPLACE FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function
-- Required inputs: schema name, table name and log schema
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_logFnctName            TEXT;
    v_logTriggerName         TEXT;
    v_truncTriggerName       TEXT;
    v_seqName                TEXT;
    v_fullSeqName            TEXT;
  BEGIN
    v_fullTableName    = quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_logTriggerName   = quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName = quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');
    v_seqName          = emaj._build_log_seq_name(v_schemaName, v_tableName);
    v_fullSeqName      = quote_ident(v_logSchema) || '.' || quote_ident(v_seqName);
-- check the table exists before dropping its triggers
    PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace
      WHERE relnamespace = pg_namespace.oid
        AND nspname = v_schemaName AND relname = v_tableName AND relkind = 'r';
    IF FOUND THEN
-- delete the log trigger on the application table
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_logTriggerName || ' ON ' || v_fullTableName;
-- delete the truncate trigger on the application table
      IF v_pgVersion >= '8.4' THEN
        EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
      END IF;
    END IF;
-- delete the log function
    EXECUTE 'DROP FUNCTION IF EXISTS ' || v_logFnctName || '()';
-- delete the sequence associated to the log table
    EXECUTE 'DROP SEQUENCE IF EXISTS ' || v_fullSeqName;
-- delete the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName || ' CASCADE';
-- delete rows related to the log sequence from emaj_sequence table
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = v_logSchema AND sequ_name = v_seqName;
-- delete rows related to the table from emaj_seq_hole table
    DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = quote_ident(v_schemaName) AND sqhl_table = quote_ident(v_tableName);
    RETURN;
  END;
$_drop_tbl$;

CREATE OR REPLACE FUNCTION emaj._create_seq(v_schemaName TEXT, v_seqName TEXT, v_groupName TEXT)
RETURNS void LANGUAGE plpgsql AS
$_create_seq$
-- The function checks whether the sequence is related to a serial column of an application table.
-- If yes, it verifies that this table also belong to the same group
-- Required inputs: schema name and sequence name
  DECLARE
    v_tableSchema            TEXT;
    v_tableName              TEXT;
    v_tableGroup             TEXT;
  BEGIN
-- get the schema and the name of the table that contains a serial column this sequence is linked to, if one exists
    SELECT nt.nspname, ct.relname INTO v_tableSchema, v_tableName
      FROM pg_catalog.pg_class cs, pg_catalog.pg_namespace ns, pg_depend,
           pg_catalog.pg_class ct, pg_catalog.pg_namespace nt
      WHERE cs.relname = v_seqName AND ns.nspname = v_schemaName -- the selected sequence
        AND cs.relnamespace = ns.oid                             -- join condition for sequence schema name
        AND ct.relnamespace = nt.oid                             -- join condition for linked table schema name
        AND pg_depend.objid = cs.oid                             -- join condition for the pg_depend table
        AND pg_depend.refobjid = ct.oid                          -- join conditions for depended table schema name
        AND pg_depend.classid = pg_depend.refclassid             -- the classid et refclassid must be 'pg_class'
        AND pg_depend.classid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_class');
    IF FOUND THEN
      SELECT grpdef_group INTO v_tableGroup FROM emaj.emaj_group_def
        WHERE grpdef_schema = v_tableSchema AND grpdef_tblseq = v_tableName;
      IF NOT FOUND THEN
        RAISE WARNING '_create_seq: Sequence %.% is linked to table %.% but this table does not belong to any tables group.', v_schemaName, v_seqName, v_tableSchema, v_tableName;
      ELSE
        IF v_tableGroup <> v_groupName THEN
          RAISE WARNING '_create_seq: Sequence %.% is linked to table %.% but this table belong to another tables group (%).', v_schemaName, v_seqName, v_tableSchema, v_tableName, v_tableGroup;
        END IF;
      END IF;
    END IF;
    RETURN;
  END;
$_create_seq$;

CREATE OR REPLACE FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT)
RETURNS void LANGUAGE plpgsql AS
$_drop_seq$
-- The function deletes the rows stored into emaj_sequence for a particular sequence
-- Required inputs: schema name and sequence name
  BEGIN
-- delete rows from emaj_sequence
    EXECUTE 'DELETE FROM emaj.emaj_sequence WHERE sequ_schema = ' || quote_literal(v_schemaName) || ' AND sequ_name = ' || quote_literal(v_seqName);
    RETURN;
  END;
$_drop_seq$;

CREATE OR REPLACE FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_lastGlobalSeq BIGINT, v_nbSession INT)
RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_tbl$
-- This function rollbacks one table to a given point in time represented by the value of the global sequence
-- The function is called by emaj._rlbk_session_exec()
-- Input: schema name and table name
--        log schema
--        global sequence value limit for rollback
-- Output: number of rolled back primary keys
-- For unlogged rollback, the log triggers have been disabled previously and will be enabled later.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_attname                TEXT;
    v_tmpTable               TEXT;
    v_tableType              TEXT;
    v_nbPk                   BIGINT;
    v_cols                   TEXT[] = '{}';
    v_colsPk                 TEXT[] = '{}';
    v_condsPk                TEXT[] = '{}';
    v_colList                TEXT;
    v_pkColList              TEXT;
    v_pkCondList             TEXT;
    r_col                    RECORD;
  BEGIN
    v_fullTableName  = quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName   = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'All log rows with emaj_gid > ' || v_lastGlobalSeq);
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
      IF v_pgVersion >= '9.1' THEN
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
-- inserted into the application table rows that were deleted or updated during the rolled back period
    EXECUTE 'INSERT INTO ' || v_fullTableName
         || '  SELECT ' || v_colList
         || '    FROM ' || v_logTableName || ' tbl, ' || v_tmpTable || ' keys '
         || '    WHERE ' || v_pkCondList || ' AND tbl.emaj_gid = keys.emaj_gid AND tbl.emaj_tuple = ''OLD''';
-- drop the now useless temporary table
    EXECUTE 'DROP TABLE ' || v_tmpTable;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nbPk || ' rolled back primary keys');
    RETURN v_nbPk;
  END;
$_rlbk_tbl$;

CREATE OR REPLACE FUNCTION emaj._delete_log_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_timestamp TIMESTAMPTZ, v_lastGlobalSeq BIGINT, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_delete_log_tbl$
-- This function deletes the part of a log table corresponding to updates that have been rolled back.
-- The function is only called for unlogged rollback.
-- It deletes marks that are not visible anymore after to the rollback.
-- It also registers the hole in sequence number generated by the deleted log rows.
-- The function is called by emaj._rlbk_session_exec()
-- Input: schema name and table name, log schema, global sequence value limit for rollback, mark timestamp,
--        flag to specify if the rollback is logged,
--        last sequence and last hole identifiers to keep (greater ones being to be deleted)
-- Output: deleted rows
  DECLARE
    v_logTableName           TEXT;
    v_seqName                TEXT;
    v_fullSeqName            TEXT;
    v_nbRows                 BIGINT;
  BEGIN
    v_logTableName   = quote_ident(v_logSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_seqName        = emaj._build_log_seq_name(v_schemaName, v_tableName);
    v_fullSeqName    = quote_ident(v_logSchema) || '.' || quote_ident(v_seqName);
-- delete obsolete log rows
    EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_gid > ' || v_lastGlobalSeq;
    GET DIAGNOSTICS v_nbRows = ROW_COUNT;
-- suppress from emaj_sequence table the rows regarding the emaj log sequence for this application table
--     corresponding to potential later intermediate marks that disappear with the rollback operation
    DELETE FROM emaj.emaj_sequence
      WHERE sequ_schema = v_logSchema AND sequ_name = v_seqName AND sequ_id > v_lastSequenceId;
-- record the sequence holes generated by the delete operation
-- this is due to the fact that log sequences are not rolled back, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_duration())
-- first delete, if exist, sequence holes that have disappeared with the rollback
    DELETE FROM emaj.emaj_seq_hole
      WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName AND sqhl_id > v_lastSeqHoleId;
-- and then insert the new sequence hole
    EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_hole_size) VALUES ('
      || quote_literal(v_schemaName) || ',' || quote_literal(v_tableName) || ', ('
      || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM ' || v_fullSeqName
      || ')-('
      || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
      || ' emaj.emaj_sequence WHERE'
      || ' sequ_schema = ' || quote_literal(v_logSchema)
      || ' AND sequ_name = ' || quote_literal(v_seqName)
      || ' AND sequ_datetime = ' || quote_literal(v_timestamp) || '))';
    RETURN v_nbRows;
  END;
$_delete_log_tbl$;

CREATE OR REPLACE FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_isLoggedRlbk BOOLEAN, v_lastSequenceId BIGINT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_seq$
-- This function rollbacks one application sequence to a given mark
-- The function is called by emaj.emaj._rlbk_end()
-- Input: schema name and table name, mark, boolean indicating whether the rollback is logged
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it is not the owner of the application sequence.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_fullSeqName            TEXT;
    v_stmt                   TEXT;
    mark_seq_rec             RECORD;
    curr_seq_rec             RECORD;
  BEGIN
-- Read sequence's characteristics at mark time
    BEGIN
      SELECT   sequ_schema, sequ_name, sequ_mark, sequ_last_val, sequ_start_val, sequ_increment,
               sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called
        INTO STRICT mark_seq_rec
        FROM emaj.emaj_sequence
        WHERE sequ_schema = v_schemaName AND sequ_name = v_seqName AND sequ_datetime = v_timestamp;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RAISE EXCEPTION '_rlbk_seq: Mark at % not found for sequence %.%.', v_timestamp, v_schemaName, v_seqName;
        WHEN TOO_MANY_ROWS THEN
          RAISE EXCEPTION '_rlbk_seq: Internal error 1.';
    END;
-- Read the current sequence's characteristics
    v_fullSeqName = quote_ident(v_schemaName) || '.' || quote_ident(v_seqName);
    v_stmt = 'SELECT last_value, ';
    IF v_pgVersion <= '8.3' THEN
       v_stmt = v_stmt || '0 as start_value, ';
    ELSE
       v_stmt = v_stmt || 'start_value, ';
    END IF;
    v_stmt = v_stmt || 'increment_by, max_value, min_value, cache_value, is_cycled, is_called FROM '
                    || v_fullSeqName;
    EXECUTE v_stmt INTO STRICT curr_seq_rec;
-- Build the ALTER SEQUENCE statement, depending on the differences between the present values and the related
--   values at the requested mark time
    v_stmt='';
    IF curr_seq_rec.last_value <> mark_seq_rec.sequ_last_val OR
       curr_seq_rec.is_called <> mark_seq_rec.sequ_is_called THEN
      IF mark_seq_rec.sequ_is_called THEN
        v_stmt=v_stmt || ' RESTART ' || mark_seq_rec.sequ_last_val + mark_seq_rec.sequ_increment;
      ELSE
        v_stmt=v_stmt || ' RESTART ' || mark_seq_rec.sequ_last_val;
      END IF;
    END IF;
    IF curr_seq_rec.start_value <> mark_seq_rec.sequ_start_val THEN
      v_stmt=v_stmt || ' START ' || mark_seq_rec.sequ_start_val;
    END IF;
    IF curr_seq_rec.increment_by <> mark_seq_rec.sequ_increment THEN
      v_stmt=v_stmt || ' INCREMENT ' || mark_seq_rec.sequ_increment;
    END IF;
    IF curr_seq_rec.min_value <> mark_seq_rec.sequ_min_val THEN
      v_stmt=v_stmt || ' MINVALUE ' || mark_seq_rec.sequ_min_val;
    END IF;
    IF curr_seq_rec.max_value <> mark_seq_rec.sequ_max_val THEN
      v_stmt=v_stmt || ' MAXVALUE ' || mark_seq_rec.sequ_max_val;
    END IF;
    IF curr_seq_rec.cache_value <> mark_seq_rec.sequ_cache_val THEN
      v_stmt=v_stmt || ' CACHE ' || mark_seq_rec.sequ_cache_val;
    END IF;
    IF curr_seq_rec.is_cycled <> mark_seq_rec.sequ_is_cycled THEN
      IF mark_seq_rec.sequ_is_cycled = 'f' THEN
        v_stmt=v_stmt || ' NO ';
      END IF;
      v_stmt=v_stmt || ' CYCLE ';
    END IF;
-- and execute the statement if at least one parameter has changed
    IF v_stmt <> '' THEN
--    RAISE NOTICE 'Rollback sequence % with%', v_fullSeqName, v_stmt;
      EXECUTE 'ALTER SEQUENCE ' || v_fullSeqName || v_stmt;
    END IF;
-- if the caller requires it, delete the rolled back intermediate sequences from the sequence table
    IF NOT v_isLoggedRlbk THEN
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = v_schemaName AND sequ_name = v_seqName AND sequ_id > v_lastSequenceId;
    END IF;
-- insert event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
    RETURN;
  END;
$_rlbk_seq$;

CREATE OR REPLACE FUNCTION emaj._log_stat_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_log_stat_tbl$
-- This function returns the number of log rows for a single table between 2 marks or between a mark and the current situation.
-- It is called by emaj_log_stat_group function
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: schema name and table name, log schema, the timestamps of both marks, the emaj_seq_hole last id of both marks
--   a NULL value as last timestamp mark indicates the current situation
-- Output: number of log rows between both marks for the table
  DECLARE
    v_fullSeqName            TEXT;
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
  BEGIN
-- get the log table id at first mark time
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_beginLastValue
       FROM emaj.emaj_sequence
       WHERE sequ_schema = v_logSchema
         AND sequ_name = emaj._build_log_seq_name(v_schemaName,v_tableName)
         AND sequ_datetime = v_tsFirstMark;
    IF v_tsLastMark IS NULL THEN
-- last mark is NULL, so examine the current state of the log table id
      v_fullSeqName = quote_ident(v_logSchema) || '.' || quote_ident(emaj._build_log_seq_name(v_schemaName, v_tableName));
      EXECUTE 'SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM ' || v_fullSeqName INTO v_endLastValue;
--   and count the sum of hole from the start mark time until now
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_id > v_firstLastSeqHoleId;
    ELSE
-- last mark is not NULL, so get the log table id at last mark time
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_endLastValue
         FROM emaj.emaj_sequence
         WHERE sequ_schema = v_logSchema
           AND sequ_name = emaj._build_log_seq_name(v_schemaName,v_tableName)
           AND sequ_datetime = v_tsLastMark;
--   and count the sum of hole from the start mark time to the end mark time
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_id > v_firstLastSeqHoleId AND sqhl_id <= v_lastLastSeqHoleId;
    END IF;
-- return the stat row for the table
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole);
  END;
$_log_stat_tbl$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_tbl(v_fullTableName TEXT, v_logTableName TEXT, v_conditions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_tbl$
-- This function generates SQL commands representing all updates performed on a table between 2 marks
-- or beetween a mark and the current situation. These command are stored into a temporary table created
-- by the _gen_sql_groups() calling function.
-- Input: - fully qualified application table to process
--        - fully qualified associated log table
--        - sql conditions corresponding to the marks range to process
-- Output: number of generated SQL statements
  DECLARE
    v_valList                TEXT;
    v_setList                TEXT;
    v_pkCondList             TEXT;
    v_unquotedType           TEXT[] = array['smallint','integer','bigint','numeric','decimal',
                                             'int2','int4','int8','serial','bigserial',
                                             'real','double precision','float','float4','float8','oid'];
    v_rqInsert               TEXT;
    v_rqUpdate               TEXT;
    v_rqDelete               TEXT;
    v_rqTruncate             TEXT;
    v_nbSQL                  INT;
    r_col                    RECORD;
  BEGIN
-- retrieve from pg_attribute all columns of the application table and build :
-- - the VALUES list used in the INSERT statements
-- - the SET list used in the UPDATE statements
    v_valList = '';
    v_setList = '';
    FOR r_col IN
      SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute
       WHERE attrelid = v_fullTableName ::regclass
         AND attnum > 0 AND NOT attisdropped
       ORDER BY attnum
    LOOP
-- test if the column format (up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric data types)
      IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
--          may be we will need to cast some column types in the future. So keep the comment for the moment...
--          v_valList = v_valList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::text,''NULL'') || ''::' || r_col.format_type || ', ';
        v_valList = v_valList || ''' || coalesce(o.' || quote_ident(r_col.attname) || '::text,''NULL'') || '', ';
--          v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.' || quote_ident(r_col.attname) || ' ::text,''NULL'') || ''::' || r_col.format_type || ', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(n.' || quote_ident(r_col.attname) || ' ::text,''NULL'') || '', ';
      ELSE
-- literal for this column must be quoted
--          v_valList = v_valList || ''' || coalesce(quote_literal(o.' || quote_ident(r_col.attname) || '),''NULL'') || ''::' || r_col.format_type || ', ';
        v_valList = v_valList || ''' || coalesce(quote_literal(o.' || quote_ident(r_col.attname) || '),''NULL'') || '', ';
--          v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(quote_literal(n.' || quote_ident(r_col.attname) || '),''NULL'') || ''::' || r_col.format_type || ', ';
        v_setList = v_setList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || coalesce(quote_literal(n.' || quote_ident(r_col.attname) || '),''NULL'') || '', ';
      END IF;
    END LOOP;
-- suppress the final separators
    v_valList = substring(v_valList FROM 1 FOR char_length(v_valList) - 2);
    v_setList = substring(v_setList FROM 1 FOR char_length(v_setList) - 2);
-- retrieve all columns that represents the pkey and build the "pkey equal" conditions set that will be used in UPDATE and DELETE statements
-- (taking column names in pg_attribute from the table's definition instead of index definition is mandatory
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
    v_pkCondList = '';
    FOR r_col IN
      SELECT attname, format_type(atttypid,atttypmod) FROM pg_catalog.pg_attribute, pg_catalog.pg_index
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey)
          AND indrelid = v_fullTableName ::regclass AND indisprimary
          AND attnum > 0 AND NOT attisdropped
    LOOP
-- test if the column format (at least up to the parenthesis) belongs to the list of formats that do not require any quotation (like numeric data types)
      IF regexp_replace (r_col.format_type,E'\\(.*$','') = ANY(v_unquotedType) THEN
-- literal for this column can remain as is
--          v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.' || quote_ident(r_col.attname) || ' || ''::' || r_col.format_type || ' AND ';
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || o.' || quote_ident(r_col.attname) || ' || '' AND ';
      ELSE
-- literal for this column must be quoted
--          v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(r_col.attname) || ') || ''::' || r_col.format_type || ' AND ';
        v_pkCondList = v_pkCondList || quote_ident(replace(r_col.attname,'''','''''')) || ' = '' || quote_literal(o.' || quote_ident(r_col.attname) || ') || '' AND ';
      END IF;
    END LOOP;
-- suppress the final separator
    v_pkCondList = substring(v_pkCondList FROM 1 FOR char_length(v_pkCondList) - 5);
-- prepare sql skeletons for each statement type
    v_rqInsert = '''INSERT INTO ' || replace(v_fullTableName,'''','''''') || ' VALUES (' || v_valList || ');''';
    v_rqUpdate = '''UPDATE ONLY ' || replace(v_fullTableName,'''','''''') || ' SET ' || v_setList || ' WHERE ' || v_pkCondList || ';''';
    v_rqDelete = '''DELETE FROM ONLY ' || replace(v_fullTableName,'''','''''') || ' WHERE ' || v_pkCondList || ';''';
    v_rqTruncate = '''TRUNCATE ' || replace(v_fullTableName,'''','''''') || ';''';
-- now scan the log table to process all statement types at once
    EXECUTE 'INSERT INTO emaj_temp_script '
         || 'SELECT o.emaj_gid, 0, o.emaj_txid, CASE '
         ||   ' WHEN o.emaj_verb = ''INS'' THEN ' || v_rqInsert
         ||   ' WHEN o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''OLD'' THEN ' || v_rqUpdate
         ||   ' WHEN o.emaj_verb = ''DEL'' THEN ' || v_rqDelete
         ||   ' WHEN o.emaj_verb = ''TRU'' THEN ' || v_rqTruncate
         || ' END '
         || ' FROM ' || v_logTableName || ' o'
         ||   ' LEFT OUTER JOIN ' || v_logTableName || ' n ON n.emaj_gid = o.emaj_gid'
         || '        AND (n.emaj_verb = ''UPD'' AND n.emaj_tuple = ''NEW'') '
         || ' WHERE NOT (o.emaj_verb = ''UPD'' AND o.emaj_tuple = ''NEW'')'
         || ' AND ' || v_conditions;
    GET DIAGNOSTICS v_nbSQL = ROW_COUNT;
    RETURN v_nbSQL;
  END;
$_gen_sql_tbl$;

------------------------------------------------
----                                        ----
----       Functions to manage groups       ----
----                                        ----
------------------------------------------------

CREATE OR REPLACE FUNCTION emaj._verify_groups(v_groups TEXT[], v_onErrorStop BOOLEAN)
RETURNS SETOF emaj._verify_groups_type LANGUAGE plpgsql AS
$_verify_groups$
-- The function verifies the consistency of a tables groups array.
-- Input: - tables groups array,
--        - a boolean indicating whether the function has to raise an exception in case of detected unconsistency.
-- If onErrorStop boolean is false, it returns a set of _verify_groups_type records, one row per detected unconsistency, including the faulting schema and table or sequence names and a detailed message.
-- If no error is detected, no row is returned.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_emajSchema             TEXT = 'emaj';
    v_hint                   TEXT = 'You may use "SELECT * FROM emaj.emaj_verify_all()" to look for other issues.';
    r_object                 RECORD;
  BEGIN
-- Note that there is no check that the supplied groups exist. This has already been done by all calling functions.
-- Let's start with some global checks that always raise an exception if an issue is detected
-- check the postgres version: E-Maj is not compatible with 8.2-
    IF v_pgVersion < '8.3' THEN
      RAISE EXCEPTION 'The current postgres version (%) is not compatible with E-Maj.', version();
    END IF;
-- check the postgres version at groups creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric.
--          But we suppose these tests will not be useful any more when pg 10.0 will appear!
--   for 8.3, both major versions must be the same
    FOR r_object IN
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE group_name = ANY (v_groups)
          AND ((v_pgVersion = '8.3' AND substring (group_pg_version FROM E'(\\d+\\.\\d+)') <> v_pgVersion)
--   for 8.4+, both major versions must be 8.4+
            OR (v_pgVersion >= '8.4' AND substring (group_pg_version FROM E'(\\d+\\.\\d+)') < '8.4'))
        ORDER BY msg
    LOOP
      RAISE EXCEPTION '_verify_groups: %',r_object.msg;
    END LOOP;
-- OK, now look for groups unconsistency
-- Unlike emaj_verify_all(), there is no direct check that application schemas exist
-- check all application relations referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT t.rel_schema, t.rel_tblseq,
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                    -- all relations known by E-Maj
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation WHERE rel_group = ANY (v_groups)
            EXCEPT                                -- all relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log table for all tables referenced in the emaj_relation table still exist
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_schema || '_' || rel_tblseq || '_log" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups)
          AND rel_kind = 'r'
          AND (rel_log_schema, rel_schema || '_' || rel_tblseq || '_log') NOT IN
              (SELECT nspname, relname FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE relnamespace = pg_namespace.oid AND relname LIKE E'%\_%\_log')
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check the log function for each table referenced in the emaj_relation table still exist
    FOR r_object IN
                                                  -- the schema and table names are rebuilt from the returned function name
      SELECT substring(fnct FROM '^(.*)_.*_.*_fnct') AS sch, substring(fnct FROM '^.*_(.*)_.*_fnct') AS tbl,
             'In group "' || r.rel_group || '", the ' ||
               CASE WHEN substring(fnct FROM '^.*_.*_(.*)_fnct') = 'log' THEN 'log' ELSE 'rollback' END ||
               ' function "' || t.rel_log_schema || '"."' || fnct || '" is not found.' AS msg
        FROM (                                    -- all expected log functions
         SELECT rel_log_schema, rel_schema || '_' || rel_tblseq || '_log_fnct' AS fnct
            FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
         EXCEPT                                   -- minus functions known by postgres
         SELECT nspname, proname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
           WHERE pronamespace = pg_namespace.oid AND proname LIKE E'%\_%\_%\_fnct'
             ) AS t, emaj.emaj_relation r         -- join with emaj_relation to get the group name
        WHERE r.rel_schema = substring(fnct FROM '^(.*)_.*_.*_fnct') AND r.rel_tblseq = substring(fnct FROM '^.*_(.*)_.*_fnct')
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log trigger
    FOR r_object IN
      SELECT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the log trigger "' ||
               rel_schema || '_' || rel_tblseq || '_emaj_log_trg" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
          AND (rel_schema, rel_tblseq, rel_schema || '_' || rel_tblseq || '_emaj_log_trg') NOT IN
              (SELECT nspname, relname, tgname
                 FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                   AND tgname LIKE E'%\_%\_emaj\_log\_trg')
        ORDER BY 1,2,3
    LOOP
      IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
--   then truncate trigger if pg 8.4+
    IF v_pgVersion >= '8.4' THEN
      FOR r_object IN
        SELECT rel_schema, rel_tblseq,
               'In group "' || rel_group || '", the truncate trigger "' ||
                 rel_schema || '_' || rel_tblseq || '_emaj_trunc_trg" is not found.' AS msg
          FROM emaj.emaj_relation
        WHERE rel_group = ANY (v_groups) AND rel_kind = 'r'
            AND (rel_schema, rel_tblseq, rel_schema || '_' || rel_tblseq || '_emaj_trunc_trg') NOT IN
                (SELECT nspname, relname, tgname
                   FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                   WHERE tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                     AND tgname LIKE E'%\_%\_emaj\_trunc\_trg')
        ORDER BY 1,2,3
      LOOP
        IF v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
        RETURN NEXT r_object;
      END LOOP;
-- TODO : merge both triggers check when pg 8.3 will not be supported any more
    END IF;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    FOR r_object IN
      SELECT DISTINCT rel_schema, rel_tblseq,
             'In group "' || rel_group || '", the structure of the application table "' ||
               rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
             rel_log_schema || '"."' || rel_schema || '_' || rel_tblseq || '_log").' AS msg
        FROM (
          (                                       -- application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          EXCEPT                                   -- minus log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
              AND relname = rel_schema || '_' || rel_tblseq || '_log'
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          )
          UNION
          (                                         -- log table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
              AND relname = rel_schema || '_' || rel_tblseq || '_log'
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          EXCEPT                                    -- minus application table's columns
          SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
            FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
              AND rel_group = ANY (v_groups) AND rel_kind = 'r'
          )) AS t
        ORDER BY 1,2,3
-- TODO : use CTE to improve performance, when pg 8.3 will not be supported any more
    LOOP
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: % %',r_object.msg,v_hint; END IF;
      RETURN NEXT r_object;
    END LOOP;
    RETURN;
  END;
$_verify_groups$;

CREATE OR REPLACE FUNCTION emaj._check_fk_groups(v_groupNames TEXT[])
RETURNS void LANGUAGE plpgsql AS
$_check_fk_groups$
-- this function checks foreign key constraints for tables of a groups array.
-- tables from audit_only groups are ignored in this check because they will never be rolled back.
-- Input: group names array
  DECLARE
    r_fk                     RECORD;
  BEGIN
-- issue a warning if a table of the groups has a foreign key that references a table outside the groups
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace nf, pg_catalog.pg_class tf, emaj.emaj_relation r, emaj.emaj_group g
        WHERE contype = 'f'                                         -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid      -- join for table and namespace
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid     -- join for referenced table and namespace
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq -- join on emaj_relation table
          AND r.rel_group = g.group_name                            -- join on emaj_group table
          AND r.rel_group = ANY (v_groupNames)                      -- only tables of the selected groups
          AND g.group_is_rollbackable                               -- only tables from rollbackable groups
          AND (nf.nspname,tf.relname) NOT IN                        -- referenced table outside the groups
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames))
      LOOP
      RAISE WARNING '_check_fk_groups: Foreign key %, from table %.%, references %.% that is outside groups (%).',
                r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
-- issue a warning if a table of the groups is referenced by a table outside the groups
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace nf, pg_catalog.pg_class tf, emaj.emaj_relation r, emaj.emaj_group g
        WHERE contype = 'f'                                           -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid        -- join for table and namespace
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid       -- join for referenced table and namespace
          AND nf.nspname = r.rel_schema AND tf.relname = r.rel_tblseq -- join with emaj_relation table
          AND r.rel_group = g.group_name                              -- join on emaj_group table
          AND r.rel_group = ANY (v_groupNames)                        -- only tables of the selected groups
          AND g.group_is_rollbackable                                 -- only tables from rollbackable groups
          AND (n.nspname,t.relname) NOT IN                            -- referenced table outside the groups
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames))
      LOOP
      RAISE WARNING '_check_fk_groups: table %.% is referenced by foreign key % from table %.% that is outside groups (%).',
                r_fk.rel_schema,r_fk.rel_tblseq,r_fk.conname,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
    RETURN;
  END;
$_check_fk_groups$;

CREATE OR REPLACE FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN)
RETURNS void LANGUAGE plpgsql AS
$_lock_groups$
-- This function locks all tables of a groups array.
-- The lock mode is provided by the calling function.
-- It only locks existing tables. It is calling function's responsability to handle cases when application tables are missing.
-- Input: array of group names, lock mode, flag indicating whether the function is called to processed several groups
  DECLARE
    v_nbRetry                SMALLINT = 0;
    v_nbTbl                  INT;
    v_ok                     BOOLEAN = false;
    v_fullTableName          TEXT;
    v_mode                   TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END,'BEGIN', array_to_string(v_groupNames,','));
-- set the value for the lock mode that will be used in the LOCK statement
    IF v_lockMode = '' THEN
      v_mode = 'ACCESS EXCLUSIVE';
    ELSE
      v_mode = v_lockMode;
    END IF;
-- acquire lock on all tables
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
-- scan all existing tables of the groups
        v_nbTbl = 0;
        FOR r_tblsq IN
            SELECT rel_priority, rel_schema, rel_tblseq
               FROM emaj.emaj_relation, pg_catalog.pg_class, pg_catalog.pg_namespace
               WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
                 AND relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
               ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
-- lock the table
          v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          EXECUTE 'LOCK TABLE ' || v_fullTableName || ' IN ' || v_mode || ' MODE';
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_lock_groups: a deadlock has been trapped while locking tables of group %.', v_groupNames;
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_lock_groups: too many (5) deadlocks encountered while locking tables of group %.',v_groupNames;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_GROUPS' ELSE 'LOCK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_lock_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function is the simplified form of the emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) function
-- The created groups are considered 'rollbackable'
-- Input: group name
-- Output: number of processed tables and sequences
  BEGIN
    RETURN emaj.emaj_create_group(v_groupName,true);
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT) IS
$$Creates a rollbackable E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- It also creates the secondary E-Maj schemas when needed
-- Input: group name, boolean indicating wether the group is rollbackable or not
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTbl                  INT = 0;
    v_nbSeq                  INT = 0;
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_logSchema              TEXT;
    v_msg                    TEXT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_defTsp                 TEXT;
    v_stmt                   TEXT;
    v_nb_trg                 INT;
    r_tblsq                  RECORD;
    r_schema                 RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName, CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- check that the group name is valid
    IF v_groupName IS NULL OR v_groupName = ''THEN
      RAISE EXCEPTION 'emaj_create_group: group name can''t be NULL or empty.';
    END IF;
-- check the group is known in emaj_group_def table
    PERFORM 0 FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName LIMIT 1;
    IF NOT FOUND THEN
       RAISE EXCEPTION 'emaj_create_group: Group % is unknown in emaj_group_def table.', v_groupName;
    END IF;
-- check that the group is not yet recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: group % is already created.', v_groupName;
    END IF;
-- performs various checks on the group's content described in the emaj_group_def table
    PERFORM emaj._check_group_content(v_groupName);
-- OK, insert group row in the emaj_group table
    INSERT INTO emaj.emaj_group (group_name, group_is_logging, group_is_rollbackable)
      VALUES (v_groupName, FALSE,v_isRollbackable);
-- look for new E-Maj secondary schemas to create
    FOR r_schema IN
      SELECT DISTINCT v_schemaPrefix || grpdef_log_schema_suffix AS log_schema FROM emaj.emaj_group_def
        WHERE grpdef_group = v_groupName
          AND grpdef_log_schema_suffix IS NOT NULL AND grpdef_log_schema_suffix <> ''
      EXCEPT
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
      ORDER BY 1
      LOOP
-- create the schema
      PERFORM emaj._create_log_schema(r_schema.log_schema);
-- and record the schema creation in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('CREATE_GROUP','SCHEMA CREATED',quote_ident(r_schema.log_schema));
    END LOOP;
-- define the default tablespace, NULL if tspemaj tablespace doesn't exist
    SELECT 'tspemaj' INTO v_defTsp FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
-- scan all classes of the group (in priority order, NULLS being processed last)
    FOR r_tblsq IN
        SELECT grpdef_priority, grpdef_schema, grpdef_tblseq, relkind,
               grpdef_log_schema_suffix, grpdef_log_dat_tsp, grpdef_log_idx_tsp
          FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
          WHERE grpdef_group = v_groupName
            AND relnamespace = pg_namespace.oid
            AND nspname = grpdef_schema AND relname = grpdef_tblseq
          ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
        LOOP
      IF r_tblsq.relkind = 'r' THEN
-- if it is a table, build the log schema name
        v_logSchema = coalesce(v_schemaPrefix || r_tblsq.grpdef_log_schema_suffix, v_emajSchema);
-- create the related emaj objects
        PERFORM emaj._create_tbl(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_logSchema, coalesce(r_tblsq.grpdef_log_dat_tsp, v_defTsp), coalesce(r_tblsq.grpdef_log_idx_tsp, v_defTsp), v_isRollbackable);
-- and record the table in the emaj_relation table
        INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind)
            VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, r_tblsq.grpdef_priority, v_logSchema, coalesce(r_tblsq.grpdef_log_dat_tsp, v_defTsp), coalesce(r_tblsq.grpdef_log_idx_tsp, v_defTsp), 'r');
        v_nbTbl = v_nbTbl + 1;
      ELSEIF r_tblsq.relkind = 'S' THEN
-- if it is a sequence, check no log schema has been set as parameter in the emaj_group_def table
        IF r_tblsq.grpdef_log_schema_suffix IS NOT NULL THEN
          RAISE EXCEPTION 'emaj_create_group: Defining a secondary log schema is not allowed for a sequence (%.%).', r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq;
        END IF;
--   check no tablespace has been set as parameter in the emaj_group_def table
        IF r_tblsq.grpdef_log_dat_tsp IS NOT NULL OR r_tblsq.grpdef_log_idx_tsp IS NOT NULL THEN
          RAISE EXCEPTION 'emaj_create_group: Defining log tablespaces is not allowed for a sequence (%.%).', r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq;
        END IF;
--   perform specific processing for sequences
        PERFORM emaj._create_seq(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName);
--   and record it in the emaj_relation table
        INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_kind)
            VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, r_tblsq.grpdef_priority, 'S');
        v_nbSeq = v_nbSeq + 1;
      END IF;
    END LOOP;
-- update tables and sequences counters in the emaj_group table
    UPDATE emaj.emaj_group SET group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
      WHERE group_name = v_groupName;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_groups (array[v_groupName]);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('CREATE_GROUP', 'END', v_groupName, v_nbTbl + v_nbSeq || ' tables/sequences processed');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_create_group$;
COMMENT ON FUNCTION emaj.emaj_create_group(TEXT, BOOLEAN) IS
$$Creates an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT)
RETURNS void LANGUAGE plpgsql AS
$emaj_comment_group$
-- This function sets or modifies a comment on a group by updating the group_comment of the emaj_group table.
-- Input: group name, comment
--   To reset an existing comment for a group, the supplied comment can be NULL.
  DECLARE
  BEGIN
-- attempt to update the group_comment column from emaj_group table
    UPDATE emaj.emaj_group SET group_comment = v_comment WHERE group_name = v_groupName;
-- check that the group has been found
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_group: group % has not been created.', v_groupName;
    END IF;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object)
      VALUES ('COMMENT_GROUP', v_groupName);
    RETURN;
  END;
$emaj_comment_group$;
COMMENT ON FUNCTION emaj.emaj_comment_group(TEXT,TEXT) IS
$$Sets a comment on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_drop_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_drop_group$
-- This function deletes the emaj objects for all tables of a group
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb                   INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, FALSE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_drop_group$;
COMMENT ON FUNCTION emaj.emaj_drop_group(TEXT) IS
$$Drops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_force_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- It differs from emaj_drop_group by the fact that:
--   - the group may be in LOGGING state
--   - a missing component in the drop processing does not generate any error
-- This allows to drop a group that is not consistent, following hasardeous operations.
-- This function should not be used, except if the emaj_drop_group fails.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb                   INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('FORCE_DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, TRUE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('FORCE_DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_force_drop_group$;
COMMENT ON FUNCTION emaj.emaj_force_drop_group(TEXT) IS
$$Drops an E-Maj group, even in LOGGING state.$$;

CREATE OR REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- It also drops secondary schemas that are not useful any more
-- Input: group name, and a boolean indicating whether the group's state has to be checked
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that secondary schemas can be dropped
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_schemaPrefix           TEXT = 'emaj';
    r_tblsq                  RECORD;
    r_schema                 RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_group: group % has not been created.', v_groupName;
    END IF;
-- if the state of the group has to be checked,
    IF NOT v_isForced THEN
--   check that the group is not in LOGGING state
      IF v_groupIsLogging THEN
        RAISE EXCEPTION '_drop_group: The group % cannot be deleted because it is in LOGGING state.', v_groupName;
      END IF;
    END IF;
-- OK, delete the emaj objets for each table of the group
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
        PERFORM emaj._drop_tbl(r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_log_schema);
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
          PERFORM emaj._drop_seq(r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- look for E-Maj secondary schemas to drop (i.e. not used by any other created group)
    FOR r_schema IN
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_log_schema <>  v_schemaPrefix
      EXCEPT
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
        WHERE rel_group <> v_groupName AND rel_log_schema <>  v_schemaPrefix
      ORDER BY 1
      LOOP
-- drop the schema
      PERFORM emaj._drop_log_schema(r_schema.rel_log_schema, v_isForced);
-- and record the schema suppression in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES (CASE WHEN v_isForced THEN 'FORCE_DROP_GROUP' ELSE 'DROP_GROUP' END,'SCHEMA DROPPED',quote_ident(r_schema.rel_log_schema));
    END LOOP;
-- delete group row from the emaj_group table.
--   By cascade, it also deletes rows from emaj_relation and emaj_mark
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName;
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_alter_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_alter_group$
-- This function alters a tables group.
-- It takes into account the changes recorded in the emaj_group_def table since the group has been created.
-- Executing emaj_alter_group() is equivalent to chaining emaj_drop_group() and emaj_create_group().
-- But only emaj objects that need to be dropped or created are processed.
-- Input: group name
-- Output: number of tables and sequences belonging to the group after the operation
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    v_schemaPrefix           TEXT = 'emaj';
    v_nbCreate               INT = 0;
    v_nbDrop                 INT = 0;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
    v_groupIsLogging         BOOLEAN;
    v_isRollbackable         BOOLEAN;
    v_logSchema              TEXT;
    v_logSchemasArray        TEXT[];
    v_msg                    TEXT;
    v_logDatTsp              TEXT;
    v_logIdxTsp              TEXT;
    v_defTsp                 TEXT;
    v_nbMsg                  INT;
    v_stmt                   TEXT;
    v_nb_trg                 INT;
    r_tblsq                  RECORD;
    r_schema                 RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('ALTER_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging, group_is_rollbackable INTO v_groupIsLogging, v_isRollbackable
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_alter_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_alter_group: The group % cannot be altered because it is in LOGGING state.', v_groupName;
    END IF;
-- check there are remaining rows for the group in emaj_group_def table
    PERFORM 0 FROM emaj.emaj_group_def WHERE grpdef_group = v_groupName LIMIT 1;
    IF NOT FOUND THEN
       RAISE EXCEPTION 'emaj_alter_group: Group % is unknown in emaj_group_def table.', v_groupName;
    END IF;
-- performs various checks on the group's content described in the emaj_group_def table
    PERFORM emaj._check_group_content(v_groupName);
-- define the default tablespace, NULL if tspemaj tablespace doesn't exist
    SELECT 'tspemaj' INTO v_defTsp FROM pg_catalog.pg_tablespace WHERE spcname = 'tspemaj';
-- OK, we can now process:
--   - relations that do not belong to the tables group any more, by dropping their emaj components
--   - relations that continue to belong to the tables group but with different characteristics,
--     by first dropping their emaj components and letting the last step recreate them
--   - new relations in the tables group, by (re)creating their emaj components
--
-- list all relations that do not belong to the tables group any more
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_log_schema
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName
          AND (rel_schema, rel_tblseq) NOT IN (
              SELECT grpdef_schema, grpdef_tblseq
                FROM emaj.emaj_group_def
                WHERE grpdef_group = v_groupName)
      UNION
-- ... and all relations that are damaged or whose log table is not synchronised with them any more
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_log_schema
        FROM (                                   -- all damaged or out of sync tables
          SELECT DISTINCT ver_schema, ver_tblseq FROM emaj._verify_groups(ARRAY[v_groupName], false)
             ) AS t, emaj.emaj_relation
        WHERE rel_schema = ver_schema AND rel_tblseq = ver_tblseq
      ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
        PERFORM emaj._drop_tbl(r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_log_schema);
-- add the log schema to the array of log schemas to potentialy drop at the end of the function
        IF r_tblsq.rel_log_schema <> v_emajSchema AND
           (v_logSchemasArray IS NULL OR r_tblsq.rel_log_schema <> ALL (v_logSchemasArray)) THEN
          v_logSchemasArray = array_append(v_logSchemasArray,r_tblsq.rel_log_schema);
        END IF;
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
        PERFORM emaj._drop_seq(r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
-- delete the related row in emaj_relation
      DELETE FROM emaj.emaj_relation WHERE rel_schema = r_tblsq.rel_schema AND rel_tblseq = r_tblsq.rel_tblseq;
      v_nbDrop = v_nbDrop + 1;
    END LOOP;
--
-- list relations that still belong to the tables group
    FOR r_tblsq IN
      SELECT rel_priority, rel_schema, rel_tblseq, rel_kind, rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, grpdef_priority, grpdef_schema, grpdef_tblseq, grpdef_log_schema_suffix, grpdef_log_dat_tsp, grpdef_log_idx_tsp
        FROM emaj.emaj_relation, emaj.emaj_group_def
        WHERE rel_schema = grpdef_schema AND rel_tblseq = grpdef_tblseq
          AND rel_group = v_groupName
          AND grpdef_group = v_groupName
      ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- now detect other changes that justify to drop and recreate the relation
-- detect if the log data tablespace in emaj_group_def has changed
      IF (r_tblsq.rel_kind = 'r' AND coalesce(r_tblsq.rel_log_dat_tsp,'') <> coalesce(r_tblsq.grpdef_log_dat_tsp, v_defTsp,''))
        OR (r_tblsq.rel_kind = 'S' AND r_tblsq.grpdef_log_dat_tsp IS NOT NULL)
-- or if the log index tablespace in emaj_group_def has changed
        OR (r_tblsq.rel_kind = 'r' AND coalesce(r_tblsq.rel_log_idx_tsp,'') <> coalesce(r_tblsq.grpdef_log_idx_tsp, v_defTsp,''))
        OR (r_tblsq.rel_kind = 'S' AND r_tblsq.grpdef_log_idx_tsp IS NOT NULL)
-- or if the log schema in emaj_group_def has changed
        OR (r_tblsq.rel_kind = 'r' AND r_tblsq.rel_log_schema <> (v_schemaPrefix || coalesce(r_tblsq.grpdef_log_schema_suffix, '')))
        OR (r_tblsq.rel_kind = 'S' AND r_tblsq.grpdef_log_schema_suffix IS NOT NULL) THEN
-- then drop the relation (it will be recreated later)
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
          PERFORM emaj._drop_tbl (r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.rel_log_schema);
-- and add the log schema to the list of log schemas to potentialy drop at the end of the function
        IF r_tblsq.rel_log_schema <> v_emajSchema AND
           (v_logSchemasArray IS NULL OR r_tblsq.rel_log_schema <> ALL (v_logSchemasArray)) THEN
            v_logSchemasArray = array_append(v_logSchemasArray,r_tblsq.rel_log_schema);
          END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
          PERFORM emaj._drop_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
        END IF;
-- delete the related row in emaj_relation
        DELETE FROM emaj.emaj_relation WHERE rel_schema = r_tblsq.grpdef_schema AND rel_tblseq = r_tblsq.grpdef_tblseq;
        v_nbDrop = v_nbDrop + 1;
-- other case ?
-- has the priority changed in emaj_group_def ? If yes, just report the change into emaj_relation
      ELSEIF (r_tblsq.rel_priority IS NULL AND r_tblsq.grpdef_priority IS NOT NULL) OR
             (r_tblsq.rel_priority IS NOT NULL AND r_tblsq.grpdef_priority IS NULL) OR
             (r_tblsq.rel_priority <> r_tblsq.grpdef_priority) THEN
        UPDATE emaj.emaj_relation SET rel_priority = r_tblsq.grpdef_priority
          WHERE rel_schema = r_tblsq.grpdef_schema AND rel_tblseq = r_tblsq.grpdef_tblseq;
      END IF;
    END LOOP;
--
-- cleanup all remaining log tables
    PERFORM emaj._reset_group(v_groupName);
-- drop useless log schemas, using the list of potential schemas to drop built previously
    IF v_logSchemasArray IS NOT NULL THEN
      FOR v_i IN 1 .. array_upper(v_logSchemasArray,1)
        LOOP
        PERFORM 0 FROM emaj.emaj_relation WHERE rel_log_schema = v_logSchemasArray [v_i] LIMIT 1;
        IF NOT FOUND THEN
-- drop the log schema
          PERFORM emaj._drop_log_schema(v_logSchemasArray [v_i], false);
-- and record the schema drop in emaj_hist table
          INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
            VALUES ('ALTER_GROUP','SCHEMA DROPPED',quote_ident(v_logSchemasArray [v_i]));
        END IF;
      END LOOP;
    END IF;
-- look for new E-Maj secondary schemas to create
    FOR r_schema IN
      SELECT DISTINCT v_schemaPrefix || grpdef_log_schema_suffix AS log_schema FROM emaj.emaj_group_def
        WHERE grpdef_group = v_groupName
          AND grpdef_log_schema_suffix IS NOT NULL AND grpdef_log_schema_suffix <> ''
      EXCEPT
      SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation
      ORDER BY 1
      LOOP
-- create the schema
      PERFORM emaj._create_log_schema(r_schema.log_schema);
-- and record the schema creation in emaj_hist table
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
        VALUES ('ALTER_GROUP','SCHEMA CREATED',quote_ident(r_schema.log_schema));
    END LOOP;
--
-- list new relations in the tables group (really new or intentionaly dropped in the preceeding steps)
    FOR r_tblsq IN
      SELECT grpdef_priority, grpdef_schema, grpdef_tblseq, relkind,
             grpdef_log_schema_suffix, grpdef_log_dat_tsp, grpdef_log_idx_tsp
        FROM emaj.emaj_group_def, pg_catalog.pg_class, pg_catalog.pg_namespace
        WHERE grpdef_group = v_groupName
          AND (grpdef_schema, grpdef_tblseq) NOT IN (
              SELECT rel_schema, rel_tblseq
                FROM emaj.emaj_relation
                WHERE rel_group = v_groupName)
          AND relnamespace = pg_namespace.oid AND nspname = grpdef_schema AND relname = grpdef_tblseq
      ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
      LOOP
--
      IF r_tblsq.relkind = 'r' THEN
-- if it is a table, build the log schema name
        v_logSchema = coalesce(v_schemaPrefix ||  r_tblsq.grpdef_log_schema_suffix, v_emajSchema);
-- create the related emaj objects
        PERFORM emaj._create_tbl(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_logSchema, coalesce(r_tblsq.grpdef_log_dat_tsp, v_defTsp), coalesce(r_tblsq.grpdef_log_idx_tsp, v_defTsp), v_isRollbackable);
-- and record the table in the emaj_relation table
        INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_log_schema, rel_log_dat_tsp, rel_log_idx_tsp, rel_kind)
            VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, r_tblsq.grpdef_priority, v_logSchema, coalesce(r_tblsq.grpdef_log_dat_tsp, v_defTsp), coalesce(r_tblsq.grpdef_log_idx_tsp, v_defTsp), 'r');
      ELSEIF r_tblsq.relkind = 'S' THEN
-- if it is a sequence, check no log schema has been set as parameter in the emaj_group_def table
        IF r_tblsq.grpdef_log_schema_suffix IS NOT NULL THEN
          RAISE EXCEPTION 'emaj_alter_group: Defining a secondary log schema is not allowed for a sequence (%.%).', r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq;
        END IF;
--   check no tablespace has been set as parameter in the emaj_group_def table
        IF r_tblsq.grpdef_log_dat_tsp IS NOT NULL OR r_tblsq.grpdef_log_idx_tsp IS NOT NULL THEN
          RAISE EXCEPTION 'emaj_alter_group: Defining log tablespaces is not allowed for a sequence (%.%).', r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq;
        END IF;
--   perform specific processing for sequences
        PERFORM emaj._create_seq(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName);
--   and record it in the emaj_relation table
        INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_kind)
            VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, r_tblsq.grpdef_priority, 'S');
      END IF;
      v_nbCreate = v_nbCreate + 1;
    END LOOP;
-- update tables and sequences counters and the last alter timestamp in the emaj_group table
    SELECT count(*) INTO v_nbTbl FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'r';
    SELECT count(*) INTO v_nbSeq FROM emaj.emaj_relation WHERE rel_group = v_groupName AND rel_kind = 'S';
    UPDATE emaj.emaj_group SET group_last_alter_datetime = transaction_timestamp(),
                               group_nb_table = v_nbTbl, group_nb_sequence = v_nbSeq
      WHERE group_name = v_groupName;
--	delete old marks of the tables group from emaj_mark
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- check foreign keys with tables outside the group
    PERFORM emaj._check_fk_groups(array[v_groupName]);
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('ALTER_GROUP', 'END', v_groupName, v_nbDrop || ' dropped relations and ' || v_nbCreate || ' (re)created relations');
    RETURN v_nbTbl + v_nbSeq;
  END;
$emaj_alter_group$;
COMMENT ON FUNCTION emaj.emaj_alter_group(TEXT) IS
$$Alter an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_group$
-- This function activates the log triggers of all the tables for a group and set a first mark
-- This is the short form for emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN)
--   where v_resetLog is true.
  BEGIN
    RETURN emaj.emaj_start_group(v_groupName, v_mark, true);
  END;
$emaj_start_group$;
COMMENT ON FUNCTION emaj.emaj_start_group(TEXT,TEXT) IS
$$Starts an E-Maj group (short form).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_group$
-- This function activates the log triggers of all the tables for a group and set a first mark
-- It may reset log tables.
-- Input: group name,
--        name of the mark to set
--          '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--          a null or '' mark is transformed into 'MARK_%',
--        boolean indicating whether the log tables of the group must be reset.
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('START_GROUP', 'BEGIN', v_groupName, CASE WHEN v_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- call the common _start_groups function
    SELECT emaj._start_groups(array[v_groupName], v_mark, false, v_resetLog) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('START_GROUP', 'END', v_groupName, v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_start_group$;
COMMENT ON FUNCTION emaj.emaj_start_group(TEXT,TEXT,BOOLEAN) IS
$$Starts an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_groups$
-- This function activates the log triggers of all the tables for a groups array and set a first mark
-- This is the short form for emaj.emaj_start_groups(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN)
--   where v_resetLog is true.
  BEGIN
    RETURN emaj.emaj_start_groups(v_groupNames, v_mark, true);
  END;
$emaj_start_groups$;
COMMENT ON FUNCTION emaj.emaj_start_groups(TEXT[],TEXT) IS
$$Starts several E-Maj groups (short form).$$;

CREATE OR REPLACE FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$emaj_start_groups$
-- This function activates the log triggers of all the tables for a groups array and set a first mark
-- Input: array of group names,
--        name of the mark to set
--          '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--          a null or '' mark is transformed into 'MARK_%',
--        boolean indicating whether the log tables of the group must be reset.
-- Output: total number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('START_GROUPS', 'BEGIN', array_to_string(v_groupNames,','), CASE WHEN v_resetLog THEN 'With log reset' ELSE 'Without log reset' END);
-- call the common _start_groups function
    SELECT emaj._start_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, true, v_resetLog) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('START_GROUPS', 'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_start_groups$;
COMMENT ON FUNCTION emaj.emaj_start_groups(TEXT[],TEXT, BOOLEAN) IS
$$Starts several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_markName               TEXT;
    v_logTableName           TEXT;
    v_fullTableName          TEXT;
    v_logTriggerName         TEXT;
    v_truncTriggerName       TEXT;
    v_cpt                    BIGINT;
    r_tblsq                  RECORD;
  BEGIN
-- purge the emaj history, if needed
    PERFORM emaj._purge_hist();
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group is recorded in emaj_group table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_start_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... and is not in LOGGING state
      IF v_groupIsLogging THEN
        RAISE EXCEPTION '_start_group: The group % cannot be started because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_groupNames[v_i];
      END IF;
    END LOOP;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- for each group,
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      if v_resetLog THEN
-- ... if requested by the user, call the emaj_reset_group function to erase remaining traces from previous logs
        SELECT emaj._reset_group(v_groupNames[v_i]) INTO v_nbTb;
      END IF;
-- ... and check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(array[v_groupNames[v_i]]);
    END LOOP;
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
    PERFORM emaj._lock_groups(v_groupNames,'',v_multiGroup);
-- ... and enable all log triggers for the groups
    v_nbTb = 0;
-- for each relation of the group,
    FOR r_tblsq IN
       SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
         WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
       LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
        v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_logTriggerName = quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
        v_truncTriggerName = quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
        EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
        IF v_pgVersion >= '8.4' THEN
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_truncTriggerName;
        END IF;
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- update the state of the group row from the emaj_group table
    UPDATE emaj.emaj_group SET group_is_logging = TRUE WHERE group_name = ANY (v_groupNames);
-- Set the first mark for each group
    PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true);
--
    RETURN v_nbTb;
  END;
$_start_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_group$
-- This function de-activates the log triggers of all the tables for a group.
-- Execute several emaj_stop_group functions for the same group doesn't produce any error.
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('STOP_GROUP', 'BEGIN', v_groupName);
-- call the common _stop_groups function
    SELECT emaj._stop_groups(array[v_groupName], 'STOP_%', false, false) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('STOP_GROUP', 'END', v_groupName, v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_stop_group$;
COMMENT ON FUNCTION emaj.emaj_stop_group(TEXT) IS
$$Stops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_group$
-- This function de-activates the log triggers of all the tables for a group.
-- Execute several emaj_stop_group functions for the same group doesn't produce any error.
-- Input: group name, stop mark name to set
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('STOP_GROUP', 'BEGIN', v_groupName);
-- call the common _stop_groups function
    SELECT emaj._stop_groups(array[v_groupName], v_mark, false, false) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('STOP_GROUP', 'END', v_groupName, v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_stop_group$;
COMMENT ON FUNCTION emaj.emaj_stop_group(TEXT,TEXT) IS
$$Stops an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[])
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_groups$
-- This function de-activates the log triggers of all the tables for a groups array.
-- Groups already not in LOGGING state are simply not processed.
-- Input: array of group names
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('STOP_GROUPS', 'BEGIN', array_to_string(v_groupNames,','));
-- call the common _stop_groups function
    SELECT emaj._stop_groups(emaj._check_names_array(v_groupNames,'group'), 'STOP_%', true, false) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('STOP_GROUPS', 'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_stop_groups$;
COMMENT ON FUNCTION emaj.emaj_stop_groups(TEXT[]) IS
$$Stops several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_stop_groups$
-- This function de-activates the log triggers of all the tables for a groups array.
-- Groups already not in LOGGING state are simply not processed.
-- Input: array of group names, stop mark name to set
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('STOP_GROUPS', 'BEGIN', array_to_string(v_groupNames,','));
-- call the common _stop_groups function
    SELECT emaj._stop_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, true, false) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('STOP_GROUPS', 'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_stop_groups$;
COMMENT ON FUNCTION emaj.emaj_stop_groups(TEXT[], TEXT) IS
$$Stops several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_force_stop_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_force_stop_group$
-- This function forces a tables group stop.
-- The differences with the standart emaj_stop_group() function are :
--   - it silently ignores errors when an application table or one of its triggers is missing
--   - no stop mark is set (to avoid error)
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('FORCE_STOP_GROUP', 'BEGIN', v_groupName);
-- call the common _stop_groups function
    SELECT emaj._stop_groups(array[v_groupName], NULL, false, true) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('FORCE_STOP_GROUP', 'END', v_groupName, v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_force_stop_group$;
COMMENT ON FUNCTION emaj.emaj_force_stop_group(TEXT) IS
$$Forces an E-Maj group stop.$$;

CREATE OR REPLACE FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group.
-- Input: array of group names, a mark name to set, and a boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_validGroupNames        TEXT[];
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
    v_markName               TEXT;
    v_fullTableName          TEXT;
    v_logTriggerName         TEXT;
    v_truncTriggerName       TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- for each group of the array,
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_stop_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE WARNING '_stop_group: Group % cannot be stopped because it is not in LOGGING state.', v_groupNames[v_i];
      ELSE
-- ... if OK, add the group into the array of groups to process
        v_validGroupNames = v_validGroupNames || array[v_groupNames[v_i]];
      END IF;
    END LOOP;
-- check and process the supplied mark name (except if the function is called by emaj_force_stop_group())
    IF NOT v_isForced THEN
      SELECT emaj._check_new_mark(v_mark, v_groupNames) INTO v_markName;
    END IF;
--
    IF v_validGroupNames IS NOT NULL THEN
-- OK (no error detected and at least one group in logging state)
-- lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
      PERFORM emaj._lock_groups(v_validGroupNames,'',v_multiGroup);
-- for each relation of the groups to process,
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            WHERE rel_group = ANY (v_validGroupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
          LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, disable the emaj log and truncate triggers
--   errors are captured so that emaj_force_stop_group() can be silently executed
          v_fullTableName  = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          v_logTriggerName = quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
          v_truncTriggerName = quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
          BEGIN
            EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
          EXCEPTION
            WHEN invalid_schema_name THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
              ELSE
                RAISE EXCEPTION '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
              END IF;
            WHEN undefined_table THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Table % does not exist any more.', v_fullTableName;
              ELSE
                RAISE EXCEPTION '_stop_group: Table % does not exist any more.', v_fullTableName;
              END IF;
            WHEN undefined_object THEN
              IF v_isForced THEN
                RAISE WARNING '_stop_group: Trigger % on table % does not exist any more.', v_logTriggerName, v_fullTableName;
              ELSE
                RAISE EXCEPTION '_stop_group: Trigger % on table % does not exist any more.', v_logTriggerName, v_fullTableName;
              END IF;
          END;
          IF v_pgVersion >= '8.4' THEN
            BEGIN
              EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
            EXCEPTION
              WHEN invalid_schema_name THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
                ELSE
                  RAISE EXCEPTION '_stop_group: Schema % does not exist any more.', quote_ident(r_tblsq.rel_schema);
                END IF;
              WHEN undefined_table THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Table % does not exist any more.', v_fullTableName;
                ELSE
                  RAISE EXCEPTION '_stop_group: Table % does not exist any more.', v_fullTableName;
                END IF;
              WHEN undefined_object THEN
                IF v_isForced THEN
                  RAISE WARNING '_stop_group: Trigger % on table % does not exist any more.', v_truncTriggerName, v_fullTableName;
                ELSE
                  RAISE EXCEPTION '_stop_group: Trigger % on table % does not exist any more.', v_truncTriggerName, v_fullTableName;
                END IF;
            END;
          END IF;
          ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
        END IF;
        v_nbTb = v_nbTb + 1;
      END LOOP;
      IF NOT v_isForced THEN
-- if the function is not called by emaj_force_stop_group(), set the stop mark for each group
        PERFORM emaj._set_mark_groups(v_validGroupNames, v_markName, v_multiGroup, true);
-- and set the number of log rows to 0 for these marks
        UPDATE emaj.emaj_mark m SET mark_log_rows_before_next = 0
          WHERE mark_group = ANY (v_validGroupNames)
            AND (mark_group, mark_id) IN                        -- select only last mark of each concerned group
                (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
                 WHERE mark_group = ANY (v_validGroupNames) AND NOT mark_is_deleted GROUP BY mark_group);
      END IF;
-- set all marks for the groups from the emaj_mark table as 'DELETED' to avoid any further rollback
      UPDATE emaj.emaj_mark SET mark_is_deleted = TRUE
        WHERE mark_group = ANY (v_validGroupNames) AND NOT mark_is_deleted;
-- update the state of the groups rows from the emaj_group table
      UPDATE emaj.emaj_group SET group_is_logging = FALSE WHERE group_name = ANY (v_validGroupNames);
    END IF;
    RETURN v_nbTb;
  END;
$_stop_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group
-- Input: group name, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_markName               TEXT;
    v_nbTb                   INT;
  BEGIN
-- insert begin into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'BEGIN', v_groupName, v_markName);
-- check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_set_mark_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF NOT v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_set_mark_group: A mark cannot be set for group % because it is not in LOGGING state. An emaj_start_group function must be previously executed.', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM 0 FROM emaj._verify_groups(array[v_groupName], true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, array[v_groupName]) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(array[v_groupName],'ROW EXCLUSIVE',false);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(array[v_groupName], v_markName, false, false) INTO v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUP', 'END', v_groupName, v_markName);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time
-- Input: array of group names, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_validGroupNames        TEXT[];
    v_groupIsLogging         BOOLEAN;
    v_markName               TEXT;
    v_nbTb                   INT;
  BEGIN
-- validate the group names array
    v_validGroupNames=emaj._check_names_array(v_groupNames,'group');
-- if the group names array is null, immediately return 0
    IF v_validGroupNames IS NULL THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('SET_MARK_GROUPS', 'BEGIN', NULL, v_mark);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES ('SET_MARK_GROUPS', 'END', NULL, v_mark);
      RETURN 0;
    END IF;
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'BEGIN', array_to_string(v_groupNames,','), v_mark);
-- for each group...
    FOR v_i IN 1 .. array_upper(v_validGroupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_validGroupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: group % has not been created.', v_validGroupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: A mark cannot be set for group % because it is not in LOGGING state. An emaj_start_group function must be previously executed.', v_validGroupNames[v_i];
      END IF;
    END LOOP;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_validGroupNames, true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_validGroupNames) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(v_validGroupNames,'ROW EXCLUSIVE',true);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(v_validGroupNames, v_markName, true, false) into v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(v_groupNames,','), v_mark);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT) IS
$$Sets a mark on several E-Maj groups.$$;

CREATE OR REPLACE FUNCTION emaj._set_mark_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN)
RETURNS int LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It also updates the previous mark of each group to setup the mark_log_rows_before_next column with the number of rows recorded into all log tables between this previous mark and the new mark.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like functions that start or rollback groups.
-- Input: group names array, mark to set,
--        boolean indicating whether the function is called by a multi group function
--        boolean indicating whether the event has to be recorded into the emaj_hist table
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_nbTb                   INT = 0;
    v_timestamp              TIMESTAMPTZ;
    v_lastSequenceId         BIGINT;
    v_lastSeqHoleId          BIGINT;
    v_lastGlobalSeq          BIGINT;
    v_fullSeqName            TEXT;
    v_seqName                TEXT;
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    r_tblsq                  RECORD;
  BEGIN
-- if requested, record the set mark begin in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_mark);
    END IF;
-- look at the clock to get the 'official' timestamp representing the mark
    v_timestamp = clock_timestamp();
-- process sequences as early as possible (no lock protect them from other transactions activity)
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
          ORDER BY rel_priority, rel_schema, rel_tblseq
      LOOP
-- for each sequence of the groups, record the sequence parameters into the emaj_sequence table
      v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
               quote_literal(r_tblsq.rel_tblseq) || ', ' || quote_literal(v_timestamp) ||
               ', ' || quote_literal(v_mark) || ', last_value, ';
      IF v_pgVersion <= '8.3' THEN
         v_stmt = v_stmt || '0, ';
      ELSE
         v_stmt = v_stmt || 'start_value, ';
      END IF;
      v_stmt = v_stmt ||
               'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || v_fullSeqName;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the number of log rows for the old last mark of each group
--   the statement returns no row in case of emaj_start_group(s)
    UPDATE emaj.emaj_mark m SET mark_log_rows_before_next =
      coalesce( (SELECT sum(stat_rows) FROM emaj.emaj_log_stat_group(m.mark_group,'EMAJ_LAST_MARK',NULL)) ,0)
      WHERE mark_group = ANY (v_groupNames)
        AND (mark_group, mark_id) IN                   -- select only the last non deleted mark of each concerned group
            (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
             WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
-- for each table of the groups, ...
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
		  ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- ... record the associated sequence parameters in the emaj sequence table
      v_seqName = emaj._build_log_seq_name(r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      v_fullSeqName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(v_seqName);
      v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
               'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
               'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
               ') SELECT '|| quote_literal(r_tblsq.rel_log_schema) || ', ' || quote_literal(v_seqName) || ', ' ||
               quote_literal(v_timestamp) || ', ' || quote_literal(v_mark) || ', ' || 'last_value, ';
      IF v_pgVersion <= '8.3' THEN
         v_stmt = v_stmt || '0, ';
      ELSE
         v_stmt = v_stmt || 'start_value, ';
      END IF;
      v_stmt = v_stmt ||
               'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
               'FROM ' || v_fullSeqName;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the marks
-- get the last id for emaj_sequence and emaj_seq_hole tables, and the last value for emaj_global_seq
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSequenceId
      FROM emaj.emaj_sequence_sequ_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSeqHoleId
      FROM emaj.emaj_seq_hole_sqhl_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastGlobalSeq
      FROM emaj.emaj_global_seq;
-- insert the marks into the emaj_mark table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_datetime, mark_global_seq, mark_is_deleted, mark_last_sequence_id, mark_last_seq_hole_id)
        VALUES (v_groupNames[v_i], v_mark, v_timestamp, v_lastGlobalSeq, FALSE, v_lastSequenceId, v_lastSeqHoleId);
    END LOOP;
-- before exiting, cleanup the state of the pending rollback events from the emaj_rlbk table
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
-- ... either through dblink if we are currently performing a rollback with a dblink connection already opened
--     this is mandatory to avoid deadlock
      PERFORM 0 FROM dblink('rlbk#1','SELECT emaj.emaj_cleanup_rollback_state()') AS (dummy INT);
    ELSE
-- ... or directly
      PERFORM emaj.emaj_cleanup_rollback_state();
    END IF;
-- if requested, record the set mark end in emaj_hist
    IF v_eventToRecord THEN
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_mark);
    END IF;
--
    RETURN v_nbTb;
  END;
$_set_mark_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT)
RETURNS void LANGUAGE plpgsql AS
$emaj_comment_mark_group$
-- This function sets or modifies a comment on a mark by updating the mark_comment of the emaj_mark table.
-- Input: group name, mark to comment, comment
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
--   To reset an existing comment for a mark, the supplied comment can be NULL.
  DECLARE
    v_realMark               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_comment_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_comment_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- OK, update the mark_comment from emaj_mark table
    UPDATE emaj.emaj_mark SET mark_comment = v_comment WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- insert event in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
      VALUES ('COMMENT_MARK_GROUP', v_groupName, 'Mark ' || v_realMark);
    RETURN;
  END;
$emaj_comment_mark_group$;
COMMENT ON FUNCTION emaj.emaj_comment_mark_group(TEXT,TEXT,TEXT) IS
$$Sets a comment on a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ)
RETURNS text LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given date and time.
-- It may return unpredictable result in case of system date or time change.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, date and time
-- Output: mark name, or NULL if there is no mark before the given date and time
  DECLARE
    v_markName               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group: group % has not been created.', v_groupName;
    END IF;
-- find the requested mark
    SELECT mark_name INTO v_markName FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_datetime < v_datetime
      ORDER BY mark_datetime DESC LIMIT 1;
    IF NOT FOUND THEN
      RETURN NULL;
    ELSE
      RETURN v_markName;
    END IF;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TIMESTAMPTZ) IS
$$Returns the latest mark name preceeding a point in time.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS text LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given mark for a group.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, mark name
--   The keyword 'EMAJ_LAST_MARK' can be used to specify the last set mark.
-- Output: mark name, or NULL if there is no mark before the given mark
  DECLARE
    v_realMark               TEXT;
    v_markName               TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the given mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_get_previous_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- find the requested mark
    SELECT mark_name INTO v_markName FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_datetime <
        (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark)
      ORDER BY mark_datetime DESC LIMIT 1;
    IF NOT FOUND THEN
      RETURN NULL;
    ELSE
      RETURN v_markName;
    END IF;
  END;
$emaj_get_previous_mark_group$;
COMMENT ON FUNCTION emaj.emaj_get_previous_mark_group(TEXT,TEXT) IS
$$Returns the latest mark name preceeding a given mark for a group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS integer LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function.
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- The statistical mark_log_rows_before_next column's content of the previous mark is also maintained
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_realMark               TEXT;
    v_markId                 BIGINT;
    v_datetimeMark           TIMESTAMPTZ;
    v_idNewMin               BIGINT;
    v_markNewMin             TEXT;
    v_datetimeNewMin         TIMESTAMPTZ;
    v_cpt                    INT;
    v_previousMark           TEXT;
    v_nextMark               TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- count the number of mark in the group
    SELECT count(*) INTO v_cpt FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- and check there are at least 2 marks for the group
    IF v_cpt < 2 THEN
       RAISE EXCEPTION 'emaj_delete_mark_group: % is the only mark. It cannot be deleted.', v_mark;
    END IF;
-- OK, now get the id and timestamp of the mark to delete
    SELECT mark_id, mark_datetime INTO v_markId, v_datetimeMark
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- ... and the id and timestamp of the future first mark
    SELECT mark_id, mark_name, mark_datetime INTO v_idNewMin, v_markNewMin, v_datetimeNewMin
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name <> v_realMark ORDER BY mark_id LIMIT 1;
    IF v_markId < v_idNewMin THEN
-- if the mark to delete is the first one,
--   ... process its deletion with _delete_before_mark_group(), as the first rows of log tables become useless
      PERFORM emaj._delete_before_mark_group(v_groupName, v_markNewMin);
    ELSE
-- otherwise,
--   ... the sequences related to the mark to delete can be suppressed
--         Delete first application sequences related data for the group
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE sequ_mark = v_realMark AND sequ_datetime = v_datetimeMark
          AND rel_group = v_groupName AND rel_kind = 'S'
          AND sequ_schema = rel_schema AND sequ_name = rel_tblseq;
--         Delete then emaj sequences related data for the group
      DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
        WHERE sequ_mark = v_realMark AND sequ_datetime = v_datetimeMark
          AND rel_group = v_groupName AND rel_kind = 'r'
          AND sequ_schema = rel_log_schema AND sequ_name = emaj._build_log_seq_name(rel_schema,rel_tblseq);
--   ... the mark to delete can be physicaly deleted
      DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
--   ... adjust the mark_log_rows_before_next column of the previous mark
--       get the name of the mark immediately preceeding the mark to delete
      SELECT mark_name INTO v_previousMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_id < v_markId ORDER BY mark_id DESC LIMIT 1;
--       get the name of the first mark succeeding the mark to delete
      SELECT mark_name INTO v_nextMark FROM emaj.emaj_mark
        WHERE mark_group = v_groupName AND mark_id > v_markId ORDER BY mark_id LIMIT 1;
      IF NOT FOUND THEN
--       no next mark, so update the previous mark with NULL
         UPDATE emaj.emaj_mark SET mark_log_rows_before_next = NULL
           WHERE mark_group = v_groupName AND mark_name = v_previousMark;
      ELSE
--       update the previous mark with the emaj_log_stat_group() call's result
         UPDATE emaj.emaj_mark SET mark_log_rows_before_next =
             (SELECT sum(stat_rows) FROM emaj.emaj_log_stat_group(v_groupName, v_previousMark, v_nextMark))
           WHERE mark_group = v_groupName AND mark_name = v_previousMark;
      END IF;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_realMark);
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS integer LANGUAGE plpgsql AS
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
    v_groupIsLogging         BOOLEAN;
    v_realMark               TEXT;
    v_markId                 BIGINT;
    v_datetimeMark           TIMESTAMPTZ;
    v_nbMark                 INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: group % has not been created.', v_groupName;
    END IF;
-- return NULL if mark name is NULL
    IF v_mark IS NULL THEN
      RETURN NULL;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_delete_before_mark_group: % is not a known mark for group %.', v_mark, v_groupName;
    END IF;
-- effectively delete all marks before the supplied mark
    SELECT emaj._delete_before_mark_group(v_groupName, v_realMark) INTO v_nbMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', v_groupName,  v_nbMark || ' marks deleted ; ' || v_realMark || ' is now the initial mark' );
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT)
RETURNS integer LANGUAGE plpgsql AS
$_delete_before_mark_group$
-- This function effectively deletes all marks set before a given mark.
-- It deletes rows corresponding to the marks to delete from emaj_mark, emaj_sequence, emaj_seq_hole.
-- It also deletes rows from all concerned log tables.
-- To complete, the function deletes oldest rows from emaj_hist
-- Input: group name, name of the new first mark
-- Output: number of deleted marks
  DECLARE
    v_markId                 BIGINT;
    v_markGlobalSeq          BIGINT;
    v_datetimeMark           TIMESTAMPTZ;
    v_logTableName           TEXT;
    v_nbMark                 INT;
    r_tblsq                  RECORD;
  BEGIN
-- retrieve the id and datetime of the new first mark
    SELECT mark_id, mark_global_seq, mark_datetime INTO v_markId, v_markGlobalSeq, v_datetimeMark
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark;
-- delete rows from all log tables
-- loop on all tables of the group
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- delete log rows prior to the new first mark
      EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_gid <= ' || v_markGlobalSeq;
    END LOOP;
-- delete all sequence holes that are prior the new first mark for the tables of the group
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table
        AND sqhl_id <= (SELECT mark_last_seq_hole_id FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark);
-- now the sequences related to the mark to delete can be suppressed
--   Delete first application sequences related data for the group
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'S'
        AND sequ_schema = rel_schema AND sequ_name = rel_tblseq
        AND (sequ_mark, sequ_datetime) IN
            (SELECT mark_name, mark_datetime FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_id < v_markId);
--   Delete then emaj sequences related data for the group
    DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r'
        AND sequ_schema = rel_log_schema AND sequ_name = emaj._build_log_seq_name(rel_schema,rel_tblseq)
        AND (sequ_mark, sequ_datetime) IN
            (SELECT mark_name, mark_datetime FROM emaj.emaj_mark
              WHERE mark_group = v_groupName AND mark_id < v_markId);
-- and finaly delete marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_id < v_markId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
-- purge the emaj history, if needed (even if no mark as been really dropped)
    PERFORM emaj._purge_hist();
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT)
RETURNS void LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be in LOGGING or not.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly.
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  DECLARE
    v_realMark               TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: mark % doesn''t exist for group %.', v_mark, v_groupName;
    END IF;
-- check the new mark name is not 'EMAJ_LAST_MARK' or NULL
    IF v_newName = 'EMAJ_LAST_MARK' OR v_newName IS NULL THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: % is not an allowed name for a new mark.', v_newName;
    END IF;
-- check if the new mark name doesn't exist for the group
    PERFORM 0 FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_newName;
    IF FOUND THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: a mark % already exists for group %.', v_newName, v_groupName;
    END IF;
-- OK, update the sequences table as well
    UPDATE emaj.emaj_sequence SET sequ_mark = v_newName
      WHERE sequ_datetime =                                         -- the right mark date and time
            (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark)
        AND (sequ_schema, sequ_name) IN
            (SELECT rel_schema, rel_tblseq FROM emaj.emaj_relation  -- filter only application sequences of the group
               WHERE rel_group = v_groupName AND rel_kind = 'S'
             UNION ALL                                              -- filter only log sequences of the group
             SELECT rel_log_schema, emaj._build_log_seq_name(rel_schema, rel_tblseq) FROM emaj.emaj_relation
               WHERE rel_group = v_groupName AND rel_kind = 'r'
            );
-- and then update the emaj_mark table
    UPDATE emaj.emaj_mark SET mark_name = v_newName
      WHERE mark_group = v_groupName AND mark_name = v_realMark;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RENAME_MARK_GROUP', 'END', v_groupName, v_realMark || ' renamed ' || v_newName);
    RETURN;
  END;
$emaj_rename_mark_group$;
COMMENT ON FUNCTION emaj.emaj_rename_mark_group(TEXT,TEXT,TEXT) IS
$$Renames a mark for an E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the group (with boolean: isLoggedRlbk = false, multiGroup = false)
    return emaj._rlbk_groups(array[v_groupName], v_mark, false, false);
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history
-- Input: array of group names, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the groups (with boolean: isLoggedRlbk = false, multiGroup = true)
    return emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, false, true);
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the group (with boolean: isLoggedRlbk = true, multiGroup = false)
    return emaj._rlbk_groups(array[v_groupName], v_mark, true, false);
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT)
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rolled back! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rolled back log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the groups (with boolean: isLoggedRlbk = true, multiGroup = true)
    return emaj._rlbk_groups(emaj._check_names_array(v_groupNames,'group'), v_mark, true, true);
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups$
-- The function rollbacks all tables and sequences of a groups array up to a mark in the history.
-- It is called by emaj_rollback_group.
-- It effectively manages the rollback operation for each table or sequence, deleting rows from log tables
-- only when asked by the calling functions.
-- Its activity is split into smaller functions that are also called by the parallel restore php function
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group
--        and a boolean saying if the rollback is a logged rollback
-- Output: number of tables and sequences effectively processed
  DECLARE
    v_rlbkId                 INT;
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check supplied parameter and prepare the rollback operation
    SELECT emaj._rlbk_init(v_groupNames, v_mark, v_isLoggedRlbk, 1, v_multiGroup) INTO v_rlbkId;
-- lock all tables
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
-- set a rollback start mark if logged rollback
    PERFORM emaj._rlbk_start_mark(v_rlbkId, v_multiGroup);
-- execute the rollback planning
    SELECT emaj._rlbk_session_exec(v_rlbkId, 1) INTO v_nbTbl;
-- process sequences and complete the rollback operation
    SELECT emaj._rlbk_end(v_rlbkId, v_multiGroup) INTO v_nbSeq;
    RETURN v_nbTbl + v_nbSeq;
  END;
$_rlbk_groups$;

CREATE OR REPLACE FUNCTION emaj._rlbk_async(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_async$
-- The function calls the main rollback functions following the initialisation phase.
-- It is only called by the phpPgAdmin plugin, in an asynchronous way, so that the rollback can be then monitored by the client.
-- Input: rollback identifier and a boolean saying if the rollback is a logged rollback
-- Output: number of tables and sequences effectively processed
  DECLARE
    v_nbTbl                  INT;
    v_nbSeq                  INT;
  BEGIN
-- simply chain the internal functions
    PERFORM emaj._rlbk_session_lock(v_rlbkId, 1);
    PERFORM emaj._rlbk_start_mark(v_rlbkId, v_multiGroup);
    SELECT emaj._rlbk_session_exec(v_rlbkId, 1) INTO v_nbTbl;
    SELECT emaj._rlbk_end(v_rlbkId, v_multiGroup) INTO v_nbSeq;
    RETURN v_nbTbl + v_nbSeq;
  END;
$_rlbk_async$;

CREATE OR REPLACE FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_init$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- By calling the _rlbk_planning() function, it defines the different elementary steps needed for the operation,
-- and spread the load on the requested number of sessions.
-- It returns a rollback id that will be needed by next steps.
  DECLARE
    v_markName               TEXT;
    v_nbTblInGroups          INT;
    v_nbSeqInGroups          INT;
    v_effNbTable             INT;
    v_histId                 BIGINT;
    v_startDateTime          TIMESTAMPTZ;
    v_stmt                   TEXT;
    v_dbLinkCnxStatus        INT;
    v_isDblinkUsable         BOOLEAN = false;
    v_rlbkId                 INT;
    v_timestampMark          TIMESTAMPTZ;
    v_msg                    TEXT;
  BEGIN
-- lock the groups to rollback
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = ANY(v_groupNames) FOR UPDATE;
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames,v_mark) INTO v_markName;
-- check that no group is damaged
    PERFORM 0 FROM emaj._verify_groups(v_groupNames, true);
-- get the mark timestamp for the 1st group (as we know this timestamp is the same for all groups of the array)
    SELECT emaj._get_mark_datetime(v_groupNames[1],v_mark) INTO v_timestampMark;
-- insert begin in the history
    IF v_isLoggedRlbk THEN
      v_msg = 'Logged';
    ELSE
      v_msg = 'Unlogged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN',
              array_to_string(v_groupNames,','),
              v_msg || ' rollback to mark ' || v_markName || ' [' || v_timestampMark || ']'
             ) RETURNING hist_id, hist_datetime INTO v_histId, v_startDateTime;
-- get the total number of tables for these groups
    SELECT sum(group_nb_table), sum(group_nb_sequence) INTO v_nbTblInGroups, v_nbSeqInGroups
      FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- first try to open a dblink connection
    SELECT emaj._dblink_open_cnx('rlbk#1') INTO v_dbLinkCnxStatus;
    v_isDblinkUsable = (v_dbLinkCnxStatus >= 0);
-- for parallel rollback (nb sessions > 1) the dblink connection must be ok
    IF v_nbSession > 1 AND NOT v_isDblinkUsable THEN
      RAISE EXCEPTION '_rlbk_init: cannot use several sessions without dblink connection capability. (Status of the dblink connection attempt = % - see E-Maj documentation)', v_dbLinkCnxStatus;
    END IF;
-- create the row representing the rollback event in the emaj_rlbk table and get the rollback id back
    v_stmt = 'INSERT INTO emaj.emaj_rlbk (rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, ' ||
             'rlbk_nb_session, rlbk_nb_table, rlbk_nb_sequence, rlbk_status, rlbk_begin_hist_id, ' ||
             'rlbk_is_dblink_used, rlbk_start_datetime ) ' ||
             'VALUES (' || quote_literal(v_groupNames) || ',' || quote_literal(v_markName) || ',' ||
             quote_literal(v_timestampMark) || ',' || v_isLoggedRlbk || ',' || v_nbSession || ',' ||
             v_nbTblInGroups || ',' || v_nbSeqInGroups || ', ''PLANNING'',' || v_histId || ',' ||
             v_isDblinkUsable || ',' || quote_literal(v_startDateTime) || ') RETURNING rlbk_id';
    IF v_isDblinkUsable THEN
-- insert a rollback event into the emaj_rlbk table ... either through dblink if possible
      SELECT rlbk_id INTO v_rlbkId FROM dblink('rlbk#1',v_stmt) AS (rlbk_id INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt INTO v_rlbkId;
    END IF;
-- issue warnings in case of foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(v_groupNames);
-- call the rollback planning function to define all the elementary steps to perform,
-- compute their estimated duration and attribute steps to sessions
    v_stmt = 'SELECT emaj._rlbk_planning(' || v_rlbkId || ')';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible (do not try to open a connection, it has already been attempted)
      SELECT eff_nb_table FROM dblink('rlbk#1',v_stmt) AS (eff_nb_table INT) INTO v_effNbTable;
    ELSE
-- ... or directly
      EXECUTE v_stmt INTO v_effNbTable;
    END IF;
-- update the emaj_rlbk table to set the real number of tables to process and adjust the rollback status
    v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_eff_nb_table = ' || v_effNbTable ||
             ', rlbk_status = ''LOCKING'' ' || ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
    RETURN v_rlbkId;
  END;
$_rlbk_init$;

CREATE OR REPLACE FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT)
RETURNS TEXT LANGUAGE plpgsql AS
$_rlbk_check$
-- This functions performs checks on group names and mark names supplied as parameter for the emaj_rollback_groups()
-- and emaj_estimate_rollback_groups() functions.
-- It returns the real mark name.
  DECLARE
    v_i                      INT;
    v_groupIsLogging         BOOLEAN;
    v_isRollbackable         BOOLEAN;
    v_markName               TEXT;
    v_markIsDeleted          BOOLEAN;
    v_cpt                    INT;
  BEGIN
-- check that each group ...
-- ...is recorded in emaj_group table
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_is_logging, group_is_rollbackable INTO v_groupIsLogging, v_isRollbackable
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i];
      IF NOT FOUND THEN
        RAISE EXCEPTION '_rlbk_check: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... is in LOGGING state
      IF NOT v_groupIsLogging THEN
        RAISE EXCEPTION '_rlbk_check: Group % is not in LOGGING state.', v_groupNames[v_i];
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_isRollbackable THEN
        RAISE EXCEPTION '_rlbk_check: Group % has been created for audit only purpose.', v_groupNames[v_i];
      END IF;
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_check: No mark % exists for group %.', v_mark, v_groupNames[v_i];
      END IF;
-- ... and this mark is ACTIVE
      SELECT mark_is_deleted INTO v_markIsDeleted FROM emaj.emaj_mark
        WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
      IF v_markIsDeleted THEN
        RAISE EXCEPTION '_rlbk_check: mark % for group % is not usable for rollback.', v_markName, v_groupNames[v_i];
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_check: Mark % does not represent the same point in time for all groups.', v_mark;
    END IF;
    RETURN v_markName;
  END;
$_rlbk_check$;

CREATE OR REPLACE FUNCTION emaj._rlbk_planning(v_rlbkId INT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_planning$
-- This function builds the rollback steps for a rollback operation.
-- It stores the result into the emaj_rlbk_plan table.
-- The function returns the effective number of tables to process.
-- It is called in an autonomous dblink transaction, if possible.
-- The function is defined as SECURITY DEFINER so that emaj_viwer role can write into rollback tables without having specific privileges to do it.
  DECLARE
    v_stmt                   TEXT;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_markDatetime           TIMESTAMPTZ;
    v_markLastSeqHoleId      BIGINT;
    v_nbSession              INT;
    v_ctrlStepName           emaj._rlbk_step_enum;
    v_effNbTable             INT;
    v_batchNumber            INT;
    v_checks                 INT;
    v_estimDuration          INTERVAL;
    v_estimMethod            INT;
    v_estimDropFkDuration    INTERVAL;
    v_estimDropFkMethod      INT;
    v_estimSetFkDefDuration  INTERVAL;
    v_estimSetFkDefMethod    INT;
    v_avg_row_rlbk           INTERVAL;
    v_avg_row_del_log        INTERVAL;
    v_avg_fkey_check         INTERVAL;
    v_fixed_step_rlbk        INTERVAL;
    v_fixed_dblink_rlbk      INTERVAL;
    v_sessionLoad            INTERVAL[];
    v_minSession             INT;
    v_minDuration            INTERVAL;
    v_nbStep                 INT;
    r_tbl                    RECORD;
    r_fk                     RECORD;
    r_batch                  RECORD;
  BEGIN
-- get the rollack characteristics for the emaj_rlbk event
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session,
           CASE WHEN rlbk_is_dblink_used THEN 'CTRL+DBLINK' ELSE 'CTRL-DBLINK' END
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_nbSession, v_ctrlStepName
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_datetime, mark_last_seq_hole_id INTO v_markDatetime, v_markLastSeqHoleId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- get all duration parameters that will be needed later from the emaj_param table,
--   or get default values for rows that are not present in emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'avg_fkey_check_duration'),'5 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_step_rollback_duration'),'2.5 millisecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_dblink_rollback_duration'),'4 millisecond'::interval)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_avg_fkey_check, v_fixed_step_rlbk, v_fixed_dblink_rlbk;
-- insert into emaj_rlbk_plan a row per table belonging to the tables groups to process.
    INSERT INTO emaj.emaj_rlbk_plan (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey)
      SELECT v_rlbkId, 'LOCK_TABLE', rel_schema, rel_tblseq, ''
        FROM emaj.emaj_relation
        WHERE rel_group = ANY(v_groupNames) AND rel_kind = 'r';
-- insert into emaj_rlbk_plan a row per table to effectively rollback.
-- the numbers of log rows is computed using the _log_stat_tbl() function.
     INSERT INTO emaj.emaj_rlbk_plan
            (rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_estimated_quantity)
      SELECT v_rlbkId, 'RLBK_TABLE', rel_schema, rel_tblseq, '',
             emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_markDatetime, NULL, v_markLastSeqHoleId, NULL)
        FROM (SELECT rel_schema, rel_tblseq, rel_log_schema
                FROM emaj.emaj_relation
                WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r') AS t
        WHERE emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_markDatetime, NULL, v_markLastSeqHoleId, NULL) > 0;
     GET DIAGNOSTICS v_effNbTable = ROW_COUNT;
--
-- group tables into batchs to process all tables linked by foreign keys as a batch
--
    v_batchNumber = 1;
--   allocate tables with rows to rollback to batch number starting with the heaviest to rollback tables
--     as reported by emaj_log_stat_group() function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE' ORDER BY rlbp_estimated_quantity DESC
      LOOP
--   is the table already allocated to a batch number (it may have been already allocated because of a fkey link) ?
      PERFORM 0 FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table AND rlbp_batch_number IS NULL;
--   no,
      IF FOUND THEN
--   allocate the table to the batch number, with all other tables linked by foreign key constraints
        PERFORM emaj._rlbk_set_batch_number(v_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table);
        v_batchNumber = v_batchNumber + 1;
      END IF;
    END LOOP;
-- TODO: when CTE will be available for all supported pg versions, use them to replace the recursive call to _rlbk_set_batch_number()
--
-- if unlogged rollback, register into emaj_rlbk_plan "disable log triggers", "deletes from log tables"
-- and "enable log trigger" steps
--
    IF NOT v_isLoggedRlbk THEN
-- compute the cost for each DIS_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DIS_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all DIS_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'DIS_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- insert all DELETE_LOG steps. But the duration estimates will be computed later
-- the estimated number of log rows to delete is set to the the estimated number of updates. This is underestimated
--   in particular when SQL UPDATES are logged. But the collected statistics used for duration estimates are also
--   based on the estimated number of updates.
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number, rlbp_estimated_quantity
        ) SELECT v_rlbkId, 'DELETE_LOG', rlbp_schema, rlbp_table, '', rlbp_batch_number, rlbp_estimated_quantity
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
-- compute the cost for each ENA_LOG_TRG step
--   if DIS_LOG_TRG statistics are available, compute an average cost
      SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDuration FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'ENA_LOG_TRG';
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
-- insert all ENA_LOG_TRG steps
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
        ) SELECT v_rlbkId, 'ENA_LOG_TRG', rlbp_schema, rlbp_table, '', rlbp_batch_number,
                 v_estimDuration, v_estimMethod
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE';
    END IF;
--
-- process foreign key to define which action to perform on them
--
-- First compute the fixed duration estimates for each 'DROP_FK' and 'SET_FK_DEF' steps
--   if DROP_FK statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimDropFkDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'DROP_FK';
    v_estimDropFkMethod = 2;
    IF v_estimDropFkDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimDropFkDuration = v_fixed_step_rlbk;
      v_estimDropFkMethod = 3;
    END IF;
--   if SET_FK_DEF statistics are available, compute an average cost
    SELECT sum(rlbt_duration) / sum(rlbt_quantity) INTO v_estimSetFkDefDuration FROM emaj.emaj_rlbk_stat
      WHERE rlbt_step = 'SET_FK_DEF';
    v_estimSetFkDefMethod = 2;
    IF v_estimSetFkDefDuration IS NULL THEN
--   if no statistics are available for this step, use the fixed_step_rollback_duration parameter
      v_estimSetFkDefDuration = v_fixed_step_rlbk;
      v_estimSetFkDefMethod = 3;
    END IF;
-- select all foreign keys belonging to or referencing the tables to process
    FOR r_fk IN
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND n.nspname = r.rlbp_schema AND t.relname = r.rlbp_table     -- join on emaj_rlbk_plan table
      UNION
      SELECT c.oid AS conoid, c.conname, n.nspname, t.relname, t.reltuples, pg_get_constraintdef(c.oid) AS def, c.condeferrable, c.condeferred, c.confupdtype, c.confdeltype, r.rlbp_batch_number
        FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t,
             pg_catalog.pg_namespace rn, pg_catalog.pg_class rt, emaj.emaj_rlbk_plan r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables to rollback
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace
          AND rn.nspname = r.rlbp_schema AND rt.relname = r.rlbp_table   -- join on emaj_rlbk_plan table
      ORDER BY nspname, relname, conname
      LOOP
-- depending on the foreign key characteristics, record as 'to be dropped' or 'to be set deffered' or 'to just be reset immediate'
      IF NOT r_fk.condeferrable OR r_fk.confupdtype <> 'a' OR r_fk.confdeltype <> 'a' THEN
-- non deferrable fkeys and deferrable fkeys with an action for UPDATE or DELETE other than 'no action' need to be dropped
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
          rlbp_estimated_duration, rlbp_estimate_method
          ) VALUES (
          v_rlbkId, 'DROP_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
          v_estimDropFkDuration, v_estimDropFkMethod
          );
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number, rlbp_fkey_def, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'ADD_FK', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, r_fk.def, r_fk.reltuples
          );
      ELSE
-- other deferrable but not deferred fkeys need to be set deferred
        IF NOT r_fk.condeferred THEN
          INSERT INTO emaj.emaj_rlbk_plan (
            rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number,
            rlbp_estimated_duration, rlbp_estimate_method
            ) VALUES (
            v_rlbkId, 'SET_FK_DEF', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number,
            v_estimSetFkDefDuration, v_estimSetFkDefMethod
            );
        END IF;
-- deferrable fkeys are recorded as 'to be set immediate at the end of the rollback operation'
-- compute the number of fkey values to check at set immediate time
        SELECT (coalesce(
--   get the number of rolled back rows in the referencing table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND rlbp_schema = r_fk.nspname AND rlbp_table = r_fk.relname)  -- referencing schema.table
            , 0)) + (coalesce(
--   get the number of rolled back rows in the referenced table, if any
           (SELECT rlbp_estimated_quantity
              FROM emaj.emaj_rlbk_plan, pg_catalog.pg_constraint c, pg_catalog.pg_namespace rn, pg_catalog.pg_class rt
              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'       -- tables of the rollback event
                AND c.oid = r_fk.conoid                                        -- constraint id
                AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced schema.table
                AND rn.nspname = rlbp_schema AND rt.relname = rlbp_table)      -- join on emaj_rlbk_plan
            , 0)) INTO v_checks;
-- and record the SET_FK_IMM step
        INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_batch_number, rlbp_estimated_quantity
          ) VALUES (
          v_rlbkId, 'SET_FK_IMM', r_fk.nspname, r_fk.relname, r_fk.conname, r_fk.rlbp_batch_number, v_checks
          );
      END IF;
    END LOOP;
--
-- Now compute the estimation duration for each complex step ('RLBK_TABLE', 'DELETE_LOG', 'ADD_FK', 'SET_FK_IMM')
--
-- Compute the rollback duration estimates for the tables
-- for each table with content to rollback
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'RLBK_TABLE' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_rlbk * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the log rows delete duration for the tables
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
      LOOP
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
          AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table
          AND rlbt_quantity / r_tbl.rlbp_estimated_quantity < 10 AND r_tbl.rlbp_estimated_quantity / rlbt_quantity < 10;
      v_estimMethod = 1;
      IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbt_duration) * r_tbl.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'DELETE_LOG' AND rlbt_quantity > 0
            AND rlbt_schema = r_tbl.rlbp_schema AND rlbt_table = r_tbl.rlbp_table;
        v_estimMethod = 2;
        IF v_estimDuration IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estimDuration = v_avg_row_del_log * r_tbl.rlbp_estimated_quantity + v_fixed_step_rlbk;
          v_estimMethod = 3;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'DELETE_LOG'
          AND rlbp_schema = r_tbl.rlbp_schema AND rlbp_table = r_tbl.rlbp_table;
    END LOOP;
-- Compute the fkey recreation duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
      LOOP
      IF r_fk.rlbp_estimated_quantity = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estimDuration = 0;
        v_estimMethod = 3;
      ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
          FROM emaj.emaj_rlbk_stat
          WHERE rlbt_step = 'ADD_FK' AND rlbt_quantity > 0
            AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey;
        v_estimMethod = 1;
        IF v_estimDuration IS NULL THEN
-- non empty table, but no statistics with at least one row are available => take the last duration for this fkey, if any
          SELECT rlbt_duration INTO v_estimDuration FROM emaj.emaj_rlbk_stat
            WHERE rlbt_step = 'ADD_FK'
              AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_tbl.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey
              AND rlbt_rlbk_datetime =
               (SELECT max(rlbt_rlbk_datetime) FROM emaj.emaj_rlbk_stat WHERE rlbt_step = 'ADD_FK'
                  AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey);
          v_estimMethod = 2;
          IF v_estimDuration IS NULL THEN
-- definitely no statistics available, compute with the avg_fkey_check_duration parameter
            v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
            v_estimMethod = 3;
          END IF;
        END IF;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'ADD_FK'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_fkey = r_fk.rlbp_fkey;
    END LOOP;
-- Compute the fkey checks duration
    FOR r_fk IN
        SELECT * FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
      LOOP
-- if fkey checks statistics are available for this fkey, compute an average cost
      SELECT sum(rlbt_duration) * r_fk.rlbp_estimated_quantity / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat
        WHERE rlbt_step = 'SET_FK_IMM' AND rlbt_quantity > 0
          AND rlbt_schema = r_fk.rlbp_schema AND rlbt_table = r_fk.rlbp_table AND rlbt_fkey = r_fk.rlbp_fkey;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
-- if no statistics are available for this fkey, use the avg_fkey_check parameter
        v_estimDuration = r_fk.rlbp_estimated_quantity * v_avg_fkey_check + v_fixed_step_rlbk;
        v_estimMethod = 3;
      END IF;
      UPDATE emaj.emaj_rlbk_plan
        SET rlbp_estimated_duration = v_estimDuration, rlbp_estimate_method = v_estimMethod
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'SET_FK_IMM'
          AND rlbp_schema = r_fk.rlbp_schema AND rlbp_table = r_fk.rlbp_table AND rlbp_fkey = r_fk.rlbp_fkey;
    END LOOP;
--
-- Allocate batch number to sessions to spread the load on sessions as best as possible
-- A batch represents all steps related to the processing of one table or several tables linked by foreign keys
--
--   initialisation
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = 0;
    END LOOP;
--   allocate tables batch to sessions, starting with the heaviest to rollback batch
    FOR r_batch IN
        SELECT rlbp_batch_number, sum(rlbp_estimated_duration) AS batch_duration
          FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number IS NOT NULL
          GROUP BY rlbp_batch_number
          ORDER BY sum(rlbp_estimated_duration) DESC
      LOOP
--   compute the least loaded session
      v_minSession=1; v_minDuration = v_sessionLoad [1];
      FOR v_session IN 2 .. v_nbSession LOOP
        IF v_sessionLoad [v_session] < v_minDuration THEN
          v_minSession = v_session;
          v_minDuration = v_sessionLoad [v_session];
        END IF;
      END LOOP;
--   allocate the batch to the session
      UPDATE emaj.emaj_rlbk_plan SET rlbp_session = v_minSession
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_batch_number = r_batch.rlbp_batch_number;
      v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + r_batch.batch_duration;
    END LOOP;
-- assign session 1 to all 'LOCK_TABLE' steps not yet affected
    UPDATE emaj.emaj_rlbk_plan SET rlbp_session = 1
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_session IS NULL;
-- Create the pseudo 'CTRL+DBLINK' or 'CTRL-DBLINK' step and compute its duration estimate
-- get the number of recorded steps (except LOCK_TABLE)
    SELECT count(*) INTO v_nbStep FROM emaj.emaj_rlbk_plan
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step <> 'LOCK_TABLE';
    IF v_nbStep > 0 THEN
-- if CTRLxDBLINK statistics are available, compute an average cost
      SELECT sum(rlbt_duration) * v_nbStep / sum(rlbt_quantity) INTO v_estimDuration
        FROM emaj.emaj_rlbk_stat WHERE rlbt_step = v_ctrlStepName AND rlbt_quantity > 0;
      v_estimMethod = 2;
      IF v_estimDuration IS NULL THEN
--   otherwise, use the fixed_step_rollback_duration parameter
        v_estimDuration = v_fixed_dblink_rlbk * v_nbStep;
        v_estimMethod = 3;
      END IF;
-- insert the 'CTRLxDBLINK' pseudo step
      INSERT INTO emaj.emaj_rlbk_plan (
          rlbp_rlbk_id, rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_estimated_quantity,
          rlbp_estimated_duration, rlbp_estimate_method
        ) VALUES (
          v_rlbkId, v_ctrlStepName, '', '', '', v_nbStep, v_estimDuration, v_estimMethod
        );
    END IF;
-- return the number of tables to effectively rollback
    RETURN v_effNbTable;
  END;
$_rlbk_planning$;

CREATE OR REPLACE FUNCTION emaj._rlbk_set_batch_number(v_rlbkId INT, v_batchNumber INT, v_schema TEXT, v_table TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_set_batch_number$
-- This function updates the emaj_rlbk_plan table to set the batch_number for one table.
-- It also looks for all tables that are linked to this table by foreign keys to force them to be allocated to the same batch number.
-- The function is called by _rlbk_planning().
-- As those linked tables can also be linked to other tables by other foreign keys, the function has to be recursiley called.
  DECLARE
    v_fullTableName          TEXT;
    r_tbl                    RECORD;
  BEGIN
-- set the batch number to this application table (there is a 'LOCK_TABLE' step and potentialy a 'RLBK_TABLE' step)
    UPDATE emaj.emaj_rlbk_plan SET rlbp_batch_number = v_batchNumber
      WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_schema = v_schema AND rlbp_table = v_table;
-- then look for all other application tables linked by foreign key relationships
    v_fullTableName = quote_ident(v_schema) || '.' || quote_ident(v_table);
    FOR r_tbl IN
        SELECT rlbp_schema, rlbp_table FROM emaj.emaj_rlbk_plan
          WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_batch_number IS NULL            -- not yet allocated
            AND (rlbp_schema, rlbp_table) IN (       -- list of (schema,table) linked to the original table by fkeys
              SELECT nspname, relname FROM pg_catalog.pg_constraint, pg_catalog.pg_class t, pg_catalog.pg_namespace n
                WHERE contype = 'f' AND confrelid = v_fullTableName::regclass
                  AND t.oid = conrelid AND relnamespace = n.oid
              UNION
              SELECT nspname, relname FROM pg_catalog.pg_constraint, pg_catalog.pg_class t, pg_catalog.pg_namespace n
                WHERE contype = 'f' AND conrelid = v_fullTableName::regclass
                  AND t.oid = confrelid AND relnamespace = n.oid
              )
      LOOP
-- recursive call to allocate these linked tables to the same batch_number
      PERFORM emaj._rlbk_set_batch_number(v_rlbkId, v_batchNumber, r_tbl.rlbp_schema, r_tbl.rlbp_table);
    END LOOP;
    RETURN;
  END;
$_rlbk_set_batch_number$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT)
RETURNS void LANGUAGE plpgsql AS
$_rlbk_session_lock$
-- It creates the session row in the emaj_rlbk_session table and then locks all the application tables for the session.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_nbRetry                SMALLINT = 0;
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
-- acquire lock on all tables
--
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- scan all tables of the session, in the order deducted from the priority defined in emaj_group_def
        FOR r_tbl IN
          SELECT quote_ident(rlbp_schema) || '.' || quote_ident(rlbp_table) AS fullName
            FROM emaj.emaj_rlbk_plan, emaj.emaj_relation
            WHERE rel_schema = rlbp_schema AND rel_tblseq = rlbp_table
            AND rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'LOCK_TABLE'
            AND rlbp_session = v_session
            ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
--   lock each table
--     The locking level is EXCLUSIVE MODE. It blocks all concurrent update capabilities of all tables of the groups
--     (including table with no logged update to rollback, in order to ensure a stable state of the group at the end
--     of the rollback operation). But these tables can be accessed by SELECT statements during the E-Maj rollback.
          EXECUTE 'LOCK TABLE ' || r_tbl.fullName || ' IN EXCLUSIVE MODE';
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

CREATE OR REPLACE FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_start_mark$
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback.
-- All concerned tables are already locked.
-- Before setting the mark, it checks no update has been recorded between the planning step and the locks set
-- for tables for which no rollback was needed at planning time.
-- It also sets the rollback status to EXECUTING.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbk_datetime          TIMESTAMPTZ;
    v_markDatetime           TIMESTAMPTZ;
    v_markLastSeqHoleId      BIGINT;
    v_markName               TEXT;
    v_errorMsg               TEXT;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = true;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_start_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_rlbk_datetime
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- get some mark attributes from emaj_mark
    SELECT mark_datetime, mark_last_seq_hole_id INTO v_markDatetime, v_markLastSeqHoleId
      FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- check that no update has been recorded between planning time and lock time for tables that did not need to
-- be rolled back at planning time.
-- This may occur and cannot be avoided because tables cannot be locked before processing the rollback planning.
-- (Sessions must lock the tables they will rollback and the planning processing distribute those tables to sessions.)
    PERFORM 1 FROM (SELECT rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
                      WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'
                        AND (rel_schema, rel_tblseq) NOT IN
                            (SELECT rlbp_schema, rlbp_table FROM emaj.emaj_rlbk_plan
                              WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step = 'RLBK_TABLE')
                    ) AS t
      WHERE emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_markDatetime, NULL, v_markLastSeqHoleId, NULL) > 0;
    IF FOUND THEN
      v_errorMsg = 'The rollback operation has been cancelled due to concurrent activity at E-Maj rollback planning time on tables to process.';
      PERFORM emaj._rlbk_error(v_rlbkId, v_errorMsg, 'rlbk#1');
      RAISE EXCEPTION '_rlbk_start_mark: % Please retry.', v_errorMsg;
    END IF;
    IF v_isLoggedRlbk THEN
-- If rollback is "logged" rollback, set a mark named with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the rollback start time
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbk_datetime, 'HH24.MI.SS.MS') || '_START';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true);
    END IF;
-- if dblink is usable, update the emaj_rlbk table to adjust the rollback status
-- (otherwise do not update a status that will be later changed and that can't be retrieve by someone else)
    IF v_isDblinkUsable THEN
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''EXECUTING'' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    END IF;
    RETURN;
-- TODO: trap and record exception during the rollback operation when pg 8.3 will not be supported any more
--  EXCEPTION
--    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
--      RAISE;
--    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
--      PERFORM emaj._rlbk_error(v_rlbkId, SQLERRM, 'rlbk#1');
--      RAISE;
  END;
$_rlbk_start_mark$;

CREATE OR REPLACE FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_session_exec$
-- This function executes the main part of a rollback operation.
-- It executes the steps identified by _rlbk_planning() and stored into emaj_rlbk_plan, for one session.
-- The function returns the effective number of processed tables.
-- It updates the emaj_rlbk_plan table, using dblink connection if possible, giving a visibility of the rollback progress.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if it doesn't own the application tables.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_effNbTable             INT = 0;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_timestampMark          TIMESTAMPTZ;
    v_isLoggedRlbk           BOOLEAN;
    v_nbSession              INT;
    v_lastGlobalSeq          BIGINT;
    v_lastSequenceId         BIGINT;
    v_lastSeqHoleId          BIGINT;
    v_logTriggerName         TEXT;
    v_nbRows                 BIGINT;
    r_step                   RECORD;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#'||v_session) THEN
      v_isDblinkUsable = true;
    END IF;
-- get the rollack characteristics from the emaj_rlbk table
    SELECT rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session
      INTO v_groupNames, v_mark, v_timestampMark, v_isLoggedRlbk, v_nbSession
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- fetch the last global sequence and the last id values of emaj_sequence and emaj_seq_hole tables at set mark time
    SELECT mark_global_seq, mark_last_sequence_id, mark_last_seq_hole_id
      INTO v_lastGlobalSeq, v_lastSequenceId, v_lastSeqHoleId FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
-- scan emaj_rlbp_plan to get all steps to process that have been affected to this session, in batch_number and step order
    FOR r_step IN
      SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey, rlbp_fkey_def
        FROM emaj.emaj_rlbk_plan,
             (VALUES ('DIS_LOG_TRG',1),('DROP_FK',2),('SET_FK_DEF',3),('RLBK_TABLE',4),
                     ('DELETE_LOG',5),('SET_FK_IMM',6),('ADD_FK',7),('ENA_LOG_TRG',8)) AS step(step_name, step_order)
        WHERE rlbp_step::text = step.step_name
          AND rlbp_rlbk_id = v_rlbkId AND rlbp_step NOT IN ('LOCK_TABLE','CTRL-DBLINK','CTRL+DBLINK')
          AND rlbp_session = v_session
        ORDER BY rlbp_batch_number, step_order, rlbp_table, rlbp_fkey
      LOOP
-- update the emaj_rlbk_plan table to set the step start time
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_start_datetime = clock_timestamp() ' ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || 'AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_fkey = ' || quote_literal(r_step.rlbp_fkey) || ' RETURNING 1';
      IF v_isDblinkUsable THEN
-- ... either through dblink if possible
        PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      ELSE
-- ... or directly
        EXECUTE v_stmt;
      END IF;
-- process the step depending on its type
      IF r_step.rlbp_step = 'DIS_LOG_TRG' THEN
-- process a log trigger disable
        v_logTriggerName = quote_ident(r_step.rlbp_schema || '_' || r_step.rlbp_table || '_emaj_log_trg');
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' DISABLE TRIGGER ' || v_logTriggerName;
      ELSEIF r_step.rlbp_step = 'DROP_FK' THEN
-- process a foreign key deletion
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' DROP CONSTRAINT ' || quote_ident(r_step.rlbp_fkey);
      ELSEIF r_step.rlbp_step = 'SET_FK_DEF' THEN
-- set a foreign key deferred
        EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                ' DEFERRED';
      ELSEIF r_step.rlbp_step = 'RLBK_TABLE' THEN
-- process a table rollback
        SELECT emaj._rlbk_tbl(rel_schema, rel_tblseq, rel_log_schema, v_lastGlobalSeq, v_nbSession) INTO v_nbRows
          FROM emaj.emaj_relation
          WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
        v_effNbTable = v_effNbTable + 1;
      ELSEIF r_step.rlbp_step = 'DELETE_LOG' THEN
-- process the deletion of log rows
        SELECT emaj._delete_log_tbl(rel_schema, rel_tblseq, rel_log_schema, v_timestampMark, v_lastGlobalSeq, v_lastSequenceId, v_lastSeqHoleId) INTO v_nbRows
          FROM emaj.emaj_relation
          WHERE rel_schema = r_step.rlbp_schema AND rel_tblseq = r_step.rlbp_table;
      ELSEIF r_step.rlbp_step = 'SET_FK_IMM' THEN
-- set a foreign key immediate
        EXECUTE 'SET CONSTRAINTS ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_fkey) ||
                ' IMMEDIATE';
      ELSEIF r_step.rlbp_step = 'ADD_FK' THEN
-- process a foreign key creation
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' ADD CONSTRAINT ' || quote_ident(r_step.rlbp_fkey) || ' ' || r_step.rlbp_fkey_def;
      ELSEIF r_step.rlbp_step = 'ENA_LOG_TRG' THEN
-- process a log trigger enable
        v_logTriggerName = quote_ident(r_step.rlbp_schema || '_' || r_step.rlbp_table || '_emaj_log_trg');
        EXECUTE 'ALTER TABLE ' || quote_ident(r_step.rlbp_schema) || '.' || quote_ident(r_step.rlbp_table) ||
                ' ENABLE TRIGGER ' || v_logTriggerName;
      END IF;
-- update the emaj_rlbk_plan table to set the step duration
-- NB: the computed duration does not include the time needed to update the emaj_rlbk_plan table
      v_stmt = 'UPDATE emaj.emaj_rlbk_plan SET rlbp_duration = ' || quote_literal(clock_timestamp()) || ' - rlbp_start_datetime';
      IF r_step.rlbp_step = 'RLBK_TABLE' OR r_step.rlbp_step = 'DELETE_LOG' THEN
--   and the effective number of processed rows for RLBK_TABLE and DELETE_LOG steps
        v_stmt = v_stmt || ' , rlbp_quantity = ' || v_nbRows;
      END IF;
      v_stmt = v_stmt ||
               ' WHERE rlbp_rlbk_id = ' || v_rlbkId || 'AND rlbp_step = ' || quote_literal(r_step.rlbp_step) ||
               ' AND rlbp_schema = ' || quote_literal(r_step.rlbp_schema) ||
               ' AND rlbp_table = ' || quote_literal(r_step.rlbp_table) ||
               ' AND rlbp_fkey = ' || quote_literal(r_step.rlbp_fkey) || ' RETURNING 1';
      IF v_isDblinkUsable THEN
-- ... either through dblink if possible
        PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
      ELSE
-- ... or directly
        EXECUTE v_stmt;
      END IF;
    END LOOP;
-- update the emaj_rlbk_session table to set the timestamp representing the end of work for the session
    v_stmt = 'UPDATE emaj.emaj_rlbk_session SET rlbs_end_datetime = clock_timestamp()' ||
             ' WHERE rlbs_rlbk_id = ' || v_rlbkId || ' AND rlbs_session = ' || v_session ||
             ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#'||v_session,v_stmt) AS (dummy INT);
--     and then close the connection for session > 1
      IF v_session > 1 THEN
        PERFORM emaj._dblink_close_cnx('rlbk#'||v_session);
      END IF;
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
    RETURN v_effNbTable;
-- TODO: trap and record exception during the rollback operation when pg 8.3 will not be supported any more
--  EXCEPTION
--    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
--      RAISE;
--    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
--      PERFORM emaj._rlbk_error(v_rlbkId, SQLERRM, 'rlbk#'||v_session);
--      RAISE;
  END;
$_rlbk_session_exec$;

CREATE OR REPLACE FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_end$
-- This is the last step of a rollback group processing. It :
--    - deletes the marks that are no longer available,
--    - copy data into the emaj_rlbk_stat table,
--    - rollbacks all sequences of the groups,
--    - set the end rollback mark if logged rollback,
--    - and finaly set the operation as COMPLETED or COMMITED.
-- It returns the number of processed sequences.
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
    v_groupNames             TEXT[];
    v_mark                   TEXT;
    v_isLoggedRlbk           BOOLEAN;
    v_rlbk_datetime          TIMESTAMPTZ;
    v_effNbTbl               INT;
    v_ctrlDuration           INTERVAL;
    v_markId                 BIGINT;
    v_timestampMark          TIMESTAMPTZ;
    v_lastSequenceId         BIGINT;
    v_nbSeq                  INT;
    v_markName               TEXT;
    v_histDateTime           TIMESTAMPTZ;
  BEGIN
-- determine whether the dblink connection for this session is opened
    IF emaj._dblink_is_cnx_opened('rlbk#1') THEN
      v_isDblinkUsable = true;
    END IF;
-- get the rollack characteristics for the emaj_rlbk
    SELECT rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_eff_nb_table, rlbk_start_datetime
      INTO v_groupNames, v_mark, v_isLoggedRlbk, v_effNbTbl, v_rlbk_datetime
      FROM emaj.emaj_rlbk WHERE rlbk_id = v_rlbkId;
-- if "unlogged" rollback, delete all marks later than the now rolled back mark
    IF NOT v_isLoggedRlbk THEN
-- get the highest mark id of the mark used for rollback, for all groups
      SELECT max(mark_id) INTO v_markId
        FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames) AND mark_name = v_mark;
-- log in the history the name of all marks that must be deleted due to the rollback
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
        SELECT CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END,
               'DELETE MARK', mark_group, 'mark ' || mark_name || ' is deleted' FROM emaj.emaj_mark
          WHERE mark_group = ANY (v_groupNames) AND mark_id > v_markId ORDER BY mark_id;
-- delete these useless marks (the related sequences have been already deleted by rollback functions)
      DELETE FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames) AND mark_id > v_markId;
-- and finaly reset the mark_log_rows_before_next column for the new last mark
      UPDATE emaj.emaj_mark SET mark_log_rows_before_next = NULL
        WHERE mark_group = ANY (v_groupNames)
          AND (mark_group, mark_id) IN                -- select only the last non deleted mark of each concerned group
              (SELECT mark_group, MAX(mark_id) FROM emaj.emaj_mark
               WHERE mark_group = ANY (v_groupNames) AND NOT mark_is_deleted GROUP BY mark_group);
    END IF;
-- delete the now useless 'LOCK TABLE' steps from the emaj_rlbk_plan table
    v_stmt = 'DELETE FROM emaj.emaj_rlbk_plan ' ||
             ' WHERE rlbp_rlbk_id = ' || v_rlbkId || ' AND rlbp_step = ''LOCK_TABLE'' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- Prepare the CTRLxDBLINK pseudo step statistic by computing the global time spent between steps
    SELECT coalesce(sum(ctrl_duration),'0'::interval) INTO v_ctrlDuration FROM (
      SELECT rlbs_session, rlbs_end_datetime - min(rlbp_start_datetime) - sum(rlbp_duration) AS ctrl_duration
        FROM emaj.emaj_rlbk_session rlbs, emaj.emaj_rlbk_plan rlbp
        WHERE rlbp_rlbk_id = rlbs_rlbk_id AND rlbp_session = rlbs_session
          AND rlbs_rlbk_id = v_rlbkID
        GROUP BY rlbs_session, rlbs_end_datetime ) AS t;
-- report duration statistics into the emaj_rlbk_stat table
    v_stmt = 'INSERT INTO emaj.emaj_rlbk_stat (rlbt_step, rlbt_schema, rlbt_table, rlbt_fkey,' ||
             '      rlbt_rlbk_id, rlbt_rlbk_datetime, rlbt_quantity, rlbt_duration)' ||
--   copy elementary steps for RLBK_TABLE, DELETE_LOG, ADD_FK and SET_FK_IMM step types
--     (record the rlbp_estimated_quantity as reference for later forecast)
             '  SELECT rlbp_step, rlbp_schema, rlbp_table, rlbp_fkey,' ||
             '      rlbp_rlbk_id, rlbk_mark_datetime, rlbp_estimated_quantity, rlbp_duration' ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''RLBK_TABLE'',''DELETE_LOG'',''ADD_FK'',''SET_FK_IMM'') ' ||
             '  UNION ALL ' ||
--   for 4 other steps, aggregate other elementary steps into a global row for each step type
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, rlbk_mark_datetime, ' ||
             '      count(*), sum(rlbp_duration)' ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''DIS_LOG_TRG'',''DROP_FK'',''SET_FK_DEF'',''ENA_LOG_TRG'') ' ||
             '    GROUP BY 1, 2, 3, 4, 5, 6' ||
             '  UNION ALL ' ||
--   and the final CTRLxDBLINK pseudo step statistic
             '  SELECT rlbp_step, '''', '''', '''', rlbp_rlbk_id, rlbk_mark_datetime, ' ||
             '      rlbp_estimated_quantity, ' || quote_literal(v_ctrlDuration) ||
             '    FROM emaj.emaj_rlbk_plan, emaj.emaj_rlbk' ||
             '    WHERE rlbk_id = rlbp_rlbk_id AND rlbp_rlbk_id = ' || v_rlbkId ||
             '      AND rlbp_step IN (''CTRL+DBLINK'',''CTRL-DBLINK'') ' ||
             ' RETURNING 1';
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
    ELSE
-- ... or directly
      EXECUTE v_stmt;
    END IF;
-- rollback the application sequences belonging to the groups
-- warning, this operation is not transaction safe (that's why it is placed at the end of the operation)!
--   get the mark timestamp and last sequence id for the 1st group
    SELECT mark_datetime, mark_last_sequence_id INTO v_timestampMark, v_lastSequenceId FROM emaj.emaj_mark
      WHERE mark_group = v_groupNames[1] AND mark_name = v_mark;
--   and rollback
    PERFORM emaj._rlbk_seq(rel_schema, rel_tblseq, v_timestampMark, v_isLoggedRlbk, v_lastSequenceId)
      FROM (SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
              WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automaticaly set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the rollback start time
    IF v_isLoggedRlbk THEN
      v_markName = 'RLBK_' || v_mark || '_' || to_char(v_rlbk_datetime, 'HH24.MI.SS.MS') || '_DONE';
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName, v_multiGroup, true);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END',
              array_to_string(v_groupNames,','),
              'Rollback_id ' || v_rlbkId || ', ' || v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed'
             ) RETURNING hist_datetime INTO v_histDateTime;
-- update the emaj_rlbk table to set the real number of tables to process, adjust the rollback status and set the result message
    IF v_isDblinkUsable THEN
-- ... either through dblink if possible
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMPLETED'', rlbk_end_datetime = ' ||
               quote_literal(v_histDateTime) || ', rlbk_msg = ''Completed: ' ||
               v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed''' ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink('rlbk#1',v_stmt) AS (dummy INT);
--     and then close the connection
      PERFORM emaj._dblink_close_cnx('rlbk#1');
    ELSE
-- ... or directly (the status can be directly set to committed, the update being in the same transaction)
      EXECUTE 'UPDATE emaj.emaj_rlbk SET rlbk_status = ''COMMITTED'', rlbk_end_datetime = ' ||
               quote_literal(v_histDateTime) || ', rlbk_msg = ''Completed: ' ||
               v_effNbTbl || ' tables and ' || v_nbSeq || ' sequences effectively processed''' ||
               ' WHERE rlbk_id = ' || v_rlbkId;
    END IF;
    RETURN v_nbSeq;
-- TODO: trap and record exception during the rollback operation when pg 8.3 will not be supported any more
--  EXCEPTION
--    WHEN SQLSTATE 'P0001' THEN             -- Do not trap the exceptions raised by the function
--      RAISE;
--    WHEN OTHERS THEN                       -- Otherwise, log the E-Maj rollback abort in emaj_rlbk, if possible
--      PERFORM emaj._rlbk_error(v_rlbkId, SQLERRM, 'rlbk#1');
--      RAISE;
  END;
$_rlbk_end$;

CREATE OR REPLACE FUNCTION emaj._rlbk_error(v_rlbkId INT, v_msg TEXT, v_cnxName TEXT)
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_error$
-- This function records a rollback error into the emaj_rlbk table, but only if a dblink connection is open
-- Input: rollback identifier, message to record and the dblink connection name to use
  DECLARE
    v_stmt                   TEXT;
    v_isDblinkUsable         BOOLEAN = false;
  BEGIN
    IF emaj._dblink_is_cnx_opened(v_cnxName) THEN
      v_stmt = 'UPDATE emaj.emaj_rlbk SET rlbk_msg = ' || quote_literal(v_msg) ||
               ' WHERE rlbk_id = ' || v_rlbkId || ' RETURNING 1';
      PERFORM 0 FROM dblink(v_cnxName,v_stmt) AS (dummy INT);
    END IF;
    RETURN;
  END;
$_rlbk_error$;

CREATE OR REPLACE FUNCTION emaj.emaj_cleanup_rollback_state()
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_cleanup_rollback_state$
-- This function examines all rollback events from the emaj_rlbk table not in "COMMITTED" or "ABORTED" state.
-- Those whose transaction(s) is/are active are left as is.
-- Among the others, those which are also visible in the emaj_hist table are set "COMMITTED",
--   while those which are not visible in the emaj_hist table are set "ABORTED".
-- Input: no parameter
-- Output: number of updated rollback events
  DECLARE
    v_nbRlbk                 INT = 0;
    v_nbVisibleTx            INT;
    v_newStatus              emaj._rlbk_status_enum;
    r_rlbk                   RECORD;
  BEGIN
-- scan all pending rollback events having all their session transactions completed (either committed or rolled back)
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session, count(rlbs_txid) AS nbVisibleTx
        FROM emaj.emaj_rlbk
             LEFT OUTER JOIN emaj.emaj_rlbk_session ON
               (    rlbk_id = rlbs_rlbk_id                                  -- main join condition
                AND txid_visible_in_snapshot(rlbs_txid,txid_current_snapshot()) -- only visible tx
                AND rlbs_txid <> txid_current()                                 -- exclude the current tx
               )
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING', 'COMPLETED')   -- only pending rollback events
        GROUP BY rlbk_id, rlbk_status, rlbk_begin_hist_id, rlbk_nb_session
        HAVING count(rlbs_txid) = rlbk_nb_session                                -- all sessions tx must be visible
        ORDER BY rlbk_id
      LOOP
-- look at the emaj_hist to find the trace of the rollback begin event
      PERFORM 0 FROM emaj.emaj_hist WHERE hist_id = r_rlbk.rlbk_begin_hist_id;
      IF FOUND THEN
-- if the emaj_hist rollback_begin event is visible, the rollback transaction has been committed.
-- then set the rollback event in emaj_rlbk as "COMMITTED"
        v_newStatus = 'COMMITTED';
      ELSE
-- otherwise, set the rollback event in emaj_rlbk as "ABORTED"
        v_newStatus = 'ABORTED';
      END IF;
      UPDATE emaj.emaj_rlbk SET rlbk_status = v_newStatus WHERE rlbk_id = r_rlbk.rlbk_id;
      INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording)
        VALUES ('CLEANUP_ROLLBACK_STATE', 'rollback id ' || r_rlbk.rlbk_id, 'set to ' || v_newStatus);
      v_nbRlbk = v_nbRlbk + 1;
    END LOOP;
    RETURN v_nbRlbk;
  END;
$emaj_cleanup_rollback_state$;
COMMENT ON FUNCTION emaj.emaj_cleanup_rollback_state() IS
$$Sets the status of pending E-Maj rollback events.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_reset_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves
-- It calls the emaj_rst_group function to do the job
-- Input: group name
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_nbTb                   INT = 0;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object)
      VALUES ('RESET_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_reset_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is not in LOGGING state
    IF v_groupIsLogging THEN
      RAISE EXCEPTION 'emaj_reset_group: Group % cannot be reset because it is in LOGGING state. An emaj_stop_group function must be previously executed.', v_groupName;
    END IF;
-- perform the reset operation
    SELECT emaj._reset_group(v_groupName) INTO v_nbTb;
    IF v_nbTb = 0 THEN
       RAISE EXCEPTION 'emaj_reset_group: Internal error (Group % is empty).', v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('RESET_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

CREATE OR REPLACE FUNCTION emaj._reset_group(v_groupName TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$_reset_group$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences saves
-- It is called by both emaj_reset_group and emaj_start_group functions
-- Input: group name
-- Output: number of processed tables
-- There is no check of the group state
-- The function is defined as SECURITY DEFINER so that an emaj_adm role can truncate log tables
  DECLARE
    v_nbTb                   INT  = 0;
    v_logTableName           TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- delete all marks for the group from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- delete all sequence holes for the tables of the group
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table;
-- then, truncate log tables
    FOR r_tblsq IN
        SELECT rel_schema, rel_tblseq, rel_log_schema, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   truncate the related log table
        v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        EXECUTE 'TRUNCATE ' || v_logTableName;
--   delete rows from emaj_sequence related to the associated log sequence
        DELETE FROM emaj.emaj_sequence
          WHERE sequ_schema = r_tblsq.rel_log_schema
            AND sequ_name = emaj._build_log_seq_name(r_tblsq.rel_schema, r_tblsq.rel_tblseq);
--   and reset the log sequence
        PERFORM setval(quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(emaj._build_log_seq_name(r_tblsq.rel_schema, r_tblsq.rel_tblseq)), 1, false);
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
        PERFORM emaj._drop_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
    RETURN v_nbTb;
  END;
$_reset_group$;

CREATE OR REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT)
RETURNS SETOF emaj.emaj_log_stat_type LANGUAGE plpgsql AS
$emaj_log_stat_group$
-- This function returns statistics on row updates executed between 2 marks or between a mark and the current situation.
-- It is used to quickly get simple statistics of updates logged between 2 marks (i.e. for one or several processing)
-- It is also used to estimate the cost of a rollback to a specified mark
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: group name, the 2 mark names defining a range
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string as last_mark indicates the current situation
--   Use a NULL or an empty string as last_mark to know the number of rows to rollback to reach the mark specified by the first_mark parameter.
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: table of log rows by table (including tables with 0 rows to rollback)
  DECLARE
    v_groupIsLogging         BOOLEAN;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_firstLastSeqHoleId     BIGINT;
    v_lastLastSeqHoleId      BIGINT;
    v_fullSeqName            TEXT;
    v_beginLastValue         BIGINT;
    v_endLastValue           BIGINT;
    v_sumHole                BIGINT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_log_stat_group: group % has not been created.', v_groupName;
    END IF;
-- if first mark is NULL or empty, retrieve the name, timestamp and last sequ_hole id of the first recorded mark for the group
    IF v_firstMark IS NULL OR v_firstMark = '' THEN
--   if no mark exists for the group (just after emaj_create_group() or emaj_reset_group() functions call),
--     v_realFirstMark remains NULL
      SELECT mark_id, mark_name, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_realFirstMark, v_tsFirstMark, v_firstLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    ELSE
-- else, check and retrieve the name, timestamp and last sequ_hole id of the supplied first mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_tsFirstMark, v_firstLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- if a last mark name is supplied, check and retrieve the name, timestamp and last sequ_hole id of the supplied end mark for the group
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime, mark_last_seq_hole_id INTO v_lastMarkId, v_tsLastMark, v_lastLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
-- if last mark is null or empty, v_realLastMark, v_lastMarkId, v_tsLastMark and v_lastLastSeqHoleId remain NULL
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_log_stat_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_firstMark, v_firstMarkId, v_tsFirstMark, v_lastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table, get the number of log rows and return the statistic
    RETURN QUERY
      SELECT v_groupName, rel_schema, rel_tblseq,
             CASE WHEN v_tsFirstMark IS NULL THEN 0 ELSE emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_tsFirstMark, v_tsLastMark, v_firstLastSeqHoleId, v_lastLastSeqHoleId) END AS nb_rows
        FROM emaj.emaj_relation
        WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_priority, rel_schema, rel_tblseq;
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
    v_groupIsLogging         BOOLEAN;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_logTableName           TEXT;
    v_stmt                   TEXT;
    r_tblsq                  RECORD;
    r_stat                   RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_is_logging INTO v_groupIsLogging
      FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: group % has not been created.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the global sequence value and the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_firstMarkId, v_firstEmajGid, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the global sequence value and the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_detailed_log_stat_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_lastMarkId, v_lastEmajGid, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the log table name and its sequence name for this table
        v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- prepare and execute the statement
        v_stmt= 'SELECT ' || quote_literal(v_groupName) || '::TEXT as emaj_group,'
             || ' ' || quote_literal(r_tblsq.rel_schema) || '::TEXT as emaj_schema,'
             || ' ' || quote_literal(r_tblsq.rel_tblseq) || '::TEXT as emaj_table,'
             || ' emaj_user,'
             || ' CASE WHEN emaj_verb = ''INS'' THEN ''INSERT'''
             ||      ' WHEN emaj_verb = ''UPD'' THEN ''UPDATE'''
             ||      ' WHEN emaj_verb = ''DEL'' THEN ''DELETE'''
             ||      ' ELSE ''?'' END::VARCHAR(6) as emaj_verb,'
             || ' count(*) as emaj_rows'
             || ' FROM ' || v_logTableName
             || ' WHERE NOT (emaj_verb = ''UPD'' AND emaj_tuple = ''OLD'')';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN v_stmt = v_stmt
             || ' AND emaj_gid > '|| v_firstEmajGid ;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN v_stmt = v_stmt
             || ' AND emaj_gid <= '|| v_lastEmajGid ;
        END IF;
        v_stmt = v_stmt
             || ' GROUP BY emaj_group, emaj_schema, emaj_table, emaj_user, emaj_verb'
             || ' ORDER BY emaj_user, emaj_verb';
        FOR r_stat IN EXECUTE v_stmt LOOP
          RETURN NEXT r_stat;
        END LOOP;
      END IF;
    END LOOP;
    RETURN;
  END;
$emaj_detailed_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_detailed_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns detailed statistics about logged events for an E-Maj group between 2 marks.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_estimate_rollback_group(v_groupName TEXT, v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql AS
$emaj_estimate_rollback_group$
-- This function computes an approximate duration of a rollback to a predefined mark for a group.
-- It uses the _estimate_rollback_group() function to effectively compute this estimate
-- Input: group name, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval
  DECLARE
  BEGIN
    RETURN emaj._estimate_rollback_groups(ARRAY[v_groupName], v_mark, v_isLoggedRlbk);
  END;
$emaj_estimate_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_group(TEXT,TEXT,BOOLEAN) IS
$$Estimates the duration of a potential rollback for a tables group to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql AS
$emaj_estimate_rollback_groups$
-- This function computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It uses the _estimate_rollback_group() function to effectively compute this estimate
-- Input: group names array, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval
  DECLARE
  BEGIN
    RETURN emaj._estimate_rollback_groups(v_groupNames, v_mark, v_isLoggedRlbk);
  END;
$emaj_estimate_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_groups(TEXT[],TEXT,BOOLEAN) IS
$$Estimates the duration of a potential rollback for a set of tables groups to a given mark.$$;

CREATE OR REPLACE FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN)
RETURNS INTERVAL LANGUAGE plpgsql SECURITY DEFINER AS
$_estimate_rollback_groups$
-- This function effectively computes an approximate duration of a rollback to a predefined mark for a groups array.
-- It simulates a rollback on 1 session, by calling the _rlbk_planning function that already estimates elementary
-- rollback steps duration. Once the global estimate is got, the rollback planning is cancelled.
-- Input: a group names array, the mark name of the rollback operation, the rollback type.
-- Output: the approximate duration that the rollback would need as time interval.
-- The function is declared SECURITY DEFINER so that emaj_viewer doesn't need a specific INSERT permission on emaj_rlbk.
  DECLARE
    v_markName               TEXT;
    v_fixed_table_rlbk       INTERVAL;
    v_rlbkId                 INT;
    v_estimDuration          INTERVAL;
    v_nbTblseq               INT;
  BEGIN
-- check supplied group names and mark parameters
    SELECT emaj._rlbk_check(v_groupNames,v_mark) INTO v_markName;
-- compute a random negative rollback-id (not to interfere with ids of real rollbacks)
    SELECT (random() * -2147483648)::int INTO v_rlbkId;
--
-- simulate a rollback planning
--
    BEGIN
-- insert a row into the emaj_rlbk table for this simulated rollback operation
      INSERT INTO emaj.emaj_rlbk (rlbk_id, rlbk_groups, rlbk_mark, rlbk_is_logged, rlbk_nb_session)
        VALUES (v_rlbkId, v_groupNames, v_mark, v_isLoggedRlbk, 1);
-- call the _rlbk_planning function
      PERFORM emaj._rlbk_planning(v_rlbkId);
-- compute the sum of the duration estimates of all elementary steps (except LOCK_TABLE)
      SELECT coalesce(sum(rlbp_estimated_duration), '0 SECONDS'::INTERVAL) INTO v_estimDuration
        FROM emaj.emaj_rlbk_plan
        WHERE rlbp_rlbk_id = v_rlbkId AND rlbp_step <> 'LOCK_TABLE';
-- cancel the effect of the rollback planning
      RAISE EXCEPTION '';
    EXCEPTION
      WHEN RAISE_EXCEPTION THEN                 -- catch the raised exception and continue
    END;
-- get the "fixed_table_rollback_duration" parameter from the emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param
                        WHERE param_key = 'fixed_table_rollback_duration'),'1 millisecond'::interval)
           INTO v_fixed_table_rlbk;
-- get the the number of tables to lock and sequences to rollback
    SELECT sum(group_nb_table)+sum(group_nb_sequence) INTO v_nbTblseq
      FROM emaj.emaj_group
      WHERE group_name = ANY(v_groupNames);
-- compute the final estimated duration
    v_estimDuration = v_estimDuration + (v_nbTblseq * v_fixed_table_rlbk);
    RETURN v_estimDuration;
  END;
$_estimate_rollback_groups$;

CREATE OR REPLACE FUNCTION emaj.emaj_rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$emaj_rollback_activity$
-- This function returns the list of rollback operations currently in execution, with information about their progress
-- It doesn't need input parameter.
-- It returns a set of emaj_rollback_activity_type records.
  BEGIN
-- cleanup the freshly completed rollback operations, if any
    PERFORM emaj.emaj_cleanup_rollback_state();
-- and retrieve information regarding the rollback operations that are always in execution
    RETURN QUERY SELECT * FROM emaj._rollback_activity();
  END;
$emaj_rollback_activity$;
COMMENT ON FUNCTION emaj.emaj_rollback_activity() IS
$$Returns the list of rollback operations currently in execution, with information about their progress.$$;

CREATE OR REPLACE FUNCTION emaj._rollback_activity()
RETURNS SETOF emaj.emaj_rollback_activity_type LANGUAGE plpgsql AS
$_rollback_activity$
-- This function effectively builds the list of rollback operations currently in execution.
-- It is called by the emaj_rollback_activity() function.
-- This is a separate function to help in testing the feature (avoiding the effects of emaj_cleanup_rollback_state()).
-- The number of parallel rollback sessions is not taken into account here,
--   as it is difficult to estimate the benefit brought by several parallel sessions.
-- The times and progression indicators reported are based on the transaction timestamp.
  DECLARE
    v_ipsDuration            INTERVAL;
    v_nyssDuration           INTERVAL;
    v_nbNyss                 INT;
    v_ctrlDuration           INTERVAL;
    v_currentTotalEstimate   INTERVAL;
    r_rlbk                   emaj.emaj_rollback_activity_type;
  BEGIN
-- retrieve all not completed rollback operations (ie in 'PLANNING', 'LOCKING' or 'EXECUTING' state)
    FOR r_rlbk IN
      SELECT rlbk_id, rlbk_groups, rlbk_mark, rlbk_mark_datetime, rlbk_is_logged, rlbk_nb_session,
             rlbk_nb_table, rlbk_nb_sequence, rlbk_eff_nb_table, rlbk_status, rlbk_start_datetime,
             transaction_timestamp() - rlbk_start_datetime AS "elapse", NULL, 0
        FROM emaj.emaj_rlbk
        WHERE rlbk_status IN ('PLANNING', 'LOCKING', 'EXECUTING')
        ORDER BY rlbk_id
        LOOP
-- compute the estimated remaining duration
--   for rollback operations in 'PLANNING' state, the remaining duration is NULL
      IF r_rlbk.rlbk_status IN ('LOCKING', 'EXECUTING') THEN
--     estimated duration of remaining work of in progress steps
        SELECT coalesce(
               sum(CASE WHEN rlbp_start_datetime + rlbp_estimated_duration - transaction_timestamp() > '0'::interval
                        THEN rlbp_start_datetime + rlbp_estimated_duration - transaction_timestamp()
                        ELSE '0'::interval END),'0'::interval) INTO v_ipsDuration
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_start_datetime IS NOT NULL AND rlbp_duration IS NULL;
--     estimated duration and number of not yet started steps
        SELECT coalesce(sum(rlbp_estimated_duration),'0'::interval), count(*) INTO v_nyssDuration, v_nbNyss
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_start_datetime IS NULL
           AND rlbp_step NOT IN ('CTRL-DBLINK','CTRL+DBLINK');
--     estimated duration of inter-step duration for not yet started steps
        SELECT coalesce(sum(rlbp_estimated_duration) * v_nbNyss / sum(rlbp_estimated_quantity),'0'::interval)
          INTO v_ctrlDuration
          FROM emaj.emaj_rlbk_plan WHERE rlbp_rlbk_id = r_rlbk.rlbk_id
           AND rlbp_step IN ('CTRL-DBLINK','CTRL+DBLINK');
--     update the global remaining duration estimate
        r_rlbk.rlbk_remaining = v_ipsDuration + v_nyssDuration + v_ctrlDuration;
      END IF;
-- compute the completion pct
--   for rollback operations in 'PLANNING' or 'LOCKING' state, the completion_pct = 0
      IF r_rlbk.rlbk_status = 'EXECUTING' THEN
--   first compute the new total duration estimate, using the estimate of the remaining work
        SELECT transaction_timestamp() - rlbk_start_datetime + r_rlbk.rlbk_remaining
          INTO v_currentTotalEstimate
          FROM emaj.emaj_rlbk WHERE rlbk_id = r_rlbk.rlbk_id;
--   and then the completion pct
        IF v_currentTotalEstimate <> '0'::interval THEN
          SELECT 100 - (extract(epoch FROM r_rlbk.rlbk_remaining) * 100
                      / extract(epoch FROM v_currentTotalEstimate))::smallint
            INTO r_rlbk.rlbk_completion_pct;
        END IF;
      END IF;
      RETURN NEXT r_rlbk;
    END LOOP;
    RETURN;
  END;
$_rollback_activity$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
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
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before
-- emaj_snap_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name, the absolute pathname of the directory where the files are to be created and the options to used in the COPY TO statements
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_fullTableName          TEXT;
    r_col                    RECORD;
    v_colList                TEXT;
    v_fileName               TEXT;
    v_stmt                   TEXT;
    v_seqCol                 TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: group % has not been created.', v_groupName;
    END IF;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_group: directory parameter cannot be NULL';
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%' THEN
      RAISE EXCEPTION 'emaj_snap_group: invalid COPY options parameter format';
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   first build the order by column list
        v_colList = '';
        PERFORM 0 FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pg_constraint
          WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
                contype = 'p' AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
        IF FOUND THEN
--   the table has a pkey,
          FOR r_col IN
              SELECT attname FROM pg_catalog.pg_attribute, pg_catalog.pg_index
                WHERE pg_attribute.attrelid = pg_index.indrelid
                  AND attnum = ANY (indkey)
                  AND indrelid = v_fullTableName::regclass AND indisprimary
                  AND attnum > 0 AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList = quote_ident(r_col.attname);
            ELSE
               v_colList = v_colList || ',' || quote_ident(r_col.attname);
            END IF;
          END LOOP;
        ELSE
--   the table has no pkey
          FOR r_col IN
              SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList = quote_ident(r_col.attname);
            ELSE
               v_colList = v_colList || ',' || quote_ident(r_col.attname);
            END IF;
          END LOOP;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO '
                || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, the statement has no order by
        IF v_pgVersion <= '8.3' THEN
          v_seqCol = 'sequence_name, last_value, 0, increment_by, max_value, min_value, cache_value, is_cycled, is_called';
        ELSE
          v_seqCol = 'sequence_name, last_value, start_value, increment_by, max_value, min_value, cache_value, is_cycled, is_called';
        END IF;
        v_stmt= 'COPY (SELECT ' || v_seqCol || ' FROM ' || v_fullTableName || ') TO '
                || quote_literal(v_fileName) || ' ' || coalesce (v_copyOptions, '');
      END IF;
-- and finaly perform the COPY
--    raise notice 'emaj_snap_group: Executing %',v_stmt;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj snap of tables group ' || v_groupName ||
            ' at ' || transaction_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark
-- For log tables, files contain all rows related to the time frame, sorted on emaj_gid.
-- For sequences, files are names <group>_sequences_at_<mark>, or <group>_sequences_at_<time> if no
--   end mark is specified. They contain one row per sequence.
-- To do its job, the function performs COPY TO statement, using the options provided by the caller.
-- There is no need for the group not to be logging.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before
-- emaj_snap_log_group function call, and
--   - maintain its content outside E-maj.
-- Input: group name, the 2 mark names defining a range, the absolute pathname of the directory where the files are to be created, options for COPY TO statements
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string can be used as last_mark indicating the current state
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_nbTb                   INT = 0;
    r_tblsq                  RECORD;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_logTableName           TEXT;
    v_fileName               TEXT;
    v_stmt                   TEXT;
    v_timestamp              TIMESTAMPTZ;
    v_pseudoMark             TEXT;
    v_fullSeqName            TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'BEGIN', v_groupName,
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_log_group: group % has not been created.', v_groupName;
    END IF;
-- check the copy options parameter doesn't contain unquoted ; that could be used for sql injection
    IF regexp_replace(v_copyOptions,'''.*''','') LIKE '%;%'  THEN
      RAISE EXCEPTION 'emaj_snap_log_group: invalid COPY options parameter format';
    END IF;
-- catch the global sequence value and the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the global sequence value and the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_snap_log_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_firstMarkId, v_firstEmajGid, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    ELSE
      SELECT mark_name, mark_id, mark_global_seq, mark_datetime INTO v_realFirstMark, v_firstMarkId, v_firstEmajGid, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    END IF;
-- catch the global sequence value and timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the global sequence value and the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_snap_log_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_global_seq, mark_datetime INTO v_lastMarkId, v_lastEmajGid, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    ELSE
      v_lastMarkId = NULL;
      v_lastEmajGid = NULL;
      v_tsLastMark = NULL;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_snap_log_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- check the supplied directory is not null
    IF v_dir IS NULL THEN
      RAISE EXCEPTION 'emaj_snap_log_group: directory parameter cannot be NULL';
    END IF;
-- process all log tables of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema, rel_kind FROM emaj.emaj_relation
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- process tables
--   build names
        v_fileName     = v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log.snap';
        v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE TRUE';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
          v_stmt = v_stmt || ' AND emaj_gid > '|| v_firstEmajGid;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
          v_stmt = v_stmt || ' AND emaj_gid <= '|| v_lastEmajGid;
        END IF;
        v_stmt = v_stmt || ' ORDER BY emaj_gid ASC) TO ' || quote_literal(v_fileName) || ' '
                        || coalesce (v_copyOptions, '');
-- and finaly perform the COPY
        EXECUTE v_stmt;
      END IF;
-- for sequences, just adjust the counter
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realFirstMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realFirstMark) || ' AND ' ||
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
            coalesce (v_copyOptions, '');
    EXECUTE v_stmt;
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- generate the file for sequences state at end mark, if specified
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || v_realLastMark;
      v_stmt= 'COPY (SELECT emaj_sequence.*' ||
              ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_realLastMark) || ' AND ' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
              ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
              coalesce (v_copyOptions, '');
      EXECUTE v_stmt;
    ELSE
-- generate the file for sequences in their current state, if no end_mark is specified,
--   by using emaj_sequence table to create temporary rows as if a mark had been set
-- look at the clock to get the 'official' timestamp representing this point in time
--   and build a pseudo mark name with it
      v_timestamp = clock_timestamp();
      v_pseudoMark = to_char(v_timestamp,'HH24.MI.SS.MS');
-- for each sequence of the groups, ...
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
            WHERE rel_group = v_groupName AND rel_kind = 'S' ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- ... temporary record the sequence parameters in the emaj sequence table
        v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' ||
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT ' || quote_literal(r_tblsq.rel_schema) || ', ' ||
                 quote_literal(r_tblsq.rel_tblseq) || ', ' || quote_literal(v_timestamp) ||
                 ', ' || quote_literal(v_pseudoMark) || ', last_value, ';
        IF v_pgVersion <= '8.3' THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt ||
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
        EXECUTE v_stmt;
      END LOOP;
-- generate the file for sequences current state
      v_fileName = v_dir || '/' || v_groupName || '_sequences_at_' || to_char(v_timestamp,'HH24.MI.SS.MS');
      v_stmt= 'COPY (SELECT emaj_sequence.*' ||
              ' FROM emaj.emaj_sequence, emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_pseudoMark) || ' AND ' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
              ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' ' ||
              coalesce (v_copyOptions, '');
      EXECUTE v_stmt;
-- delete sequences state that have just been inserted into the emaj_sequence table.
      EXECUTE 'DELETE FROM emaj.emaj_sequence USING emaj.emaj_relation' ||
              ' WHERE sequ_mark = ' || quote_literal(v_pseudoMark) || ' AND' ||
              ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
              ' sequ_schema = rel_schema AND sequ_name = rel_tblseq';
    END IF;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' ||
            quote_literal('E-Maj log tables snap of group ' || v_groupName ||
            ' between marks ' || v_realFirstMark || ' and ' ||
            coalesce(v_realLastMark,'current state') || ' at ' || transaction_timestamp()) ||
            ') TO ' || quote_literal(v_dir || '/_INFO') || ' ' || coalesce (v_copyOptions, '');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$emaj_gen_sql_group$
-- This function generates a SQL script representing all updates performed on a tables group between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables group
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_cumNbSQL              INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUP', 'BEGIN', v_groupName,
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_location);
-- call the _gen_sql_groups() function that effectively processes the request
    SELECT emaj._gen_sql_groups(array[v_groupName], v_firstMark, v_lastMark, v_location, NULL) INTO v_cumNbSQL;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUP', 'END', v_groupName, v_cumNbSQL || ' generated statements');
    RETURN v_cumNbSQL;
  END;
$emaj_gen_sql_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) IS
$$Generates a sql script corresponding to all updates performed on a tables group between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$emaj_gen_sql_group$
-- This function generates a SQL script representing all updates performed on a tables group between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables group
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - array of schema qualified table and sequence names to only process those tables and sequences
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_cumNbSQL              INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUP', 'BEGIN', v_groupName,
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_location || ' with tables/sequences filtering');
-- call the _gen_sql_groups() function that effectively processes the request
    SELECT emaj._gen_sql_groups(array[v_groupName], v_firstMark, v_lastMark, v_location, emaj._check_names_array(v_tblseqs,'table/sequence')) INTO v_cumNbSQL;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUP', 'END', v_groupName, v_cumNbSQL || ' generated statements');
    RETURN v_cumNbSQL;
  END;
$emaj_gen_sql_group$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a tables group between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$emaj_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a set of tables groups between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables groups array
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_cumNbSQL               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUPS', 'BEGIN', array_to_string(v_groupNames,','),
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_location);
-- call the _gen_sql_groups() function that effectively processes the request
    SELECT emaj._gen_sql_groups(emaj._check_names_array(v_groupNames,'group'), v_firstMark, v_lastMark, v_location, NULL)
      INTO v_cumNbSQL;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUPS', 'END', array_to_string(v_groupNames,','), v_cumNbSQL || ' generated statements');
    RETURN v_cumNbSQL;
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) IS
$$Generates a sql script corresponding to all updates performed on a set of tables groups between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$emaj_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a set of tables groups between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- It call the _gen_sql_groups() function to effetively process the request
-- Input: - tables groups array
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - array of schema qualified table and sequence names to only process those tables and sequences
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_cumNbSQL               INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUPS', 'BEGIN', array_to_string(v_groupNames,','),
       CASE WHEN v_firstMark IS NULL OR v_firstMark = '' THEN 'From initial mark' ELSE 'From mark ' || v_firstMark END ||
       CASE WHEN v_lastMark IS NULL OR v_lastMark = '' THEN ' to current situation' ELSE ' to mark ' || v_lastMark END || ' towards '
       || v_location || ' with tables/sequences filtering');
-- call the _gen_sql_groups() function that effectively processes the request
    SELECT emaj._gen_sql_groups(emaj._check_names_array(v_groupNames,'group'), v_firstMark, v_lastMark, v_location, emaj._check_names_array(v_tblseqs,'table/sequence'))
      INTO v_cumNbSQL;
-- insert end in the history and return
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('GEN_SQL_GROUPS', 'END', array_to_string(v_groupNames,','), v_cumNbSQL || ' generated statements');
    RETURN v_cumNbSQL;
  END;
$emaj_gen_sql_groups$;
COMMENT ON FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) IS
$$Generates a sql script corresponding to all updates performed on a set of tables groups between two marks and stores it into a given file.$$;

CREATE OR REPLACE FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[])
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER SET standard_conforming_strings = ON AS
$_gen_sql_groups$
-- This function generates a SQL script representing all updates performed on a tables groups array between 2 marks
-- or beetween a mark and the current situation. The result is stored into an external file.
-- The function can process groups that are in LOGGING state or not.
-- The sql statements are placed between a BEGIN TRANSACTION and a COMMIT statements.
-- The output file can be reused as input file to a psql command to replay the updates scenario. Just '\\'
-- character strings (double antislash), if any, must be replaced by '\' (single antislash) before feeding
-- the psql command.
-- Input: - tables groups array
--        - start mark, NULL representing the first mark
--        - end mark, NULL representing the current situation, and 'EMAJ_LAST_MARK' the last set mark for the group
--        - absolute pathname describing the file that will hold the result
--        - optional array of schema qualified table and sequence names to only process those tables and sequences
-- Output: number of generated SQL statements (non counting comments and transaction management)
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_groupIsLogging         BOOLEAN;
    v_cpt                    INT;
    v_firstMarkCopy          TEXT = v_firstMark;
    v_realFirstMark          TEXT;
    v_realLastMark           TEXT;
    v_firstMarkId            BIGINT;
    v_lastMarkId             BIGINT;
    v_firstEmajGid           BIGINT;
    v_lastEmajGid            BIGINT;
    v_tsFirstMark            TIMESTAMPTZ;
    v_tsLastMark             TIMESTAMPTZ;
    v_firstLastSeqHoleId     BIGINT;
    v_lastLastSeqHoleId      BIGINT;
    v_tblseqErr              TEXT;
    v_nbSQL                  INT;
    v_nbSeq                  INT;
    v_cumNbSQL               INT = 0;
    v_fullTableName          TEXT;
    v_logTableName           TEXT;
    v_fullSeqName            TEXT;
    v_endComment             TEXT;
    v_conditions             TEXT;
    v_rqSeq                  TEXT;
    r_tblsq                  RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group ...
    FOR v_i IN 1 .. array_upper(v_groupNames,1) LOOP
-- ...is recorded into the emaj_group table
      SELECT group_is_logging INTO v_groupIsLogging
        FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i];
      IF NOT FOUND THEN
        RAISE EXCEPTION '_gen_sql_groups: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... has no tables without pkey
      SELECT count(*) INTO v_cpt FROM pg_catalog.pg_class, pg_catalog.pg_namespace, emaj.emaj_relation
        WHERE relnamespace = pg_namespace.oid
          AND nspname = rel_schema AND relname =  rel_tblseq
          AND rel_group = v_groupNames[v_i] AND rel_kind = 'r'
          AND relhaspkey = false;
      IF v_cpt > 0 THEN
        RAISE EXCEPTION '_gen_sql_groups: Tables group % contains % tables without pkey.', v_groupNames[v_i], v_cpt;
      END IF;
-- If the first mark supplied is NULL or empty, get the first mark for the current processed group
--   (in fact the first one) and override the supplied first mark
      IF v_firstMarkCopy IS NULL OR v_firstMarkCopy = '' THEN
        SELECT mark_name INTO v_firstMarkCopy
          FROM emaj.emaj_mark WHERE mark_group = v_groupNames[v_i] ORDER BY mark_id LIMIT 1;
        IF NOT FOUND THEN
           RAISE EXCEPTION '_gen_sql_groups: No initial mark can be found for group %.', v_groupNames[v_i];
        END IF;
      END IF;
-- ... owns the requested first mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_firstMarkCopy) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION '_gen_sql_groups: No mark % exists for group %.', v_firstMarkCopy, v_groupNames[v_i];
      END IF;
-- ... and owns the requested last mark, if supplied
      IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
        SELECT emaj._get_mark_name(v_groupNames[v_i],v_lastMark) INTO v_realLastMark;
        IF v_realLastMark IS NULL THEN
          RAISE EXCEPTION '_gen_sql_groups: No mark % exists for group %.', v_lastMark, v_groupNames[v_i];
        END IF;
      END IF;
    END LOOP;
-- check that the first mark timestamp is the same for all groups of the array
    SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_firstMarkCopy)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_gen_sql_groups: Mark % does not represent the same point in time for all groups.', v_firstMarkCopy;
    END IF;
-- check that the last mark timestamp, if supplied, is the same for all groups of the array
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
      SELECT count(DISTINCT emaj._get_mark_datetime(group_name,v_lastMark)) INTO v_cpt FROM emaj.emaj_group
        WHERE group_name = ANY (v_groupNames);
      IF v_cpt > 1 THEN
        RAISE EXCEPTION '_gen_sql_groups: Mark % does not represent the same point in time for all groups.', v_lastMark;
      END IF;
    END IF;
-- retrieve the name, the global sequence value and the timestamp of the supplied first mark for the 1st group
--   (the global sequence value and the timestamp are the same for all groups of the array
--    and the mark ids are just used to check that first mark is prior the last mark)
    SELECT mark_id, mark_global_seq, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_firstEmajGid, v_tsFirstMark, v_firstLastSeqHoleId
      FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_realFirstMark;
-- if last mark is NULL or empty, there is no timestamp to register
    IF v_lastMark IS NULL OR v_lastMark = '' THEN
      v_lastMarkId = NULL;
      v_lastEmajGid = NULL;
      v_tsLastMark = NULL;
      v_lastLastSeqHoleId = NULL;
    ELSE
-- else, retrieve the name, timestamp and last sequ_hole id of the supplied end mark for the 1st group
      SELECT mark_id, mark_global_seq, mark_datetime, mark_last_seq_hole_id INTO v_lastMarkId, v_lastEmajGid, v_tsLastMark, v_lastLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupNames[1] AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION '_gen_sql_groups: mark id for % (% = %) is greater than mark id for % (% = %).', v_firstMarkCopy, v_firstMarkId, v_tsFirstMark, v_lastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- check the array of tables and sequences to filter, if supplied.
-- each table/sequence of the filter must be known in emaj_relation and be owned by one of the supplied table groups
    IF v_tblseqs IS NOT NULL THEN
      IF v_tblseqs = array[''] THEN
        RAISE EXCEPTION '_gen_sql_groups: filtered table/sequence names array cannot be empty.';
      END IF;
      v_tblseqErr = '';
      FOR r_tblsq IN
        SELECT t FROM regexp_split_to_table(array_to_string(v_tblseqs,'?'),E'\\?') AS t
          EXCEPT
        SELECT rel_schema || '.' || rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames)
-- TODO regexp_split_to_table(array_to_string) to be transformed into unnest() when pg 8.3 will not be supported any more
        LOOP
        v_tblseqErr = v_tblseqErr || r_tblsq.t || ', ';
      END LOOP;
      IF v_tblseqErr <> '' THEN
        v_tblseqErr = substring(v_tblseqErr FROM 1 FOR char_length(v_tblseqErr) - 2);
        RAISE EXCEPTION '_gen_sql_groups: some tables and/or sequences (%) do not belong to any of the selected tables groups.', v_tblseqErr;
      END IF;
    END IF;
-- test the supplied output file name by inserting a temporary line (trap NULL or bad file name)
    BEGIN
      EXECUTE 'COPY (SELECT ''-- _gen_sql_groups() function in progress - started at '
                     || statement_timestamp() || ''') TO ' || quote_literal(v_location);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION '_gen_sql_groups: file % cannot be used as script output file.', v_location;
    END;
-- create temporary table
    DROP TABLE IF EXISTS emaj_temp_script;
    CREATE TEMP TABLE emaj_temp_script (
      scr_emaj_gid           BIGINT,              -- the emaj_gid of the corresponding log row,
                                                  --   0 for initial technical statements,
                                                  --   NULL for final technical statements
      scr_subid              INT,                 -- used to distinguish several generated sql per log row
      scr_emaj_txid          BIGINT,              -- for future use, to insert commit statement at each txid change
      scr_sql                TEXT                 -- the generated sql text
    );
-- for each application table referenced in the emaj_relation table, build SQL statements and process the related log table
-- build the restriction conditions on emaj_gid, depending on supplied mark range (the same for all tables)
    v_conditions = 'o.emaj_gid > ' || v_firstEmajGid;
    IF v_tsLastMark IS NOT NULL THEN
      v_conditions = v_conditions || ' AND o.emaj_gid <= ' || v_lastEmajGid;
    END IF;
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_log_schema FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r'                        -- tables of the groups
            AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs)) -- filtered or not by the user
                                                                               -- only tables having updates to process
            AND emaj._log_stat_tbl(rel_schema, rel_tblseq, rel_log_schema, v_tsFirstMark,
                                   v_tsLastMark, v_firstLastSeqHoleId, v_lastLastSeqHoleId) > 0
          ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fullTableName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      v_logTableName = quote_ident(r_tblsq.rel_log_schema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- process the application table, by calling the _gen_sql_tbl function
      SELECT emaj._gen_sql_tbl(v_fullTableName, v_logTableName, v_conditions) INTO v_nbSQL;
      v_cumNbSQL = v_cumNbSQL + v_nbSQL;
    END LOOP;
-- process sequences
    v_nbSeq = 0;
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation
          WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S'                        -- sequences of the groups
            AND (v_tblseqs IS NULL OR rel_schema || '.' || rel_tblseq = ANY (v_tblseqs)) -- filtered or not by the user
          ORDER BY rel_priority DESC, rel_schema DESC, rel_tblseq DESC
        LOOP
      v_fullSeqName = quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF v_tsLastMark IS NULL THEN
-- no supplied last mark, so get current sequence characteritics
        IF v_pgVersion <= '8.3' THEN
-- .. in pg 8.3-
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM ' || v_fullSeqName INTO v_rqSeq;
--raise notice '1 - sequence % -> %',v_fullSeqName,v_rqSeq;
        ELSE
-- .. in pg 8.4+
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN is_called THEN last_value + increment_by ELSE last_value END || '' START '' || start_value || '' INCREMENT '' || increment_by  || '' MAXVALUE '' || max_value  || '' MINVALUE '' || min_value || '' CACHE '' || cache_value || CASE WHEN NOT is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM ' || v_fullSeqName INTO v_rqSeq;
--raise notice '2 - sequence % -> %',v_fullSeqName,v_rqSeq;
        END IF;
      ELSE
-- a last mark is supplied, so get sequence characteristics from emaj_sequence table
        IF v_pgVersion <= '8.3' THEN
-- .. in pg 8.3-
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_datetime = ''' || v_tsLastMark || '''' INTO v_rqSeq;
--raise notice '3 - sequence % -> %',v_fullSeqName,v_rqSeq;
        ELSE
-- .. in pg 8.4+
          EXECUTE 'SELECT ''ALTER SEQUENCE ' || replace(v_fullSeqName,'''','''''')
               || ''' || '' RESTART '' || CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END || '' START '' || sequ_start_val || '' INCREMENT '' || sequ_increment  || '' MAXVALUE '' || sequ_max_val  || '' MINVALUE '' || sequ_min_val || '' CACHE '' || sequ_cache_val || CASE WHEN NOT sequ_is_cycled THEN '' NO'' ELSE '''' END || '' CYCLE;'' '
               || 'FROM emaj.emaj_sequence '
               || 'WHERE sequ_schema = ' || quote_literal(r_tblsq.rel_schema)
               || '  AND sequ_name = ' || quote_literal(r_tblsq.rel_tblseq)
               || '  AND sequ_datetime = ''' || v_tsLastMark || '''' INTO v_rqSeq;
--raise notice '4 - sequence % -> %',v_fullSeqName,v_rqSeq;
        END IF;
      END IF;
-- insert into temp table
      v_nbSeq = v_nbSeq + 1;
      EXECUTE 'INSERT INTO emaj_temp_script '
           || 'SELECT NULL, -1 * ' || v_nbSeq || ', txid_current(), ' || quote_literal(v_rqSeq);
    END LOOP;
-- add initial comments
    IF v_tsLastMark IS NOT NULL THEN
      v_endComment = ' and mark ' || v_realLastMark;
    ELSE
      v_endComment = ' and the current situation';
    END IF;
    INSERT INTO emaj_temp_script SELECT 0, 1, 0, '-- SQL script generated by E-Maj at ' || statement_timestamp();
    INSERT INTO emaj_temp_script SELECT 0, 2, 0, '--    for tables group(s): ' || array_to_string(v_groupNames,',');
    INSERT INTO emaj_temp_script SELECT 0, 3, 0, '--    processing logs between mark ' || v_realFirstMark || v_endComment;
    IF v_tblseqs IS NOT NULL THEN
      INSERT INTO emaj_temp_script SELECT 0, 4, 0, '--    only for the following tables/sequences: ' || array_to_string(v_tblseqs,',');
    END IF;
-- encapsulate the sql statements inside a TRANSACTION
-- and manage the standard_conforming_strings option to properly handle special characters
    INSERT INTO emaj_temp_script SELECT 0, 10, 0, 'SET standard_conforming_strings = ON;';
    INSERT INTO emaj_temp_script SELECT 0, 11, 0, 'BEGIN TRANSACTION;';
    INSERT INTO emaj_temp_script SELECT NULL, 1, txid_current(), 'COMMIT;';
    INSERT INTO emaj_temp_script SELECT NULL, 2, txid_current(), 'RESET standard_conforming_strings;';
-- write the SQL script on the external file
    EXECUTE 'COPY (SELECT scr_sql FROM emaj_temp_script ORDER BY scr_emaj_gid NULLS LAST, scr_subid ) TO '
          || quote_literal(v_location);
-- drop temporary table ?
--    DROP TABLE IF EXISTS emaj_temp_script;
-- return the number of sql verbs generated into the output file
    v_cumNbSQL = v_cumNbSQL + v_nbSeq;
    RETURN v_cumNbSQL;
  END;
$_gen_sql_groups$;

------------------------------------
--                                --
-- Global purpose functions       --
--                                --
------------------------------------

CREATE OR REPLACE FUNCTION emaj._verify_all_groups()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_groups$
-- The function verifies the consistency of all E-Maj groups.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_emajSchema             TEXT = 'emaj';
    r_object                 RECORD;
  BEGIN
-- check the postgres version at creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric.
--          But we suppose these tests will not be useful any more when pg 10.0 will appear!
--   for 8.3, both major versions must be the same
    RETURN QUERY
      SELECT 'The group "' || group_name || '" has been created with a non compatible postgresql version (' ||
               group_pg_version || '). It must be dropped and recreated.' AS msg
        FROM emaj.emaj_group
        WHERE ((v_pgVersion = '8.3' AND substring (group_pg_version FROM E'(\\d+\\.\\d+)') <> v_pgVersion)
--   for 8.4+, both major versions must be 8.4+
            OR (v_pgVersion >= '8.4' AND substring (group_pg_version FROM E'(\\d+\\.\\d+)') < '8.4'))
        ORDER BY msg;
-- check all application schemas referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'The application schema "' || rel_schema || '" does not exist any more.' AS msg
        FROM (
          SELECT DISTINCT rel_schema FROM emaj.emaj_relation
            EXCEPT
          SELECT nspname FROM pg_catalog.pg_namespace
             ) AS t
        ORDER BY msg;
-- check all application relations referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'In group "' || r.rel_group || '", the ' ||
               CASE WHEN t.rel_kind = 'r' THEN 'table "' ELSE 'sequence "' END ||
               t.rel_schema || '"."' || t.rel_tblseq || '" does not exist any more.' AS msg
        FROM (                                        -- all expected application relations
          SELECT rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation
            EXCEPT                                    -- minus relations known by postgres
          SELECT nspname, relname, relkind FROM pg_catalog.pg_class, pg_catalog.pg_namespace
            WHERE relnamespace = pg_namespace.oid AND relkind IN ('r','S')
             ) AS t, emaj.emaj_relation r             -- join with emaj_relation to get the group name
        WHERE t.rel_schema = r.rel_schema AND t.rel_tblseq = r.rel_tblseq
        ORDER BY t.rel_schema, t.rel_tblseq, 1;
-- check the log table for all tables referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log table "' ||
               rel_log_schema || '"."' || rel_schema || '_' || rel_tblseq || '_log" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND (rel_log_schema, rel_schema || '_' || rel_tblseq || '_log') NOT IN
              (SELECT nspname, relname FROM pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE relnamespace = pg_namespace.oid AND relname LIKE E'%\_%\_log')
        ORDER BY rel_schema, rel_tblseq, 1;
-- check the log function for each table referenced in the emaj_relation table still exist
    RETURN QUERY
      SELECT msg FROM (
        SELECT substring(fnct FROM '^(.*)_.*_log_fnct') AS sch, substring(fnct FROM '^.*_(.*)_log_fnct') AS tbl,
                                                -- schema and table names are rebuilt from the returned function name
               'In group "' || r.rel_group || '", the log function "' || t.rel_log_schema || '"."' ||
               fnct || '" is not found.' AS msg
          FROM (                                        -- all expected log functions
           SELECT rel_log_schema, rel_schema || '_' || rel_tblseq || '_log_fnct' AS fnct
              FROM emaj.emaj_relation
              WHERE rel_kind = 'r'
           EXCEPT                                       -- minus functions known by postgres with the same name profile
           SELECT nspname, proname FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
             WHERE pronamespace = pg_namespace.oid AND proname LIKE E'%\_%\_log\_fnct'
               ) AS t, emaj.emaj_relation r             -- join with emaj_relation to get the group name
          WHERE r.rel_schema = substring(fnct FROM '^(.*)_.*_log_fnct')
            AND r.rel_tblseq = substring(fnct FROM '^.*_(.*)_log_fnct')
          ORDER BY 1,2,3
      ) AS t;
-- check log and truncate triggers for all tables referenced in the emaj_relation table still exist
--   start with log trigger
    RETURN QUERY
      SELECT 'In group "' || rel_group || '", the log trigger "' ||
               rel_schema || '_' || rel_tblseq || '_emaj_log_trg" is not found.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_kind = 'r'
          AND (rel_schema, rel_tblseq, rel_schema || '_' || rel_tblseq || '_emaj_log_trg') NOT IN
              (SELECT nspname, relname, tgname
                 FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                 WHERE tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                   AND tgname LIKE E'%\_%\_emaj\_log\_trg')
                         -- do not issue a row if the application table does not exist,
                         -- this case has been already detected
          AND (rel_schema, rel_tblseq) IN
              (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE relnamespace = pg_namespace.oid)
        ORDER BY rel_schema, rel_tblseq, 1;
--   then truncate trigger if pg 8.4+
    IF v_pgVersion >= '8.4' THEN
      RETURN QUERY
        SELECT 'In group "' || rel_group || '", the truncate trigger "' ||
                 rel_schema || '_' || rel_tblseq || '_emaj_trunc_trg" is not found.' AS msg
          FROM emaj.emaj_relation
          WHERE rel_kind = 'r'
            AND (rel_schema, rel_tblseq, rel_schema || '_' || rel_tblseq || '_emaj_trunc_trg') NOT IN
                (SELECT nspname, relname, tgname
                   FROM pg_catalog.pg_trigger, pg_catalog.pg_namespace, pg_catalog.pg_class
                   WHERE tgrelid = pg_class.oid AND relnamespace = pg_namespace.oid
                     AND tgname LIKE E'%\_%\_emaj\_trunc\_trg')
                         -- do not issue a row if the application table does not exist,
                         -- this case has been already detected
            AND (rel_schema, rel_tblseq) IN
                (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                   WHERE relnamespace = pg_namespace.oid)
          ORDER BY rel_schema, rel_tblseq, 1;
-- TODO : merge both triggers check when pg 8.3 will not be supported any more
    END IF;
-- check all log tables have a structure consistent with the application tables they reference
--      (same columns and same formats). It only returns one row per faulting table.
    RETURN QUERY
      SELECT msg FROM (
        SELECT DISTINCT rel_schema, rel_tblseq,
               'In group "' || rel_group || '", the structure of the application table "' ||
                 rel_schema || '"."' || rel_tblseq || '" is not coherent with its log table ("' ||
               rel_log_schema || '"."' || rel_schema || '_' || rel_tblseq || '_log").' AS msg
          FROM (
            (                                           -- application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
                AND rel_kind = 'r'
            EXCEPT                                      -- minus log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_schema || '_' || rel_tblseq || '_log'
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
                AND rel_kind = 'r'
            )
            UNION
            (                                           -- log table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_log_schema
                AND relname = rel_schema || '_' || rel_tblseq || '_log'
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
                AND rel_kind = 'r'
            EXCEPT                                      --  minus application table's columns
            SELECT rel_group, rel_schema, rel_tblseq, rel_log_schema, attname, atttypid, attlen, atttypmod
              FROM emaj.emaj_relation, pg_catalog.pg_attribute, pg_catalog.pg_class, pg_catalog.pg_namespace
              WHERE relnamespace = pg_namespace.oid AND nspname = rel_schema AND relname = rel_tblseq
                AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
                AND rel_kind = 'r'
            )) AS t
                           -- do not issue a row if the log or application table does not exist,
                           -- these cases have been already detected
        WHERE (rel_log_schema, rel_schema || '_' || rel_tblseq || '_log') IN
              (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE relnamespace = pg_namespace.oid)
          AND (rel_schema, rel_tblseq) IN
              (SELECT nspname, relname FROM pg_catalog.pg_class, pg_catalog.pg_namespace
                 WHERE relnamespace = pg_namespace.oid)
        ORDER BY 1,2,3
        ) AS t;
-- TODO : use CTE to improve performance, when pg 8.3 will not be supported any more
    RETURN;
  END;
$_verify_all_groups$;

CREATE OR REPLACE FUNCTION emaj._verify_all_schemas()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$_verify_all_schemas$
-- The function verifies that all E-Maj schemas only contains E-Maj objects.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, no row is returned.
  DECLARE
    v_emajSchema             TEXT = 'emaj';
    r_object                 RECORD;
  BEGIN
-- verify that the expected E-Maj schemas still exist
    RETURN QUERY
      SELECT DISTINCT 'The E-Maj schema "' || rel_log_schema || '" does not exist any more.' AS msg
        FROM emaj.emaj_relation
        WHERE rel_log_schema NOT IN (SELECT nspname FROM pg_catalog.pg_namespace)
        ORDER BY msg;
-- detect all objects that are not directly linked to a known table groups in all E-Maj schemas
-- scan pg_class, pg_proc, pg_type, pg_conversion, pg_operator, pg_opclass
    RETURN QUERY
      SELECT msg FROM (
-- look for unexpected tables
        SELECT nspname, 1, 'In schema "' || nspname ||
               '", the table "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace
           WHERE relnamespace = pg_namespace.oid AND relkind = 'r'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal tables
             AND (nspname, relname) NOT IN                                    -- exclude emaj log tables
                (SELECT rel_log_schema, rel_schema || '_' || rel_tblseq || '_log' FROM emaj.emaj_relation)
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected sequences
        SELECT nspname, 2, 'In schema "' || nspname ||
               '", the sequence "' || nspname || '"."' || relname || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace
           WHERE relnamespace = pg_namespace.oid AND relkind = 'S'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal sequences
             AND (nspname, relname) NOT IN                                    -- exclude emaj log table sequences
                (SELECT rel_log_schema, emaj._build_log_seq_name(rel_schema, rel_tblseq) FROM emaj.emaj_relation)
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected functions
        SELECT nspname, 3, 'In schema "' || nspname ||
               '", the function "' || nspname || '"."' || proname  || '" is not linked to any created tables group.' AS msg
           FROM pg_catalog.pg_proc, pg_catalog.pg_namespace
           WHERE pronamespace = pg_namespace.oid
             AND (nspname <> v_emajSchema OR (proname NOT LIKE E'emaj\\_%' AND proname NOT LIKE E'\\_%'))
                                                                              -- exclude emaj internal functions
             AND (nspname, proname) NOT IN (                                  -- exclude emaj log functions
               SELECT rel_log_schema, rel_schema || '_' || rel_tblseq || '_log_fnct' FROM emaj.emaj_relation)
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected composite types
        SELECT nspname, 4, 'In schema "' || nspname ||
               '", the type "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace
           WHERE relnamespace = pg_namespace.oid AND relkind = 'c'
             AND (nspname <> v_emajSchema OR (relname NOT LIKE E'emaj\\_%' AND relname NOT LIKE E'\\_%'))
                                                                              -- exclude emaj internal types
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected views
        SELECT nspname, 5, 'In schema "' || nspname ||
               '", the view "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace
           WHERE relnamespace = pg_namespace.oid  AND relkind = 'v'
             AND (nspname <> v_emajSchema OR relname NOT LIKE E'emaj\\_%')    -- exclude emaj internal views
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected foreign tables
        SELECT nspname, 6, 'In schema "' || nspname ||
               '", the foreign table "' || nspname || '"."' || relname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_class, pg_catalog.pg_namespace
           WHERE relnamespace = pg_namespace.oid  AND relkind = 'f'
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected domains
        SELECT nspname, 7, 'In schema "' || nspname ||
               '", the domain "' || nspname || '"."' || typname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_type, pg_catalog.pg_namespace
           WHERE typnamespace = pg_namespace.oid AND typisdefined and typtype = 'd'
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected conversions
        SELECT nspname, 8, 'In schema "' || nspname ||
               '", the conversion "' || nspname || '"."' || conname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_conversion, pg_catalog.pg_namespace
           WHERE connamespace = pg_namespace.oid
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected operators
        SELECT nspname, 9, 'In schema "' || nspname ||
               '", the operator "' || nspname || '"."' || oprname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_operator, pg_catalog.pg_namespace
           WHERE oprnamespace = pg_namespace.oid
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        UNION ALL
-- look for unexpected operator classes
        SELECT nspname, 10, 'In schema "' || nspname ||
               '", the operator class "' || nspname || '"."' || opcname || '" is not an E-Maj component.' AS msg
           FROM pg_catalog.pg_opclass, pg_catalog.pg_namespace
           WHERE opcnamespace = pg_namespace.oid
             AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)
        ORDER BY 1, 2, 3
      ) AS t;
-- TODO: when pg version 8.3- will not be supported, the following CTE could be used to minimize the number of emaj_relation scan to build the schemas list
--        WITH logschemas AS ( SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation )
--  replacing all "AND nspname IN (SELECT DISTINCT rel_log_schema FROM emaj.emaj_relation)" conditions by
--                "AND nspname IN (SELECT rel_log_schema FROM logschemas)"
    RETURN;
  END;
$_verify_all_schemas$;

CREATE OR REPLACE FUNCTION emaj.emaj_verify_all()
RETURNS SETOF TEXT LANGUAGE plpgsql AS
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and
-- emaj objects related to tables and sequences referenced in emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_pgVersion              TEXT = emaj._pg_version();
    v_errorFound             BOOLEAN = FALSE;
    r_object                 RECORD;
  BEGIN
-- Global checks
-- detect if the current postgres version is at least 8.2
    IF v_pgVersion < '8.3' THEN
      RETURN NEXT 'The current postgres version (' || version() || ') is not compatible with E-Maj.';
      v_errorFound = TRUE;
    END IF;
-- check all E-Maj primary and secondary schemas
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_schemas() msg
    LOOP
      RETURN NEXT r_object.msg;
      v_errorFound = TRUE;
    END LOOP;
-- check all groups components
    FOR r_object IN
      SELECT msg FROM emaj._verify_all_groups() msg
    LOOP
      RETURN NEXT r_object.msg;
      v_errorFound = TRUE;
    END LOOP;
-- final message if no error has been yet detected
    IF NOT v_errorFound THEN
      RETURN NEXT 'No error detected';
    END IF;
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

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
-- rights to emaj roles           --
--                                --
------------------------------------

-- emaj_adm needs the appropriate rights on emaj objects to execute all emaj functions

GRANT ALL ON SCHEMA emaj TO emaj_adm;

GRANT ALL ON SEQUENCE emaj.emaj_global_seq TO emaj_adm;

GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_param        TO emaj_adm;
GRANT SELECT ON emaj.emaj_visible_param                     TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_hist         TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group_def    TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group        TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_relation     TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_mark         TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_sequence     TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_seq_hole     TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk         TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk_session TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk_plan    TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk_stat    TO emaj_adm;

GRANT ALL ON SEQUENCE emaj.emaj_hist_hist_id_seq     TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_mark_mark_id_seq     TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_rlbk_rlbk_id_seq     TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_sequence_sequ_id_seq TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_seq_hole_sqhl_id_seq TO emaj_adm;

-- emaj_viewer can only view the emaj objects (content of emaj and log tables)

GRANT USAGE ON SCHEMA emaj TO emaj_viewer;

GRANT SELECT ON SEQUENCE emaj.emaj_global_seq TO emaj_viewer;

GRANT SELECT ON emaj.emaj_visible_param       TO emaj_viewer;
GRANT SELECT ON emaj.emaj_hist                TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group_def           TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group               TO emaj_viewer;
GRANT SELECT ON emaj.emaj_relation            TO emaj_viewer;
GRANT SELECT ON emaj.emaj_mark                TO emaj_viewer;
GRANT SELECT ON emaj.emaj_sequence            TO emaj_viewer;
GRANT SELECT ON emaj.emaj_seq_hole            TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk                TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk_session        TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk_plan           TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk_stat           TO emaj_viewer;

GRANT SELECT ON SEQUENCE emaj.emaj_hist_hist_id_seq     TO emaj_viewer;
GRANT SELECT ON SEQUENCE emaj.emaj_mark_mark_id_seq     TO emaj_viewer;
GRANT SELECT ON SEQUENCE emaj.emaj_rlbk_rlbk_id_seq     TO emaj_viewer;
GRANT SELECT ON SEQUENCE emaj.emaj_sequence_sequ_id_seq TO emaj_viewer;
GRANT SELECT ON SEQUENCE emaj.emaj_seq_hole_sqhl_id_seq TO emaj_viewer;

-- revoke grants on all functions from PUBLIC
REVOKE ALL ON FUNCTION emaj._pg_version() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._dblink_open_cnx(v_cnxName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._dblink_is_cnx_opened(v_cnxName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._dblink_close_cnx(v_cnxName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._purge_hist() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._get_mark_name(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._build_log_seq_name(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_names_array(v_groupNames TEXT[], v_type TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_group_content(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._forbid_truncate_fnct() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._log_truncate_fnct() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._create_log_schema(v_logSchemaName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_log_schema(v_logSchemaName TEXT, v_isForced BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_logDatTsp TEXT, v_logIdxTsp TEXT, v_isRollbackable BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._create_seq(v_schemaName TEXT, v_seqName TEXT, v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_lastGlobalSeq BIGINT, v_nbSession INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_log_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_timestamp TIMESTAMPTZ, v_lastGlobalSeq BIGINT, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_isLoggedRlbk BOOLEAN, v_lastSequenceId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._log_stat_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._gen_sql_tbl(v_fullTableName TEXT, v_logTableName TEXT, v_conditions TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._verify_groups(v_groupNames TEXT[], v_onErrorStop boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_fk_groups(v_groupName TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_alter_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT, v_resetLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_force_stop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_async(v_rlbkId INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_planning(v_rlbkId INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_set_batch_number(v_rlbkId INT, v_batchNumber INT, v_schema TEXT, v_table TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_error(v_rlbkId INT,v_msg TEXT, v_cnxName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_cleanup_rollback_state() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._reset_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_estimate_rollback_group(v_groupName TEXT, v_mark TEXT, v_isLoggedRlbk BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rollback_activity() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rollback_activity() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_gen_sql_groups(v_groupName TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_gen_sql_groups(v_groupName TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._verify_all_groups() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._verify_all_schemas() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_verify_all() FROM PUBLIC;

-- give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._pg_version() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._dblink_open_cnx(v_cnxName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._dblink_is_cnx_opened(v_cnxName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._dblink_close_cnx(v_cnxName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._purge_hist() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._get_mark_name(TEXT, TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._build_log_seq_name(TEXT, TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_names_array(v_groupNames TEXT[], v_type TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_group_content(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._forbid_truncate_fnct() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._log_truncate_fnct() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._create_log_schema(v_logSchemaName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_log_schema(v_logSchemaName TEXT, v_isForced BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_logDatTsp TEXT, v_logIdxTsp TEXT, v_isRollbackable BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._create_seq(v_schemaName TEXT, v_seqName TEXT, v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_lastGlobalSeq BIGINT, v_nbSession INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_log_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_timestamp TIMESTAMPTZ, v_lastGlobalSeq BIGINT, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_isLoggedRlbk BOOLEAN, v_lastSequenceId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_tbl(v_fullTableName TEXT, v_logTableName TEXT, v_conditions TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._verify_groups(v_groupNames TEXT[], v_onErrorStop boolean) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_fk_groups(v_groupName TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_alter_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT, v_resetLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_force_stop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;

GRANT EXECUTE ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_async(v_rlbkId INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_planning(v_rlbkId INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_set_batch_number(v_rlbkId INT, v_batchNumber INT, v_schema TEXT, v_table TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_error(v_rlbkId INT,v_msg TEXT, v_cnxName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_cleanup_rollback_state() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._reset_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_group(v_groupName TEXT, v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_activity() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rollback_activity() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._verify_all_groups() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._verify_all_schemas() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_adm;

-- give appropriate rights on functions to emaj_viewer role
GRANT EXECUTE ON FUNCTION emaj._pg_version() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._dblink_is_cnx_opened(v_cnxName TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_mark_name(TEXT, TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._build_log_seq_name(TEXT, TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_tbl(v_schemaName TEXT, v_tableName TEXT, v_logSchema TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_groups(v_groupNames TEXT[], v_onErrorStop boolean) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_planning(v_rlbkId INT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rlbk_set_batch_number(v_rlbkId INT, v_batchNumber INT, v_schema TEXT, v_table TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_cleanup_rollback_state() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_group(v_groupName TEXT, v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._rollback_activity() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_all_groups() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_all_schemas() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_viewer;

-- add grants to emaj roles on some system functions, needed for ppa plugin
GRANT EXECUTE ON FUNCTION pg_catalog.pg_database_size(name) TO emaj_adm, emaj_viewer;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_size_pretty(bigint) TO emaj_adm, emaj_viewer;

--------------------------------------
--                                  --
-- specific operation for extension --
--                                  --
--------------------------------------
-- register emaj tables content as candidate for pg_dump
SELECT pg_catalog.pg_extension_config_dump('emaj_param','WHERE param_key <> ''emaj_version''');
SELECT pg_catalog.pg_extension_config_dump('emaj_hist','');
SELECT pg_catalog.pg_extension_config_dump('emaj_group_def','');
SELECT pg_catalog.pg_extension_config_dump('emaj_group','');
SELECT pg_catalog.pg_extension_config_dump('emaj_relation','');
SELECT pg_catalog.pg_extension_config_dump('emaj_mark','');
SELECT pg_catalog.pg_extension_config_dump('emaj_sequence','');
SELECT pg_catalog.pg_extension_config_dump('emaj_seq_hole','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_session','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_plan','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_stat','');

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj <NEXT_VERSION>', 'Initialisation completed');

-- check the current max_prepared_transactions setting and report a warning if its value is too low for parallel rollback
DO LANGUAGE plpgsql
$tmp$
  DECLARE
    v_mpt                    INTEGER;
  BEGIN
-- check the max_prepared_transactions GUC value
    SELECT setting INTO v_mpt FROM pg_catalog.pg_settings WHERE name = 'max_prepared_transactions';
    IF v_mpt <= 1 THEN
      RAISE WARNING 'E-Maj installation: as the max_prepared_transactions parameter is set to % on this cluster, no parallel rollback is possible.',v_mpt;
    END IF;
-- check the dblink contrib or extension is already installed
    PERFORM 0 FROM pg_catalog.pg_proc WHERE proname = 'dblink';
    IF NOT FOUND THEN
      RAISE WARNING 'E-Maj installation: the dblink contrib/extension is not installed. It is advisable to have the dblink contrib/extension installed in order to take benefit of the E-Maj rollback monitoring and E-Maj parallel rollback capabilities.';
    END IF;
    RETURN;
  END;
$tmp$;
