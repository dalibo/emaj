--
-- E-Maj : logs and rollbacks table updates : V 0.10.1
--
-- This software is distributed under the GNU General Public License.
--
-- This script is automatically called by a "CREATE EXTENSION emaj;" statement in postgres 9.1+.
--
-- This script must be executed by a role having SUPERUSER privileges.
-- Before its execution:
-- 	-> the concerned cluster must contain a tablespace named "tspemaj", for instance previously created by
--	   CREATE TABLESPACE tspemaj LOCATION '/.../tspemaj',
--	-> the plpgsql language must have been created in the concerned database,
--  (-> the dblink contrib/extension must have been installed.)

COMMENT ON SCHEMA emaj IS
$$Contains all E-Maj related objects.$$;

-- create emaj roles and the _txid_current() function
DO LANGUAGE plpgsql
$tmp$
  DECLARE
    v_pgVersion     TEXT := substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)');
    v_stmt          TEXT;
  BEGIN
-- create emaj roles (NOLOGIN), if they do not exist 
-- does 'emaj_adm' already exist ?
    PERFORM 1 FROM pg_roles WHERE rolname = 'emaj_adm';
-- if no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_adm;
      COMMENT ON ROLE emaj_adm IS
        $$This role may be granted to other roles in charge of E-Maj administration.$$;
    END IF;
-- does 'emaj_viewer' already exist ?
    PERFORM 1 FROM pg_roles WHERE rolname = 'emaj_viewer';
-- if no, create it
    IF NOT FOUND THEN
      CREATE ROLE emaj_viewer;
      COMMENT ON ROLE emaj_viewer IS 
        $$This role may be granted to other roles allowed to view E-Maj objects content.$$;
    END IF;
-- create a SQL emaj_txid_function that encapsulates the standart txid_current function with postgres 8.3+
-- or just returns 0 with postgres 8.2
    v_stmt = 'CREATE or REPLACE FUNCTION emaj._txid_current() RETURNS BIGINT LANGUAGE SQL AS $$ SELECT ';
    IF v_pgVersion < '8.3' THEN
      v_stmt = v_stmt || '0::BIGINT;$$';
    ELSE
      v_stmt = v_stmt || 'txid_current();$$';
    END IF;
    EXECUTE v_stmt;
--
    RETURN; 
  END;
$tmp$;

------------------------------------
--                                --
-- emaj technical tables          --
--                                --
------------------------------------

-- table containing Emaj parameters
CREATE TABLE emaj.emaj_param (
    param_key                TEXT        NOT NULL,       -- parameter key
    param_value_text         TEXT,                       -- value if type is text, otherwise NULL
    param_value_int          BIGINT,                     -- value if type is bigint, otherwise NULL
    param_value_boolean      BOOLEAN,                    -- value if type is boolean, otherwise NULL
    param_value_interval     INTERVAL,                   -- value if type is interval, otherwise NULL
    PRIMARY KEY (param_key) 
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_param IS 
$$Contains E-Maj parameters.$$;

-- table containing the history of all E-Maj events  
CREATE TABLE emaj.emaj_hist (                            -- records the history of 
    hist_id                  BIGSERIAL   NOT NULL,       -- internal id
    hist_datetime            TIMESTAMPTZ NOT NULL 
                             DEFAULT clock_timestamp(),  -- insertion time
    hist_function            TEXT        NOT NULL,       -- main E-Maj function generating the event
    hist_event               TEXT,                       -- type of event (often BEGIN or END)
    hist_object              TEXT,                       -- object supporting the event (often the group name)
    hist_wording             TEXT,                       -- additional comment
    hist_user                TEXT
                             DEFAULT session_user,       -- the user who call the E-Maj function
    hist_txid                BIGINT
                             DEFAULT emaj._txid_current(), -- and its tx_id
    PRIMARY KEY (hist_id)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_hist IS
$$Contains E-Maj events history.$$;

-- table containing the definition of groups' content. Filled and maintained by the user, it is used by emaj_create_group function.
CREATE TABLE emaj.emaj_group_def (
    grpdef_group             TEXT        NOT NULL,       -- name of the group containing this table or sequence
    grpdef_schema            TEXT        NOT NULL,       -- schema name of this table or sequence
    grpdef_tblseq            TEXT        NOT NULL,       -- table or sequence name
    grpdef_priority          INTEGER,                    -- priority level (tables are processed in ascending 
                                                         --   order, with NULL last)
    PRIMARY KEY (grpdef_group, grpdef_schema, grpdef_tblseq) 
-- the group name is included in the pkey so that a table/sequence can be temporarily assigned to several groups 
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_group_def IS
$$Contains E-Maj groups definition, supplied by the E-Maj administrator.$$;

-- table containing the defined groups
--     rows are created at emaj_create_group time and deleted at emaj_drop_group time
CREATE TABLE emaj.emaj_group (
    group_name               TEXT        NOT NULL,
    group_state              TEXT        NOT NULL,       -- 2 possibles states: 
                                                         --   'LOGGING' between emaj_start_group and emaj_stop_group
                                                         --   'IDLE' in other cases
    group_nb_table           INT,                        -- number of tables at emaj_create_group time
    group_nb_sequence        INT,                        -- number of sequences at emaj_create_group time
    group_is_rollbackable    BOOLEAN,                    -- false for 'AUDIT_ONLY' groups, true for 'ROLLBACKABLE' groups
    group_creation_datetime  TIMESTAMPTZ NOT NULL        -- start time of the transaction that created the group
                             DEFAULT transaction_timestamp(),
    group_pg_version         TEXT        NOT NULL        -- postgres version at emaj_create_group() time
                             DEFAULT substring (version() from E'PostgreSQL\\s([.,0-9,A-Z,a-z]*)'),
    group_comment            TEXT,                       -- optional user comment
    PRIMARY KEY (group_name)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_group IS
$$Contains created E-Maj groups.$$;

-- table containing the relations (tables and sequences) of created tables groups
CREATE TABLE emaj.emaj_relation (
    rel_schema               TEXT        NOT NULL,       -- schema name containing the relation
    rel_tblseq               TEXT        NOT NULL,       -- table or sequence name
    rel_group                TEXT        NOT NULL,       -- name of the group that owns the relation
    rel_priority             INTEGER,                    -- priority level of processing inside the group
    rel_kind                 TEXT,                       -- similar to the relkind column of pg_class table 
                                                         --   ('r' = table, 'S' = sequence) 
    rel_session              INT,                        -- rollback session id
    rel_rows                 BIGINT,                     -- number of rows to rollback, computed at rollback time
    PRIMARY KEY (rel_schema, rel_tblseq),
    FOREIGN KEY (rel_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_relation IS
$$Contains the content (tables and sequences) of created E-Maj groups.$$;

-- table containing the marksl
CREATE TABLE emaj.emaj_mark (
    mark_id                  BIGSERIAL   NOT NULL,       -- serial id used to order rows (not to rely on timestamps 
                                                         -- that are not safe if system time changes)
    mark_group               TEXT        NOT NULL,       -- group for which the mark has been set
    mark_name                TEXT        NOT NULL,       -- mark name
    mark_datetime            TIMESTAMPTZ NOT NULL,       -- precise timestamp of the mark creation, used as a reference
                                                         --   for other tables like emaj_sequence and all log tables
    mark_state               TEXT,                       -- state of the mark, with 2 possible values:
                                                         --   'ACTIVE' and 'DELETED'
    mark_comment             TEXT,                       -- optional user comment
    mark_txid                BIGINT                      -- id of the tx that has set the mark
                             DEFAULT emaj._txid_current(),
    mark_last_sequence_id    BIGINT,                     -- last sequ_id for the group at the end of the _set_mark_groups operation
    mark_last_seq_hole_id    BIGINT,                     -- last sqhl_id for the group at _set_mark_groups time 
    PRIMARY KEY (mark_group, mark_name),
    FOREIGN KEY (mark_group) REFERENCES emaj.emaj_group (group_name) ON DELETE CASCADE
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_mark IS
$$Contains marks set on E-Maj tables groups.$$;

-- table containing the sequences characteristics log 
-- (to record at mark time the state of application sequences and sequences used by log tables) 
CREATE TABLE emaj.emaj_sequence (
    sequ_id                  BIGSERIAL   NOT NULL,       -- serial id used to delete oldest or newest rows (not to rely
                                                         -- on timestamps that are not safe if system time changes)
    sequ_schema              TEXT        NOT NULL,       -- application or 'emaj' schema or that owns the sequence
    sequ_name                TEXT        NOT NULL,       -- application or emaj sequence name
    sequ_datetime            TIMESTAMPTZ NOT NULL,       -- timestamp the sequence characteristics have been recorded
                                                         --   the same timestamp as referenced in emaj_mark table 
    sequ_mark                TEXT        NOT NULL,       -- name of the mark associated to the insertion timestamp 
    sequ_last_val            BIGINT      NOT NULL,       -- sequence last value
    sequ_start_val           BIGINT      NOT NULL,       -- sequence start value, (0 with postgres 8.2)
    sequ_increment           BIGINT      NOT NULL,       -- sequence increment
    sequ_max_val             BIGINT      NOT NULL,       -- sequence max value
    sequ_min_val             BIGINT      NOT NULL,       -- sequence min value
    sequ_cache_val           BIGINT      NOT NULL,       -- sequence cache value
    sequ_is_cycled           BOOLEAN     NOT NULL,       -- sequence flag 'is cycled ?'
    sequ_is_called           BOOLEAN     NOT NULL,       -- sequence flag 'is called ?'
    PRIMARY KEY (sequ_schema, sequ_name, sequ_datetime)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_sequence IS
$$Contains values of sequences at E-Maj set_mark times.$$;

-- table containing the holes in sequences log
-- these holes are due to rollback operations that do not adjust log sequences
-- the hole size = difference of sequence's current last_value and last value at the rollback mark
CREATE TABLE emaj.emaj_seq_hole (
    sqhl_id                  BIGSERIAL   NOT NULL,       -- serial id used to delete oldest or newest rows (not to rely
                                                         -- on timestamps that are not safe if system time changes)
    sqhl_schema              TEXT        NOT NULL,       -- schema that owns the table
    sqhl_table               TEXT        NOT NULL,       -- application table for which a sequence hole is recorded
                                                         --   in the associated log table
    sqhl_datetime            TIMESTAMPTZ NOT NULL        -- timestamp of the rollback operation that generated the hole
                             DEFAULT transaction_timestamp(),
    sqhl_hole_size           BIGINT      NOT NULL,       -- hole size computed as the difference of 2 sequence last-values
    PRIMARY KEY (sqhl_schema, sqhl_table, sqhl_datetime)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_seq_hole IS
$$Contains description of holes in sequence values for E-Maj log tables.$$;

-- table containing statistics about previously executed rollback operations
-- and used to estimate rollback durations 
CREATE TABLE emaj.emaj_rlbk_stat (
    rlbk_operation           TEXT        NOT NULL,       -- type of operation, can contains 'rlbk', 'del_log', 'cre_fk'
    rlbk_schema              TEXT        NOT NULL,       -- schema that owns the table or the foreign key
    rlbk_tbl_fk              TEXT        NOT NULL,       -- table or foreign key name
    rlbk_datetime            TIMESTAMPTZ NOT NULL,       -- timestamp of the rollback that has generated the statistic
    rlbk_nb_rows             BIGINT      NOT NULL,       -- number of rows processed by the operation
    rlbk_duration            INTERVAL    NOT NULL,       -- duration of the elementary operation
    PRIMARY KEY (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_rlbk_stat IS
$$Contains statistics about previous E-Maj rollback durations.$$;

-- working storage table containing foreign key definition
-- (used at table rollback time to drop and later recreate foreign keys)
CREATE TABLE emaj.emaj_fk (
    fk_groups                TEXT[]      NOT NULL,       -- groups for which the rollback operation is performed
    fk_session               INT         NOT NULL,       -- session number (for parallel rollback purpose)
    fk_name                  TEXT        NOT NULL,       -- foreign key name
    fk_schema                TEXT        NOT NULL,       -- schema name of the table that owns the foreign key
    fk_table                 TEXT        NOT NULL,       -- name of the table that owns the foreign key
    fk_def                   TEXT        NOT NULL,       -- foreign key definition as reported by pg_get_constraintdef
    PRIMARY KEY (fk_groups, fk_name, fk_schema, fk_table)
    ) TABLESPACE tspemaj;
COMMENT ON TABLE emaj.emaj_fk IS
$$Contains temporary description of foreign keys suppressed by E-Maj rollback operations.$$;

------------------------------------
--                                --
-- emaj types                     --
--                                --
------------------------------------

CREATE TYPE emaj.emaj_log_stat_type AS (
    stat_group     TEXT,
    stat_schema    TEXT,
    stat_table     TEXT,
    stat_rows      BIGINT
    );
COMMENT ON TYPE emaj.emaj_log_stat_type IS
$$Represents the structure of rows returned by the emaj_log_stat_group() function.$$;

CREATE TYPE emaj.emaj_detailed_log_stat_type AS (
    stat_group     TEXT,
    stat_schema    TEXT,
    stat_table     TEXT,
    stat_role      VARCHAR(32),
    stat_verb      VARCHAR(6),
    stat_rows      BIGINT
    );
COMMENT ON TYPE emaj.emaj_detailed_log_stat_type IS
$$Represents the structure of rows returned by the emaj_detailed_log_stat_group() function.$$;

------------------------------------
--                                --
-- 'Fixed' parameters             --
--                                --
------------------------------------
INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('emaj_version','0.10.1');

-- Other parameters are optional. They are set by users if needed.

-- The history_retention parameter defines the time interval when a row remains in the emaj history table - default is 1 month
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','1 month'::interval);

-- 4 parameters are used by the emaj_estimate_rollback_duration function as a default value to compute the approximate duration of a rollback operation.
-- The avg_row_rollback_duration parameter defines the average duration needed to rollback a row.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_rollback_duration','100 microsecond'::interval);
-- The avg_row_delete_log_duration parameter defines the average duration needed to delete log rows.
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('avg_row_delete_log_duration','10 -microsecond'::interval);
-- The fixed_table_rollback_duration parameter defines the fixed rollback cost for any table or sequence belonging to a group
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_rollback_duration','5 millisecond'::interval);
-- The fixed_table_with_rollback_duration parameter defines the additional fixed rollback cost for any table that has effective rows to rollback
--   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('fixed_table_with_rollback_duration','2.5 millisecond'::interval);
------------------------------------
--                                --
-- Low level Functions            --
--                                --
------------------------------------
CREATE or REPLACE FUNCTION emaj._pg_version() returns TEXT language sql IMMUTABLE as
$$
-- This functions returns as a string the 2 major parts of the current postgresql version (x.y)
SELECT substring (version() from E'PostgreSQL\\s(\\d+\\.\\d+)');
$$;

CREATE or REPLACE FUNCTION emaj._purge_hist() returns void language sql as
$$
-- This function purges the emaj history by deleting all rows prior the 'history_retention' parameter 
--   and not deleting rows generated by groups that are currently in logging state
    DELETE FROM emaj.emaj_hist WHERE hist_datetime < 
      (SELECT MIN(datetime) FROM 
        (
                 -- compute the least recent start_group time of groups in logging state
          (SELECT MIN(hist_datetime) FROM emaj.emaj_group, emaj.emaj_hist 
             WHERE group_name = ANY (string_to_array(hist_object,',')) AND group_state = 'LOGGING' AND 
                   (hist_function = 'START_GROUP' OR hist_function = 'START_GROUPS') AND hist_event = 'BEGIN')
         UNION 
                 -- compute the timestamp of now minus the history_retention (1 month by default)
          (SELECT current_timestamp - 
                  coalesce((SELECT param_value_interval FROM emaj.emaj_param WHERE param_key = 'history_retention'),'1 MONTH'))
        ) AS tmst(datetime));
$$;

CREATE or REPLACE FUNCTION emaj._get_mark_name(TEXT, TEXT) returns TEXT language sql as
$$
-- This function returns a mark name if exists for a group, processing the EMAJ_LAST_MARK keyword.
-- input: group name and mark name
-- output: mark name or NULL
SELECT case 
         when $2 = 'EMAJ_LAST_MARK' then
              (SELECT mark_name FROM emaj.emaj_mark WHERE mark_group = $1 ORDER BY mark_datetime DESC LIMIT 1)
         else (SELECT mark_name FROM emaj.emaj_mark WHERE mark_group = $1 AND mark_name = $2)
       end
$$;

CREATE or REPLACE FUNCTION emaj._get_mark_datetime(TEXT, TEXT) returns TIMESTAMPTZ language sql as
$$
-- This function returns the creation timestamp of a mark if exists for a group, 
--   processing the EMAJ_LAST_MARK keyword.
-- input: group name and mark name
-- output: mark date-time or NULL
SELECT case 
         when $2 = 'EMAJ_LAST_MARK' then
              (SELECT MAX(mark_datetime) FROM emaj.emaj_mark WHERE mark_group = $1)
         else (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = $1 AND mark_name = $2)
       end
$$;

CREATE or REPLACE FUNCTION emaj._check_group_names_array(v_groupNames TEXT[])
RETURNS TEXT[] LANGUAGE plpgsql AS
$_check_group_names_array$
-- This function build a array of group names similar to the supplied array, except that NULL 
-- values, empty string and duplicate names are suppressed. Issue a warning if the result array is NULL.
-- Input: group names array
-- Output: validated group names array
  DECLARE
    v_gn           TEXT[];
    v_i            INT;
  BEGIN
    IF array_upper(v_groupNames,1) >= 1 THEN
-- if there are elements, build the result array
      FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
-- look for not NULL & not empty group name
        IF v_groupNames[v_i] IS NULL OR v_groupNames[v_i] = '' THEN
          RAISE WARNING '_check_group_names_array: a group name is NULL or empty.';
-- look for duplicate name
        ELSEIF v_gn IS NOT NULL AND v_groupNames[v_i] = ANY (v_gn) THEN
          RAISE WARNING '_check_group_names_array: duplicate group name %.',v_groupNames[v_i];
        ELSE
-- OK, keep the name
          v_gn = array_append (v_gn, v_groupNames[v_i]);
        END IF;
      END LOOP;
    END IF;
-- check for NULL result
    IF v_gn IS NULL THEN
      RAISE WARNING '_check_group_names_array: No group name to process.';
    END IF;
    RETURN v_gn;
  END;
$_check_group_names_array$;

CREATE or REPLACE FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) 
RETURNS TEXT LANGUAGE plpgsql AS
$_check_class$
-- This function verifies that an application table or sequence exists in pg_class
-- It also protects from a recursive use : tables or sequences from emaj schema cannot be managed by EMAJ
-- Input: the names of the schema and the class (table or sequence)
-- Output: the relkind of the class : 'r' for a table and 's' for a sequence
-- If the schema or the class is not known, the function stops.
  DECLARE
    v_relkind      TEXT;
    v_schemaOid    OID;
  BEGIN
    IF v_schemaName = 'emaj' THEN
      RAISE EXCEPTION '_check_class: object from schema % cannot be managed by EMAJ.', v_schemaName;
    END IF;
    SELECT oid INTO v_schemaOid FROM pg_namespace WHERE nspname = v_schemaName;
    IF NOT found THEN
      RAISE EXCEPTION '_check_class: schema % doesn''t exist.', v_schemaName;
    END IF;
    SELECT relkind INTO v_relkind FROM pg_class 
      WHERE relNameSpace = v_schemaOid AND relName = v_className AND relkind in ('r','S');
    IF NOT found THEN
      RAISE EXCEPTION '_check_class: table or sequence % doesn''t exist.', v_className;
    END IF; 
    RETURN v_relkind;
  END;
$_check_class$;

CREATE or REPLACE FUNCTION emaj._check_new_mark(v_mark TEXT, v_groupNames TEXT[]) 
RETURNS TEXT LANGUAGE plpgsql AS 
$_check_new_mark$
-- This function verifies that a new mark name supplied in emaj_start_group(s) or emaj_set_mark_group(s) is valid.
-- It processes the possible NULL mark value and the replacement of % wild characters.
-- It also checks that the mark name do not already exist for any group.
-- Input: name of the mark to set, array of group names 
--        The array of group names may be NULL to avoid the check against groups
-- Output: internal name of the mark
  DECLARE
    v_i             INT;
    v_markName      TEXT := v_mark;
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
      FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
-- ... if a mark with the same name already exists for the group, stop
        PERFORM 1 FROM emaj.emaj_mark
          WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
        IF FOUND THEN
           RAISE EXCEPTION '_check_new_mark: Group % already contains a mark named %.', v_groupNames[v_i], v_markName;
        END IF;
      END LOOP;
    END IF;
    RETURN v_markName;
  END;
$_check_new_mark$;

CREATE or REPLACE FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_create_tbl$
-- This function creates all what is needed to manage the log and rollback operations for an application table
-- Input: schema name (mandatory even for the 'public' schema), table name, boolean indicating whether the table belongs to a rollbackable group
-- Are created: 
--    - the associated log table, with its own sequence
--    - the function that logs the tables updates, defined as a trigger
--    - the rollback function (one per table)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
-- variables for the name of tables, functions, triggers,...
    v_fullTableName         TEXT;
    v_emajSchema            TEXT := 'emaj';
    v_emajTblSpace          TEXT := 'tspemaj';
    v_logTableName          TEXT;
    v_logFnctName           TEXT;
    v_rlbkFnctName          TEXT;
    v_exceptionRlbkFnctName TEXT;
    v_logTriggerName        TEXT;
    v_truncTriggerName      TEXT;
    v_sequenceName          TEXT;
-- variables to hold pieces of SQL
    v_pkCondList            TEXT;
    v_colList               TEXT;
    v_setList               TEXT;
-- other variables
    v_attname               TEXT;
    v_relhaspkey            BOOLEAN;
    v_pgVersion             TEXT := emaj._pg_version();
    r_trigger               RECORD;
    v_triggerList           TEXT := '';
-- cursor to retrieve all columns of the application table
    col1_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute 
        WHERE attrelid = tbl 
          AND attnum > 0
          AND attisdropped = false;
-- cursor to retrieve all columns of table's primary key
-- (taking column names in pg_attribute from the table's definition instead of index definition is mandatory 
--  starting from pg9.0, joining tables with indkey instead of indexrelid)
    col2_curs CURSOR (tbl regclass) FOR 
      SELECT attname FROM pg_attribute, pg_index 
        WHERE pg_attribute.attrelid = pg_index.indrelid
          AND attnum = ANY (indkey) 
          AND indrelid = tbl AND indisprimary
          AND attnum > 0 AND attisdropped = false;
  BEGIN
-- check the table has a primary key
    SELECT true INTO v_relhaspkey FROM pg_class, pg_namespace, pg_constraint WHERE 
        relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
        contype = 'p' AND nspname = v_schemaName AND relname = v_tableName;
    IF NOT FOUND THEN
      v_relhaspkey = false;
    END IF;
    IF v_isRollbackable AND v_relhaspkey = FALSE THEN
      RAISE EXCEPTION '_create_tbl: table % has no PRIMARY KEY.', v_tableName;
    END IF;
-- OK, build the different name for table, trigger, functions,...
    v_fullTableName    := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_rlbkFnctName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_exceptionRlbkFnctName=substring(quote_literal(v_rlbkFnctName) FROM '^.(.*).$');
    v_logTriggerName   := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');
    v_sequenceName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_emaj_id_seq');

-- creation of the log table: the log table looks like the application table, with some additional technical columns
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName;
    EXECUTE 'CREATE TABLE ' || v_logTableName
         || '( LIKE ' || v_fullTableName || ') TABLESPACE ' || v_emajTblSpace ;
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_verb VARCHAR(3)';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_tuple VARCHAR(3)';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_id BIGSERIAL PRIMARY KEY';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_changed TIMESTAMPTZ DEFAULT clock_timestamp()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_txid BIGINT DEFAULT emaj._txid_current()';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_user VARCHAR(32) DEFAULT session_user';
    EXECUTE 'ALTER TABLE ' || v_logTableName
         || ' ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr()';
-- alter the sequence associated to the emaj_id column to set the increment to 2 (so that an update operation can safely have its 2 log rows)
    EXECUTE 'ALTER SEQUENCE ' || v_sequenceName || ' INCREMENT 2';

-- creation of the log fonction that will be mapped to the log trigger later
-- The new row is logged for each INSERT, the old row is logged for each DELETE 
-- and the old and the new rows are logged for each UPDATE
    EXECUTE 'CREATE or REPLACE FUNCTION ' || v_logFnctName || '() RETURNS trigger AS $logfnct$'
         || 'DECLARE'
         || '  V_EMAJ_ID   BIGINT;'
         || 'BEGIN'
         || '  IF (TG_OP = ''DELETE'') THEN'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''DEL'', ''OLD'';'
         || '    RETURN OLD;'
         || '  ELSIF (TG_OP = ''UPDATE'') THEN'
         || '    SELECT NEXTVAL(' || quote_literal(v_sequenceName) || ') INTO V_EMAJ_ID;'
         || '    INSERT INTO ' || v_logTableName || ' SELECT OLD.*, ''UPD'', ''OLD'', V_EMAJ_ID;'
         || '    V_EMAJ_ID = V_EMAJ_ID + 1;'
         || '    INSERT INTO ' || v_logTableName || ' SELECT NEW.*, ''UPD'', ''NEW'', V_EMAJ_ID;'
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
-- creation of the trigger that blocks any TRUNCATE on the application table, using the common _forbid_truncate_fnct() function 
-- But the trigger is not immediately activated (it will be at emaj_start_group time)
    IF v_pgVersion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
      EXECUTE 'CREATE TRIGGER ' || v_truncTriggerName
           || ' BEFORE TRUNCATE ON ' || v_fullTableName
           || '  FOR EACH STATEMENT EXECUTE PROCEDURE emaj._forbid_truncate_fnct()';
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
    END IF;
--
-- create the rollback function, if the table belongs to a rollbackable group 
--
    IF v_isRollbackable THEN
-- First build some pieces of the CREATE FUNCTION statement
--   build the tables's columns list
--     and the SET clause for the UPDATE, from the same columns list
      v_colList := '';
      v_setList := '';
      OPEN col1_curs (v_fullTableName);
      LOOP
        FETCH col1_curs INTO v_attname;
        EXIT WHEN NOT FOUND;
        IF v_colList = '' THEN
           v_colList := 'rec_log.' || quote_ident(v_attname);
           v_setList := quote_ident(v_attname) || ' = rec_old_log.' || quote_ident(v_attname);
        ELSE
           v_colList := v_colList || ', rec_log.' || quote_ident(v_attname);
           v_setList := v_setList || ', ' || quote_ident(v_attname) || ' = rec_old_log.' || quote_ident(v_attname);
        END IF;
      END LOOP;
      CLOSE col1_curs;
--   build "equality on the primary key" conditions, from the list of the primary key's columns
      v_pkCondList := '';
      OPEN col2_curs (v_fullTableName);
      LOOP
        FETCH col2_curs INTO v_attname;
        EXIT WHEN NOT FOUND;
        IF v_pkCondList = '' THEN
           v_pkCondList := quote_ident(v_attname) || ' = rec_log.' || quote_ident(v_attname);
        ELSE
           v_pkCondList := v_pkCondList || ' AND ' || quote_ident(v_attname) || ' = rec_log.' || quote_ident(v_attname);
        END IF;
      END LOOP;
      CLOSE col2_curs;
-- Then create the rollback function associated to the table
-- At execution, it will loop on each row from the log table in reverse order
--  It will insert the old deleted rows, delete the new inserted row 
--  and update the new rows by setting back the old rows
-- The function returns the number of rollbacked elementary operations or rows
-- All these functions will be called by the emaj_rlbk_tbl function, which is activated by the
--  emaj_rollback_group function
      EXECUTE 'CREATE or REPLACE FUNCTION ' || v_rlbkFnctName || ' (v_rollback_id_limit bigint)'
           || ' RETURNS bigint AS $rlbkfnct$'
           || '  DECLARE'
           || '    v_nb_rows       bigint := 0;'
           || '    v_nb_proc_rows  integer;'
           || '    rec_log     ' || v_logTableName || '%ROWTYPE;'
           || '    rec_old_log ' || v_logTableName || '%ROWTYPE;'
           || '    log_curs CURSOR FOR '
           || '      SELECT * FROM ' || v_logTableName
           || '        WHERE emaj_id >= v_rollback_id_limit '
           || '        ORDER BY emaj_id DESC;'
           || '  BEGIN'
           || '    OPEN log_curs;'
           || '    LOOP '
           || '      FETCH log_curs INTO rec_log;'
           || '      EXIT WHEN NOT FOUND;'
           || '      IF rec_log.emaj_verb = ''INS'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; INS'', rec_log.emaj_id;'
           || '          DELETE FROM ' || v_fullTableName || ' WHERE ' || v_pkCondList || ';'
           || '      ELSIF rec_log.emaj_verb = ''UPD'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; UPD ; %'', rec_log.emaj_id,rec_log.emaj_tuple;'
           || '          FETCH log_curs into rec_old_log;'
--         || '          RAISE NOTICE ''emaj_id = % ; UPD ; %'', rec_old_log.emaj_id,rec_old_log.emaj_tuple;'
           || '          UPDATE ' || v_fullTableName || ' SET ' || v_setList || ' WHERE ' || v_pkCondList || ';'
           || '      ELSIF rec_log.emaj_verb = ''DEL'' THEN'
--         || '          RAISE NOTICE ''emaj_id = % ; DEL'', rec_log.emaj_id;'
           || '          INSERT INTO ' || v_fullTableName || ' VALUES (' || v_colList || ');'
           || '      ELSE'
           || '          RAISE EXCEPTION ''' || v_exceptionRlbkFnctName || ': internal error - emaj_verb = % unknown, emaj_id = %.'','
           || '            rec_log.emaj_verb, rec_log.emaj_id;' 
           || '      END IF;'
           || '      GET DIAGNOSTICS v_nb_proc_rows = ROW_COUNT;'
           || '      IF v_nb_proc_rows <> 1 THEN'
           || '        RAISE EXCEPTION ''' || v_exceptionRlbkFnctName || ': internal error - emaj_verb = %, emaj_id = %, # processed rows = % .'''
           || '           ,rec_log.emaj_verb, rec_log.emaj_id, v_nb_proc_rows;' 
           || '      END IF;'
           || '      v_nb_rows := v_nb_rows + 1;'
           || '    END LOOP;'
           || '    CLOSE log_curs;'
--         || '    RAISE NOTICE ''Table ' || v_fullTableName || ' -> % rollbacked rows.'', v_nb_rows;'
           || '    RETURN v_nb_rows;'
           || '  END;'
           || '$rlbkfnct$ LANGUAGE plpgsql;';
      END IF;
-- check if the table has (neither internal - ie. created for fk - nor previously created by emaj) trigger,
-- This check is not done for postgres 8.2 because column tgconstraint doesn't exist
    IF v_pgVersion >= '8.3' THEN
      FOR r_trigger IN 
        SELECT tgname FROM pg_trigger WHERE tgrelid = v_fullTableName::regclass AND tgconstraint = 0 AND tgname NOT LIKE E'%emaj\\_%\\_trg'
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
    END IF;
-- grant appropriate rights to both emaj roles
    EXECUTE 'GRANT SELECT ON TABLE ' || v_logTableName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON TABLE ' || v_logTableName || ' TO emaj_adm';
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || v_sequenceName || ' TO emaj_viewer';
    EXECUTE 'GRANT ALL PRIVILEGES ON SEQUENCE ' || v_sequenceName || ' TO emaj_adm';
    RETURN;
  END;
$_create_tbl$;

CREATE or REPLACE FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_drop_tbl$
-- The function deletes all what has been created by _create_tbl function
-- Required inputs: schema name (mandatory even if "public") and table name
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema       TEXT := 'emaj';
    v_pgVersion        TEXT := emaj._pg_version();
    v_fullTableName    TEXT;
    v_logTableName     TEXT;
    v_logFnctName      TEXT;
    v_rlbkFnctName     TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_seqName          TEXT;
  BEGIN
    v_fullTableName    := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_logFnctName      := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_fnct');
    v_rlbkFnctName     := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_logTriggerName   := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_truncTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_trunc_trg');
    v_seqName          := v_schemaName || '_' || v_tableName || '_log_emaj_id_seq';
-- delete the log trigger on the application table
    EXECUTE 'DROP TRIGGER IF EXISTS ' || v_logTriggerName || ' ON ' || v_fullTableName;
-- delete the truncate trigger on the application table
    IF v_pgVersion >= '8.4' THEN
      EXECUTE 'DROP TRIGGER IF EXISTS ' || v_truncTriggerName || ' ON ' || v_fullTableName;
    END IF;
-- delete log and rollback functions,
    EXECUTE 'DROP FUNCTION IF EXISTS ' || v_logFnctName || '()';
    IF v_isRollbackable THEN
      EXECUTE 'DROP FUNCTION IF EXISTS ' || v_rlbkFnctName || '(bigint)';
    END IF;
-- delete the log table
    EXECUTE 'DROP TABLE IF EXISTS ' || v_logTableName || ' CASCADE';
-- delete rows related to the log sequence from emaj_sequence table
    DELETE FROM emaj.emaj_sequence WHERE sequ_schema = 'emaj' AND sequ_name = v_seqName;
-- delete rows related to the table from emaj_seq_hole table
    DELETE FROM emaj.emaj_seq_hole WHERE sqhl_schema = quote_ident(v_schemaName) AND sqhl_table = quote_ident(v_tableName);
    RETURN;
  END;
$_drop_tbl$;

CREATE or REPLACE FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) 
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

CREATE or REPLACE FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ,  v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_rlbk_tbl$
-- This function rollbacks one table to a given timestamp
-- The function is called by emaj._rlbk_groups_step5()
-- Input: schema name and table name, timestamp limit for rollback, flag to specify if log trigger 
--        must be disable during rollback operation, flag to specify if rollbacked log rows must be deleted,
--        last sequence and last hole identifiers to keep (greater ones being to be deleted)
-- These flags must be respectively:
--   - true and true   for common (unlogged) rollback,
--   - false and false for logged rollback, 
--   - true and false  for unlogged rollback with undeleted log rows (for emaj_rollback_and_stop_group function)
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application table.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_fullTableName  TEXT;
    v_logTableName   TEXT;
    v_rlbkFnctName   TEXT;
    v_logTriggerName TEXT;
    v_fullSeqName    TEXT;
    v_seqName        TEXT;
    v_emaj_id        BIGINT;
    v_nb_rows        BIGINT;
    v_tsrlbk_start   TIMESTAMP;
    v_tsrlbk_end     TIMESTAMP;
    v_tsdel_start    TIMESTAMP;
    v_tsdel_end      TIMESTAMP;
  BEGIN
    v_fullTableName  := quote_ident(v_schemaName) || '.' || quote_ident(v_tableName);
    v_logTableName   := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log');
    v_rlbkFnctName   := quote_ident(v_emajSchema) || '.' || 
                        quote_ident(v_schemaName || '_' || v_tableName || '_rlbk_fnct');
    v_logTriggerName := quote_ident(v_schemaName || '_' || v_tableName || '_emaj_log_trg');
    v_seqName        := v_schemaName || '_' || v_tableName || '_log_emaj_id_seq';
    v_fullSeqName    := quote_ident(v_seqName);
-- get the emaj_id to rollback to from the sequence (first emaj_id to delete)
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_emaj_id
      FROM emaj.emaj_sequence
      WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_timestamp;
    IF NOT FOUND THEN
       RAISE EXCEPTION '_rlbk_tbl: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_timestamp;
    END IF;
-- insert begin event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_TABLE', 'BEGIN', v_fullTableName, 'Up to log_id ' || v_emaj_id);
-- deactivate the log trigger on the application table, if needed (unlogged rollback)
    IF v_disableTrigger THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
    END IF;
-- record the time at the rollback start
    SELECT clock_timestamp() INTO v_tsrlbk_start;
-- rollback the table
    EXECUTE 'SELECT ' || v_rlbkFnctName || '(' || v_emaj_id || ')' INTO v_nb_rows;
-- record the time at the rollback
    SELECT clock_timestamp() INTO v_tsrlbk_end;
-- insert rollback duration into the emaj_rlbk_stat table, if at least 1 row has been processed
    IF v_nb_rows > 0 THEN
      INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
         VALUES ('rlbk', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsrlbk_end - v_tsrlbk_start);
    END IF;
-- if the caller requires it, suppress the rollbacked log part 
    IF v_deleteLog THEN
-- record the time at the delete start
      SELECT clock_timestamp() INTO v_tsdel_start;
-- delete obsolete log rows
      EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_id >= ' || v_emaj_id;
-- ... and suppress from emaj_sequence table the rows regarding the emaj log sequence for this application table
--     corresponding to potential later intermediate marks that disappear with the rollback operation
      DELETE FROM emaj.emaj_sequence
        WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_id > v_lastSequenceId;
-- record the sequence holes generated by the delete operation 
-- this is due to the fact that log sequences are not rollbacked, this information will be used by the emaj_log_stat_group
--   function (and indirectly by emaj_estimate_rollback_duration())
-- first delete, if exist, sequence holes that have disappeared with the rollback
      DELETE FROM emaj.emaj_seq_hole
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName AND sqhl_id > v_lastSeqHoleId;
-- and then insert the new sequence hole
      EXECUTE 'INSERT INTO emaj.emaj_seq_hole (sqhl_schema, sqhl_table, sqhl_hole_size) VALUES (' 
        || quote_literal(v_schemaName) || ',' || quote_literal(v_tableName) || ', ('
        || ' SELECT CASE WHEN is_called THEN last_value + increment_by ELSE last_value END FROM ' 
        || v_emajSchema || '.' || v_fullSeqName || ')-('
        || ' SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END FROM '
        || ' emaj.emaj_sequence WHERE'
        || ' sequ_schema = ''' || v_emajSchema 
        || ''' AND sequ_name = ' || quote_literal(v_seqName) 
        || ' AND sequ_datetime = ' || quote_literal(v_timestamp) || '))';
-- record the time at the delete
      SELECT clock_timestamp() INTO v_tsdel_end;
-- insert delete duration into the emaj_rlbk_stat table, if at least 1 row has been processed
      IF v_nb_rows > 0 THEN
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
           VALUES ('del_log', v_schemaName, v_tableName, v_tsrlbk_start, v_nb_rows, v_tsdel_end - v_tsdel_start);
      END IF;
    END IF;
-- re-activate the log trigger on the application table, if previously disabled
    IF v_disableTrigger THEN
      EXECUTE 'ALTER TABLE ' || v_fullTableName || ' ENABLE TRIGGER ' || v_logTriggerName;
    END IF;
-- insert end event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('ROLLBACK_TABLE', 'END', v_fullTableName, v_nb_rows || ' rollbacked rows');
    RETURN;
  END;
$_rlbk_tbl$;

CREATE or REPLACE FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS 
$_rlbk_seq$
-- This function rollbacks one sequence to a given mark
-- The function is called by emaj.emaj._rlbk_groups_step7()
-- Input: schema name and table name, mark
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of the application sequence.
  DECLARE
    v_pgVersion     TEXT := emaj._pg_version();
    v_fullSeqName   TEXT;
    v_stmt          TEXT;
    mark_seq_rec    RECORD;
    curr_seq_rec    RECORD;

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
    v_fullSeqName := quote_ident(v_schemaName) || '.' || quote_ident(v_seqName);
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
-- if the caller requires it, delete the rollbacked intermediate sequences from the sequence table
    IF v_deleteLog THEN
      DELETE FROM emaj.emaj_sequence 
        WHERE sequ_schema = v_schemaName AND sequ_name = v_seqName AND sequ_id > v_lastSequenceId;
    END IF;
-- insert event in history
    INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) 
      VALUES ('ROLLBACK_SEQUENCE', v_fullSeqName, substr(v_stmt,2));
    RETURN;
  END;
$_rlbk_seq$;

CREATE or REPLACE FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) 
RETURNS BIGINT LANGUAGE plpgsql AS 
$_log_stat_table$
-- This function returns the number of log rows for a single table between 2 marks or between a mark and the current situation.
-- It is called by emaj_log_stat_group function
-- These statistics are computed using the serial id of log tables and holes is sequences recorded into emaj_seq_hole at rollback time
-- Input: schema name and table name, the timestamps of both marks, the emaj_seq_hole last id of both marks
--   a NULL value as last timestamp mark indicates the current situation
-- Output: number of log rows between both marks for the table
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_fullSeqName     TEXT;
    v_beginLastValue  BIGINT;
    v_endLastValue    BIGINT;
    v_sumHole         BIGINT;
  BEGIN
-- get the log table id at first mark time 
    SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_beginLastValue
       FROM emaj.emaj_sequence
       WHERE sequ_schema = v_emajSchema 
         AND sequ_name = v_schemaName || '_' || v_tableName || '_log_emaj_id_seq'
         AND sequ_datetime = v_tsFirstMark;
    IF v_tsLastMark IS NULL THEN
-- last mark is NULL, so examine the current state of the log table id
      v_fullSeqName := quote_ident(v_emajSchema) || '.' || quote_ident(v_schemaName || '_' || v_tableName || '_log_emaj_id_seq');
      EXECUTE 'SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END FROM ' || v_fullSeqName INTO v_endLastValue;
--   and count the sum of hole from the start mark time until now
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole 
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_id > v_firstLastSeqHoleId;
    ELSE
-- last mark is not NULL, so get the log table id at last mark time
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val ELSE sequ_last_val - sequ_increment END INTO v_endLastValue
         FROM emaj.emaj_sequence
         WHERE sequ_schema = v_emajSchema 
           AND sequ_name = v_schemaName || '_' || v_tableName || '_log_emaj_id_seq'
           AND sequ_datetime = v_tsLastMark;
--   and count the sum of hole from the start mark time to the end mark time
      SELECT coalesce(sum(sqhl_hole_size),0) INTO v_sumHole FROM emaj.emaj_seq_hole 
        WHERE sqhl_schema = v_schemaName AND sqhl_table = v_tableName
          AND sqhl_id > v_firstLastSeqHoleId AND sqhl_id <= v_lastLastSeqHoleId;
    END IF;
-- return the stat row for the table
    RETURN (v_endLastValue - v_beginLastValue - v_sumHole)/2;
  END;
$_log_stat_table$;

CREATE or REPLACE FUNCTION emaj.emaj_verify_all() 
RETURNS SETOF TEXT LANGUAGE plpgsql AS 
$emaj_verify_all$
-- The function verifies the consistency between all emaj objects present inside emaj schema and 
-- emaj objects related to tables and sequences referenced in emaj_relation table.
-- It returns a set of warning messages for discovered discrepancies. If no error is detected, a single row is returned.
  DECLARE
    v_emajSchema     TEXT := 'emaj';
    v_pgVersion      TEXT := emaj._pg_version();
    v_finalMsg       TEXT := 'Global checking: no error encountered';
    r_object         RECORD;
    r_group          RECORD;
  BEGIN
-- detect if the current postgres version is at least 8.1
    IF v_pgVersion < '8.2' THEN
      RETURN NEXT 'Global checking: the current postgres version (' || version() || ') is not compatible with E-Maj.';
      v_finalMsg = '';
    END IF;
-- detect log tables that don't correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Global checking: table ' || relname || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_class, pg_namespace
        WHERE relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname LIKE E'%\\_log'
          AND relname NOT IN (SELECT rel_schema || '_' || rel_tblseq || '_log' FROM emaj.emaj_relation) 
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- verify that all log, rollback and truncate functions correspond to a row in the groups table
    FOR r_object IN 
      SELECT 'Global checking: function ' || proname  || ' is not linked to an application table declared in the emaj_relation table' AS msg
        FROM pg_proc, pg_namespace
        WHERE pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND 
          ((proname LIKE E'%\\_log\\_fnct' AND proname NOT IN (
            SELECT rel_schema || '_' || rel_tblseq || '_log_fnct' FROM emaj.emaj_relation))
           OR
           (proname LIKE E'%\\_rlbk\\_fnct' AND proname NOT IN (
            SELECT rel_schema || '_' || rel_tblseq || '_rlbk_fnct' FROM emaj.emaj_relation))
          )
    LOOP
      RETURN NEXT r_object.msg;
      v_finalMsg = '';
    END LOOP;
-- final message for global check if no error has been yet detected 
    IF v_finalMsg <> '' THEN
      RETURN NEXT v_finalMsg;
    END IF;
-- verify all groups defined in emaj_group
    FOR r_group IN
      SELECT group_name FROM emaj.emaj_group ORDER BY 1
    LOOP 
      FOR r_object IN 
        SELECT msg FROM emaj._verify_group(r_group.group_name, false) msg
      LOOP
        RETURN NEXT r_object.msg;
      END LOOP;
    END LOOP;
    RETURN;
  END;
$emaj_verify_all$;
COMMENT ON FUNCTION emaj.emaj_verify_all() IS
$$Verifies the consistency between existing E-Maj and application objects.$$;

CREATE or REPLACE FUNCTION emaj._forbid_truncate_fnct() RETURNS TRIGGER AS $_forbid_truncate_fnct$
-- The function is triggered by the execution of TRUNCATE SQL verb on tables of a group in logging mode.
-- It can only be called with postgresql in a version greater or equal 8.4
BEGIN
  IF (TG_OP = 'TRUNCATE') THEN
    RAISE EXCEPTION 'emaj._forbid_truncate_fnct: TRUNCATE is not allowed while updates on this table (%) are currently protected by E-Maj. Consider stopping the group before issuing a TRUNCATE.', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
END;
$_forbid_truncate_fnct$ LANGUAGE plpgsql SECURITY DEFINER;

------------------------------------------------
----                                        ----
----       Functions to manage groups       ----
----                                        ----
------------------------------------------------

CREATE or REPLACE FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) 
RETURNS SETOF TEXT LANGUAGE plpgsql AS 
$_verify_group$
-- The function verifies the consistency between log and application tables for a group
-- Input: group name, boolean to specify if a detected error must raise an exception
-- If onErrorStop boolean is false, it returns a set of warning messages for discovered discrepancies.
-- If no error is detected, a single row is returned.
  DECLARE
    v_emajSchema        TEXT := 'emaj';
    v_pgVersion         TEXT := emaj._pg_version();
    v_finalMsg          TEXT;
    v_isRollbackable    BOOLEAN;
    v_creationPgVersion TEXT;
    v_msgPrefix         TEXT;
    v_msg               TEXT;
    v_fullTableName     TEXT;
    v_logTableName      TEXT;
    v_logFnctName       TEXT;
    v_rlbkFnctName      TEXT;
    v_logTriggerName    TEXT;
    v_truncTriggerName  TEXT;
    r_tblsq             RECORD;
  BEGIN
-- for 8.1-, E-Maj is not compatible
    IF v_pgVersion < '8.2' THEN
      RAISE EXCEPTION 'The current postgres version (%) is not compatible with E-Maj.', version();
    END IF;
-- get some characteristics of the group
    SELECT group_is_rollbackable, group_pg_version INTO v_isRollbackable, v_creationPgVersion 
      FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_verify_group: group % has not been created.', v_groupName;
    END IF;
-- Build message parts
    v_msgPrefix = 'Checking ' || v_groupName || ': ';
    v_finalMsg = v_msgPrefix || 'no error encountered';
-- check the postgres version at creation time is compatible with the current version
-- Warning: comparisons on version numbers are alphanumeric. 
--          But we suppose these tests will not be useful anymore when pg 10.0 will appear!
--   for 8.2 and 8.3, both major versions must be the same
    IF ((v_pgVersion = '8.2' OR v_pgVersion = '8.3') AND substring (v_creationPgVersion FROM E'(\\d+\\.\\d+)') <> v_pgVersion) OR
--   for 8.4+, both major versions must be 8.4+
       (v_pgVersion >= '8.4' AND substring (v_creationPgVersion FROM E'(\\d+\\.\\d+)') < '8.4') THEN
      v_msg = v_msgPrefix || 'the group has been created with a non compatible postgresql version (' || v_creationPgVersion || '). It must be dropped and recreated.';
      if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
      RETURN NEXT v_msg;
      v_finalMsg = '';
    END IF;
-- per table verifications
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
-- check the class is unchanged
      IF r_tblsq.rel_kind <> emaj._check_class(r_tblsq.rel_schema, r_tblsq.rel_tblseq) THEN
        v_msg = v_msgPrefix || 'the relation type for ' || r_tblsq.rel_schema || '.' || r_tblsq.rel_tblseq || ' has changed (was ''' || r_tblsq.rel_kind || ''' at emaj_create_group time).';
        if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
        RETURN NEXT v_msg;
        v_finalMsg = '';
      END IF;
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, ...
        v_logTableName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log';
        v_logFnctName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_fnct';
        v_rlbkFnctName     := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_rlbk_fnct';
        v_logTriggerName   := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg';
        v_truncTriggerName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg';
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
--   -> check boths functions exists
        PERFORM proname FROM pg_proc , pg_namespace WHERE 
          pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_logFnctName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log function ' || v_logFnctName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        END IF;
        IF v_isRollbackable THEN
          PERFORM proname FROM pg_proc , pg_namespace WHERE 
            pronamespace = pg_namespace.oid AND nspname = v_emajSchema AND proname = v_rlbkFnctName;
          IF NOT FOUND THEN
            v_msg = v_msgPrefix || 'rollback function ' || v_rlbkFnctName || ' not found.';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
--   -> check both triggers exist
        PERFORM tgname FROM pg_trigger WHERE tgname = v_logTriggerName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log trigger ' || v_logTriggerName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        END IF;
        IF v_pgVersion >= '8.4' THEN
          PERFORM tgname FROM pg_trigger WHERE tgname = v_truncTriggerName;
          IF NOT FOUND THEN
            v_msg = v_msgPrefix || 'truncate trigger ' || v_truncTriggerName || ' not found.';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
--   -> check the log table exists
        PERFORM relname FROM pg_class, pg_namespace WHERE 
          relnamespace = pg_namespace.oid AND nspname = v_emajSchema AND relkind = 'r' AND relname = v_logTableName;
        IF NOT FOUND THEN
          v_msg = v_msgPrefix || 'log table ' || v_logTableName || ' not found.';
          if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
          RETURN NEXT v_msg;
          v_finalMsg = '';
        ELSE
--   -> check that the log tables structure is consistent with the application tables structure
--      (same columns and same formats)
--      - added or changed column in application table
          PERFORM attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace 
            WHERE nspname = r_tblsq.rel_schema AND relnamespace = pg_namespace.oid AND relname = r_tblsq.rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false
          EXCEPT
          SELECT attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace
            WHERE nspname = v_emajSchema AND relnamespace = pg_namespace.oid AND relname = v_logTableName
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%';
          IF FOUND THEN
            v_msg = v_msgPrefix || 'the structure of log table ' || v_logTableName || ' is not coherent with ' || v_fullTableName || ' (added or changed column?).';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
--      - missing or changed column in application table
          PERFORM attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace
            WHERE nspname = v_emajSchema AND relnamespace = pg_namespace.oid AND relname = v_logTableName
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false AND attname NOT LIKE 'emaj%'
          EXCEPT
          SELECT attname, atttypid, attlen, atttypmod FROM pg_attribute, pg_class, pg_namespace 
            WHERE nspname = r_tblsq.rel_schema AND relnamespace = pg_namespace.oid AND relname = r_tblsq.rel_tblseq
              AND attrelid = pg_class.oid AND attnum > 0 AND attisdropped = false;
          IF FOUND THEN
            v_msg = v_msgPrefix || 'the structure of log table ' || v_logTableName || ' is not coherent with ' || v_fullTableName || ' (dropped or changed column?).';
            if v_onErrorStop THEN RAISE EXCEPTION '_verify_group: %',v_msg; END IF;
            RETURN NEXT v_msg;
            v_finalMsg = '';
          END IF;
        END IF;
-- if it is a sequence, nothing to do
      END IF;
    END LOOP;
    IF v_finalMsg <> '' THEN
-- OK, no error for the group
      RETURN NEXT v_finalMsg;
    END IF;
    RETURN;
  END;
$_verify_group$;

CREATE or REPLACE FUNCTION emaj._check_fk_groups(v_groupNames TEXT[]) 
RETURNS void LANGUAGE plpgsql AS 
$_check_fk_groups$
-- this function checks foreign key constraints for tables of a groups array:
-- Input: group names array
  DECLARE
    r_fk             RECORD;
  BEGIN
-- issue a warning if a table of the groups has a foreign key that references a table outside the groups
    FOR r_fk IN
      SELECT c.conname,r.rel_schema,r.rel_tblseq,nf.nspname,tf.relname 
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace nf,pg_class tf, emaj.emaj_relation r
        WHERE contype = 'f'                                      -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid   -- join for table and namespace 
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid  -- join for referenced table and namespace
                                                                 -- join with emaj_relation table
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq AND r.rel_group = ANY (v_groupNames)  
          AND (nf.nspname,tf.relname) NOT IN                     -- referenced table outside the groups
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames))
      LOOP
      RAISE WARNING '_check_fk_groups: Foreign key %, from table %.%, references %.% that is outside groups (%).',
                r_fk.conname,r_fk.rel_schema,r_fk.rel_tblseq,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
-- issue a warning if a table of the groups is referenced by a table outside the groups
    FOR r_fk IN
      SELECT c.conname,n.nspname,t.relname,r.rel_schema,r.rel_tblseq 
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace nf,pg_class tf, emaj.emaj_relation r
        WHERE contype = 'f'                                      -- FK constraints only
          AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid   -- join for table and namespace 
          AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid  -- join for referenced table and namespace
                                                                 -- join with emaj_relation table
          AND nf.nspname = r.rel_schema AND tf.relname = r.rel_tblseq AND r.rel_group = ANY (v_groupNames)
          AND (n.nspname,t.relname) NOT IN                       -- referenced table outside the groups
              (SELECT rel_schema,rel_tblseq FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames))
      LOOP
      RAISE WARNING '_check_fk_groups: table %.% is referenced by foreign key % from table %.% that is outside groups (%).',
                r_fk.rel_schema,r_fk.rel_tblseq,r_fk.conname,r_fk.nspname,r_fk.relname,array_to_string(v_groupNames,',');
    END LOOP;
    RETURN;
  END;
$_check_fk_groups$;

CREATE or REPLACE FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN)
RETURNS void LANGUAGE plpgsql AS 
$_lock_groups$
-- This function locks all tables of a groups array. 
-- The lock mode is provided by the calling function
-- Input: array of group names, lock mode, flag indicating whether the function is called to processed several groups
  DECLARE
    v_nbRetry       SMALLINT := 0;
    v_nbTbl         INT;
    v_ok            BOOLEAN := false;
    v_fullTableName TEXT;
    v_mode          TEXT;
    r_tblsq         RECORD;
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
-- scan all tables of the group
        v_nbTbl = 0;
        FOR r_tblsq IN
            SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation 
               WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' 
               ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
-- lock the table
          v_fullTableName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
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

CREATE or REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT) 
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

CREATE or REPLACE FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_create_group$
-- This function creates emaj objects for all tables of a group
-- Input: group name, boolean indicating wether the group is rollbackable or not
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTbl         INT := 0;
    v_nbSeq         INT := 0;
    v_msg           TEXT;
    v_relkind       TEXT;
    v_stmt          TEXT;
    v_nb_trg        INT;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('CREATE_GROUP', 'BEGIN', v_groupName, CASE WHEN v_isRollbackable THEN 'rollbackable' ELSE 'audit_only' END);
-- check that the group name is valid (not '' and not containing ',' characters)
    IF v_groupName IS NULL THEN
      RAISE EXCEPTION 'emaj_create_group: group name can''t be NULL.';
    END IF;
    IF v_groupName = '' THEN
      RAISE EXCEPTION 'emaj_create_group: group name must at least contain 1 character.';
    END IF;
-- check that the group is not yet recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF FOUND THEN
      RAISE EXCEPTION 'emaj_create_group: group % is already created.', v_groupName;
    END IF;
-- check that no table or sequence of the new group already belong to another created group
    v_msg = '';
    FOR r_tblsq IN
        SELECT grpdef_schema, grpdef_tblseq, rel_group FROM emaj.emaj_group_def, emaj.emaj_relation
          WHERE grpdef_schema = rel_schema AND grpdef_tblseq = rel_tblseq AND grpdef_group = v_groupName
      LOOP
      IF v_msg <> '' THEN
        v_msg = v_msg || ', ';
      END IF;
      v_msg = v_msg || r_tblsq.grpdef_schema || '.' || r_tblsq.grpdef_tblseq || ' in ' || r_tblsq.rel_group;
    END LOOP;
    IF v_msg <> '' THEN
      RAISE EXCEPTION 'emaj_create_group: one or several tables already belong to another group (%).', v_msg;
    END IF;
-- OK, insert group row in the emaj_group table
    INSERT INTO emaj.emaj_group (group_name, group_state, group_is_rollbackable) VALUES (v_groupName, 'IDLE',v_isRollbackable);
-- scan all classes of the group (in priority order, NULLS being processed last)
    FOR r_tblsq IN
        SELECT grpdef_priority, grpdef_schema, grpdef_tblseq FROM emaj.emaj_group_def 
          WHERE grpdef_group = v_groupName ORDER BY grpdef_priority, grpdef_schema, grpdef_tblseq
        LOOP
-- check the class is valid
      v_relkind = emaj._check_class(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq);
      IF v_relkind = 'r' THEN
-- if it is a table, create the related emaj objects
         PERFORM emaj._create_tbl(r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_isRollbackable);
         v_nbTbl = v_nbTbl + 1;
        ELSEIF v_relkind = 'S' THEN
-- if it is a sequence, just count
         v_nbSeq = v_nbSeq + 1;
      END IF;
-- record this table or sequence in the emaj_relation table
      INSERT INTO emaj.emaj_relation (rel_schema, rel_tblseq, rel_group, rel_priority, rel_kind)
          VALUES (r_tblsq.grpdef_schema, r_tblsq.grpdef_tblseq, v_groupName, r_tblsq.grpdef_priority, v_relkind);
    END LOOP;
    IF v_nbTbl + v_nbSeq = 0 THEN
       RAISE EXCEPTION 'emaj_create_group: Group % is unknown in emaj_relation table.', v_groupName;
    END IF;
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

CREATE or REPLACE FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT) 
RETURNS void LANGUAGE plpgsql AS
$emaj_comment_group$
-- This function sets or modifies a comment on a group by updating the group_comment of the emaj_group table.
-- Input: group name, comment
--   To reset an existing comment for a group, the supplied comment can be NULL.
  DECLARE
    v_groupState     TEXT;
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

CREATE or REPLACE FUNCTION emaj.emaj_drop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_drop_group$
-- This function deletes the emaj objects for all tables of a group
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb          INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, TRUE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_drop_group$;
COMMENT ON FUNCTION emaj.emaj_drop_group(TEXT) IS
$$Drops an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_force_drop_group$
-- This function deletes the emaj objects for all tables of a group.
-- It differs from emaj_drop_group by the fact that no check is done on group's state.
-- This allows to drop a group that is not consistent, following hasardeous operations.
-- This function should not be used, except if the emaj_drop_group fails. 
-- Input: group name
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTb          INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('FORCE_DROP_GROUP', 'BEGIN', v_groupName);
-- effectively drop the group
    SELECT emaj._drop_group(v_groupName, FALSE) INTO v_nbTb;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('FORCE_DROP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_force_drop_group$;
COMMENT ON FUNCTION emaj.emaj_force_drop_group(TEXT) IS
$$Drops an E-Maj group, even in LOGGING state.$$;

CREATE or REPLACE FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS 
$_drop_group$
-- This function effectively deletes the emaj objects for all tables of a group
-- Input: group name, and a boolean indicating whether the group's state has to be checked 
-- Output: number of processed tables and sequences
  DECLARE
    v_groupState     TEXT;
    v_isRollbackable BOOLEAN;
    v_nbTb           INT := 0;
    r_tblsq          RECORD; 
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state, group_is_rollbackable INTO v_groupState, v_isRollbackable FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION '_drop_group: group % has not been created.', v_groupName;
    END IF;
-- if the state of the group has to be checked,
    IF v_checkState THEN
--   check that the group is IDLE (i.e. not in a LOGGING) state
      IF v_groupState <> 'IDLE' THEN
        RAISE EXCEPTION '_drop_group: The group % cannot be deleted because it is not in idle state.', v_groupName;
      END IF;
    END IF;
-- OK, delete the emaj objets for each table of the group
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, delete the related emaj objects
        PERFORM emaj._drop_tbl (r_tblsq.rel_schema, r_tblsq.rel_tblseq, v_isRollbackable);
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
          PERFORM emaj._drop_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- delete group rows from the emaj_fk table.
    DELETE FROM emaj.emaj_fk WHERE v_groupName = ANY (fk_groups);
-- delete group row from the emaj_group table.
--   By cascade, it also deletes rows from emaj_relation and emaj_mark
    DELETE FROM emaj.emaj_group WHERE group_name = v_groupName;
    RETURN v_nbTb;
  END;
$_drop_group$;

CREATE or REPLACE FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_start_group$
-- This function activates the log triggers of all the tables for a group and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: group name, name of the mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_nbTblSeq         INT;
  BEGIN
-- purge the emaj history, if needed
    PERFORM emaj._purge_hist();
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('START_GROUP', 'BEGIN', v_groupName);
-- call the common _start_groups function
    SELECT emaj._start_groups(array[v_groupName], v_mark, false, true) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('START_GROUP', 'END', v_groupName, v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_start_group$;
COMMENT ON FUNCTION emaj.emaj_start_group(TEXT,TEXT) IS
$$Starts an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_start_groups$
-- This function activates the log triggers of all the tables for a groups array and set a first mark
-- It also delete oldest rows in emaj_hist table
-- Input: array of group names, name of the mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: total number of processed tables and sequences
  DECLARE
    v_nbTblSeq         INT;
  BEGIN
-- purge the emaj history, if needed
    PERFORM emaj._purge_hist();
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('START_GROUPS', 'BEGIN', array_to_string(v_groupNames,','));
-- call the common _start_groups function
	    SELECT emaj._start_groups(emaj._check_group_names_array(v_groupNames), v_mark, true, true) INTO v_nbTblSeq;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('START_GROUPS', 'END', array_to_string(v_groupNames,','), v_nbTblSeq || ' tables/sequences processed');
    RETURN v_nbTblSeq;
  END;
$emaj_start_groups$;
COMMENT ON FUNCTION emaj.emaj_start_groups(TEXT[],TEXT) IS
$$Starts several E-Maj groups.$$;

CREATE or REPLACE FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_reset BOOLEAN) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$_start_groups$
-- This function activates the log triggers of all the tables for one or several groups and set a first mark
-- Input: array of group names, name of the mark to set, boolean indicating whether the function is called by a multi group function, boolean indicating whether the function must reset the group at start time 
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion        TEXT := emaj._pg_version();
    v_i                INT;
    v_groupState       TEXT;
    v_nbTb             INT := 0;
    v_markName         TEXT;
    v_logTableName     TEXT;
    v_fullTableName    TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    v_cpt              BIGINT;
    r_tblsq            RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- check that each group is recorded in emaj_group table
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_start_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... and is in IDLE (i.e. not in a LOGGING) state
      IF v_groupState <> 'IDLE' THEN
        RAISE EXCEPTION '_start_group: The group % cannot be started because it is not in idle state. An emaj_stop_group function must be previously executed.', v_groupNames[v_i];
      END IF;
-- ... and is not damaged
      PERFORM 0 FROM emaj._verify_group(v_groupNames[v_i], true);
    END LOOP;
-- check and process the supplied mark name
-- (the group names array is set to NULL to not check the existence of the mark against groups as groups may have old deleted marks)
    SELECT emaj._check_new_mark(v_mark, NULL) INTO v_markName;
-- for each group, 
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      if v_reset THEN
-- ... if requested by the user, call the emaj_reset_group function to erase remaining traces from previous logs
        SELECT emaj._rst_group(v_groupNames[v_i]) INTO v_nbTb;
        IF v_nbTb = 0 THEN
          RAISE EXCEPTION '_start_group: Internal error - emaj_res_group for group % returned 0.', v_groupNames[v_i];
        END IF;
      END IF;
-- ... and check foreign keys with tables outside the group
      PERFORM emaj._check_fk_groups(array[v_groupNames[v_i]]);
    END LOOP;
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
    PERFORM emaj._lock_groups(v_groupNames,'',v_multiGroup);
-- ... and enable all log triggers for the group
    v_nbTb = 0;
-- for each relation of the group,
    FOR r_tblsq IN
       SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
         WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
       LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, enable the emaj log and truncate triggers
        v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
        v_logTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
        v_truncTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
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
    UPDATE emaj.emaj_group SET group_state = 'LOGGING' WHERE group_name = ANY (v_groupNames);
-- Set the first mark for each group
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_markName);
    PERFORM emaj._set_mark_groups(v_groupNames, v_markName);
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_markName);
--
    RETURN v_nbTb;
  END;
$_start_groups$;

CREATE or REPLACE FUNCTION emaj.emaj_stop_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_stop_group$
-- This function de-activates the log triggers of all the tables for a group. 
-- Execute several emaj_stop_group functions for the same group doesn't produce any error.
-- Input: group name
-- Output: number of processed tables and sequences
  BEGIN
-- just call the common _stop_groups function
    RETURN emaj._stop_groups(array[v_groupName], false);
  END;
$emaj_stop_group$;
COMMENT ON FUNCTION emaj.emaj_stop_group(TEXT) IS
$$Stops an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]) 
RETURNS INT LANGUAGE plpgsql AS 
$emaj_stop_groups$
-- This function de-activates the log triggers of all the tables for a groups array.
-- Groups already in IDDLE state are simply not processed.
-- Input: array of group names
-- Output: number of processed tables and sequences
  BEGIN
-- just call the common _stop_groups function
    RETURN emaj._stop_groups(emaj._check_group_names_array(v_groupNames), true);
  END;
$emaj_stop_groups$;
COMMENT ON FUNCTION emaj.emaj_stop_groups(TEXT[]) IS
$$Stops several E-Maj groups.$$;

CREATE or REPLACE FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$_stop_groups$
-- This function effectively de-activates the log triggers of all the tables for a group. 
-- Input: array of group names, boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables and sequences.
  DECLARE
    v_pgVersion        TEXT := emaj._pg_version();
    v_validGroupNames  TEXT[];
    v_i                INT;
    v_groupState       TEXT;
    v_nbTb             INT := 0;
    v_fullTableName    TEXT;
    v_logTriggerName   TEXT;
    v_truncTriggerName TEXT;
    r_tblsq            RECORD;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS' ELSE 'STOP_GROUP' END, 'BEGIN', 
              array_to_string(v_groupNames,','));
-- for each group of the array,
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
      SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_stop_group: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF v_groupState <> 'LOGGING' THEN
        RAISE WARNING '_stop_group: Group % cannot be stopped because it is not in logging state.', v_groupNames[v_i];
      ELSE
-- ... if OK, add the group into the array of groups to process
        v_validGroupNames = v_validGroupNames || array[v_groupNames[v_i]];
      END IF;
    END LOOP;
    IF v_validGroupNames IS NOT NULL THEN
-- OK, lock all tables to get a stable point ...
-- (the ALTER TABLE statements will also set EXCLUSIVE locks, but doing this for all tables at the beginning of the operation decreases the risk for deadlock)
      PERFORM emaj._lock_groups(v_validGroupNames,'',v_multiGroup);
-- for each relation of the groups to process,
      FOR r_tblsq IN
          SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
            WHERE rel_group = ANY (v_validGroupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
          LOOP
        IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, disable the emaj log and truncate triggers
          v_fullTableName  := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          v_logTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_log_trg');
          v_truncTriggerName := quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_emaj_trunc_trg');
          EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_logTriggerName;
          IF v_pgVersion >= '8.4' THEN
            EXECUTE 'ALTER TABLE ' || v_fullTableName || ' DISABLE TRIGGER ' || v_truncTriggerName;
          END IF;
          ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, nothing to do
        END IF;
        v_nbTb = v_nbTb + 1;
      END LOOP;
-- set all marks for the groups from the emaj_mark table in 'DELETED' state to avoid any further rollback
      UPDATE emaj.emaj_mark SET mark_state = 'DELETED' WHERE mark_group = ANY (v_validGroupNames) AND mark_state <> 'DELETED';
-- update the state of the groups rows from the emaj_group table
      UPDATE emaj.emaj_group SET group_state = 'IDLE' WHERE group_name = ANY (v_validGroupNames);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'STOP_GROUPS' ELSE 'STOP_GROUP' END, 'END', 
              array_to_string(v_groupNames,','), v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$_stop_groups$;

CREATE or REPLACE FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_group$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the group
-- Input: group name, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_groupState    TEXT;
    v_markName      TEXT;
    v_nbTb          INT;
  BEGIN
-- insert begin into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'BEGIN', v_groupName, v_markName);
-- check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_set_mark_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is in LOGGING state
    IF v_groupState <> 'LOGGING' THEN
      RAISE EXCEPTION 'emaj_set_mark_group: A mark cannot be set for group % because it is not in logging state. An emaj_start_group function must be previously executed.', v_groupName;
    END IF;
-- check if the emaj group is OK
    PERFORM 0 FROM emaj._verify_group(v_groupName, true);
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, array[v_groupName]) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(array[v_groupName],'ROW EXCLUSIVE',false);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(array[v_groupName], v_markName) into v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUP', 'END', v_groupName, v_markName);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_group$;
COMMENT ON FUNCTION emaj.emaj_set_mark_group(TEXT,TEXT) IS
$$Sets a mark on an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$emaj_set_mark_groups$
-- This function inserts a mark in the emaj_mark table and takes an image of the sequences definitions for several groups at a time
-- Input: array of group names, mark to set
--        '%' wild characters in mark name are transformed into a characters sequence built from the current timestamp
--        a null or '' mark is transformed into 'MARK_%'
-- Output: number of processed tables and sequences
  DECLARE
    v_validGroupNames TEXT[];
    v_groupState      TEXT;
    v_markName        TEXT;
    v_nbTb            INT;
  BEGIN
-- validate the group names array
    v_validGroupNames=emaj._check_group_names_array(v_groupNames);
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
    FOR v_i in 1 .. array_upper(v_validGroupNames,1) LOOP
-- ... check that the group is recorded in emaj_group table
-- (the SELECT is coded FOR UPDATE to lock the accessed group, avoiding any operation on this group at the same time)
      SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_validGroupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: group % has not been created.', v_validGroupNames[v_i];
      END IF;
-- ... check that the group is in LOGGING state
      IF v_groupState <> 'LOGGING' THEN
        RAISE EXCEPTION 'emaj_set_mark_groups: A mark cannot be set for group % because it is not in logging state. An emaj_start_group function must be previously executed.', v_validGroupNames[v_i];
      END IF;
-- ... check if the group is OK
      PERFORM 0 FROM emaj._verify_group(v_validGroupNames[v_i], true);
    END LOOP;
-- check and process the supplied mark name
    SELECT emaj._check_new_mark(v_mark, v_validGroupNames) INTO v_markName;
-- OK, lock all tables to get a stable point ...
-- use a ROW EXCLUSIVE lock mode, preventing for a transaction currently updating data, but not conflicting with simple read access or vacuum operation.
    PERFORM emaj._lock_groups(v_validGroupNames,'ROW EXCLUSIVE',true);
-- Effectively set the mark using the internal _set_mark_groups() function
    SELECT emaj._set_mark_groups(v_validGroupNames, v_markName) into v_nbTb;
-- insert end into the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SET_MARK_GROUPS', 'END', array_to_string(v_groupNames,','), v_mark);
--
    RETURN v_nbTb;
  END;
$emaj_set_mark_groups$;
COMMENT ON FUNCTION emaj.emaj_set_mark_groups(TEXT[],TEXT) IS 
$$Sets a mark on several E-Maj groups.$$;

CREATE or REPLACE FUNCTION emaj._set_mark_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS int LANGUAGE plpgsql AS
$_set_mark_groups$
-- This function effectively inserts a mark in the emaj_mark table and takes an image of the sequences definitions for the array of groups.
-- It is called by emaj_set_mark_group and emaj_set_mark_groups functions but also by other functions that set internal marks, like functions that start or rollback groups.
-- Input: group names array, mark to set, boolean indicating if the function is called by a multi group function
-- Output: number of processed tables and sequences
-- The insertion of the corresponding event in the emaj_hist table is performed by callers.
  DECLARE
    v_pgVersion       TEXT := emaj._pg_version();
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    v_timestamp       TIMESTAMPTZ;
    v_lastSequenceId  BIGINT;
    v_lastSeqHoleId   BIGINT;
    v_fullSeqName     TEXT;
    v_seqName         TEXT;
    v_stmt            TEXT;
    r_tblsq           RECORD;
  BEGIN
-- look at the clock to get the 'official' timestamp representing the mark
    v_timestamp = clock_timestamp();
-- for each member of the groups, ...
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = ANY (v_groupNames) ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- ... if it is a table, record the emaj_id associated sequence parameters in the emaj sequence table
        v_seqName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_fullSeqName := quote_ident(v_emajSchema) || '.' || quote_ident(v_seqName);
        v_stmt = 'INSERT INTO emaj.emaj_sequence (' ||
                 'sequ_schema, sequ_name, sequ_datetime, sequ_mark, sequ_last_val, sequ_start_val, ' || 
                 'sequ_increment, sequ_max_val, sequ_min_val, sequ_cache_val, sequ_is_cycled, sequ_is_called ' ||
                 ') SELECT '|| quote_literal(v_emajSchema) || ', ' || quote_literal(v_seqName) || ', ' ||
                 quote_literal(v_timestamp) || ', ' || quote_literal(v_mark) || ', ' || 'last_value, ';
        IF v_pgVersion <= '8.3' THEN
           v_stmt = v_stmt || '0, ';
        ELSE
           v_stmt = v_stmt || 'start_value, ';
        END IF;
        v_stmt = v_stmt || 
                 'increment_by, max_value, min_value, cache_value, is_cycled, is_called ' ||
                 'FROM ' || v_fullSeqName;
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- ... if it is a sequence, record the sequence parameters in the emaj sequence table
        v_fullSeqName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
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
      END IF;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- record the marks
-- get the last id for emaj_sequence and emaj_seq_hole tables insert the marks into the emaj_mark table
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSequenceId 
      FROM emaj.emaj_sequence_sequ_id_seq;
    SELECT CASE WHEN is_called THEN last_value ELSE last_value - increment_by END INTO v_lastSeqHoleId 
      FROM emaj.emaj_seq_hole_sqhl_id_seq;
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      INSERT INTO emaj.emaj_mark (mark_group, mark_name, mark_datetime, mark_state, mark_last_sequence_id, mark_last_seq_hole_id) 
        VALUES (v_groupNames[v_i], v_mark, v_timestamp, 'ACTIVE', v_lastSequenceId, v_lastSeqHoleId);
    END LOOP;
-- 
    RETURN v_nbTb;
  END;
$_set_mark_groups$;

CREATE or REPLACE FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT) 
RETURNS void LANGUAGE plpgsql AS
$emaj_comment_mark_group$
-- This function sets or modifies a comment on a mark by updating the mark_comment of the emaj_mark table.
-- Input: group name, mark to comment, comment
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
--   To reset an existing comment for a mark, the supplied comment can be NULL.
  DECLARE
    v_groupState     TEXT;
    v_realMark       TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName;
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

CREATE or REPLACE FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) 
RETURNS text LANGUAGE plpgsql AS
$emaj_get_previous_mark_group$
-- This function returns the name of the mark that immediately precedes a given date and time.
-- It may return unpredictable result in case of system date or time change.
-- The function can be called by both emaj_adm and emaj_viewer roles.
-- Input: group name, date and time
-- Output: mark name, or NULL if there is no mark before the given date and time
  DECLARE
    v_groupState    TEXT;
    v_markName      TEXT;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
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

CREATE or REPLACE FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS integer LANGUAGE plpgsql AS
$emaj_delete_mark_group$
-- This function deletes all traces from a previous set_mark_group(s) function. 
-- Then, any rollback on the deleted mark will not be possible.
-- It deletes rows corresponding to the mark to delete from emaj_mark and emaj_sequence 
-- If this mark is the first mark, it also deletes rows from all concerned log tables and holes from emaj_seq_hole.
-- At least one mark must remain after the operation (otherwise it is not worth having a group in LOGGING state !).
-- Input: group name, mark to delete
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to delete to specify the last set mark.
-- Output: number of deleted marks, i.e. 1
  DECLARE
    v_groupState     TEXT;
    v_realMark       TEXT;
    v_markId         BIGINT;
    v_datetimeMark   TIMESTAMPTZ;
    v_idNewMin       BIGINT;
    v_markNewMin     TEXT;
    v_datetimeNewMin TIMESTAMPTZ;
    v_cpt            INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
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
          AND sequ_schema = 'emaj' AND sequ_name = rel_schema || '_' || rel_tblseq || '_log_emaj_id_seq';
--   ... and the mark to delete can be physicaly deleted
      DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_MARK_GROUP', 'END', v_groupName, v_mark);
    RETURN 1;
  END;
$emaj_delete_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_mark_group(TEXT,TEXT) IS
$$Deletes a mark for an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) 
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
    v_groupState     TEXT;
    v_realMark       TEXT;
    v_markId         BIGINT;
    v_datetimeMark   TIMESTAMPTZ;
    v_nbMark         INT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
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
      VALUES ('DELETE_BEFORE_MARK_GROUP', 'END', v_groupName,  v_nbMark || ' marks deleted');
    RETURN v_nbMark;
  END;
$emaj_delete_before_mark_group$;
COMMENT ON FUNCTION emaj.emaj_delete_before_mark_group(TEXT,TEXT) IS
$$Deletes all marks preceeding a given mark for an E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT) 
RETURNS integer LANGUAGE plpgsql AS
$_delete_before_mark_group$
-- This function effectively deletes all marks set before a given mark. 
-- It deletes rows corresponding to the marks to delete from emaj_mark, emaj_sequence, emaj_seq_hole.  
-- It also deletes rows from all concerned log tables.
-- Input: group name, name of the new first mark
-- Output: number of deleted marks
  DECLARE
    v_markId         BIGINT;
    v_datetimeMark   TIMESTAMPTZ;
    v_emajSchema     TEXT := 'emaj';
    v_seqName        TEXT;
    v_logTableName   TEXT;
    v_emaj_id        BIGINT;
    v_nbMark         INT;
    r_tblsq          RECORD;
  BEGIN
-- retrieve the id and datetime of the new first mark
    SELECT mark_id, mark_datetime INTO v_markId, v_datetimeMark
      FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_mark;
-- delete rows from all log tables
-- loop on all tables of the group
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_priority, rel_schema, rel_tblseq
    LOOP
      v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
      v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- get the emaj_id corresponding to the new first mark
      SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_emaj_id
        FROM emaj.emaj_sequence
        WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_datetimeMark;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_delete_before_mark_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_datetime;
      END IF;
-- delete log rows prior to the new first mark
      EXECUTE 'DELETE FROM ' || v_logTableName || ' WHERE emaj_id < ' || v_emaj_id;
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
        AND sequ_schema = 'emaj' AND sequ_name = rel_schema || '_' || rel_tblseq || '_log_emaj_id_seq'
        AND (sequ_mark, sequ_datetime) IN 
            (SELECT mark_name, mark_datetime FROM emaj.emaj_mark 
              WHERE mark_group = v_groupName AND mark_id < v_markId);
-- and finaly delete marks
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_id < v_markId;
    GET DIAGNOSTICS v_nbMark = ROW_COUNT;
    RETURN v_nbMark;
  END;
$_delete_before_mark_group$;

CREATE or REPLACE FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT)
RETURNS void LANGUAGE plpgsql AS
$emaj_rename_mark_group$
-- This function renames an existing mark.
-- The group can be either in LOGGING or IDLE state.
-- Rows from emaj_mark and emaj_sequence tables are updated accordingly.
-- Input: group name, mark to rename, new name for the mark
--   The keyword 'EMAJ_LAST_MARK' can be used as mark to rename to specify the last set mark.
  DECLARE
    v_realMark       TEXT;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('RENAME_MARK_GROUP', 'BEGIN', v_groupName, v_mark);
-- check that the group is recorded in emaj_group table
    PERFORM 1 FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: group % has not been created.', v_groupName;
    END IF;
-- retrieve and check the mark name
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_realMark;
    IF v_realMark IS NULL THEN
      RAISE EXCEPTION 'emaj_rename_mark_group: mark % doesn''t exist for group %.', v_mark, v_groupName;
    END IF;
-- check the new mark name is not 'EMAJ_LAST_MARK'
    IF v_newName = 'EMAJ_LAST_MARK' THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: % is not an allowed name for a new mark.', v_newName;
    END IF;
-- check if the new mark name doesn't exist for the group 
    PERFORM 1 FROM emaj.emaj_mark
      WHERE mark_group = v_groupName AND mark_name = v_newName LIMIT 1;
    IF FOUND THEN
       RAISE EXCEPTION 'emaj_rename_mark_group: a mark % already exists for group %.', v_newName, v_groupName;
    END IF;
-- OK, update the sequences table as well
    UPDATE emaj.emaj_sequence SET sequ_mark = v_newName 
      WHERE sequ_datetime = (SELECT mark_datetime FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realMark);
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

CREATE or REPLACE FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the group, with log table deletion
--   (with boolean: unloggedRlbk = true, deleteLog = true, multiGroup = false)
    return emaj._rlbk_groups(array[v_groupName], v_mark, true, true, false);
  END;
$emaj_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_group(TEXT,TEXT) IS
$$Rollbacks an E-Maj group to a given mark.$$;

CREATE or REPLACE FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history
-- Input: array of group names, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just (unlogged) rollback the groups, with log table deletion
--   (with boolean: unloggedRlbk = true, deleteLog = true, multiGroup = true)
    return emaj._rlbk_groups(emaj._check_group_names_array(v_groupNames), v_mark, true, true, true);
  END;
$emaj_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_groups(TEXT[],TEXT) IS
$$Rollbacks an set of E-Maj groups to a given mark.$$;

CREATE or REPLACE FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_and_stop_group$
-- The function rollbacks all tables and sequences of a group up to a mark in the history
-- and then stop the group without deleting log tables.
-- Input: group name, mark to rollback to
-- Output: number of tables and sequences processed by the rollback function
  DECLARE
    v_ret_rollback   INT;
    v_ret_stop       INT;
  BEGIN
-- (unlogged) rollback the group without log table deletion
--   (with boolean: unloggedRlbk = true, deleteLog = false, multiGroup = false)
    SELECT emaj._rlbk_groups(array[v_groupName], v_mark, true, false, false) INTO v_ret_rollback;
-- and stop
    SELECT emaj.emaj_stop_group(v_groupName) INTO v_ret_stop;
-- return the number of rollbacked tables and sequences
    RETURN v_ret_rollback;
  END;
$emaj_rollback_and_stop_group$;
COMMENT ON FUNCTION emaj.emaj_rollback_and_stop_group(TEXT,TEXT) IS
$$Rollbacks an E-Maj group to a given mark and stops the group.$$;

CREATE or REPLACE FUNCTION emaj.emaj_rollback_and_stop_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_rollback_and_stop_groups$
-- The function rollbacks all tables and sequences of a group array up to a mark in the history
-- and then stop the group array without deleting log tables.
-- Input: array of group names, mark to rollback to
-- Output: number of tables and sequences processed by the rollback function
  DECLARE
    v_ret_rollback    INT;
    v_ret_stop        INT;
    v_validGroupNames TEXT[];
  BEGIN
-- check mark names array
    v_validGroupNames = emaj._check_group_names_array(v_groupNames);
-- (unlogged) rollback the groups without log table deletion
--   (with boolean: unloggedRlbk = true, deleteLog = false, multiGroup = true)
    SELECT emaj._rlbk_groups(v_validGroupNames, v_mark, true, false, true) INTO v_ret_rollback;
-- and stop them
    SELECT emaj.emaj_stop_groups(v_validGroupNames) INTO v_ret_stop;
-- return the number of rollbacked tables and sequences
    RETURN v_ret_rollback;
  END;
$emaj_rollback_and_stop_groups$;
COMMENT ON FUNCTION emaj.emaj_rollback_and_stop_groups(TEXT[],TEXT) IS
$$Rollbacks an E-Maj set of groups to a given mark and stops the groups.$$;

CREATE or REPLACE FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_group$
-- The function performs a logged rollback of all tables and sequences of a group up to a mark in the history.
-- A logged rollback is a rollback which can be later rollbacked! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rollbacked log rows and any marks inside the rollback time frame are kept.
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the group, with log table deletion
--   (with boolean: unloggedRlbk = false, deleteLog = false, multiGroup = false)
    return emaj._rlbk_groups(array[v_groupName], v_mark, false, false, false);
  END;
$emaj_logged_rollback_group$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_group(TEXT,TEXT) IS
$$Performs a logged (cancellable) rollbacks of an E-Maj group to a given mark.$$;

CREATE or REPLACE FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT) 
RETURNS INT LANGUAGE plpgsql AS
$emaj_logged_rollback_groups$
-- The function performs a logged rollback of all tables and sequences of a groups array up to a mark in the history.
-- A logged rollback is a rollback which can be later rollbacked! To achieve this:
-- - log triggers are not disabled at rollback time,
-- - a mark is automaticaly set at the beginning and at the end of the rollback operation,
-- - rollbacked log rows and any marks inside the rollback time frame are kept.
-- Input: array of group names, mark in the history, as it is inserted by emaj.emaj_set_mark_group(s)
-- Output: number of processed tables and sequences
  BEGIN
-- just "logged-rollback" the groups, with log table deletion
--   (with boolean: unloggedRlbk = false, deleteLog = false, multiGroup = true)
    return emaj._rlbk_groups(emaj._check_group_names_array(v_groupNames), v_mark, false, false, true);
  END;
$emaj_logged_rollback_groups$;
COMMENT ON FUNCTION emaj.emaj_logged_rollback_groups(TEXT[],TEXT) IS
$$Performs a logged (cancellable) rollbacks for a set of E-Maj groups to a given mark.$$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups$
-- The function rollbacks all tables and sequences of a groups array up to a mark in the history.
-- It is called by emaj_rollback_group and emaj_rollback_and_stop_group.
-- It effectively manages the rollback operation for each table or sequence, deleting rows from log tables 
-- only when asked by the calling functions.
-- Its activity is split into 6 smaller functions that are also called by the parallel restore php function
-- Input: group name, mark in the history, as it is inserted by emaj.emaj_set_mark_group
--        and a boolean saying if the log rows must be deleted from log table or not
-- Output: number of tables and sequences effectively processed
  DECLARE
    v_nbTbl             INT;
    v_nbTblInGroup      INT;
    v_nbSeq             INT;
  BEGIN
-- if the group names array is null, immediately return 0
    IF v_groupNames IS NULL THEN
      RETURN 0;
    END IF;
-- Step 1: prepare the rollback operation
    SELECT emaj._rlbk_groups_step1(v_groupNames, v_mark, v_unloggedRlbk, 1, v_multiGroup) INTO v_nbTblInGroup;
-- Step 2: lock all tables
    PERFORM emaj._rlbk_groups_step2(v_groupNames, 1, v_multiGroup);
-- Step 3: set a rollback start mark if logged rollback
    PERFORM emaj._rlbk_groups_step3(v_groupNames, v_mark, v_unloggedRlbk, v_multiGroup);
-- Step 4: record and drop foreign keys
    PERFORM emaj._rlbk_groups_step4(v_groupNames, 1);
-- Step 5: effectively rollback tables
    SELECT emaj._rlbk_groups_step5(v_groupNames, v_mark, 1, v_unloggedRlbk, v_deleteLog) INTO v_nbTbl;
-- checks that we have the expected number of processed tables
    IF v_nbTbl <> v_nbTblInGroup THEN
       RAISE EXCEPTION '_rlbk_group: Internal error 1 (%,%).',v_nbTbl,v_nbTblInGroup;
    END IF;
-- Step 6: recreate foreign keys
    PERFORM emaj._rlbk_groups_step6(v_groupNames, 1);
-- Step 7: process sequences and complete the rollback operation record
    SELECT emaj._rlbk_groups_step7(v_groupNames, v_mark, v_nbTbl, v_unloggedRlbk, v_deleteLog, v_multiGroup) INTO v_nbSeq;
    RETURN v_nbTbl + v_nbSeq;
  END;
$_rlbk_groups$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN) 
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups_step1$
-- This is the first step of a rollback group processing.
-- It tests the environment, the supplied parameters and the foreign key constraints.
-- It builds the requested number of sessions with the list of tables to process, trying to spread the load over all sessions.
-- It finaly inserts into the history the event about the rollback start
  DECLARE
    v_i                   INT;
    v_groupState          TEXT;
    v_isRollbackable      BOOLEAN;
    v_markName            TEXT;
    v_markState           TEXT;
    v_cpt                 INT;
    v_nbTblInGroup        INT;
    v_nbUnchangedTbl      INT;
    v_timestampMark       TIMESTAMPTZ;
    v_session             INT;
    v_sessionLoad         INT [];
    v_minSession          INT;
    v_minRows             INT;
    v_fullTableName       TEXT;
    v_msg                 TEXT;
    r_tbl                 RECORD;
    r_tbl2                RECORD;
  BEGIN
-- check that each group ...
-- ...is recorded in emaj_group table
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
      SELECT group_state, group_is_rollbackable INTO v_groupState, v_isRollbackable FROM emaj.emaj_group WHERE group_name = v_groupNames[v_i] FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION '_rlbk_groups_step1: group % has not been created.', v_groupNames[v_i];
      END IF;
-- ... is in LOGGING state
      IF v_groupState <> 'LOGGING' THEN
        RAISE EXCEPTION '_rlbk_groups_step1: Group % cannot be rollbacked because it is not in logging state.', v_groupNames[v_i];
      END IF;
-- ... is ROLLBACKABLE
      IF NOT v_isRollbackable THEN
        RAISE EXCEPTION '_rlbk_groups_step1: Group % has been created for audit only purpose. It cannot be rollbacked.', v_groupNames[v_i];
      END IF;
-- ... is not damaged
      PERFORM 0 FROM emaj._verify_group(v_groupNames[v_i],true);
-- ... owns the requested mark
      SELECT emaj._get_mark_name(v_groupNames[v_i],v_mark) INTO v_markName;
      IF NOT FOUND OR v_markName IS NULL THEN
        RAISE EXCEPTION '_rlbk_groups_step1: No mark % exists for group %.', v_mark, v_groupNames[v_i];
      END IF;
-- ... and this mark is ACTIVE
      SELECT mark_state INTO v_markState FROM emaj.emaj_mark 
        WHERE mark_group = v_groupNames[v_i] AND mark_name = v_markName;
      IF v_markState <> 'ACTIVE' THEN
        RAISE EXCEPTION '_rlbk_groups_step1: mark % for group % is not in ACTIVE state.', v_markName, v_groupNames[v_i];
      END IF;
    END LOOP;
-- get the mark timestamp and check it is the same for all groups of the array
    SELECT count(distinct emaj._get_mark_datetime(group_name,v_mark)) INTO v_cpt FROM emaj.emaj_group
      WHERE group_name = ANY (v_groupNames);
    IF v_cpt > 1 THEN
      RAISE EXCEPTION '_rlbk_groups_step1: Mark % does not represent the same point in time for all groups.', v_mark;
    END IF;
-- get the mark timestamp for the 1st group (as we know this timestamp is the same for all groups of the array)
    SELECT emaj._get_mark_datetime(v_groupNames[1],v_mark) INTO v_timestampMark;
-- insert begin in the history
    IF v_unloggedRlbk THEN
      v_msg = 'Unlogged';
    ELSE
      v_msg = 'Logged';
    END IF;
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'BEGIN', 
              array_to_string(v_groupNames,','), v_msg || ' rollback to mark ' || v_mark || ' [' || v_timestampMark || ']');
-- get the total number of tables for these groups
    SELECT sum(group_nb_table) INTO v_nbTblInGroup FROM emaj.emaj_group WHERE group_name = ANY (v_groupNames) ;
-- issue warnings in case of foreign keys with tables outside the groups
    PERFORM emaj._check_fk_groups(v_groupNames);
-- create sessions, using the number of sessions requested by the caller
-- session id for sequences will remain NULL
--   initialisation
--     accumulated counters of number of log rows to rollback for each parallel session 
    FOR v_session IN 1 .. v_nbSession LOOP
      v_sessionLoad [v_session] = 0;
    END LOOP;
    FOR v_i in 1 .. array_upper(v_groupNames,1) LOOP
--     fkey table
      DELETE FROM emaj.emaj_fk WHERE v_groupNames[v_i] = ANY (fk_groups);
--     relation table: for each group, session set to NULL and 
--       numbers of log rows computed by emaj_log_stat_group function
      UPDATE emaj.emaj_relation SET rel_session = NULL, rel_rows = stat_rows 
        FROM emaj.emaj_log_stat_group (v_groupNames[v_i], v_mark, NULL) stat
        WHERE rel_group = v_groupNames[v_i]
          AND rel_group = stat_group AND rel_schema = stat_schema AND rel_tblseq = stat_table;
    END LOOP;
--   count the number of tables that have no update to rollback
    SELECT count(*) INTO v_nbUnchangedTbl FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames) AND rel_rows = 0;
--   allocate tables with rows to rollback to sessions starting with the heaviest to rollback tables
--     as reported by emaj_log_stat_group function
    FOR r_tbl IN
        SELECT * FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'r' ORDER BY rel_rows DESC
        LOOP
--   is the table already allocated to a session (it may have been already allocated because of a fkey link) ?
      PERFORM 1 FROM emaj.emaj_relation 
        WHERE rel_group = ANY (v_groupNames) AND rel_schema = r_tbl.rel_schema AND rel_tblseq = r_tbl.rel_tblseq 
          AND rel_session IS NULL;
--   no, 
      IF FOUND THEN
--   compute the least loaded session
        v_minSession=1; v_minRows = v_sessionLoad [1];
        FOR v_session IN 2 .. v_nbSession LOOP
          IF v_sessionLoad [v_session] < v_minRows THEN
            v_minSession = v_session;
            v_minRows = v_sessionLoad [v_session];
          END IF;
        END LOOP;
--   allocate the table to the session, with all other tables linked by foreign key constraints
        v_sessionLoad [v_minSession] = v_sessionLoad [v_minSession] + 
                 emaj._rlbk_groups_set_session(v_groupNames, r_tbl.rel_schema, r_tbl.rel_tblseq, v_minSession, r_tbl.rel_rows);
      END IF;
    END LOOP;
    RETURN v_nbTblInGroup - v_nbUnchangedTbl;
  END;
$_rlbk_groups_step1$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_set_session(v_groupNames TEXT[], v_schema TEXT, v_table TEXT, v_session INT, v_rows BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS
$_rlbk_groups_set_session$
-- This function updates the emaj_relation table and set the predefined session id for one table. 
-- It also looks for all tables that are linked to this table by foreign keys to force them to be allocated to the same session.
-- As those linked table can also be linked to other tables by other foreign keys, the function has to be recursiley called.
-- The function returns the accumulated number of rows contained into all log tables of these linked by foreign keys tables.
  DECLARE
    v_cumRows       BIGINT;    -- accumulate the number of rows of the related log tables ; will be returned by the function
    v_fullTableName TEXT;
    r_tbl           RECORD;
  BEGIN
    v_cumRows=v_rows;
-- first set the session of the emaj_relation table for this application table
    UPDATE emaj.emaj_relation SET rel_session = v_session 
      WHERE rel_group = ANY (v_groupNames) AND rel_schema = v_schema AND rel_tblseq = v_table;
-- then look for other application tables linked by foreign key relationships
    v_fullTableName := quote_ident(v_schema) || '.' || quote_ident(v_table);
    FOR r_tbl IN
        SELECT rel_schema, rel_tblseq, rel_rows FROM emaj.emaj_relation 
          WHERE rel_group = ANY (v_groupNames)
            AND rel_session IS NULL                          -- not yet allocated
            AND (rel_schema, rel_tblseq) IN (                -- list of (schema,table) linked to the original table by foreign keys
            SELECT nspname, relname FROM pg_constraint, pg_class t, pg_namespace n 
              WHERE contype = 'f' AND confrelid = v_fullTableName::regclass 
                AND t.oid = conrelid AND relnamespace = n.oid
            UNION
            SELECT nspname, relname FROM pg_constraint, pg_class t, pg_namespace n
              WHERE contype = 'f' AND conrelid = v_fullTableName::regclass 
                AND t.oid = confrelid AND relnamespace = n.oid
            ) 
        LOOP
-- recursive call to allocate these linked tables to the same session id and get the accumulated number of rows to rollback
      SELECT v_cumRows + emaj._rlbk_groups_set_session(v_groupNames, r_tbl.rel_schema, r_tbl.rel_tblseq, v_session, r_tbl.rel_rows) INTO v_cumRows;
    END LOOP;
    RETURN v_cumRows;
  END;
$_rlbk_groups_set_session$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step2(v_groupNames TEXT[], v_session INT, v_multiGroup BOOLEAN) 
RETURNS void LANGUAGE plpgsql AS
$_rlbk_groups_step2$
-- This is the second step of a rollback group processing. It just locks the tables for a session.
  DECLARE
    v_nbRetry       SMALLINT := 0;
    v_ok            BOOLEAN := false;
    v_nbTbl         INT;
    r_tblsq         RECORD;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_SESSIONS' ELSE 'LOCK_SESSION' END, 'BEGIN', array_to_string(v_groupNames,','), 'Session #' || v_session);
-- acquire lock on all tables
-- in case of deadlock, retry up to 5 times
    WHILE NOT v_ok AND v_nbRetry < 5 LOOP
      BEGIN
        v_nbTbl = 0;
-- scan all tables of the session
        FOR r_tblsq IN
            SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation 
              WHERE rel_group = ANY (v_groupNames) AND rel_session = v_session AND rel_kind = 'r' 
              ORDER BY rel_priority, rel_schema, rel_tblseq
            LOOP
--   lock each table
          EXECUTE 'LOCK TABLE ' || quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
          v_nbTbl = v_nbTbl + 1;
        END LOOP;
-- ok, all tables locked
        v_ok = true;
      EXCEPTION
        WHEN deadlock_detected THEN
          v_nbRetry = v_nbRetry + 1;
          RAISE NOTICE '_rlbk_group_step2: a deadlock has been trapped while locking tables for groups %.', array_to_string(v_groupNames,',');
      END;
    END LOOP;
    IF NOT v_ok THEN
      RAISE EXCEPTION '_rlbk_group_step2: too many (5) deadlocks encountered while locking tables of group %.',v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'LOCK_SESSIONS' ELSE 'LOCK_SESSION' END, 'END', array_to_string(v_groupNames,','), 'Session #' || v_session || ' : ' || v_nbTbl || ' tables locked, ' || v_nbRetry || ' deadlock(s)');
    RETURN;
  END;
$_rlbk_groups_step2$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN) 
RETURNS VOID LANGUAGE plpgsql AS
$_rlbk_groups_step3$
-- This is the third step of a rollback group processing.
-- For logged rollback, it sets a mark that materialize the point in time just before the tables rollback. 
-- All concerned tables are already locked.
  DECLARE
    v_realMark       TEXT;
    v_markName       TEXT;
  BEGIN
    IF NOT v_unloggedRlbk THEN
-- If rollback is "logged" rollback, build a mark name with the pattern:
-- 'RLBK_<mark name to rollback to>_%_START', where % represents the current time
-- Get the real mark name (using first supplied group name, and check that all groups 
--   can use the same name has already been done at step 1) 
      SELECT emaj._get_mark_name(v_groupNames[1],v_mark) INTO v_realMark;
      IF v_realMark IS NULL THEN
        RAISE EXCEPTION '_rlbk_groups_step3: Internal error - mark % not found for group %.', v_mark, v_groupNames[1];
      END IF;
--   compute the generated mark name
      v_markName = 'RLBK_' || v_realMark || '_' || to_char(current_timestamp, 'HH24.MI.SS.MS') || '_START';
-- ...  and set it
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_markName);
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_markName);
    END IF;
    RETURN;
  END;
$_rlbk_groups_step3$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step4(v_groupNames TEXT[], v_session INT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_groups_step4$
-- This is the fourth step of a rollback group processing. It drops all foreign keys involved in a rollback session.
-- Before dropping, it records them to be able to recreate them at step 5.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    r_fk                RECORD;
  BEGIN
-- record and drop the foreign keys involved in all tables of the group, if any
    INSERT INTO emaj.emaj_fk (fk_groups, fk_session, fk_name, fk_schema, fk_table, fk_def)
--    record the foreign keys of the session's tables
      SELECT v_groupNames, v_session, c.conname, n.nspname, t.relname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c, pg_namespace n, pg_class t, emaj.emaj_relation r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND n.nspname = r.rel_schema AND t.relname = r.rel_tblseq      -- join on group table
          AND r.rel_group = ANY (v_groupNames) AND r.rel_session = v_session
      UNION
--           and the foreign keys referencing the session's tables
      SELECT v_groupNames, v_session, c.conname, n.nspname, t.relname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace rn, pg_class rt, emaj.emaj_relation r
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace 
          AND rn.nspname = r.rel_schema AND rt.relname = r.rel_tblseq    -- join on group table
          AND r.rel_group = ANY (v_groupNames) AND r.rel_session = v_session;
--    and drop all these foreign keys
    FOR r_fk IN
      SELECT fk_schema, fk_table, fk_name FROM emaj.emaj_fk 
        WHERE fk_groups = v_groupNames  AND fk_session = v_session ORDER BY fk_schema, fk_table, fk_name
      LOOP
        RAISE NOTICE '_rlbk_groups_step4: groups (%), session #% -> foreign key constraint % dropped for table %.%.', array_to_string(v_groupNames,','), v_session, r_fk.fk_name, r_fk.fk_schema, r_fk.fk_table;
        EXECUTE 'ALTER TABLE ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_table) || ' DROP CONSTRAINT ' || quote_ident(r_fk.fk_name);
    END LOOP;
  END;
$_rlbk_groups_step4$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step5(v_groupNames TEXT[], v_mark TEXT, v_session INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups_step5$
-- This is the fifth step of a rollback group processing. It performs the rollback of all tables of a session.
  DECLARE
    v_nbTbl             INT := 0;
    v_timestampMark     TIMESTAMPTZ;
    v_lastSequenceId    BIGINT;
    v_lastSeqHoleId     BIGINT;
  BEGIN
-- fetch the timestamp mark again
    SELECT emaj._get_mark_datetime(v_groupNames[1],v_mark) INTO v_timestampMark;
    IF v_timestampMark IS NULL THEN
      RAISE EXCEPTION '_rlbk_groups_step5: Internal error - mark % not found for group %.', v_mark, v_groupNames[1];
    END IF;
-- fetch the last id value of emaj_seq_hole table at set mark time
    SELECT mark_last_sequence_id, mark_last_seq_hole_id INTO v_lastSequenceId, v_lastSeqHoleId FROM emaj.emaj_mark 
      WHERE mark_group = v_groupNames[1] AND mark_name = emaj._get_mark_name(v_groupNames[1],v_mark);
-- rollback all tables of the session, having rows to rollback, in priority order (sequences are processed later)
-- (for the _rlbk_tbl() function call, the disableTrigger boolean always equals unloggedRlbk boolean)
    PERFORM emaj._rlbk_tbl(rel_schema, rel_tblseq, v_timestampMark, v_unloggedRlbk, v_deleteLog, v_lastSequenceId, v_lastSeqHoleId)
      FROM (SELECT rel_priority, rel_schema, rel_tblseq FROM emaj.emaj_relation 
              WHERE rel_group = ANY (v_groupNames) AND rel_session = v_session AND rel_kind = 'r' AND rel_rows > 0
              ORDER BY rel_priority, rel_schema, rel_tblseq) as t;
-- and return the number of processed tables
    GET DIAGNOSTICS v_nbTbl = ROW_COUNT;
    RETURN v_nbTbl;
  END;
$_rlbk_groups_step5$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT) 
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS
$_rlbk_groups_step6$
-- This is the sixth step of a rollback group processing. It recreates the previously deleted foreign keys.
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_ts_start          TIMESTAMP;
    v_ts_end            TIMESTAMP;
    r_fk                RECORD;
  BEGIN
    FOR r_fk IN
-- get all recorded fk plus the number of rows of the related table as estimated by postgresql (pg_class.reltuples)
      SELECT fk_schema, fk_table, fk_name, fk_def, pg_class.reltuples 
        FROM emaj.emaj_fk, pg_namespace, pg_class
        WHERE fk_groups = v_groupNames AND fk_session = v_session AND                         -- restrictions
              pg_namespace.oid = relnamespace AND relname = fk_table AND nspname = fk_schema  -- joins
        ORDER BY fk_schema, fk_table, fk_name
      LOOP
-- record the time at the alter table start
        SELECT clock_timestamp() INTO v_ts_start;
-- recreate the foreign key
        EXECUTE 'ALTER TABLE ' || quote_ident(r_fk.fk_schema) || '.' || quote_ident(r_fk.fk_table) || ' ADD CONSTRAINT ' || quote_ident(r_fk.fk_name) || ' ' || r_fk.fk_def;
-- record the time after the alter table and insert FK creation duration into the emaj_rlbk_stat table
        SELECT clock_timestamp() INTO v_ts_end;
        INSERT INTO emaj.emaj_rlbk_stat (rlbk_operation, rlbk_schema, rlbk_tbl_fk, rlbk_datetime, rlbk_nb_rows, rlbk_duration) 
           VALUES ('add_fk', r_fk.fk_schema, r_fk.fk_name, v_ts_start, r_fk.reltuples, v_ts_end - v_ts_start);
-- send a message about the fk creation completion 
        RAISE NOTICE '_rlbk_group_step6: groups (%), session #% -> foreign key constraint % recreated for table %.%.', array_to_string(v_groupNames,','), v_session, r_fk.fk_name, r_fk.fk_schema, r_fk.fk_table;
    END LOOP;
    RETURN;
  END;
$_rlbk_groups_step6$;

CREATE or REPLACE FUNCTION emaj._rlbk_groups_step7(v_groupNames TEXT[], v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN)
RETURNS INT LANGUAGE plpgsql AS
$_rlbk_groups_step7$
-- This is the last step of a rollback group processing. It :
--    - deletes the marks that are no longer available,
--    - rollbacks all sequences of the group.
-- It returns the number of processed sequences.
  DECLARE
    v_realMark          TEXT;
    v_markId            BIGINT;
    v_timestampMark     TIMESTAMPTZ;
    v_lastSequenceId    BIGINT;
    v_nbSeq             INT;
    v_markName          TEXT;
  BEGIN
-- get the real mark name
    SELECT emaj._get_mark_name(v_groupNames[1],v_mark) INTO v_realMark;
    IF NOT FOUND OR v_realMark IS NULL THEN
      RAISE EXCEPTION '_rlbk_groups_step7: Internal error - mark % not found for group %.', v_mark, v_groupNames[1];
    END IF;
-- if "unlogged" rollback, delete all marks later than the now rollbacked mark
    IF v_unloggedRlbk THEN
-- get the highest mark id of the mark used for rollback, for all groups
      SELECT max(mark_id) INTO v_markId
        FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames) AND mark_name = v_realMark;
-- log in the history the name of all marks that must be deleted due to the rollback
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        SELECT CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 
               'MARK DELETED', mark_group, 'mark ' || mark_name || ' has been deleted' FROM emaj.emaj_mark 
          WHERE mark_group = ANY (v_groupNames) AND mark_id > v_markId ORDER BY mark_id;
-- and finaly delete these useless marks (the related sequences have been already deleted by rollback functions)
      DELETE FROM emaj.emaj_mark WHERE mark_group = ANY (v_groupNames) AND mark_id > v_markId; 
    END IF;
-- rollback the application sequences belonging to the group
-- warning, this operation is not transaction safe (that's why it is placed at the end of the operation)!
--   get the mark timestamp and last sequence id for the 1st group
    SELECT mark_datetime, mark_last_sequence_id INTO v_timestampMark, v_lastSequenceId FROM emaj.emaj_mark 
      WHERE mark_group = v_groupNames[1] AND mark_name = v_realMark;
--   and rollback
    PERFORM emaj._rlbk_seq(rel_schema, rel_tblseq, v_timestampMark, v_deleteLog, v_lastSequenceId) 
       FROM emaj.emaj_relation WHERE rel_group = ANY (v_groupNames) AND rel_kind = 'S';
    GET DIAGNOSTICS v_nbSeq = ROW_COUNT;
-- if rollback is "logged" rollback, automaticaly set a mark representing the tables state just after the rollback.
-- this mark is named 'RLBK_<mark name to rollback to>_%_DONE', where % represents the current time
    IF NOT v_unloggedRlbk THEN
-- get the mark name set at the beginning of the rollback operation (i.e. the last set mark)
      SELECT mark_name INTO v_markName 
        FROM emaj.emaj_mark
        WHERE mark_group = v_groupNames[1] ORDER BY mark_id DESC LIMIT 1;
      IF NOT FOUND OR substr(v_markName,1,5) <> 'RLBK_' THEN
        RAISE EXCEPTION '_rlbk_groups_step7: Internal error - rollback start mark not found for group %.', v_groupNames[1];
      END IF;
-- compute the mark name that ends the rollback operation, replacing the '_START' suffix of the rollback start mark by '_DONE'
      v_markName = substring(v_markName FROM '(.*)_START$') || '_DONE';
-- ...  and set it
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'BEGIN', array_to_string(v_groupNames,','), v_markName);
      PERFORM emaj._set_mark_groups(v_groupNames, v_markName);
      INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
        VALUES (CASE WHEN v_multiGroup THEN 'SET_MARK_GROUPS' ELSE 'SET_MARK_GROUP' END, 'END', array_to_string(v_groupNames,','), v_markName);
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES (CASE WHEN v_multiGroup THEN 'ROLLBACK_GROUPS' ELSE 'ROLLBACK_GROUP' END, 'END', 
              array_to_string(v_groupNames,','), v_nbTb || ' tables and ' || v_nbSeq || ' sequences effectively processed');
    RETURN v_nbSeq;
  END;
$_rlbk_groups_step7$;

CREATE or REPLACE FUNCTION emaj.emaj_reset_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS
$emaj_reset_group$
-- This function empties the log tables for all tables of a group and deletes the sequences saves
-- It calls the emaj_rst_group function to do the job
-- Input: group name
-- Output: number of processed tables
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use it even if he is not the owner of application tables.
  DECLARE
    v_groupState  TEXT;
    v_nbTb        INT := 0;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object) 
      VALUES ('RESET_GROUP', 'BEGIN', v_groupName);
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_reset_group: group % has not been created.', v_groupName;
    END IF;
-- check that the group is in IDLE state (i.e. not in LOGGING) state
    IF v_groupState <> 'IDLE' THEN
      RAISE EXCEPTION 'emaj_reset_group: Group % cannot be reset because it is not in idle state. An emaj_stop_group function must be previously executed.', v_groupName;
    END IF;
-- perform the reset operation
    SELECT emaj._rst_group(v_groupName) INTO v_nbTb;
    IF v_nbTb = 0 THEN
       RAISE EXCEPTION 'emaj_reset_group: Group % is unknown.', v_groupName;
    END IF;
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('RESET_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_reset_group$;
COMMENT ON FUNCTION emaj.emaj_reset_group(TEXT) IS
$$Resets all log tables content of a stopped E-Maj group.$$;

CREATE or REPLACE FUNCTION emaj._rst_group(v_groupName TEXT) 
RETURNS INT LANGUAGE plpgsql AS 
$_rst_group$
-- This function empties the log tables for all tables of a group, using a TRUNCATE, and deletes the sequences saves
-- It is called by both emaj_reset_group and emaj_start_group functions
-- Input: group name
-- Output: number of processed tables
-- There is no check of the group state
  DECLARE
    v_nbTb          INT  := 0;
    v_emajSchema    TEXT := 'emaj';
    v_logTableName  TEXT;
    v_fullSeqName   TEXT;
    r_tblsq         RECORD;
  BEGIN
-- delete all marks for the group from the emaj_mark table
    DELETE FROM emaj.emaj_mark WHERE mark_group = v_groupName;
-- delete all sequence holes for the tables of the group
    DELETE FROM emaj.emaj_seq_hole USING emaj.emaj_relation
      WHERE rel_group = v_groupName AND rel_kind = 'r' AND rel_schema = sqhl_schema AND rel_tblseq = sqhl_table;
-- then, truncate log tables
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, 
--   truncate the related log table
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        EXECUTE 'TRUNCATE ' || v_logTableName;
--   delete rows from emaj_sequence related to the associated emaj_id sequence
        v_fullSeqName := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        DELETE FROM emaj.emaj_sequence WHERE sequ_name = v_fullSeqName;
--   and reset the log sequence
        PERFORM setval(quote_ident(v_emajSchema) || '.' || quote_ident(v_fullSeqName), 1, false);
      ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, delete all related data from emaj_sequence table
        PERFORM emaj._drop_seq (r_tblsq.rel_schema, r_tblsq.rel_tblseq);
      END IF;
      v_nbTb = v_nbTb + 1;
    END LOOP;
    IF v_nbTb = 0 THEN
       RAISE EXCEPTION '_rst_group: Internal error (Group % is empty).', v_groupName;
    END IF;
    RETURN v_nbTb;
  END;
$_rst_group$;

CREATE or REPLACE FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) 
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
    v_groupState         TEXT;
    v_emajSchema         TEXT := 'emaj';
    v_realFirstMark      TEXT;
    v_realLastMark       TEXT;
    v_firstMarkId        BIGINT;
    v_lastMarkId         BIGINT;
    v_tsFirstMark        TIMESTAMPTZ;
    v_tsLastMark         TIMESTAMPTZ;
    v_firstLastSeqHoleId BIGINT;
    v_lastLastSeqHoleId  BIGINT;
    v_fullSeqName        TEXT;
    v_beginLastValue     BIGINT;
    v_endLastValue       BIGINT;
    v_sumHole            BIGINT;
    r_tblsq              RECORD;
    r_stat               RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_log_stat_group: group % has not been created.', v_groupName;
    END IF;
-- if first mark is NULL or empty, retrieve the name, timestamp and last sequ_hole id of the first recorded mark for the group
    IF v_firstMark IS NULL OR v_firstMark = '' THEN
      SELECT mark_id, mark_name, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_realFirstMark, v_tsFirstMark, v_firstLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'emaj_log_stat_group: No initial mark can be found for group %.', v_groupName;
      END IF;
    ELSE
-- else, check and retrieve the name, timestamp and last sequ_hole id of the supplied first mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime, mark_last_seq_hole_id INTO v_firstMarkId, v_tsFirstMark, v_firstLastSeqHoleId
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- if last mark is NULL or empty, there is no timestamp to register
    IF v_lastMark IS NULL OR v_lastMark = '' THEN
      v_lastMarkId = NULL;
      v_tsLastMark = NULL;
      v_lastLastSeqHoleId = NULL;
    ELSE
-- else, check and retrieve the name, timestamp and last sequ_hole id of the supplied end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_log_stat_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime, mark_last_seq_hole_id INTO v_lastMarkId, v_tsLastMark, v_lastLastSeqHoleId 
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_lastMarkId IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_log_stat_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_firstMark, v_firstMarkId, v_tsFirstMark, v_lastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table, get the number of log rows and return the statistic
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, emaj._log_stat_table(rel_schema, rel_tblseq, v_tsFirstMark, v_tsLastMark, v_firstLastSeqHoleId, v_lastLastSeqHoleId) AS nb_rows FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName AND rel_kind = 'r' ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      SELECT v_groupName, r_tblsq.rel_schema, r_tblsq.rel_tblseq, r_tblsq.nb_rows INTO r_stat;
      RETURN NEXT r_stat;
    END LOOP;
    RETURN;
  END;
$emaj_log_stat_group$;
COMMENT ON FUNCTION emaj.emaj_log_stat_group(TEXT,TEXT,TEXT) IS
$$Returns global statistics about logged events for an E-Maj group between 2 marks.$$;

CREATE or REPLACE FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) 
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
    v_groupState         TEXT;
    v_emajSchema         TEXT := 'emaj';
    v_realFirstMark      TEXT;
    v_realLastMark       TEXT;
    v_firstMarkId        BIGINT;
    v_lastMarkId         BIGINT;
    v_tsFirstMark        TIMESTAMPTZ;
    v_tsLastMark         TIMESTAMPTZ;
    v_firstEmajId        BIGINT;
    v_lastEmajId         BIGINT;
    v_logTableName       TEXT;
    v_seqName            TEXT;
    v_stmt               TEXT;
    r_tblsq              RECORD;
    r_stat               RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table
    SELECT group_state INTO v_groupState FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: group % has not been created.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_detailed_log_stat_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_detailed_log_stat_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_lastMarkId, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_detailed_log_stat_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- for each table of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table, count the number of operations per type (INSERT, UPDATE and DELETE) and role
-- compute the log table name and its sequence name for this table
        v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
-- get the next emaj_id for the first mark from the sequence
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_firstEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsFirstMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_detailed_log_stat_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsFirstMark;
          END IF;
        END IF;
-- get the next emaj_id for the last mark from the sequence
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_lastEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsLastMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_detailed_log_stat_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsLastMark;
          END IF;
        END IF;
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
             || ' AND emaj_id >= '|| v_firstEmajId ;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN v_stmt = v_stmt
             || ' AND emaj_id < '|| v_lastEmajId ;
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

CREATE or REPLACE FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) 
RETURNS interval LANGUAGE plpgsql AS 
$emaj_estimate_rollback_duration$
-- This function computes an approximate duration of a rollback to a predefined mark for a group.
-- It takes into account the content of emaj_rollback_stat table filled by previous rollback operations.
-- It also uses several parameters from emaj_param table.
-- "Logged" and "Unlogged" rollback durations are estimated with the same algorithm. (the cost of log insertion
-- for logged rollback balances the cost of log deletion of unlogged rollback) 
-- Input: group name, the mark name of the rollback operation
-- Output: the approximate duration that the rollback would need as time interval
  DECLARE
    v_nbTblSeq              INTEGER;
    v_markName              TEXT;
    v_markState             TEXT;
    v_estim_duration        INTERVAL;
    v_avg_row_rlbk          INTERVAL;
    v_avg_row_del_log       INTERVAL;
    v_fixed_table_rlbk      INTERVAL;
    v_fixed_table_with_rlbk INTERVAL;
    v_estim                 INTERVAL;
    r_tblsq                 RECORD;
    r_fkey		            RECORD;
  BEGIN
-- check that the group is recorded in emaj_group table and get the number of tables and sequences
    SELECT group_nb_table + group_nb_sequence INTO v_nbTblSeq FROM emaj.emaj_group 
      WHERE group_name = v_groupName and group_state = 'LOGGING';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: group % has not been created or is not in LOGGING state.', v_groupName;
    END IF;
-- check the mark exists
    SELECT emaj._get_mark_name(v_groupName,v_mark) INTO v_markName;
    IF NOT FOUND OR v_markName IS NULL THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: no mark % exists for group %.', v_mark, v_groupName;
    END IF;
-- check the mark is ACTIVE
    SELECT mark_state INTO v_markState FROM emaj.emaj_mark 
      WHERE mark_group = v_groupName AND mark_name = v_markName;
    IF v_markState <> 'ACTIVE' THEN
      RAISE EXCEPTION 'emaj_estimate_rollback_duration: mark % for group % is not in ACTIVE state.', v_markName, v_groupName;
    END IF;
-- get all needed duration parameters from emaj_param table, 
--   or get default values for rows that are not present in emaj_param table
    SELECT coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'avg_row_rollback_duration'),'100 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'avg_row_delete_log_duration'),'10 microsecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'fixed_table_rollback_duration'),'5 millisecond'::interval),
           coalesce ((SELECT param_value_interval FROM emaj.emaj_param 
                        WHERE param_key = 'fixed_table_with_rollback_duration'),'2.5 millisecond'::interval)
           INTO v_avg_row_rlbk, v_avg_row_del_log, v_fixed_table_rlbk, v_fixed_table_with_rlbk;
-- compute the fixed cost for the group
    v_estim_duration = v_nbTblSeq * v_fixed_table_rlbk;
--
-- walk through the list of tables with their number of rows to rollback as returned by the emaj_log_stat_group function
--
-- for each table with content to rollback
    FOR r_tblsq IN
        SELECT stat_schema, stat_table, stat_rows FROM emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) WHERE stat_rows > 0
        LOOP
--
-- compute the rollback duration estimate for the table
--
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'rlbk' AND rlbk_nb_rows > 0
          AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'rlbk' AND rlbk_nb_rows > 0
            AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
        IF v_estim IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estim = v_avg_row_rlbk * r_tblsq.stat_rows;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_fixed_table_with_rlbk + v_estim;
--
-- compute the log rows delete duration for the table
--
-- first look at the previous rollback durations for the table and with similar rollback volume (same order of magnitude)
      SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
        WHERE rlbk_operation = 'del_log' AND rlbk_nb_rows > 0
          AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table
          AND rlbk_nb_rows / r_tblsq.stat_rows < 10 AND r_tblsq.stat_rows / rlbk_nb_rows < 10;
      IF v_estim IS NULL THEN
-- if there is no previous rollback operation with similar volume, take statistics for the table with all available volumes
        SELECT sum(rlbk_duration) * r_tblsq.stat_rows / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat 
          WHERE rlbk_operation = 'del_log' AND rlbk_nb_rows > 0 
            AND rlbk_schema = r_tblsq.stat_schema AND rlbk_tbl_fk = r_tblsq.stat_table;
        IF v_estim IS NULL THEN
-- if there is no previous rollback operation, use the avg_row_rollback_duration from the emaj_param table
          v_estim = v_avg_row_del_log * r_tblsq.stat_rows;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_estim;
    END LOOP;
--
-- walk through the list of foreign key constraints concerned by the estimated rollback
--
-- for each foreign key
    FOR r_fkey IN
      SELECT c.conname, n.nspname, t.relname, t.reltuples
        FROM pg_constraint c, pg_namespace n, pg_class t, emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) s
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND stat_rows > 0                                              -- table to effectively rollback only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND n.nspname = s.stat_schema AND t.relname = s.stat_table     -- join on group tables to effectively rollback
      UNION
--           and the foreign keys referenced by tables that are concerned by the rollback operation
      SELECT c.conname, n.nspname, t.relname, t.reltuples
        FROM pg_constraint c, pg_namespace n, pg_class t, pg_namespace rn, pg_class rt, emaj.emaj_log_stat_group(v_groupName, v_mark, NULL) s
        WHERE c.contype = 'f'                                            -- FK constraints only
          AND stat_rows > 0                                              -- table to effectively rollback only
          AND c.conrelid  = t.oid AND t.relnamespace  = n.oid            -- joins for table and namespace 
          AND c.confrelid  = rt.oid AND rt.relnamespace  = rn.oid        -- joins for referenced table and namespace 
          AND rn.nspname = s.stat_schema AND rt.relname = s.stat_table   -- join on group tables to effectively rollback
        LOOP
-- estimate the recreation duration of a fkey 
      IF r_fkey.reltuples = 0 THEN
-- empty table (or table not analyzed) => duration = 0
        v_estim = 0;
	  ELSE
-- non empty table and statistics (with at least one row) are available
        SELECT sum(rlbk_duration) * r_fkey.reltuples / sum(rlbk_nb_rows) INTO v_estim FROM emaj.emaj_rlbk_stat
          WHERE rlbk_operation = 'add_fk' AND rlbk_nb_rows > 0
            AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname;
        IF v_estim IS NULL THEN
-- non empty table, but no statistics with at least one row are available => take the last duration for this fkey, if any
          SELECT rlbk_duration INTO v_estim FROM emaj.emaj_rlbk_stat
            WHERE rlbk_operation = 'add_fk' AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname AND rlbk_datetime =
             (SELECT max(rlbk_datetime) FROM emaj.emaj_rlbk_stat
                WHERE rlbk_operation = 'add_fk' AND rlbk_schema = r_fkey.nspname AND rlbk_tbl_fk = r_fkey.conname);
          IF v_estim IS NULL THEN
-- definitely no statistics available
            v_estim = 0;
          END IF;
        END IF;
      END IF;
      v_estim_duration = v_estim_duration + v_estim;
    END LOOP;
    RETURN v_estim_duration;
  END;
$emaj_estimate_rollback_duration$;
COMMENT ON FUNCTION emaj.emaj_estimate_rollback_duration(TEXT,TEXT) IS
$$Estimates the duration of a potential rollback of an E-Maj group to a given mark.$$;

CREATE or REPLACE FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_snap_group$
-- This function creates a file for each table and sequence belonging to the group.
-- For tables, these files contain all rows sorted on primary key.
-- For sequences, they contain a single row describing the sequence.
-- To do its job, the function performs COPY TO statement, with all default parameters.
-- For table without primary key, rows are sorted on all columns.
-- There is no need for the group to be in IDLE state.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before 
-- emaj_snap_group function call, and 
--   - maintain its content outside E-maj.
-- Input: group name, the absolute pathname of the directory where the files are to be created
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    r_tblsq           RECORD;
    v_fullTableName   TEXT;
    r_col             RECORD;
    v_colList         TEXT;
    v_fileName        TEXT;
    v_stmt text;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_group: group % has not been created.', v_groupName;
    END IF;
-- for each table/sequence of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      v_fileName := v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '.snap';
      v_fullTableName := quote_ident(r_tblsq.rel_schema) || '.' || quote_ident(r_tblsq.rel_tblseq);
      IF r_tblsq.rel_kind = 'r' THEN
-- if it is a table,
--   first build the order by column list
        v_colList := '';
        PERFORM 0 FROM pg_class, pg_namespace, pg_constraint 
          WHERE relnamespace = pg_namespace.oid AND connamespace = pg_namespace.oid AND conrelid = pg_class.oid AND
                contype = 'p' AND nspname = r_tblsq.rel_schema AND relname = r_tblsq.rel_tblseq;
        IF FOUND THEN
--   the table has a pkey,
          FOR r_col IN
              SELECT attname FROM pg_attribute, pg_index 
                WHERE pg_attribute.attrelid = pg_index.indrelid 
                  AND attnum = ANY (indkey) 
                  AND indrelid = v_fullTableName::regclass AND indisprimary
                  AND attnum > 0 AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList := r_col.attname;
            ELSE
               v_colList := v_colList || ',' || r_col.attname;
            END IF;
          END LOOP;
        ELSE
--   the table has no pkey
          FOR r_col IN
              SELECT attname FROM pg_attribute
                WHERE attrelid = v_fullTableName::regclass
                  AND attnum > 0  AND attisdropped = false
              LOOP
            IF v_colList = '' THEN
               v_colList := r_col.attname;
            ELSE
               v_colList := v_colList || ',' || r_col.attname;
            END IF;
          END LOOP;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ' ORDER BY ' || v_colList || ') TO ' || quote_literal(v_fileName) || ' CSV';
        ELSEIF r_tblsq.rel_kind = 'S' THEN
-- if it is a sequence, the statement has no order by
        v_stmt= 'COPY (SELECT * FROM ' || v_fullTableName || ') TO ' || quote_literal(v_fileName) || ' CSV';
      END IF;
-- and finaly perform the COPY
--    raise notice 'emaj_snap_group: Executing %',v_stmt;
      EXECUTE v_stmt;
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' || 
            quote_literal('E-Maj snap of tables group ' || v_groupName || ' at ' || to_char(transaction_timestamp(),'DD/MM/YYYY HH24:MI:SS')) || 
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_group$;
COMMENT ON FUNCTION emaj.emaj_snap_group(TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

CREATE or REPLACE FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) 
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS 
$emaj_snap_log_group$
-- This function creates a file for each log table belonging to the group.
-- It also creates 2 files containing the state of sequences respectively at start mark and end mark
-- For log tables, files contain all rows related to the time frame, sorted on emaj_id.
-- For sequences, files are names <group>_sequences_at_<mark>. They contain one row per sequence.
-- To do its job, the function performs COPY TO statement, using the CSV option.
-- There is no need for the group to be in IDLE state.
-- As all COPY statements are executed inside a single transaction:
--   - the function can be called while other transactions are running,
--   - the snap files will present a coherent state of tables.
-- It's users responsability :
--   - to create the directory (with proper permissions allowing the cluster to write into) before 
-- emaj_snap_log_group function call, and 
--   - maintain its content outside E-maj.
-- Input: group name, the 2 mark names defining a range, the absolute pathname of the directory where the files are to be created
--   a NULL value or an empty string as first_mark indicates the first recorded mark
--   a NULL value or an empty string can NOT be used as last_mark (the current sequences state is not recorded in to the emaj_sequence table)
--   The keyword 'EMAJ_LAST_MARK' can be used as first or last mark to specify the last set mark.
-- Output: number of processed tables and sequences
-- The function is defined as SECURITY DEFINER so that emaj_adm role can use.
  DECLARE
    v_emajSchema      TEXT := 'emaj';
    v_nbTb            INT := 0;
    r_tblsq           RECORD;
    v_seqName         TEXT;
    v_realFirstMark   TEXT;
    v_realLastMark    TEXT;
    v_firstMarkId     BIGINT;
    v_lastMarkId      BIGINT;
    v_tsFirstMark     TIMESTAMPTZ;
    v_tsLastMark      TIMESTAMPTZ;
    v_firstEmajId     BIGINT;
    v_lastEmajId      BIGINT;
    v_logTableName    TEXT;
    v_fileName        TEXT;
    v_stmt text;
  BEGIN
-- insert begin in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording)
      VALUES ('SNAP_LOG_GROUP', 'BEGIN', v_groupName, v_dir);
-- check that the group is recorded in emaj_group table
    PERFORM 0 FROM emaj.emaj_group WHERE group_name = v_groupName;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'emaj_snap_log_group: group % has not been created.', v_groupName;
    END IF;
-- catch the timestamp of the first mark
    IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN
-- check and retrieve the timestamp of the start mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_firstMark) INTO v_realFirstMark;
      IF v_realFirstMark IS NULL THEN
          RAISE EXCEPTION 'emaj_snap_log_group: Start mark % is unknown for group %.', v_firstMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realFirstMark;
    ELSE
      SELECT mark_name, mark_id, mark_datetime INTO v_realFirstMark, v_firstMarkId, v_tsFirstMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName ORDER BY mark_id LIMIT 1;
    END IF;
-- catch the timestamp of the last mark
    IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN
-- else, check and retrieve the timestamp of the end mark for the group
      SELECT emaj._get_mark_name(v_groupName,v_lastMark) INTO v_realLastMark;
      IF v_realLastMark IS NULL THEN
        RAISE EXCEPTION 'emaj_snap_log_group: End mark % is unknown for group %.', v_lastMark, v_groupName;
      END IF;
      SELECT mark_id, mark_datetime INTO v_lastMarkId, v_tsLastMark
        FROM emaj.emaj_mark WHERE mark_group = v_groupName AND mark_name = v_realLastMark;
    ELSE
      RAISE EXCEPTION 'emaj_snap_log_group: an explicit end mark must be supplied.';
    END IF;
-- check that the first_mark < end_mark
    IF v_realFirstMark IS NOT NULL AND v_realLastMark IS NOT NULL AND v_firstMarkId > v_lastMarkId THEN
      RAISE EXCEPTION 'emaj_snap_log_group: mark id for % (% = %) is greater than mark id for % (% = %).', v_realFirstMark, v_firstMarkId, v_tsFirstMark, v_realLastMark, v_lastMarkId, v_tsLastMark;
    END IF;
-- process all log tables of the emaj_relation table
    FOR r_tblsq IN
        SELECT rel_priority, rel_schema, rel_tblseq, rel_kind FROM emaj.emaj_relation 
          WHERE rel_group = v_groupName ORDER BY rel_priority, rel_schema, rel_tblseq
        LOOP
      IF r_tblsq.rel_kind = 'r' THEN
-- process tables
-- compute names
        v_fileName     := v_dir || '/' || r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log.snap';
        v_logTableName := quote_ident(v_emajSchema) || '.' || quote_ident(r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log');
        v_seqName      := r_tblsq.rel_schema || '_' || r_tblsq.rel_tblseq || '_log_emaj_id_seq';
-- get the next emaj_id for the first mark from the sequence
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_firstEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsFirstMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_snap_log_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsFirstMark;
          END IF;
        END IF;
-- get the next emaj_id for the last mark from the sequence
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          SELECT CASE WHEN sequ_is_called THEN sequ_last_val + sequ_increment ELSE sequ_last_val END INTO v_lastEmajId
            FROM emaj.emaj_sequence
            WHERE sequ_schema = v_emajSchema AND sequ_name = v_seqName AND sequ_datetime = v_tsLastMark;
          IF NOT FOUND THEN
             RAISE EXCEPTION 'emaj_snap_log_group: internal error - sequence for % and % not found in emaj_sequence.',v_seqName, v_tsLastMark;
          END IF;
        END IF;
--   prepare the COPY statement
        v_stmt= 'COPY (SELECT * FROM ' || v_logTableName || ' WHERE TRUE';
        IF v_firstMark IS NOT NULL AND v_firstMark <> '' THEN 
          v_stmt = v_stmt || ' AND emaj_id >= '|| v_firstEmajId ;
        END IF;
        IF v_lastMark IS NOT NULL AND v_lastMark <> '' THEN 
          v_stmt = v_stmt || ' AND emaj_id < '|| v_lastEmajId ;
        END IF;
        v_stmt = v_stmt || ' ORDER BY emaj_id ASC) TO ' || quote_literal(v_fileName) || ' CSV';
-- and finaly perform the COPY
--      raise notice 'emaj_snap_log_group: Executing %',v_stmt;
        EXECUTE v_stmt;
      END IF;
-- for sequences, just adjust the counter
      v_nbTb = v_nbTb + 1;
    END LOOP;
-- generate the file for sequences state at start mark
    v_fileName := v_dir || '/' || v_groupName || '_sequences_at_' || v_realFirstMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM ' || v_emajSchema || '.emaj_sequence, ' || v_emajSchema || '.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realFirstMark) || ' AND ' || 
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' CSV';
--  raise notice 'emaj_snap_log_group: Executing %',v_stmt;
    EXECUTE v_stmt;
-- generate the file for sequences state at end mark
    v_fileName := v_dir || '/' || v_groupName || '_sequences_at_' || v_realLastMark;
    v_stmt= 'COPY (SELECT emaj_sequence.*' ||
            ' FROM ' || v_emajSchema || '.emaj_sequence, ' || v_emajSchema || '.emaj_relation' ||
            ' WHERE sequ_mark = ' || quote_literal(v_realLastMark) || ' AND ' || 
            ' rel_kind = ''S'' AND rel_group = ' || quote_literal(v_groupName) || ' AND' ||
            ' sequ_schema = rel_schema AND sequ_name = rel_tblseq' ||
            ' ORDER BY sequ_schema, sequ_name) TO ' || quote_literal(v_fileName) || ' CSV';
--  raise notice 'emaj_snap_log_group: Executing %',v_stmt;
    EXECUTE v_stmt;
-- create the _INFO file to keep general information about the snap operation
    EXECUTE 'COPY (SELECT ' || 
            quote_literal('E-Maj log tables snap of group ' || v_groupName || ' between marks ' || v_realFirstMark || ' and ' || v_realLastMark || ' at ' || to_char(transaction_timestamp(),'DD/MM/YYYY HH24:MI:SS')) || 
            ') TO ' || quote_literal(v_dir || '/_INFO');
-- insert end in the history
    INSERT INTO emaj.emaj_hist (hist_function, hist_event, hist_object, hist_wording) 
      VALUES ('SNAP_LOG_GROUP', 'END', v_groupName, v_nbTb || ' tables/sequences processed');
    RETURN v_nbTb;
  END;
$emaj_snap_log_group$;
COMMENT ON FUNCTION emaj.emaj_snap_log_group(TEXT,TEXT,TEXT,TEXT) IS
$$Snaps all application tables and sequences of an E-Maj group into a given directory.$$;

-- Set comments for all internal functions, 
-- by directly inserting a row in the pg_description table for all emaj functions that do not have yet a recorded comment
INSERT INTO pg_description (objoid, classoid, objsubid, description)
  SELECT pg_proc.oid, pg_class.oid, 0 , 'E-Maj internal function'
    FROM pg_proc, pg_class
    WHERE pg_class.relname = 'pg_proc'
      AND pg_proc.oid IN               -- list all emaj functions that do not have yet a comment in pg_description
       (SELECT pg_proc.oid 
          FROM pg_proc
               JOIN pg_namespace ON (pronamespace=pg_namespace.oid)
               LEFT OUTER JOIN pg_description ON (pg_description.objoid = pg_proc.oid 
                                     AND classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_proc')
                                     AND objsubid=0)
          WHERE nspname = 'emaj' AND (proname LIKE E'emaj\\_%' OR proname LIKE E'\\_%')
            AND pg_description.description IS NULL
       );

------------------------------------
--                                --
-- rights to emaj roles           --
--                                --
------------------------------------

-- -> emaj_viewer can view the emaj objects (content of emaj and log tables)
GRANT USAGE ON SCHEMA emaj TO emaj_viewer;

GRANT SELECT ON emaj.emaj_param      TO emaj_viewer;
GRANT SELECT ON emaj.emaj_hist       TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group_def  TO emaj_viewer;
GRANT SELECT ON emaj.emaj_group      TO emaj_viewer;
GRANT SELECT ON emaj.emaj_relation   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_mark       TO emaj_viewer;
GRANT SELECT ON emaj.emaj_sequence   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_seq_hole   TO emaj_viewer;
GRANT SELECT ON emaj.emaj_fk         TO emaj_viewer;
GRANT SELECT ON emaj.emaj_rlbk_stat  TO emaj_viewer;

-- -> emaj_adm can execute all emaj functions

GRANT CREATE ON TABLESPACE tspemaj TO emaj_adm;

GRANT ALL ON SCHEMA emaj TO emaj_adm;

GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_param      TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_hist       TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group_def  TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_group      TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_relation   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_mark       TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_sequence   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_seq_hole   TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_fk         TO emaj_adm;
GRANT SELECT,INSERT,UPDATE,DELETE ON emaj.emaj_rlbk_stat  TO emaj_adm;

GRANT ALL ON SEQUENCE emaj.emaj_hist_hist_id_seq TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_mark_mark_id_seq TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_seq_hole_sqhl_id_seq TO emaj_adm;
GRANT ALL ON SEQUENCE emaj.emaj_sequence_sequ_id_seq TO emaj_adm;

-- revoke grants on all function from PUBLIC
REVOKE ALL ON FUNCTION emaj._txid_current() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._pg_version() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._purge_hist() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._get_mark_name(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_group_names_array(v_groupNames TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_verify_all() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._forbid_truncate_fnct() FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._check_fk_groups(v_groupName TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_reset BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_rollback_and_stop_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbsession INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_set_session(v_groupNames TEXT[], v_schema TEXT, v_table TEXT, v_session INT, v_rows BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step2(v_groupNames TEXT[], v_session INT, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step4(v_groupNames TEXT[], v_session INT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step5(v_groupNames TEXT[], v_mark TEXT, v_session INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rlbk_groups_step7(v_groupNames TEXT[], v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj._rst_group(v_groupName TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) FROM PUBLIC; 
REVOKE ALL ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) FROM PUBLIC;

-- and give appropriate rights on functions to emaj_adm role
GRANT EXECUTE ON FUNCTION emaj._txid_current() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._pg_version() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._purge_hist() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._get_mark_name(TEXT, TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_group_names_array(v_groupNames TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._forbid_truncate_fnct() TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._check_fk_groups(v_groupName TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_create_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_reset BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_rollback_and_stop_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_set_session(v_groupNames TEXT[], v_schema TEXT, v_table TEXT, v_session INT, v_rows BIGINT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step2(v_groupNames TEXT[], v_session INT, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step4(v_groupNames TEXT[], v_session INT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step5(v_groupNames TEXT[], v_mark TEXT, v_session INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rlbk_groups_step7(v_groupNames TEXT[], v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_reset_group(v_groupName TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj._rst_group(v_groupName TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) TO emaj_adm;
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT) TO emaj_adm; 
GRANT EXECUTE ON FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT) TO emaj_adm; 

-- and give appropriate rights on functions to emaj_viewer role
GRANT EXECUTE ON FUNCTION emaj._pg_version() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_mark_name(TEXT, TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._get_mark_datetime(TEXT, TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_verify_all() TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer; 
GRANT EXECUTE ON FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT) TO emaj_viewer;
GRANT EXECUTE ON FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT) TO emaj_viewer;

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
SELECT pg_catalog.pg_extension_config_dump('emaj_fk','');
SELECT pg_catalog.pg_extension_config_dump('emaj_rlbk_stat','');

-- and insert the init record in the operation history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INIT','E-Maj 0.10.1', 'Initialisation completed');

-- check the current max_prepared_transactions setting and report a warning if its value is too low for parallel rollback
DO LANGUAGE plpgsql
$tmp$
  DECLARE
    v_mpt           INTEGER;
  BEGIN
    SELECT setting INTO v_mpt FROM pg_settings WHERE name = 'max_prepared_transactions';
    IF v_mpt <= 1 THEN
      RAISE WARNING 'As the max_prepared_transactions parameter is set to % on this cluster, no parallel rollback is possible.',v_mpt;
    END IF;
    RETURN;
  END;
$tmp$;
