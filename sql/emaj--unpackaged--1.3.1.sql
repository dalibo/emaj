-- emaj--unpackaged--1.3.1.sql
--
-- This script registers as an EXTENSION an already installed version of E-Maj 1.3.1 

-- link emaj enum types

ALTER EXTENSION emaj ADD TYPE emaj._rlbk_status_enum;
ALTER EXTENSION emaj ADD TYPE emaj._rlbk_step_enum;

-- link emaj technical tables and sequences

ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_global_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_param;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_hist;
ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_hist_hist_id_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_group_def;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_group;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_relation;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_mark;
ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_mark_mark_id_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_sequence;
ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_sequence_sequ_id_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_seq_hole;
ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_seq_hole_sqhl_id_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_rlbk;
ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_rlbk_rlbk_id_seq;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_rlbk_session;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_rlbk_plan;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_rlbk_stat;

-- link emaj views

ALTER EXTENSION emaj ADD VIEW emaj.emaj_visible_param;

-- link emaj composite types

ALTER EXTENSION emaj ADD TYPE emaj.emaj_log_stat_type;
ALTER EXTENSION emaj ADD TYPE emaj.emaj_detailed_log_stat_type;
ALTER EXTENSION emaj ADD TYPE emaj.emaj_rollback_activity_type;
ALTER EXTENSION emaj ADD TYPE _verify_groups_type;

-- link emaj functions

ALTER EXTENSION emaj ADD FUNCTION emaj._pg_version_num();
ALTER EXTENSION emaj ADD FUNCTION emaj._dblink_open_cnx(v_cnxName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._dblink_is_cnx_opened(v_cnxName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._dblink_close_cnx(v_cnxName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._purge_hist();
ALTER EXTENSION emaj ADD FUNCTION emaj._get_mark_name(TEXT, TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._get_mark_datetime(TEXT, TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_names_array(v_groupNames TEXT[], v_type TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_group_content(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._forbid_truncate_fnct();
ALTER EXTENSION emaj ADD FUNCTION emaj._log_truncate_fnct();
ALTER EXTENSION emaj ADD FUNCTION emaj._create_log_schema(v_logSchemaName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_log_schema(v_logSchemaName TEXT, v_isForced BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._create_tbl(grpdef emaj.emaj_group_def, v_groupName TEXT, v_isRollbackable BOOLEAN, v_defTsp TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_tbl(r_rel emaj.emaj_relation);
ALTER EXTENSION emaj ADD FUNCTION emaj._create_seq(grpdef emaj.emaj_group_def, v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_seq(r_rel emaj.emaj_relation);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_tbl(r_rel emaj.emaj_relation, v_lastGlobalSeq BIGINT, v_nbSession INT);
ALTER EXTENSION emaj ADD FUNCTION emaj._delete_log_tbl(r_rel emaj.emaj_relation, v_timestamp TIMESTAMPTZ, v_lastGlobalSeq BIGINT, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_seq(r_rel emaj.emaj_relation, v_timestamp TIMESTAMPTZ, v_isLoggedRlbk BOOLEAN, v_lastSequenceId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._log_stat_tbl(r_rel emaj.emaj_relation, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._gen_sql_tbl(r_rel emaj.emaj_relation, v_conditions TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._verify_groups(v_groupNames TEXT[], v_onErrorStop boolean);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_fk_groups(v_groupName TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_create_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_group(v_groupName TEXT, v_isForced BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_alter_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_force_stop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_isForced BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_protect_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_unprotect_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_eventToRecord BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_protect_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_unprotect_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_async(v_rlbkId INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_init(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN, v_nbSession INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_check(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_planning(v_rlbkId INT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_set_batch_number(v_rlbkId INT, v_batchNumber INT, v_schema TEXT, v_table TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_session_exec(v_rlbkId INT, v_session INT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_session_lock(v_rlbkId INT, v_session INT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_start_mark(v_rlbkId INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_end(v_rlbkId INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_error(v_rlbkId INT,v_msg TEXT, v_cnxName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_cleanup_rollback_state();
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_reset_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._reset_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_estimate_rollback_group(v_groupName TEXT, v_mark TEXT, v_isLoggedRlbk BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._estimate_rollback_groups(v_groupNames TEXT[], v_mark TEXT, v_isLoggedRlbk BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_activity();
ALTER EXTENSION emaj ADD FUNCTION emaj._rollback_activity();
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_gen_sql_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._gen_sql_groups(v_groupNames TEXT[], v_firstMark TEXT, v_lastMark TEXT, v_location TEXT, v_tblseqs TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._verify_all_groups();
ALTER EXTENSION emaj ADD FUNCTION emaj._verify_all_schemas();
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_verify_all();

-- and insert the operation into the history
INSERT INTO emaj.emaj_hist (hist_function, hist_object, hist_wording) VALUES ('EMAJ_INSTALL','E-Maj 1.3.0', 'E-Maj transformed into an EXTENSION');

