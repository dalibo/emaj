-- emaj--unpackaged--0.11.0.sql
--
-- This script registers as an EXTENSION an already installed version of E-Maj 0.11.0 

-- link emaj schema

--ALTER EXTENSION emaj ADD SCHEMA emaj;

-- link emaj technical tables and sequence

ALTER EXTENSION emaj ADD SEQUENCE emaj.emaj_global_seq;

ALTER EXTENSION emaj ADD TABLE emaj.emaj_param;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_hist;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_group_def;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_group;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_relation;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_mark;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_sequence;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_seq_hole;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_fk;
ALTER EXTENSION emaj ADD TABLE emaj.emaj_rlbk_stat;

-- link emaj types

ALTER EXTENSION emaj ADD TYPE emaj.emaj_log_stat_type;
ALTER EXTENSION emaj ADD TYPE emaj.emaj_detailed_log_stat_type;

-- link emaj functions

ALTER EXTENSION emaj ADD FUNCTION emaj._txid_current();
ALTER EXTENSION emaj ADD FUNCTION emaj._pg_version();
ALTER EXTENSION emaj ADD FUNCTION emaj._purge_hist();
ALTER EXTENSION emaj ADD FUNCTION emaj._get_mark_name(TEXT, TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._get_mark_datetime(TEXT, TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._build_log_seq_name(TEXT, TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_group_names_array(v_groupNames TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_lastGlobalSeq BIGINT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_verify_all();
ALTER EXTENSION emaj ADD FUNCTION emaj._forbid_truncate_fnct();
ALTER EXTENSION emaj ADD FUNCTION emaj._log_truncate_fnct();
ALTER EXTENSION emaj ADD FUNCTION emaj._verify_group(v_groupName TEXT, v_onErrorStop boolean);
ALTER EXTENSION emaj ADD FUNCTION emaj._check_fk_groups(v_groupName TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_create_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_resetLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_group(v_groupName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]);
ALTER EXTENSION emaj ADD FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_rollback_and_stop_groups(v_groupNames TEXT[], v_mark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbsession INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_set_session(v_groupNames TEXT[], v_schema TEXT, v_table TEXT, v_session INT, v_rows BIGINT);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step2(v_groupNames TEXT[], v_session INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step4(v_groupNames TEXT[], v_session INT); 
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step5(v_groupNames TEXT[], v_mark TEXT, v_session INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN);
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT); 
ALTER EXTENSION emaj ADD FUNCTION emaj._rlbk_groups_step7(v_groupNames TEXT[], v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_reset_group(v_groupName TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj._rst_group(v_groupName TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT, v_copyOptions TEXT); 
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_snap_log_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_dir TEXT, v_copyOptions TEXT);
ALTER EXTENSION emaj ADD FUNCTION emaj.emaj_generate_sql(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT, v_location TEXT);

