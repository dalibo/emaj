-- emaj--0.10.0--unpackaged.sql
--
-- This script unregisters an already installed 0.10.0 E-Maj EXTENSION 

-- unlink emaj technical sequences

ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_hist_hist_id_seq;
ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_mark_mark_id_seq;
ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_sequence_sequ_id_seq;
ALTER EXTENSION emaj DROP SEQUENCE emaj.emaj_seq_hole_sqhl_id_seq;

-- unlink emaj technical tables

ALTER EXTENSION emaj DROP TABLE emaj.emaj_param;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_hist;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_group_def;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_group;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_relation;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_mark;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_sequence;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_seq_hole;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_fk;
ALTER EXTENSION emaj DROP TABLE emaj.emaj_rlbk_stat;

-- unlink emaj types

ALTER EXTENSION emaj DROP TYPE emaj.emaj_log_stat_type;
ALTER EXTENSION emaj DROP TYPE emaj.emaj_detailed_log_stat_type;

-- unlink emaj functions

ALTER EXTENSION emaj DROP FUNCTION emaj._txid_current();
ALTER EXTENSION emaj DROP FUNCTION emaj._pg_version();
ALTER EXTENSION emaj DROP FUNCTION emaj._purge_hist();
ALTER EXTENSION emaj DROP FUNCTION emaj._get_mark_name(TEXT, TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._get_mark_datetime(TEXT, TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._check_group_names_array(v_groupNames TEXT[]);
ALTER EXTENSION emaj DROP FUNCTION emaj._check_class(v_schemaName TEXT, v_className TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._check_new_mark(INOUT v_mark TEXT, v_groupNames TEXT[]);
ALTER EXTENSION emaj DROP FUNCTION emaj._create_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._drop_tbl(v_schemaName TEXT, v_tableName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._drop_seq(v_schemaName TEXT, v_seqName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_tbl(v_schemaName TEXT, v_tableName TEXT, v_timestamp TIMESTAMPTZ, v_disableTrigger BOOLEAN, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT, v_lastSeqHoleId BIGINT);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_seq(v_schemaName TEXT, v_seqName TEXT, v_timestamp TIMESTAMPTZ, v_deleteLog BOOLEAN, v_lastSequenceId BIGINT);
ALTER EXTENSION emaj DROP FUNCTION emaj._log_stat_table(v_schemaName TEXT, v_tableName TEXT, v_tsFirstMark TIMESTAMPTZ, v_tsLastMark TIMESTAMPTZ, v_firstLastSeqHoleId BIGINT, v_lastLastSeqHoleId BIGINT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_verify_all();
ALTER EXTENSION emaj DROP FUNCTION emaj._forbid_truncate_fnct();
ALTER EXTENSION emaj DROP FUNCTION emaj._verify_group(v_groupName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._check_fk_groups(v_groupName TEXT[]);
ALTER EXTENSION emaj DROP FUNCTION emaj._lock_groups(v_groupNames TEXT[], v_lockMode TEXT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_create_group(v_groupName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_create_group(v_groupName TEXT, v_isRollbackable BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_comment_group(v_groupName TEXT, v_comment TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_force_drop_group(v_groupName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._drop_group(v_groupName TEXT, v_checkState BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_start_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_start_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._start_groups(v_groupNames TEXT[], v_mark TEXT, v_multiGroup BOOLEAN, v_reset BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_stop_group(v_groupName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_stop_groups(v_groupNames TEXT[]);
ALTER EXTENSION emaj DROP FUNCTION emaj._stop_groups(v_groupNames TEXT[], v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_set_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_set_mark_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._set_mark_groups(v_groupName TEXT[], v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_comment_mark_group(v_groupName TEXT, v_mark TEXT, v_comment TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_get_previous_mark_group(v_groupName TEXT, v_datetime TIMESTAMPTZ);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_delete_mark_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_delete_before_mark_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._delete_before_mark_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_rename_mark_group(v_groupName TEXT, v_mark TEXT, v_newName TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_rollback_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_rollback_groups(v_groupNames TEXT[], v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_rollback_and_stop_group(v_groupName TEXT, v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_rollback_and_stop_groups(v_groupNames TEXT[], v_mark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_logged_rollback_group(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_logged_rollback_groups(v_groupNames TEXT[], v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step1(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_nbsession INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_set_session(v_groupNames TEXT[], v_schema TEXT, v_table TEXT, v_session INT, v_rows BIGINT);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step2(v_groupNames TEXT[], v_session INT, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step3(v_groupNames TEXT[], v_mark TEXT, v_unloggedRlbk BOOLEAN, v_multiGroup BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step4(v_groupNames TEXT[], v_session INT); 
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step5(v_groupNames TEXT[], v_mark TEXT, v_session INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN);
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step6(v_groupNames TEXT[], v_session INT); 
ALTER EXTENSION emaj DROP FUNCTION emaj._rlbk_groups_step7(v_groupNames TEXT[], v_mark TEXT, v_nbTb INT, v_unloggedRlbk BOOLEAN, v_deleteLog BOOLEAN, v_multiGroup BOOLEAN); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_reset_group(v_groupName TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj._rst_group(v_groupName TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT); 
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_detailed_log_stat_group(v_groupName TEXT, v_firstMark TEXT, v_lastMark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_estimate_rollback_duration(v_groupName TEXT, v_mark TEXT);
ALTER EXTENSION emaj DROP FUNCTION emaj.emaj_snap_group(v_groupName TEXT, v_dir TEXT); 

