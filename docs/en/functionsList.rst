E-Maj functions list
====================

The E-Maj functions that are available to users can be grouped into 3 categories. They are listed below, in alphabetic order.

They are all callable by roles having *emaj_adm* privileges. The charts also specifys those callable by *emaj_viewer* roles (sign *(V)* behind the function name).

Tables or sequences level functions
-----------------------------------

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Functions                                        | Input parameters              | Output data                           |
+==================================================+===============================+=======================================+
| :ref:`emaj_assign_sequence                       | | schema TEXT,                | 1 INT                                 |
| <assign_table_sequence>`                         | | sequence TEXT,              |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_sequences                      | | schema TEXT,                | #.sequences INT                       |
| <assign_table_sequence>`                         | | sequences.array TEXT[],     |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_sequences                      | | schema TEXT,                | #.sequences INT                       |
| <assign_table_sequence>`                         | | sequences.to.include.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | sequences.to.exclude.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_table                          | | schema TEXT,                | 1 INT                                 |
| <assign_table_sequence>`                         | | table TEXT,                 |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ properties JSONB ]        |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_tables                         | | schema TEXT,                | #.tables INT                          |
| <assign_table_sequence>`                         | | tables.array TEXT[],        |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ properties JSONB ]        |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_tables                         | | schema TEXT,                | #.tables INT                          |
| <assign_table_sequence>`                         | | tables.to.include.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | tables.to.exclude.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | group TEXT,                 |                                       |
|                                                  | | [ properties JSONB ]        |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_current_log_table                 | | schema TEXT,                | (log.schema TEXT, log.table TEXT)     |
| <emaj_get_current_log_table>` (V)                | | table TEXT                  |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_table                          | | schema TEXT,                | #.tables INT                          |
| <modify_table>`                                  | | table TEXT,                 |                                       |
|                                                  | | properties JSONB,           |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_tables                         | | schema TEXT,                | #.tables INT                          |
| <modify_table>`                                  | | tables.array TEXT[],        |                                       |
|                                                  | | properties JSONB,           |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_tables                         | | schema TEXT,                | #.tables INT                          |
| <modify_table>`                                  | | tables.to.include.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | tables.to.exclude.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | properties JSONB,           |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequence                         | | schema TEXT,                | 1 INT                                 |
| <move_table_sequence>`                           | | sequence TEXT,              |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequences                        | | schema TEXT,                | #.sequences INT                       |
| <move_table_sequence>`                           | | sequences.array TEXT[],     |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequences                        | | schema TEXT,                | #.sequences INT                       |
| <move_table_sequence>`                           | | sequences.to.include.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | sequences.to.exclude.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_table                            | | schema TEXT,                | 1 INT                                 |
| <move_table_sequence>`                           | | table TEXT,                 |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_tables                           | | schema TEXT,                | #.tables INT                          |
| <move_table_sequence>`                           | | tables.array TEXT[],        |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_tables                           | | schema TEXT,                | #.tables INT                          |
| <move_table_sequence>`                           | | tables.to.include.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | tables.to.exclude.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | new.group TEXT,             |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequence                       | | schema TEXT,                | 1 INT                                 |
| <remove_table_sequence>`                         | | sequence TEXT,              |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequences                      | | schema TEXT,                | #.sequences INT                       |
| <remove_table_sequence>`                         | | sequences.array TEXT[],     |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequences                      | | schema TEXT,                | #.sequences INT                       |
| <remove_table_sequence>`                         | | sequences.to.include.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | sequences.to.exclude.filter |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_table                          | | schema TEXT,                | 1 INT                                 |
| <remove_table_sequence>`                         | | table TEXT,                 |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_tables                         | | schema TEXT,                | #.tables INT                          |
| <remove_table_sequence>`                         | | tables.array TEXT[],        |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_tables                         | | schema TEXT,                | #.tables INT                          |
| <remove_table_sequence>`                         | | tables.to.include.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | tables.to.exclude.filter    |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | [ mark TEXT ]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+


Groups level functions
----------------------

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Functions                                        | Input parameters              | Output data                           |
+==================================================+===============================+=======================================+
| :ref:`emaj_comment_group                         | | group TEXT,                 |                                       |
| <emaj_comment_group>`                            | | comment TEXT                |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_comment_mark_group                    | | group TEXT,                 |                                       |
| <emaj_comment_mark_group>`                       | | mark TEXT,                  |                                       |
|                                                  | | comment TEXT                |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_consolidate_rollback_group            | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_consolidate_rollback_group>`               | | end.rollback.mark TEXT      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_create_group                          | | group TEXT,                 | 1 INT                                 |
| <emaj_create_group>`                             | | [is.rollbackable BOOLEAN],  |                                       |
|                                                  | | [comment TEXT]              |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_delete_before_mark_group              | | group TEXT,                 | #.deleted.marks INT                   |
| <emaj_delete_before_mark_group>`                 | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_delete_mark_group                     | | group TEXT,                 | 1 INT                                 |
| <emaj_delete_mark_group>`                        | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_detailed_log_stat_group               | | group TEXT,                 | SETOF emaj_detailed_log_stat_type     |
| <emaj_detailed_log_stat_group>` (V)              | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_detailed_log_stat_groups              | | groups.array TEXT[],        | SETOF emaj_detailed_log_stat_type     |
| <multiGroupsFunctions>` (V)                      | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_drop_group                            | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_drop_group>`                               |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_dump_changes_group                    | | group TEXT,                 | msg.#.files BIGINT                    |
| <emaj_dump_changes_group>`                       | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | options.list TEXT           |                                       |
|                                                  | | tables.seq.array TEXT[]     |                                       |
|                                                  | | output.directory TEXT,      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_estimate_rollback_group               | | group TEXT,                 | duration INTERVAL                     |
| <emaj_estimate_rollback_group>` (V)              | | mark TEXT,                  |                                       |
|                                                  | | is.logged BOOLEAN           |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_estimate_rollback_groups              | | groups.array TEXT[],        | duration INTERVAL                     |
| <multiGroupsFunctions>` (V)                      | | mark TEXT,                  |                                       |
|                                                  | | is.logged BOOLEAN           |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_drop_group                      | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_force_drop_group>`                         |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_stop_group                      | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_force_stop_group>`                         |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_forget_group                          | | group TEXT                  | #.erased.traces INT                   |
| <emaj_forget_group>`                             |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_gen_sql_dump_changes_group            | | group TEXT,                 | msg.#.statements BIGINT               |
| <emaj_gen_sql_dump_changes_group>` (V)           | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | options.list TEXT           |                                       |
|                                                  | | tables.seq.array TEXT[]     |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_gen_sql_dump_changes_group            | | group TEXT,                 | msg.#.statements BIGINT               |
| <emaj_gen_sql_dump_changes_group>`               | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | options.list TEXT           |                                       |
|                                                  | | tables.seq.array TEXT[]     |                                       |
|                                                  | | output.directory TEXT,      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_gen_sql_group                         | | group TEXT,                 | #.gen.statements BIGINT               |
| <emaj_gen_sql_group>`                            | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | output.file.path TEXT,      |                                       |
|                                                  | | [tables.seq.array TEXT[]]   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_gen_sql_groups                        | | groups.array TEXT[],        | #.gen.statements BIGINT               |
| <multiGroupsFunctions>`                          | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | output.file.path TEXT,      |                                       |
|                                                  | | [tables.seq.array TEXT[]]   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | group TEXT,                 | mark TEXT                             |
| <emaj_get_previous_mark_group>` (V)              | | date.time TIMESTAMPTZ       |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | group TEXT,                 | mark TEXT                             |
| <emaj_get_previous_mark_group>` (V)              | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_log_stat_group                        | | group TEXT,                 | SETOF emaj_log_stat_type              |
| <emaj_log_stat_group>` (V)                       | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_log_stat_groups                       | | groups.array TEXT[],        | SETOF emaj_log_stat_type              |
| <multiGroupsFunctions>` (V)                      | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_logged_rollback_group                 | | group TEXT,                 | SETOF (severity TEXT, message TEXT)   |
| <emaj_logged_rollback_group>`                    | | mark TEXT,                  |                                       |
|                                                  | | [is.alter.group.allowed     |                                       |
|                                                  | |  BOOLEAN]                   |                                       |
|                                                  | | [comment TEXT]              |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_logged_rollback_groups                | | groups.array TEXT[],        | SETOF (severity TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | mark TEXT,                  |                                       |
|                                                  | | [is.alter.group.allowed     |                                       |
|                                                  | |  BOOLEAN]                   |                                       |
|                                                  | | [comment TEXT]              |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_protect_group                         | | group TEXT                  | 0/1 INT                               |
| <emaj_protect_group>`                            |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_protect_mark_group                    | | group TEXT,                 | 0/1 INT                               |
| <emaj_protect_mark_group>`                       | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rename_mark_group                     | | group TEXT,                 |                                       |
| <emaj_rename_mark_group>`                        | | mark TEXT,                  |                                       |
|                                                  | | new.name TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_reset_group                           | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_reset_group>`                              |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rollback_group                        | | group TEXT,                 | SETOF (severity TEXT, message TEXT)   |
| <emaj_rollback_group>`                           | | mark TEXT,                  |                                       |
|                                                  | | [is_alter_group_allowed     |                                       |
|                                                  | |  BOOLEAN]                   |                                       |
|                                                  | | [comment TEXT]              |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_rollback_groups                       | | groups.array TEXT[],        | SETOF (severity TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | mark TEXT,                  |                                       |
|                                                  | | [is_alter_group_allowed     |                                       |
|                                                  | |  BOOLEAN]                   |                                       |
|                                                  | | [comment TEXT]              |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_sequence_stat_group                   | | group TEXT,                 | SETOF emaj_sequence_stat_type         |
| <emaj_sequence_stat_group>` (V)                  | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_sequence_stat_groups                  | | groups.array TEXT[],        | SETOF emaj_sequence_stat_type         |
| <multiGroupsFunctions>` (V)                      | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_set_mark_group                        | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_set_mark_group>`                           | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_set_mark_groups                       | | groups.array TEXT[],        | #.tables.and.seq INT                  |
| <multiGroupsFunctions>`                          | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_snap_group                            | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_snap_group>`                               | | directory TEXT,             |                                       |
|                                                  | | copy.options TEXT           |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_start_group                           | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_start_group>`                              | | [mark TEXT],                |                                       |
|                                                  | | [reset.log BOOLEAN]         |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_start_groups                          | | groups.array TEXT[],        | #.tables.and.seq INT                  |
| <multiGroupsFunctions>`                          | | [mark TEXT],                |                                       |
|                                                  | | [reset.log BOOLEAN]         |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_stop_group                            | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_stop_group>`                               | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_stop_groups                           | | groups.array TEXT[],        | #.tables.and.seq INT                  |
| <multiGroupsFunctions>`                          | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_unprotect_group                       | | group TEXT                  | 0/1 INT                               |
| <emaj_unprotect_group>`                          |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_unprotect_mark_group                  | | group TEXT,                 | 0/1 INT                               |
| <emaj_unprotect_mark_group>`                     | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+

General purpose functions
-------------------------
+--------------------------------------------------+-------------------------------+---------------------------------------+
| Functions                                        | Input parameters              | Output data                           |
+==================================================+===============================+=======================================+
| :ref:`emaj_cleanup_rollback_state                |                               | #.rollback INT                        |
| <emaj_cleanup_rollback_state>`                   |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_comment_rollback                      | | rollback.id INT,            |                                       |
| <emaj_comment_rollback>`                         | | comment TEXT                |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_disable_protection_by_event_triggers  |                               | #.triggers INT                        |
| <emaj_disable_protection_by_event_triggers>`     |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_drop_extension                        |                               |                                       |
| <uninstall>`                                     |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_enable_protection_by_event_triggers   |                               | #.triggers INT                        |
| <emaj_enable_protection_by_event_triggers>`      |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_export_groups_configuration           | | NULL,                       | configuration JSON                    |
| <export_groups_conf>`                            | | [groups.array TEXT[]]       |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_export_groups_configuration           | | file.path TEXT,             | #.groups INT                          |
| <export_groups_conf>`                            | | [groups.array TEXT[]]       |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_export_parameters_configuration       |                               | parameters JSON                       |
| <export_param_conf>`                             |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_export_parameters_configuration       | file.path TEXT                | #.parameters INT                      |
| <export_param_conf>`                             |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_consolidable_rollbacks            |                               | SETOF emaj_consolidable_rollback_type |
| <emaj_get_consolidable_rollbacks>` (V)           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_version                           |                               | version TEXT                          |
| <emaj_get_version>` (V)                          |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_import_groups_configuration           | | groups JSON,                | #.groups INT                          |
| <import_groups_conf>`                            | | [groups.array TEXT[]],      |                                       |
|                                                  | | [alter.logging.groups       |                                       |
|                                                  | |  BOOLEAN],                  |                                       |
|                                                  | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_import_groups_configuration           | | file.path TEXT,             | #.groups INT                          |
| <import_groups_conf>`                            | | [groups.array TEXT[]],      |                                       |
|                                                  | | [alter.logging.groups       |                                       |
|                                                  | |  BOOLEAN],                  |                                       |
|                                                  | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_import_parameters_configuration       | | parameters JSON,            | #.parameters INT                      |
| <import_param_conf>`                             | | [delete.conf BOOLEAN)]      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_import_parameters_configuration       | | file.path TEXT,             | #.parameters INT                      |
| <import_param_conf>`                             | | [delete.conf BOOLEAN)]      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_purge_histories                       | retention.delay INTERVAL      |                                       |
| <emaj_purge_histories>`                          |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rollback_activity                     |                               | SETOF emaj_rollback_activity_type     |
| <emaj_rollback_activity>` (V)                    |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_verify_all                            |                               | SETOF TEXT                            |
| <emaj_verify_all>` (V)                           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
