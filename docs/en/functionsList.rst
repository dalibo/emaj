E-Maj functions list
====================

E-Maj functions that are available to users are listed in alphabetic order below. They are all callable by roles having *emaj_adm* privileges. The chart also specifies those callable by *emaj_viewer* roles (sign *(V)* behind the function name).

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Functions                                        | Input parameters              | Output data                           |
+==================================================+===============================+=======================================+
| :doc:`emaj_alter_group                           | | group TEXT,                 | #.tables.and.seq INT                  |
| <alterGroups>`                                   | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_alter_groups                          | | groups.array TEXT[],        | #.tables.and.seq INT                  |
| <multiGroupsFunctions>`                          | | [mark TEXT]                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_cleanup_rollback_state                |                               | #.rollback INT                        |
| <emaj_cleanup_rollback_state>`                   |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
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
| :ref:`emaj_create_group                          | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_create_group>`                             | | [is.rollbackable BOOLEAN],  |                                       |
|                                                  | | [is.empty BOOLEAN]          |                                       |
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
| :ref:`emaj_disable_protection_by_event_triggers  |                               | #.triggers INT                        |
| <emaj_disable_protection_by_event_triggers>`     |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_drop_group                            | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_drop_group>`                               |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_enable_protection_by_event_triggers   |                               | #.triggers INT                        |
| <emaj_enable_protection_by_event_triggers>`      |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_estimate_rollback_group               | | group TEXT,                 | duration INTERVAL                     |
| <emaj_estimate_rollback_group>` (V)              | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_estimate_rollback_groups              | | groups.array TEXT[],        | duration INTERVAL                     |
| <multiGroupsFunctions>` (V)                      | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_drop_group                      | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_force_drop_group>`                         |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_stop_group                      | | group TEXT                  | #.tables.and.seq INT                  |
| <emaj_force_stop_group>`                         |                               |                                       |
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
| :ref:`emaj_get_consolidable_rollbacks            |                               | SETOF emaj_consolidable_rollback_type |
| <emaj_get_consolidable_rollbacks>` (V)           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | group TEXT,                 | mark TEXT                             |
| <emaj_get_previous_mark_group>` (V)              | | date.time TIMESTAMPTZ       |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | group TEXT,                 | mark TEXT                             |
| <emaj_get_previous_mark_group>` (V)              | | mark TEXT                   |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_ignore_app_trigger                    | | action TEXT,                | #.triggers INT                        |
| <emaj_ignore_app_trigger>`                       | | schema TEXT,                |                                       |
|                                                  | | table TEXT,                 |                                       |
|                                                  | | trigger TEXT                |                                       |
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
|                                                  | | is_alter_group_allowed      |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_logged_rollback_groups                | | groups.array TEXT[],        | SETOF (severity TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | mark TEXT,                  |                                       |
|                                                  | | is_alter_group_allowed      |                                       |
|                                                  | |  BOOLEAN                    |                                       |
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
| :ref:`emaj_rollback_activity                     |                               | SETOF emaj_rollback_activity_type     |
| <emaj_rollback_activity>` (V)                    |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rollback_group                        | | group TEXT,                 | SETOF (severity TEXT, message TEXT)   |
| <emaj_rollback_group>`                           | | mark TEXT,                  |                                       |
|                                                  | | is_alter_group_allowed      |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_rollback_groups                       | | groups.array TEXT[],        | SETOF (severity TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | mark TEXT,                  |                                       |
|                                                  | | is_alter_group_allowed      |                                       |
|                                                  | |  BOOLEAN                    |                                       |
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
| :ref:`emaj_snap_log_group                        | | group TEXT,                 | #.tables.and.seq INT                  |
| <emaj_snap_log_group>`                           | | start.mark TEXT,            |                                       |
|                                                  | | end.mark TEXT,              |                                       |
|                                                  | | directory TEXT,             |                                       |
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
| :ref:`emaj_verify_all                            |                               | Setof TEXT                            |
| <emaj_verify_all>` (V)                           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+

