Traces of operations
====================

.. _emaj_hist:

The emaj_hist table
-------------------

All operations performed by E-Maj, and that impact in any way a tables group, are traced into a table named *emaj_hist*.

Any user having *emaj_adm* or *emaj_viewer* rights may look at the *emaj_hist* content.

The **emaj_hist** table structure is the following:

+--------------+-------------+------------------------------------------------------------+
|Column        | Type        | Description                                                |
+==============+=============+============================================================+
|hist_id       | BIGSERIAL   | serial number identifying a row in this history table      |
+--------------+-------------+------------------------------------------------------------+
|hist_datetime | TIMESTAMPTZ | recording date and time of the row                         |
+--------------+-------------+------------------------------------------------------------+
|hist_function | TEXT        | function associated to the traced event                    |
+--------------+-------------+------------------------------------------------------------+
|hist_event    | TEXT        | kind of event                                              |
+--------------+-------------+------------------------------------------------------------+
|hist_object   | TEXT        | object related to the event (group, table or sequence)     |
+--------------+-------------+------------------------------------------------------------+
|hist_wording  | TEXT        | additional comments                                        |
+--------------+-------------+------------------------------------------------------------+
|hist_user     | TEXT        | role whose action has generated the event                  |
+--------------+-------------+------------------------------------------------------------+
|hist_txid     | BIGINT      | identifier of the transaction that has generated the event |
+--------------+-------------+------------------------------------------------------------+

The *hist_function* column can take the following values:

+----------------------------------+----------------------------------------------------------------------------+
| Value                            | Meaning                                                                    |
+==================================+============================================================================+
| ADJUST_GROUP_PROPERTIES          | ajust the group_has_waiting_changes column content of the emaj_group table |
+----------------------------------+----------------------------------------------------------------------------+
| ASSIGN_SEQUENCE                  | sequence assigned to a tables group                                        |
+----------------------------------+----------------------------------------------------------------------------+
| ASSIGN_SEQUENCES                 | sequences assigned to a tables group                                       |
+----------------------------------+----------------------------------------------------------------------------+
| ASSIGN_TABLE                     | table assigned to a tables group                                           |
+----------------------------------+----------------------------------------------------------------------------+
| ASSIGN_TABLES                    | tables assigned to a tables group                                          |
+----------------------------------+----------------------------------------------------------------------------+
| CLEANUP_RLBK_STATE               | cleanup the state of recently completed rollback operations                |
+----------------------------------+----------------------------------------------------------------------------+
| COMMENT_GROUP                    | comment set on a group                                                     |
+----------------------------------+----------------------------------------------------------------------------+
| COMMENT_MARK_GROUP               | comment set on a mark for a tables group                                   |
+----------------------------------+----------------------------------------------------------------------------+
| COMMENT_ROLLBACK                 | comment set on an E-Maj rollback                                           |
+----------------------------------+----------------------------------------------------------------------------+
| CONSOLIDATE_RLBK_GROUP           | consolidate a logged rollback operation                                    |
+----------------------------------+----------------------------------------------------------------------------+
| CREATE_GROUP                     | tables group creation                                                      |
+----------------------------------+----------------------------------------------------------------------------+
| DBLINK_OPEN_CNX                  | open a dblink connection for a rollback operation                          |
+----------------------------------+----------------------------------------------------------------------------+
| DBLINK_CLOSE_CNX                 | close a dblink connection for a rollback operation                         |
+----------------------------------+----------------------------------------------------------------------------+
| DELETE_MARK_GROUP                | mark deletion for a tables group                                           |
+----------------------------------+----------------------------------------------------------------------------+
| DISABLE_PROTECTION               | desactivate event triggers                                                 |
+----------------------------------+----------------------------------------------------------------------------+
| DROP_GROUP                       | tables group suppression                                                   |
+----------------------------------+----------------------------------------------------------------------------+
| EMAJ_INSTALL                     | E-Maj installation or version update                                       |
+----------------------------------+----------------------------------------------------------------------------+
| ENABLE_PROTECTION                | activate event triggers                                                    |
+----------------------------------+----------------------------------------------------------------------------+
| EXPORT_GROUPS                    | export a tables groups configuration                                       |
+----------------------------------+----------------------------------------------------------------------------+
| EXPORT_PARAMETERS                | export an E-maj parameters configuration                                   |
+----------------------------------+----------------------------------------------------------------------------+
| FORCE_DROP_GROUP                 | tables group forced suppression                                            |
+----------------------------------+----------------------------------------------------------------------------+
| FORCE_STOP_GROUP                 | tables group forced stop                                                   |
+----------------------------------+----------------------------------------------------------------------------+
| GEN_SQL_GROUP                    | generation of a psql script to replay updates for a tables group           |
+----------------------------------+----------------------------------------------------------------------------+
| GEN_SQL_GROUPS                   | generation of a psql script to replay updates for several tables groups    |
+----------------------------------+----------------------------------------------------------------------------+
| IMPORT_GROUPS                    | import a tables groups configuration                                       |
+----------------------------------+----------------------------------------------------------------------------+
| IMPORT_PARAMETERS                | import an E-maj parameters configuration                                   |
+----------------------------------+----------------------------------------------------------------------------+
| LOCK_GROUP                       | lock set on tables of a group                                              |
+----------------------------------+----------------------------------------------------------------------------+
| LOCK_GROUPS                      | lock set on tables of several groups                                       |
+----------------------------------+----------------------------------------------------------------------------+
| LOCK_SESSION                     | lock set on tables for a rollback session                                  |
+----------------------------------+----------------------------------------------------------------------------+
| MODIFY_TABLE                     | table properties change                                                    |
+----------------------------------+----------------------------------------------------------------------------+
| MODIFY_TABLES                    | tables properties change                                                   |
+----------------------------------+----------------------------------------------------------------------------+
| MOVE_SEQUENCE                    | sequence moved to another tables group                                     |
+----------------------------------+----------------------------------------------------------------------------+
| MOVE_SEQUENCES                   | sequences moved to another tables group                                    |
+----------------------------------+----------------------------------------------------------------------------+
| MOVE_TABLE                       | table moved to another tables group                                        |
+----------------------------------+----------------------------------------------------------------------------+
| MOVE_TABLES                      | tables moved to another tables group                                       |
+----------------------------------+----------------------------------------------------------------------------+
| PROTECT_GROUP                    | set a protection against rollbacks on a group                              |
+----------------------------------+----------------------------------------------------------------------------+
| PROTECT_MARK_GROUP               | set a protection against rollbacks on a mark for a group                   |
+----------------------------------+----------------------------------------------------------------------------+
| PURGE_HISTORIES                  | delete from the historical tables the events prior the retention delay     |
+----------------------------------+----------------------------------------------------------------------------+
| REMOVE_SEQUENCE                  | sequence removed from its tables group                                     |
+----------------------------------+----------------------------------------------------------------------------+
| REMOVE_SEQUENCES                 | sequences removed from their tables group                                  |
+----------------------------------+----------------------------------------------------------------------------+
| REMOVE_TABLE                     | table removed from its tables group                                        |
+----------------------------------+----------------------------------------------------------------------------+
| REMOVE_TABLES                    | tables removed from their tables group                                     |
+----------------------------------+----------------------------------------------------------------------------+
| RENAME_MARK_GROUP                | mark rename for a tables group                                             |
+----------------------------------+----------------------------------------------------------------------------+
| RESET_GROUP                      | log tables content reset for a group                                       |
+----------------------------------+----------------------------------------------------------------------------+
| ROLLBACK_GROUP                   | rollback updates for a tables group                                        |
+----------------------------------+----------------------------------------------------------------------------+
| ROLLBACK_GROUPS                  | rollback updates for several tables groups                                 |
+----------------------------------+----------------------------------------------------------------------------+
| ROLLBACK_SEQUENCE                | rollback one sequence                                                      |
+----------------------------------+----------------------------------------------------------------------------+
| ROLLBACK_TABLE                   | rollback updates for one table                                             |
+----------------------------------+----------------------------------------------------------------------------+
| SET_MARK_GROUP                   | mark set on a tables group                                                 |
+----------------------------------+----------------------------------------------------------------------------+
| SET_MARK_GROUPS                  | mark set on several tables groups                                          |
+----------------------------------+----------------------------------------------------------------------------+
| SNAP_GROUP                       | snap all tables and sequences for a group                                  |
+----------------------------------+----------------------------------------------------------------------------+
| SNAP_LOG_GROUP                   | snap all log tables for a group                                            |
+----------------------------------+----------------------------------------------------------------------------+
| START_GROUP                      | tables group start                                                         |
+----------------------------------+----------------------------------------------------------------------------+
| START_GROUPS                     | tables groups start                                                        |
+----------------------------------+----------------------------------------------------------------------------+
| STOP_GROUP                       | tables group stop                                                          |
+----------------------------------+----------------------------------------------------------------------------+
| STOP_GROUPS                      | tables groups stop                                                         |
+----------------------------------+----------------------------------------------------------------------------+
| UNPROTECT_GROUP                  | remove a protection against rollbacks on a group                           |
+----------------------------------+----------------------------------------------------------------------------+
| UNPROTECT_MARK_GROUP             | remove a protection against rollbacks on a mark for a group                |
+----------------------------------+----------------------------------------------------------------------------+

The *hist_event* column can take the following values:

+------------------------------+----------------------------------------------------------------+
| Value                        | Meaning                                                        |
+==============================+================================================================+
| BEGIN                        |                                                                |
+------------------------------+----------------------------------------------------------------+
| DELETED PARAMETER            | parameter deleted from *emaj_param*                            |
+------------------------------+----------------------------------------------------------------+
| END                          |                                                                |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGER RECREATED      |                                                                |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGERS DISABLED      |                                                                |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGERS ENABLED       |                                                                |
+------------------------------+----------------------------------------------------------------+
| GROUP CREATED                | new tables group created                                       |
+------------------------------+----------------------------------------------------------------+
| INSERTED PARAMETER           | parameter inserted into *emaj_param*                           |
+------------------------------+----------------------------------------------------------------+
| LOG DATA TABLESPACE CHANGED  | tablespace for the log table modified                          |
+------------------------------+----------------------------------------------------------------+
| LOG INDEX TABLESPACE CHANGED | tablespace for the log index modified                          |
+------------------------------+----------------------------------------------------------------+
| LOG_SCHEMA CREATED           | secondary schema created                                       |
+------------------------------+----------------------------------------------------------------+
| LOG_SCHEMA DROPPED           | secondary schema dropped                                       |
+------------------------------+----------------------------------------------------------------+
| MARK DELETED                 |                                                                |
+------------------------------+----------------------------------------------------------------+
| NAMES PREFIX CHANGED         | E-Maj names prefix modified                                    |
+------------------------------+----------------------------------------------------------------+
| NOTICE                       | warning message issued by a rollback                           |
+------------------------------+----------------------------------------------------------------+
| PRIORITY CHANGED             | priority level modified                                        |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE ADDED               | sequence added to a logging tables group                       |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE MOVED               | sequence moved from one group to another                       |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE REMOVED             | sequence removed from a logging tables group                   |
+------------------------------+----------------------------------------------------------------+
| TABLE ADDED                  | table  added to a logging tables group                         |
+------------------------------+----------------------------------------------------------------+
| TABLE MOVED                  | table moved from one group to another                          |
+------------------------------+----------------------------------------------------------------+
| TABLE REMOVED                | table removed from a logging tables group                      |
+------------------------------+----------------------------------------------------------------+
| TABLE REPAIRED               | table repaired for E-Maj                                       |
+------------------------------+----------------------------------------------------------------+
| TIME STAMP SET               | internal time stamp recorded                                   |
+------------------------------+----------------------------------------------------------------+
| TRIGGERS TO IGNORE CHANGED   | set of application triggers to ignore at rollback time changed |
+------------------------------+----------------------------------------------------------------+
| UPDATED PARAMETER            | parameter updated in *emaj_param*                              |
+------------------------------+----------------------------------------------------------------+
| WARNING                      | warning message issued by a rollback                           |
+------------------------------+----------------------------------------------------------------+

Purge obsolete traces
---------------------

When a tables group is started, using the :ref:`emaj_start_group() <emaj_start_group>` function, or when old marks are deleted, using the :ref:`emaj_delete_before_mark_group() <emaj_delete_before_mark_group>` function, the oldest events are deleted from *emaj_hist* tables. The events kept are those not older than a parametrised retention delay and not older than the oldest mark and not older than the oldest uncompleted rollback operation. By default, the retention delay for events equals 1 year. But this value can be modified at any time by inserting the *history_retention* parameter into the :ref:`emaj_param <emaj_param>` table with a SQL statement. The same retention applies to the tables that log elementary steps of tables groups alter or rollback operations.

The obsolete traces purge can also be initiated by explicitely calling the :ref:`emaj_purge_histories() <emaj_purge_histories>` function. The input parameter of the function defines a retention delay that overloads the *history_retention* parameter of the *emaj_param* table.

In order to schedule purges periodically, it is possible to:

* set the *history_retention* parameter to a very high value (for instance '100 YEARS'), so that tables groups starts and oldest marks deletions do not perform any purge, and
* schedule purge operations by any means (*crontab*, *pgAgent*, *pgTimeTable* or any other tool).
