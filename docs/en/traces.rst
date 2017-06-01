Traces of operations
====================

.. _emaj_hist:

All operations performed by E-Maj, and that impact in any way a tables group, are traced into a table named *emaj_hist*.
 
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

+------------------------+-------------------------------------------------------------------------+
| Value                  | Meaning                                                                 |
+========================+=========================================================================+
| ALTER_GROUP            | tables group change                                                     |
+------------------------+-------------------------------------------------------------------------+
| ALTER_GROUPS           | tables groups change                                                    |
+------------------------+-------------------------------------------------------------------------+
| CLEANUP_RLBK_STATE     | cleanup the state of recently completed rollback operations             |
+------------------------+-------------------------------------------------------------------------+
| COMMENT_GROUP          | comment set on a group                                                  |
+------------------------+-------------------------------------------------------------------------+
| COMMENT_MARK_GROUP     | comment set on a mark for a tables group                                |
+------------------------+-------------------------------------------------------------------------+
| CONSOLIDATE_RLBK_GROUP | consolidate a logged rollback operation                                 |
+------------------------+-------------------------------------------------------------------------+
| CREATE_GROUP           | tables group creation                                                   |
+------------------------+-------------------------------------------------------------------------+
| DBLINK_OPEN_CNX        | open a dblink connection for a rollback operation                       |
+------------------------+-------------------------------------------------------------------------+
| DBLINK_CLOSE_CNX       | close a dblink connection for a rollback operation                      |
+------------------------+-------------------------------------------------------------------------+
| DELETE_MARK_GROUP      | mark deletion for a tables group                                        |
+------------------------+-------------------------------------------------------------------------+
| DISABLE_EVENT_TRIGGERS | desactivate event triggers                                              |
+------------------------+-------------------------------------------------------------------------+
| DROP_GROUP             | tables group suppression                                                |
+------------------------+-------------------------------------------------------------------------+
| EMAJ_INSTALL           | E-Maj installation or version update                                    |
+------------------------+-------------------------------------------------------------------------+
| ENABLE_EVENT_TRIGGERS  | activate event triggers                                                 |
+------------------------+-------------------------------------------------------------------------+
| FORCE_DROP_GROUP       | tables group forced suppression                                         |
+------------------------+-------------------------------------------------------------------------+
| FORCE_STOP_GROUP       | tables group forced stop                                                |
+------------------------+-------------------------------------------------------------------------+
| GEN_SQL_GROUP          | generation of a psql script to replay updates for a tables group        |
+------------------------+-------------------------------------------------------------------------+
| GEN_SQL_GROUPS         | generation of a psql script to replay updates for several tables groups |
+------------------------+-------------------------------------------------------------------------+
| LOCK_GROUP             | lock set on tables of a group                                           |
+------------------------+-------------------------------------------------------------------------+
| LOCK_GROUPS            | lock set on tables of several groups                                    |
+------------------------+-------------------------------------------------------------------------+
| LOCK_SESSION           | lock set on tables for a rollback session                               |
+------------------------+-------------------------------------------------------------------------+
| PROTECT_GROUP          | set a protection against rollbacks on a group                           |
+------------------------+-------------------------------------------------------------------------+
| PROTECT_MARK_GROUP     | set a protection against rollbacks on a mark for a group                |
+------------------------+-------------------------------------------------------------------------+
| PURGE_HISTORY          | delete from the *emaj_hist* table the events prior the retention delay  |
+------------------------+-------------------------------------------------------------------------+
| RENAME_MARK_GROUP      | mark rename for a tables group                                          |
+------------------------+-------------------------------------------------------------------------+
| RESET_GROUP            | log tables content reset for a group                                    |
+------------------------+-------------------------------------------------------------------------+
| ROLLBACK_GROUP         | rollback updates for a tables group                                     |
+------------------------+-------------------------------------------------------------------------+
| ROLLBACK_GROUPS        | rollback updates for several tables groups                              |
+------------------------+-------------------------------------------------------------------------+
| ROLLBACK_SEQUENCE      | rollback one sequence                                                   |
+------------------------+-------------------------------------------------------------------------+
| ROLLBACK_TABLE         | rollback updates for one table                                          |
+------------------------+-------------------------------------------------------------------------+
| SET_MARK_GROUP         | mark set on a tables group                                              |
+------------------------+-------------------------------------------------------------------------+
| SET_MARK_GROUPS        | mark set on several tables groups                                       |
+------------------------+-------------------------------------------------------------------------+
| SNAP_GROUP             | snap all tables and sequences for a group                               |
+------------------------+-------------------------------------------------------------------------+
| SNAP_LOG_GROUP         | snap all log tables for a group                                         |
+------------------------+-------------------------------------------------------------------------+
| START_GROUP            | tables group start                                                      |
+------------------------+-------------------------------------------------------------------------+
| START_GROUPS           | tables groups start                                                     |
+------------------------+-------------------------------------------------------------------------+
| STOP_GROUP             | tables group stop                                                       |
+------------------------+-------------------------------------------------------------------------+
| STOP_GROUPS            | tables groups stop                                                      |
+------------------------+-------------------------------------------------------------------------+
| UNPROTECT_GROUP        | remove a protection against rollbacks on a group                        |
+------------------------+-------------------------------------------------------------------------+
| UNPROTECT_MARK_GROUP   | remove a protection against rollbacks on a mark for a group             |
+------------------------+-------------------------------------------------------------------------+

The *hist_event* column can take the following values:

+------------------------------+-----------------------------------------+
| Value                        | Meaning                                 |
+==============================+=========================================+
| BEGIN                        |                                         |
+------------------------------+-----------------------------------------+
| END                          |                                         |
+------------------------------+-----------------------------------------+
| EVENT TRIGGERS DISABLED      |                                         |
+------------------------------+-----------------------------------------+
| EVENT TRIGGERS ENABLED       |                                         |
+------------------------------+-----------------------------------------+
| MARK DELETED                 |                                         |
+------------------------------+-----------------------------------------+
| SCHEMA CREATED               | secondary schema created                |
+------------------------------+-----------------------------------------+
| SCHEMA DROPPED               | secondary schema dropped                |
+------------------------------+-----------------------------------------+
| LOG SCHEMA CHANGED           |                                         |
+------------------------------+-----------------------------------------+
| NAMES PREFIX CHANGED         | E-Maj names prefix modified             |
+------------------------------+-----------------------------------------+
| LOG DATA TABLESPACE CHANGED  | Tablespace for the log table modified   |
+------------------------------+-----------------------------------------+
| LOG INDEX TABLESPACE CHANGED | Tablespace for the log index modified   |
+------------------------------+-----------------------------------------+

The *emaj_hist* content can be viewed by anyone who has the proper access rights on this table (*superuser*, *emaj_adm* or *emaj_viewer* roles).

Two other internal tables keep traces of groups alter or rollback operations:

* *emaj_alter_plan* lists the elementary steps performed during the execution of :doc:`emaj_alter_group() <alterGroups>` and related functions,
* *emaj_rlbk_plan* lists the elementary steps performed during the execution of :ref:`emaj_rollback_group() <emaj_rollback_group>` and related functions.

When a tables group is started, using the :ref:`emaj_start_group() <emaj_start_group>` function, or when old marks are deleted, using the :ref:`emaj_delete_before_mark_group() <emaj_delete_before_mark_group>` function, the oldest events are deleted from *emaj_hist* tables. The events kept are those not older than a parametrised retention delay and not older than the oldest active mark and not older than the oldest uncompleted rollback operation. By default, the retention delay for events equals 1 year. But this value can be modified at any time by inserting the *history_retention* parameter into the :ref:`emaj_param <emaj_param>` table with a SQL statement. The same retention applies to the tables that log elementary steps of tables groups alter or rollback operations.

