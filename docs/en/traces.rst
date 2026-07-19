Traces of Operations
====================

.. _emaj_hist:

The emaj_hist Table
-------------------

All significant operations performed by E-Maj are traced in the *emaj_hist* table.

Any user with *emaj_adm* or *emaj_viewer* rights can view the *emaj_hist* content.

Table Structure
^^^^^^^^^^^^^^^

The **emaj_hist** table structure is as follows:

+--------------+-------------+---------------------------------------------------------+
| Column       | Type        | Description                                             |
+==============+=============+=========================================================+
| hist_id      | BIGSERIAL   | Serial number identifying a row in this history table   |
+--------------+-------------+---------------------------------------------------------+
| hist_datetime| TIMESTAMPTZ | Recording date and time of the row                      |
+--------------+-------------+---------------------------------------------------------+
| hist_function| TEXT        | Function associated with the traced event               |
+--------------+-------------+---------------------------------------------------------+
| hist_event   | TEXT        | Kind of event                                           |
+--------------+-------------+---------------------------------------------------------+
| hist_object  | TEXT        | Object related to the event (group, table, or sequence) |
+--------------+-------------+---------------------------------------------------------+
| hist_wording | TEXT        | Additional comments                                     |
+--------------+-------------+---------------------------------------------------------+
| hist_user    | TEXT        | Role whose action generated the event                   |
+--------------+-------------+---------------------------------------------------------+
| hist_txid    | BIGINT      | Identifier of the transaction that generated the event  |
+--------------+-------------+---------------------------------------------------------+

The *hist_function* Column
^^^^^^^^^^^^^^^^^^^^^^^^^^

The *hist_function* column can take the following values:

+----------------------------------+---------------------------------------------------------------------------+
| Value                            | Meaning                                                                   |
+==================================+===========================================================================+
| ASSIGN_SEQUENCE                  | Sequence assigned to a table group                                        |
+----------------------------------+---------------------------------------------------------------------------+
| ASSIGN_SEQUENCES                 | Sequences assigned to a table group                                       |
+----------------------------------+---------------------------------------------------------------------------+
| ASSIGN_TABLE                     | Table assigned to a table group                                           |
+----------------------------------+---------------------------------------------------------------------------+
| ASSIGN_TABLES                    | Tables assigned to a table group                                          |
+----------------------------------+---------------------------------------------------------------------------+
| CLEANUP_RLBK_STATE               | Clean up the state of recently completed rollback operations              |
+----------------------------------+---------------------------------------------------------------------------+
| COMMENT_GROUP                    | Comment set on a group                                                    |
+----------------------------------+---------------------------------------------------------------------------+
| COMMENT_MARK_GROUP               | Comment set on a mark for a table group                                   |
+----------------------------------+---------------------------------------------------------------------------+
| COMMENT_ROLLBACK                 | Comment set on an E-Maj rollback                                          |
+----------------------------------+---------------------------------------------------------------------------+
| CONSOLIDATE_RLBK_GROUP           | Consolidate a logged rollback operation                                   |
+----------------------------------+---------------------------------------------------------------------------+
| CREATE_GROUP                     | Table group creation                                                      |
+----------------------------------+---------------------------------------------------------------------------+
| DBLINK_OPEN_CNX                  | Open a dblink connection for a rollback operation                         |
+----------------------------------+---------------------------------------------------------------------------+
| DBLINK_CLOSE_CNX                 | Close a dblink connection for a rollback operation                        |
+----------------------------------+---------------------------------------------------------------------------+
| DELETE_MARK_GROUP                | Mark deletion for a table group                                           |
+----------------------------------+---------------------------------------------------------------------------+
| DISABLE_PROTECTION               | Deactivate event triggers                                                 |
+----------------------------------+---------------------------------------------------------------------------+
| DROP_GROUP                       | Table group suppression                                                   |
+----------------------------------+---------------------------------------------------------------------------+
| EMAJ_INSTALL                     | E-Maj installation or version update                                      |
+----------------------------------+---------------------------------------------------------------------------+
| ENABLE_PROTECTION                | Activate event triggers                                                   |
+----------------------------------+---------------------------------------------------------------------------+
| EXPORT_GROUPS                    | Export a table groups configuration                                       |
+----------------------------------+---------------------------------------------------------------------------+
| EXPORT_PARAMETERS                | Export an E-Maj parameters configuration                                  |
+----------------------------------+---------------------------------------------------------------------------+
| FORCE_DROP_GROUP                 | Table group forced suppression                                            |
+----------------------------------+---------------------------------------------------------------------------+
| FORCE_STOP_GROUP                 | Table group forced stop                                                   |
+----------------------------------+---------------------------------------------------------------------------+
| FORGET_GROUP                     | Erase historical traces for a dropped table group                         |
+----------------------------------+---------------------------------------------------------------------------+
| GEN_SQL_GROUP                    | Generate a *psql* script to replay updates for a table group              |
+----------------------------------+---------------------------------------------------------------------------+
| GEN_SQL_GROUPS                   | Generate a *psql* script to replay updates for several table groups       |
+----------------------------------+---------------------------------------------------------------------------+
| IMPORT_GROUPS                    | Import a table groups configuration                                       |
+----------------------------------+---------------------------------------------------------------------------+
| IMPORT_PARAMETERS                | Import an E-Maj parameters configuration                                  |
+----------------------------------+---------------------------------------------------------------------------+
| LOCK_GROUP                       | Lock set on tables of a group                                             |
+----------------------------------+---------------------------------------------------------------------------+
| LOCK_GROUPS                      | Lock set on tables of several groups                                      |
+----------------------------------+---------------------------------------------------------------------------+
| LOCK_SESSION                     | Lock set on tables for a rollback session                                 |
+----------------------------------+---------------------------------------------------------------------------+
| MODIFY_TABLE                     | Table properties change                                                   |
+----------------------------------+---------------------------------------------------------------------------+
| MODIFY_TABLES                    | Tables properties change                                                  |
+----------------------------------+---------------------------------------------------------------------------+
| MOVE_SEQUENCE                    | Sequence moved to another table group                                     |
+----------------------------------+---------------------------------------------------------------------------+
| MOVE_SEQUENCES                   | Sequences moved to another table group                                    |
+----------------------------------+---------------------------------------------------------------------------+
| MOVE_TABLE                       | Table moved to another table group                                        |
+----------------------------------+---------------------------------------------------------------------------+
| MOVE_TABLES                      | Tables moved to another table group                                       |
+----------------------------------+---------------------------------------------------------------------------+
| PROTECT_GROUP                    | Set protection against rollbacks on a group                               |
+----------------------------------+---------------------------------------------------------------------------+
| PROTECT_MARK_GROUP               | Set protection against rollbacks on a mark for a group                    |
+----------------------------------+---------------------------------------------------------------------------+
| PURGE_HISTORIES                  | Delete events older than the retention delay from historical tables       |
+----------------------------------+---------------------------------------------------------------------------+
| REMOVE_SEQUENCE                  | Sequence removed from its table group                                     |
+----------------------------------+---------------------------------------------------------------------------+
| REMOVE_SEQUENCES                 | Sequences removed from their table group                                  |
+----------------------------------+---------------------------------------------------------------------------+
| REMOVE_TABLE                     | Table removed from its table group                                        |
+----------------------------------+---------------------------------------------------------------------------+
| REMOVE_TABLES                    | Tables removed from their table group                                     |
+----------------------------------+---------------------------------------------------------------------------+
| RENAME_MARK_GROUP                | Mark renamed for a table group                                            |
+----------------------------------+---------------------------------------------------------------------------+
| RESET_GROUP                      | Log tables content reset for a group                                      |
+----------------------------------+---------------------------------------------------------------------------+
| ROLLBACK_GROUP                   | Rollback updates for a table group                                        |
+----------------------------------+---------------------------------------------------------------------------+
| ROLLBACK_GROUPS                  | Rollback updates for several table groups                                 |
+----------------------------------+---------------------------------------------------------------------------+
| ROLLBACK_SEQUENCE                | Rollback one sequence                                                     |
+----------------------------------+---------------------------------------------------------------------------+
| ROLLBACK_TABLE                   | Rollback updates for one table                                            |
+----------------------------------+---------------------------------------------------------------------------+
| SET_MARK_GROUP                   | Mark set on a table group                                                 |
+----------------------------------+---------------------------------------------------------------------------+
| SET_MARK_GROUPS                  | Mark set on several table groups                                          |
+----------------------------------+---------------------------------------------------------------------------+
| SET_PARAM                        | Parameter value change                                                    |
+----------------------------------+---------------------------------------------------------------------------+
| SNAP_GROUP                       | Snap all tables and sequences for a group                                 |
+----------------------------------+---------------------------------------------------------------------------+
| SNAP_LOG_GROUP                   | Snap all log tables for a group                                           |
+----------------------------------+---------------------------------------------------------------------------+
| START_GROUP                      | Table group start                                                         |
+----------------------------------+---------------------------------------------------------------------------+
| START_GROUPS                     | Table groups start                                                        |
+----------------------------------+---------------------------------------------------------------------------+
| STOP_GROUP                       | Table group stop                                                          |
+----------------------------------+---------------------------------------------------------------------------+
| STOP_GROUPS                      | Table groups stop                                                         |
+----------------------------------+---------------------------------------------------------------------------+
| UNPROTECT_GROUP                  | Remove protection against rollbacks on a group                            |
+----------------------------------+---------------------------------------------------------------------------+
| UNPROTECT_MARK_GROUP             | Remove protection against rollbacks on a mark for a group                 |
+----------------------------------+---------------------------------------------------------------------------+

The *hist_event* Column
^^^^^^^^^^^^^^^^^^^^^^^

The *hist_event* column can take the following values:

+------------------------------+----------------------------------------------------------------+
| Value                        | Meaning                                                        |
+==============================+================================================================+
| BEGIN                        | Beginning of an operation                                      |
+------------------------------+----------------------------------------------------------------+
| DELETED PARAMETER            | Parameter deleted from *emaj_param*                            |
+------------------------------+----------------------------------------------------------------+
| END                          | End of an operation                                            |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGER RECREATED      | Event trigger recreated                                        |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGERS DISABLED      | Event triggers disabled                                        |
+------------------------------+----------------------------------------------------------------+
| EVENT TRIGGERS ENABLED       | Event triggers enabled                                         |
+------------------------------+----------------------------------------------------------------+
| GROUP CREATED                | New table group created                                        |
+------------------------------+----------------------------------------------------------------+
| INSERTED PARAMETER           | Parameter inserted into *emaj_param*                           |
+------------------------------+----------------------------------------------------------------+
| LOG DATA TABLESPACE CHANGED  | Tablespace for the log table modified                          |
+------------------------------+----------------------------------------------------------------+
| LOG INDEX TABLESPACE CHANGED | Tablespace for the log index modified                          |
+------------------------------+----------------------------------------------------------------+
| LOG_SCHEMA CREATED           | Secondary schema created                                       |
+------------------------------+----------------------------------------------------------------+
| LOG_SCHEMA DROPPED           | Secondary schema dropped                                       |
+------------------------------+----------------------------------------------------------------+
| MARK DELETED                 | Mark deleted                                                   |
+------------------------------+----------------------------------------------------------------+
| NOTICE                       | Warning message issued by a rollback                           |
+------------------------------+----------------------------------------------------------------+
| PRIORITY CHANGED             | Priority level modified                                        |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE ADDED               | Sequence added to a logging table group                        |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE MOVED               | Sequence moved from one group to another                       |
+------------------------------+----------------------------------------------------------------+
| SEQUENCE REMOVED             | Sequence removed from a logging table group                    |
+------------------------------+----------------------------------------------------------------+
| TABLE ADDED                  | Table added to a logging table group                           |
+------------------------------+----------------------------------------------------------------+
| TABLE MOVED                  | Table moved from one group to another                          |
+------------------------------+----------------------------------------------------------------+
| TABLE REMOVED                | Table removed from a logging table group                       |
+------------------------------+----------------------------------------------------------------+
| TABLE REPAIRED               | Table repaired for E-Maj                                       |
+------------------------------+----------------------------------------------------------------+
| TIME STAMP SET               | Internal timestamp recorded                                    |
+------------------------------+----------------------------------------------------------------+
| TRIGGERS TO IGNORE CHANGED   | Set of application triggers to ignore at rollback time changed |
+------------------------------+----------------------------------------------------------------+
| UPDATED PARAMETER            | Parameter updated in *emaj_param*                              |
+------------------------------+----------------------------------------------------------------+
| WARNING                      | Warning message issued by a rollback                           |
+------------------------------+----------------------------------------------------------------+

----

Other History Tables
--------------------

Several other internal tables store historical data:

* **emaj_version_hist**: Keeps track of extension version changes.
* **emaj_group_hist**: Records table group creations and drops.
* **emaj_rel_hist**: Keeps track of tables and sequences assignments to table groups.
* **emaj_log_session**: Records the periods when table groups are enabled (started).
* Several other tables handle E-Maj rollback data.

The Emaj_web client is the easiest way to examine the content of these tables.

----

Purge Obsolete Traces
---------------------

When a table group is started with reset (:ref:`emaj_start_group() <emaj_start_group>` function) or when old marks are deleted (:ref:`emaj_delete_before_mark_group() <emaj_delete_before_mark_group>` function), the oldest events are deleted from most historical tables. The events kept are those not older than:

* A configurable retention delay.
* The oldest mark.
* The oldest uncompleted rollback operation.

By default, the retention delay for events is 1 year. However, this value can be modified at any time by changing the *history_retention* :ref:`E-Maj parameter<emaj_param>`. If the *history_retention* parameter is set to 100 years or more, no history purge is attempted.

The obsolete traces purge can also be initiated by explicitly calling the :ref:`emaj_purge_histories() <emaj_purge_histories>` function. The input parameter of the function defines a retention delay that overrides the *history_retention* E-Maj parameter.

To schedule purges periodically, it is possible to set the *history_retention* parameter to a very high value (e.g., '100 YEARS'), and schedule purge operations using any tool (*crontab*, *pgAgent*, *pgTimeTable*, or any other tool).
