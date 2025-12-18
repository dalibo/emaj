Manage E-Maj rollbacks
======================

Aside the functions that :ref:`perform an E-Maj rollback<emaj_rollback_group>`, there are several other rollbacks management functions.

.. _emaj_estimate_rollback_group:

Estimate the rollback duration
------------------------------

The *emaj_estimate_rollback_group()* function returns an idea of the time needed to rollback a tables group to a given mark. It can be called with a statement like::

   SELECT emaj.emaj_estimate_rollback_group('<group.name>', '<mark.name>', <is.logged>);

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The third parameter indicates whether the E-Maj rollback to simulate is a *logged rollback* or not.

The function returns an *INTERVAL* value.

The tables group must be in *LOGGING* state and the supplied mark must be usable for an E-Maj rollback.

This duration estimate is approximative. It takes into account:

* the number of updates in log tables to process, as returned by the :ref:`emaj_log_stat_group() <emaj_log_stat_group>` function,
* recorded duration of already performed rollbacks for the same tables,  
* 6 generic :doc:`parameters <parameters>` that are used as default values when no statistics have been already recorded for the tables to process.

The precision of the result cannot be high. The first reason is that, *INSERT*, *UPDATE* and *DELETE* having not the same cost, the part of each SQL type may vary. The second reason is that the load of the server at rollback time can be very different from one run to another. However, if there is a time constraint, the order of magnitude delivered by the function can be helpful to determine of the rollback operation can be performed in the available time interval.

If no statistics on previous rollbacks are available and if the results quality is poor, it is possible to adjust the generic :doc:`parameters <parameters>`. It is also possible to manually change the *emaj.emaj_rlbk_stat* table's content that keep a trace of the previous rollback durations, for instance by deleting rows corresponding to rollback operations performed in unusual load conditions.

Using the *emaj_estimate_rollback_groups()* function, it is possible to estimate the duration of a rollback operation on several groups::

   SELECT emaj.emaj_estimate_rollback_groups('<group.names.array>', '<mark.name>', <is.logged>);

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

.. _emaj_rollback_activity:

Monitor rollback operations
---------------------------

When the volume of recorded updates to cancel leads to a long rollback, it may be interesting to monitor the operation to appreciate how it progresses. A function, named *emaj_rollback_activity()*, and a client, :doc:`emajRollbackMonitor.php <rollbackMonitorClient>`, fit this need. 

.. _emaj_rollback_activity_prerequisites:

Prerequisite
^^^^^^^^^^^^

To allow E-Maj administrators to monitor the progress of a rollback operation, the activated functions update several technical tables as the process progresses. To ensure that these updates are visible while the transaction managing the rollback is in progress, they are performed through a *dblink* connection.

If not already present, the *dblink* extension is automatically installed at *emaj* extension creation. But monitoring rollback operations also requires to insert a connection identifier usable by *dblink* into the :ref:`emaj_param <emaj_param>` table. ::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) 
   VALUES ('dblink_user_password','user=<user> password=<password>');

The declared connection role must have been granted the *emaj_adm* rights (or be a *superuser*).

If the extension has been installed by a non *SUPERUSER* role, he must have been granted :ref:`the right to execute the dblink_connect_u(text,text)<rollbacks_limits>` function.

Lastly, the main transaction managing the rollback operation must be in a “*read committed*” concurrency mode (the default value).

Monitoring function
^^^^^^^^^^^^^^^^^^^

The *emaj_rollback_activity()* function allows one to see the progress of rollback operations.

Invoke it with the following statement::

   SELECT * FROM emaj.emaj_rollback_activity();

The function does not require any input parameter.

It returns a set of rows of type *emaj.emaj_rollback_activity_type*. Each row represents an in progress rollback operation, with the following columns:

+-----------------------------+-------------+---------------------------------------------------------------+
| Column                      | Type        | Description                                                   |
+=============================+=============+===============================================================+
| rlbk_id                     | INT         | rollback identifier                                           |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_groups                 | TEXT[]      | tables groups array associated to the rollback                |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_mark                   | TEXT        | mark to rollback to                                           |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_mark_datetime          | TIMESTAMPTZ | date and time when the mark to rollback to has been set       |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_is_logged              | BOOLEAN     | boolean taking the “true” value for logged rollbacks          |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_is_alter_group_allowed | BOOLEAN     | | boolean indicating whether the rollback can target a mark   |
|                             |             | | set before a tables groups structure change                 |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_comment                | TEXT        | comment                                                       |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_session             | INT         | number of parallel sessions                                   |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_table               | INT         | number of tables contained in the processed tables groups     |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_sequence            | INT         | number of sequences contained in the processed tables groups  |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_eff_nb_table           | INT         | number of tables having updates to cancel                     |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_eff_nb_sequence        | INT         | number of sequences having attributes to change               |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_status                 | ENUM        | rollback operation state                                      |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_start_datetime         | TIMESTAMPTZ | rollback operation start timestamp                            |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_planning_duration      | INTERVAL    | planning phase duration                                       |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_locking_duration       | INTERVAL    | tables locking phase duration                                 |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_elapse                 | INTERVAL    | elapse time spent since the rollback operation start          |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_remaining              | INTERVAL    | estimated remaining duration                                  |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_completion_pct         | SMALLINT    | estimated percentage of the completed work                    |
+-----------------------------+-------------+---------------------------------------------------------------+

An in progress rollback operation is in one of the following state:

* PLANNING : the operation is in its initial planning phase,
* LOCKING : the operation is setting locks,
* EXECUTING : the operation is currently executing one of the planned steps.

If the functions executing rollback operations cannot use *dblink* connections (extension not installed, missing or incorrect connection parameters,...), the *emaj_rollback_activity()* does not return any rows.

The remaining duration estimate is approximate. Its precision is similar to the precision of the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.

.. _emaj_comment_rollback:

Comment a rollback operation
----------------------------

When calling *emaj_rollback_group()*, *emaj_logged_rollback_group()*, *emaj_rollback_groups()* or *emaj_logged_rollback_groups()* functions, one of the supplied parameters allows to record a comment associated to the rollback operation. Using the *emaj_comment_rollback()* function, this comment can be updated or deleted. The same function allows to set a comment when it has not been done at rollback submission time. ::

   SELECT emaj.emaj_comment_rollback('<rollback.id>', <comment>);

The rollback identifier is an integer. It is available in the execution report delivered at the rollback operation completion. It is also visible in the :ref:`emaj_rollback_activity()<emaj_rollback_activity>` function report.

If the comment parameter is set to NULL, the existing comment, if any, is deleted.

The function does not return any data.

The comment can be added, modified or deleted when the operation is completed, but also when it is in progress if it is visible, i.e. if the *dblink_user_password* parameter is set into the :ref:`emaj_param <emaj_param>` table.

.. _emaj_consolidate_rollback_group:

"Consolidate" a logged rollback
-------------------------------

Following the execution of a “*logged rollback*”, and once the rollback operation recording becomes useless, it is possible to “*consolidate*” this rollback, meaning to some extent to transform it into “*unlogged rollback*”. A the end of the consolidation operation, marks and logs between the rollback target mark and the end rollback mark are deleted. The *emaj_consolidate_rollback_group()* function fits this need.::

   SELECT emaj.emaj_consolidate_rollback_group('<group.name>', <end.rollback.mark>);

The concerned logged rollback operation is identified by the name of the mark generated at the end of the rollback. This mark must always exist, but may have been renamed.

The *'EMAJ_LAST_MARK'* keyword may be used as mark name to reference the last set mark.

The :ref:`emaj_get_consolidable_rollbacks() <emaj_get_consolidable_rollbacks>` function may help to identify the rollbacks that may be condolidated.

Like rollback functions, the *emaj_consolidate_rollback_group()* function returns the number of effectively processed tables and sequences.

The tables group may be in *LOGGING* or in *IDLE* state.

The rollback target mark must always exist but may have been renamed. However, intermediate marks may have been deleted.

When the consolidation is complete, only the rollback target mark and the end rollback mark are kept.

The disk space of deleted rows will become reusable as soon as these log tables will be “vacuumed”.

Of course, once consolidated, a “*logged rollback*” cannot be cancelled (or rolled back) anymore, the start rollback mark and the logs covering this rollback being deleted.

The consolidation operation is not sensitive to the protections set on groups or marks, if any.

If a database has enough disk space, it may be interesting to replace a simple *unlogged rollback* by a *logged rollback* followed by a *consolidation* so that the application tables remain readable during the rollback operation, thanks to the lower locking mode used for logged rollbacks.

.. _emaj_get_consolidable_rollbacks:

List “consolidable rollbacks”
-----------------------------

The *emaj_get_consolidable_rollbacks()* function help to identify the rollbacks that may be consolidated.::

   SELECT * FROM emaj.emaj_get_consolidable_rollbacks();

The function returns a set of rows with the following columns:

+-------------------------------+-------------+-------------------------------------------+
| Column                        | Type        | Description                               |
+===============================+=============+===========================================+
| cons_group                    | TEXT        | rolled back tables group                  |
+-------------------------------+-------------+-------------------------------------------+
| cons_target_rlbk_mark_name    | TEXT        | rollback target mark name                 |
+-------------------------------+-------------+-------------------------------------------+
| cons_target_rlbk_mark_time_id | BIGINT      | temporal reference of the target mark (*) |
+-------------------------------+-------------+-------------------------------------------+
| cons_end_rlbk_mark_name       | TEXT        | rollback end mark name                    |
+-------------------------------+-------------+-------------------------------------------+
| cons_end_rlbk_mark_time_id    | BIGINT      | temporal reference of the end mark (*)    |
+-------------------------------+-------------+-------------------------------------------+
| cons_rows                     | BIGINT      | number of intermediate updates            |
+-------------------------------+-------------+-------------------------------------------+
| cons_marks                    | INT         | number of intermediate marks              |
+-------------------------------+-------------+-------------------------------------------+

(*) emaj_time_stamp table identifiers ; this table contains the time stamps of the most important events of the tables groups life.

Using this function, it is easy to consolidate at once all “*consolidable*” rollbacks for all tables groups in order to recover as much as possible disk space::

   SELECT emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark__name)
          FROM emaj.emaj_get_consolidable_rollbacks();

The *emaj_get_consolidable_rollbacks()* function may be used by *emaj_adm* and *emaj_viewer* roles.


.. _emaj_cleanup_rollback_state:

Update rollback operations state
--------------------------------

The *emaj_rlbk* technical table and its derived tables contain the history of E-Maj rollback operations.

When rollback functions cannot use *dblink* connections, all updates of these technical tables are all performed inside a single transaction. Therefore:

* any rollback operation that has not been completed is invisible in these technical tables,
* any rollback operation that has been validated is visible in these technical tables with a “*COMMITTED*” state.

When rollback functions can use *dblink* connections, all updates of *emaj_rlbk* and its related tables are performed in autonomous transactions. In this working mode, rollback functions leave the operation in a “*COMPLETED*” state when finished. A dedicated internal function is in charge of transforming the “*COMPLETED*” operations either into a “*COMMITTED*” state or into an “*ABORTED*” state, depending on how the main rollback transaction has ended. This function is automatically called when a new mark is set and when the rollback monitoring function is used.

If the E-Maj administrator wishes to check the status of recently executed rollback operations, he can use the *emaj_cleanup_rollback_state()* function at any time::

   SELECT emaj.emaj_cleanup_rollback_state();

The function returns the number of modified rollback operations.
