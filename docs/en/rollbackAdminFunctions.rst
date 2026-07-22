Managing E-Maj Rollbacks
========================

Aside from the functions that :ref:`perform an E-Maj rollback<emaj_rollback_group>`, there are several other rollback management functions.

.. _emaj_estimate_rollback_group:

Estimating Rollback Duration
----------------------------

The ``emaj_estimate_rollback_group()`` function returns an estimate of the time needed to roll back a table group to a given mark. It can be called with a statement like::

   SELECT emaj.emaj_estimate_rollback_group(p_group, p_mark, p_isLoggedRlbk);

**Input Parameters**

- ``p_group`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): Target **mark name**. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_isLoggedRlbk`` (*BOOLEAN*):

   - *FALSE*: The simulated E-Maj rollback is an unlogged rollback.
   - *TRUE*: The simulated E-Maj rollback is a logged rollback.

**Returned data**

The function returns an *INTERVAL* value representing the estimated rollback duration.

**Notes**

The table group must be in *LOGGING* state, and the supplied mark must be usable for an E-Maj rollback.

This duration estimate is computed using:

* The number of changes in log tables to process, as returned by the :ref:`emaj_log_stat_group() <emaj_log_stat_group>` function.
* Recorded durations of already performed rollbacks for the same tables.
* Six generic :doc:`parameters<parameters>` that are used as default values when no statistics have been recorded for the tables to process.

The duration estimate **precision** cannot be high. Among the reasons:

- The average cost of rows *INSERT*, *UPDATE*, and *DELETE* is not the same and the proportion of each SQL type vary.
- The server load at rollback time can be very different from one run to another.

However, if there is a time constraint, the order of magnitude delivered by the function can be helpful in determining whether the rollback operation can be performed in the available time interval.

If no statistics on previous rollbacks are available and the result quality is poor, it is possible to adjust the generic :doc:`parameters<parameters>`.

It is also possible to manually change the *emaj.emaj_rlbk_stat* table content, which keeps a trace of previous rollback durations. In particular, rows corresponding to rollback operations performed under unusual load conditions can be deleted.

**Multi-groups operation**

Using the ``emaj_estimate_rollback_groups()`` function, it is possible to estimate the duration of a rollback operation on **several groups**::

   SELECT emaj.emaj_estimate_rollback_groups(p_groups, p_mark, p_isLoggedRlbk);

The difference with the *emaj_estimate_rollback_group()* function is:

- The first parameter is a *TEXT array* representing all table groups to process. For more information, see :doc:`multi-groups functions <multiGroupsFunctions>`.

----

.. _emaj_rollback_activity:

Monitoring Rollback Operations
------------------------------

When the volume of recorded updates to cancel leads to a long rollback, it may be useful to monitor the operation to see how it progresses. The ``emaj_rollback_activity()`` function and the :doc:`emajRollbackMonitor client<rollbackMonitorClient>` meet this need.

.. _emaj_rollback_activity_prerequisites:

Prerequisites
^^^^^^^^^^^^^

To allow E-Maj administrators to monitor the progress of a rollback operation, the activated functions update several technical tables as the process progresses. To ensure that these updates are visible while the transaction managing the rollback is in progress, they are performed in separate transactions through a *dblink* connection.

If not already present, the *dblink* extension is automatically installed at *emaj* extension creation. However, monitoring rollback operations also requires setting the :ref:`'dblink_user_password'<emaj_param>` extension parameter with connection identifiers usable by *dblink*::

   SELECT emaj.emaj_set_param('dblink_user_password','user=<user> password=<password>');

The declared connection role must have been granted the *emaj_adm* privilege (or be a *SUPERUSER*).

If the extension has been installed by a non-*SUPERUSER* role, the role must have been granted the privilege to execute the :ref:`dblink_connect_u(text,text)<rollbacks_limits>` function.

Lastly, the main transaction managing the rollback operation must be in a "*READ COMMITTED*" concurrency mode (the default value).

Monitoring Function
^^^^^^^^^^^^^^^^^^^

The ``emaj_rollback_activity()`` function allows monitoring the progress of rollback operations. Invoke it with the following statement::

   SELECT * FROM emaj.emaj_rollback_activity();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns a set of rows of type *emaj.emaj_rollback_activity_type*. Each row represents an in-progress rollback operation with the following columns:

+-----------------------------+-------------+---------------------------------------------------------------+
| Column                      | Type        | Description                                                   |
+=============================+=============+===============================================================+
| rlbk_id                     | INT         | Rollback identifier                                           |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_groups                 | TEXT[]      | Table groups array associated with the rollback               |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_mark                   | TEXT        | Mark to roll back to                                          |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_mark_datetime          | TIMESTAMPTZ | Date and time when the mark to roll back to was set           |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_is_logged              | BOOLEAN     | Boolean taking the *TRUE* value for logged rollbacks          |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_is_alter_group_allowed | BOOLEAN     | Boolean indicating whether the rollback can target a mark     |
|                             |             | set before a table group structure change                     |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_comment                | TEXT        | Comment                                                       |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_session             | INT         | Number of parallel sessions                                   |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_table               | INT         | Number of tables contained in the processed table groups      |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_sequence            | INT         | Number of sequences contained in the processed table groups   |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_eff_nb_table           | INT         | Number of tables having updates to cancel                     |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_eff_nb_sequence        | INT         | Number of sequences having attributes to change               |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_status                 | ENUM        | Rollback operation state                                      |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_start_datetime         | TIMESTAMPTZ | Rollback operation start timestamp                            |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_planning_duration      | INTERVAL    | Planning phase duration                                       |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_locking_duration       | INTERVAL    | Tables locking phase duration                                 |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_elapse                 | INTERVAL    | Time spent since the rollback operation start                 |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_remaining              | INTERVAL    | Estimated remaining duration                                  |
+-----------------------------+-------------+---------------------------------------------------------------+
| rlbk_completion_pct         | SMALLINT    | Estimated percentage of the completed work                    |
+-----------------------------+-------------+---------------------------------------------------------------+

**Notes**

An in-progress **rollback** operation is in one of the following **states**:

* *PLANNING*: The operation is in its initial planning phase.
* *LOCKING*: The operation is setting locks.
* *EXECUTING*: The operation is currently executing one of the planned steps.

If the functions executing rollback operations cannot use *dblink* connections (extension not installed, missing or incorrect connection parameters, etc.), the *emaj_rollback_activity()* function does not return any rows.

The remaining duration estimate is **approximate**. Its precision is similar to the precision of the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.

----

.. _emaj_comment_rollback:

Commenting a Rollback Operation
-------------------------------

The ``emaj_comment_rollback()`` function allows to set, modify or delete a comment for an already started E-Maj rollback operation::

   SELECT emaj.emaj_comment_rollback(p_rlbkId, p_comment);

**Input Parameters**

- ``p_rlbkId`` (*INTEGER*): E-Maj **rollback identifier**.
- ``p_comment`` (*TEXT*): **Comment** describing the rollback. A *NULL* value deletes any existing comment for the rollback.

**Returned data**

The function does not return any data.

**Notes**

The rollback identifier is available in the execution report delivered at the rollback operation completion. It is also visible in the :ref:`emaj_rollback_activity()<emaj_rollback_activity>` function report.

The comment can be added, modified, or deleted when the operation is:

- either completed,
- or even in progress, if it is visible (i.e., if the :ref:`'dblink_user_password'<emaj_param>` E-Maj parameter is set).

:ref:`emaj_rollback_group(), emaj_rollback_groups()<emaj_rollback_group>`, :ref:`emaj_logged_rollback_group() and emaj_logged_rollback_groups()<emaj_logged_rollback_group>` functions have a *p_comment* parameter that allows immediately setting a comment at E-Maj rollback submission time.

----

.. _emaj_consolidate_rollback_group:

"Consolidating" a Logged Rollback
---------------------------------

Following the execution of an E-Maj *logged rollback*, and once the rollback operation recording becomes useless, it is possible to "*consolidate*" this rollback, meaning to some extent transforming it into an *unlogged rollback*. At the consolidation operation completion, marks and logs between the rollback target mark and the end rollback mark are deleted. The ``emaj_consolidate_rollback_group()`` function meets this need::

   SELECT emaj.emaj_consolidate_rollback_group(p_group, p_endRlbkMark);

**Input Parameters**

- ``p_group`` (*TEXT*): **Table group name**.
- ``p_endRlbkMark`` (*TEXT*): The name of the **mark that closed** the rollback operation. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns the number of effectively processed tables and sequences.

**Notes**

The concerned logged rollback operation is identified by the name of the mark generated at the end of the rollback. This mark must always exist but may have been renamed.

The table group may be in *LOGGING* or in *IDLE* **state**.

The consolidation operation is not sensitive to the **protections** set on groups or marks, if any.

When the consolidation is complete:

- Only the rollback target mark and the end rollback mark are kept.
- Marks set before the target mark remain candidate for further rollback operations.
- The **disk space** of deleted rows will become reusable as soon as these log tables are vacuumed.

If a database has enough disk space, it may be useful to replace a simple *unlogged rollback* with a *logged rollback* followed by a *consolidation* so that the application tables remain readable during the rollback operation, thanks to the lower locking mode used for logged rollbacks.

The :ref:`emaj_get_consolidable_rollbacks() <emaj_get_consolidable_rollbacks>` function may help identify the rollbacks that may be consolidated.

----

.. _emaj_get_consolidable_rollbacks:

Listing Consolidable Rollbacks
------------------------------

The ``emaj_get_consolidable_rollbacks()`` function helps identify the rollbacks that may be consolidated::

   SELECT * FROM emaj.emaj_get_consolidable_rollbacks();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns a set of rows with the following columns:

+-------------------------------+-------------+---------------------------------------+
| Column                        | Type        | Description                           |
+===============================+=============+=======================================+
| cons_group                    | TEXT        | Rolled-back table group               |
+-------------------------------+-------------+---------------------------------------+
| cons_target_rlbk_mark_name    | TEXT        | Rollback target mark name             |
+-------------------------------+-------------+---------------------------------------+
| cons_target_rlbk_mark_time_id | BIGINT      | Temporal reference of the target mark |
+-------------------------------+-------------+---------------------------------------+
| cons_end_rlbk_mark_name       | TEXT        | Rollback end mark name                |
+-------------------------------+-------------+---------------------------------------+
| cons_end_rlbk_mark_time_id    | BIGINT      | Temporal reference of the end mark    |
+-------------------------------+-------------+---------------------------------------+
| cons_rows                     | BIGINT      | Number of intermediate updates        |
+-------------------------------+-------------+---------------------------------------+
| cons_marks                    | INT         | Number of intermediate marks          |
+-------------------------------+-------------+---------------------------------------+

**Notes**

Mark temporal references are identifiers of the *emaj_time_stamp* table, which contains the timestamps of the most important events in the life of the table groups.

The *emaj_get_consolidable_rollbacks()* function may be used by *emaj_adm* and *emaj_viewer* roles.

Using this *emaj_get_consolidable_rollbacks()* function, it is easy to consolidate at once all consolidable rollbacks for all table groups to recover as much disk space as possible::

   SELECT emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark_name)
      FROM emaj.emaj_get_consolidable_rollbacks();

----

.. _emaj_cleanup_rollback_state:

Updating Rollback Operations State
----------------------------------

The *emaj_rlbk* technical table and its derived tables contain the history of E-Maj rollback operations.

When rollback functions cannot use *dblink* connections, all updates of these technical tables are performed inside a single transaction. Therefore:

* Any rollback operation that has not been completed is invisible in these technical tables.
* Any rollback operation that has been validated is visible in these technical tables with a *COMMITTED* state.

When rollback functions can use *dblink* connections, all updates of *emaj_rlbk* and its related tables are performed in autonomous transactions. In this working mode, rollback functions leave the operation in a *COMPLETED* state when finished. A dedicated internal function is responsible for transforming *COMPLETED* operations either into a *COMMITTED* state or into an *ABORTED* state, depending on how the main rollback transaction ended. This function is automatically called when a new mark is set and when the rollback monitoring function is used.

The E-Maj administrator who wishes to check the status of recently executed rollback operations, can call the ``emaj_cleanup_rollback_state()`` function at any time::

   SELECT emaj.emaj_cleanup_rollback_state();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns the number of modified rollback operations.
