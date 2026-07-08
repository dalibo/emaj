User Responsibilities
=====================

Defining Table Groups Content
-----------------------------

Defining the **content** of a **table group** is essential to guarantee database integrity. It is the E-Maj administrator's responsibility to ensure that all tables updated by a given operation are included in a single table group.

----

Appropriate Use of Main Functions
---------------------------------

The :ref:`emaj_start_group() <emaj_start_group>`, :ref:`emaj_stop_group() <emaj_stop_group>`, :ref:`emaj_set_mark_group() <emaj_set_mark_group>`, :ref:`emaj_rollback_group() <emaj_rollback_group>`, and :ref:`emaj_logged_rollback_group() <emaj_logged_rollback_group>` functions (and their related multi-group functions) set explicit locks on the tables of the group. These locks ensure that no transactions updating these tables are running simultaneously.

However, it is the user's responsibility to execute these operations **"at the right time"**, i.e., at moments that truly correspond to a stable point in the life of these tables. The user must also pay attention to warning messages that may be reported by E-Maj rollback functions.

----

.. _application_triggers:

Managing Application Triggers
-----------------------------

Triggers may have been created on application tables. It is not uncommon for these triggers to perform one or more updates on other tables.
In such cases, it is the E-Maj administrator's responsibility to **understand the impact** of E-Maj rollback operations on tables affected by triggers and, if needed, to take appropriate measures.

By default, E-Maj rollback functions neutralize application triggers during the operation.
However, the E-Maj administrator can change this behavior using the ``ignored_triggers`` and ``ignored_triggers_profiles`` properties of the :ref:`emaj_assign_table() <assign_table_sequence>`, :ref:`emaj_assign_tables() <assign_table_sequence>`, :ref:`emaj_modify_table() <modify_table>`, and :ref:`emaj_modify_tables() <modify_table>` functions.

If the trigger simply adjusts the content of the row to be inserted or updated, the logged data contain the final column values. In case of rollback, the log table contains the correct column content to apply. Thus, the trigger must be disabled at rollback time (the default behavior) so that it does not interfere with the processing.

If the trigger updates another table, two cases must be considered:

* If the updated table belongs to the same table group, the automatic trigger disabling and the rollback of both tables leave them in the expected state.
* If the updated table does not belong to the same table group, it is essential to analyze the consequences of a rollback operation to avoid desynchronization between both tables. If needed, the triggers can be left enabled. However, additional actions may also be required.

For more complex triggers, it is essential to fully understand their impact on E-Maj rollbacks and to take any appropriate measures at rollback time.

For parallel rollback operations, a trigger that is kept enabled and updates other tables from the same table group would likely generate a freeze between sessions.

----

Internal E-Maj Table or Sequence Changes
----------------------------------------

With the privileges they have been granted, ``emaj_adm`` roles and ``superusers`` can update any E-Maj internal table.

.. caution::
   However, any manual change to an internal table or sequence may lead to **data corruption**.