User's responsibility
=====================

Defining tables groups content
------------------------------

Defining the content of tables group is essential to guarantee the database integrity. It is the E-Maj administrator's responsibility to ensure that all tables updated by a given operation are really included in a single tables group.

Appropriate call of main functions
----------------------------------

The :ref:`emaj_start_group() <emaj_start_group>`, :ref:`emaj_stop_group() <emaj_stop_group>`, :ref:`emaj_set_mark_group() <emaj_set_mark_group>`, :ref:`emaj_rollback_group() <emaj_rollback_group>` and :ref:`emaj_logged_rollback_group() <emaj_logged_rollback_group>` functions (and their related multi-groups functions) set explicit locks on tables of the group to be sure that no transactions updating these tables are running at the same time. But it is the user's responsibility to execute these operations “at the right time”, i.e. at moments that really correspond to a stable point in the life of these tables. He must also take care of warning messages that may be reported by E-Maj rollback functions.

.. _application_triggers:

Management of application triggers
----------------------------------

Triggers may have been created on application tables. It is not rare that these triggers perform one or more updates on other tables. In such a case, it is the E-Maj administrator's responsibility to understand the impact of E-Maj rollback operations on tables concerned by triggers, and if needed, to take the appropriate measures.

By default, E-Maj rollback functions automatically disable application triggers at the operation’s start and reset them in their previous state at operation’s end. But the E-Maj administrator can change this behaviour using the *"ignored_triggers"* and *"ignored_triggers_profiles"* properties of the :ref:`emaj_assign_table(), emaj_assign_tables()<assign_table_sequence>`, :ref:`emaj_modify_table() and emaj_modify_tables()<modify_table>` functions.

If the trigger simply adjusts the content of the row to insert or update, the logged data contain the final columns values. In case of rollback, the log table contains the right columns content to apply. So the trigger must be disabled at rollback time (the default behaviour), so that it does not disturb the processing.

If the trigger updates another table, two cases must be considered:

* if the updated table belongs to the same tables group, the automatic trigger disabling and the rollback of both tables let them in the expected state,
* if the updated table does not belong to the same tables group, it is essential to analyse the consequences of a rollback operation, in order to avoid a de-synchronisation between both tables. If needed, the triggers can be left enabled. But some other actions may also be required.

For more complex triggers, it is essential to perfectly understand their impacts on E-Maj rollbacks and take any appropriate mesure at rollback time.

Internal E-Maj table or sequence change
---------------------------------------

With the rights they have been granted, *emaj_adm* roles and *superusers* can update any E-Maj internal table.

.. caution::
   But in practice, only the *emaj_param* table may be updated by these users. Any other internal table or sequence update my lead to data corruption.
