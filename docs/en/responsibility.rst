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

If the trigger simply adjusts the content of the row to insert or update, the logged data will contain the final columns value. So in case of rollback, the log table contains the right data to process. And as the trigger is by default automatically disabled at rollback time, the trigger cannot disturb the rollback processing.

If the trigger updates another table, two cases must be considered:

* if the updated table belongs to the same tables group, the automatic trigger disabling and the rollback of both tables will let them in the expected state,
* if the updated table does not belong to the same tables group, it is essential to analyse the consequences of a rollback operation, in order to avoid a de-synchronisation between both tables. If needed, the :ref:`emaj_ignore_app_trigger()<emaj_ignore_app_trigger>` function can be used to not disable the trigger at rollback time. But merely deactivating the trigger may not be sufficient and some other actions may be required.

For more complex triggers, it is essential to perfectly understand their impacts on E-Maj rollbacks and take any appropriate mesure at rollback time.

Internal E-Maj table or sequence change
---------------------------------------

With the rights they have been granted, *emaj_adm* roles and *superusers* can update any E-Maj internal table.

.. caution::
   But in practice, only two tables may be updated by these users: *emaj_group_def* and *emaj_param*. Any other internal table or sequence update my lead to data corruption during rollback operations.

