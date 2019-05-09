Other functions
===============

.. _emaj_verify_all:

Check the consistency of the E-Maj environment
----------------------------------------------

A function is also available to check the consistency of the E-Maj environment. 
It consists in checking the integrity of all E-Maj schemas and all created tables groups. This function can be called with the following SQL statement::

   SELECT * FROM emaj.emaj_verify_all();

For each E-Maj schema (*emaj* and each log schema) the function verifies that:

* all tables, functions, sequences and types contained in the schema are either objects of the extension, or linked to created tables groups,
* they don't contain any view, foreign table, domain, conversion, operator or operator class.

Then, for each created tables group, the function performs the same checks as those performed when a group is started, a mark is set, or a rollback is executed (:ref:`more details <internal_checks>`).

The function returns a set of rows describing the detected discrepancies. If no error is detected, the function returns a single row containing the following messages::

   'No error detected'

The *emaj_verify_all()* function can be executed by any role belonging to *emaj_adm* or *emaj_viewer* roles.

If errors are detected, for instance after an application table referenced in a tables group has been dropped, appropriate measures must be taken. Typically, the potential orphan log tables or functions must be manually dropped. 

.. _emaj_keep_enabled_trigger:

Not disabling application triggers at E-Maj rollback time
---------------------------------------------------------

Application triggers are automatically disabled during E-Maj rollback operations. Under some circumstances, it may be desirable to keep them enabled (more details :ref:`here <application_triggers>`). The *emaj_keep_enabled_trigger()* function achieves this. It allows to add or remove triggers into/from a list of triggers that do not need to be disabled during rollback operations. ::

	SELECT emaj.emaj_keep_enabled_trigger(<action>, <schema.name>, <table.name>, <trigger.name>);

The *<action>* parameter accepts 2 values: ‘ADD’ to add a trigger to the list or ‘REMOVE’ to delete a trigger from the list.

The trigger is identified by the 3 components: schema name, table name and trigger name.

The trigger name may contain ‘%’ and ‘_’ wildcard characters. These characters have the same meaning as in the *LIKE* clause of the SQL language. Thus several triggers of a single table can be processed by a unique function call.

The function returns the number of triggers effectively added or removed.

The function does not process E-Maj triggers (log triggers or triggers protecting against *TRUNCATE*).

The triggers referenced as “not to be automatically disabled during E-Maj rollbacks” are registered into the *emaj.emaj_enabled_trigger* table. This table contains 3 columns:

* trg_schema : schema of the table holding the trigger
* trg_table : table holding the trigger
* trg_name : trigger name

In order to know the list of registered triggers, just display the table’s content.

.. _emaj_rollback_activity:

Monitoring rollback operations
------------------------------

When the volume of recorded updates to cancel leads to a long rollback, it may be interesting to monitor the operation to appreciate how it progresses. A function, named *emaj_rollback_activity()*, and a client, :doc:`emajRollbackMonitor.php <rollbackMonitorClient>`, fit this need. 

Prerequisite
^^^^^^^^^^^^

To allow E-Maj administrators to monitor the progress of a rollback operation, the activated functions update several technical tables as the process progresses. To ensure that these updates are visible while the transaction managing the rollback is in progress, they are performed through a *dblink* connection.

As a result, monitoring rollback operations requires the :doc:`installation of the dblink extension <setup>` as well as the insertion of a connection identifier usable by *dblink* into the :ref:`emaj_param <emaj_param>` table.

Recording the connection identifier can be performed with a statement like::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) 
   VALUES ('dblink_user_password','user=<user> password=<password>');

The declared connection role must have been granted the *emaj_adm* rights (or be a *superuser*).

Lastly, the main transaction managing the rollback operation must be in a “*read committed*” concurrency mode (the default value).

Monitoring function
^^^^^^^^^^^^^^^^^^^

The *emaj_rollback_activity()* function allows one to see the progress of rollback operations.

Invoke it with the following statement::

   SELECT * FROM emaj.emaj_rollback_activity();

The function does not require any input parameter.

It returns a set of rows of type *emaj.emaj_rollback_activity_type*. Each row represents an in progress rollback operation, with the following columns:

+---------------------+-------------+---------------------------------------------------------------+
| Column              | Type        | Description                                                   |
+=====================+=============+===============================================================+
| rlbk_id             | INT         | rollback identifier                                           |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_groups         | TEXT[]      | tables groups array associated to the rollback                |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_mark           | TEXT        | mark to rollback to                                           |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_mark_datetime  | TIMESTAMPTZ | date and time when the mark to rollback to has been set       |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_is_logged      | BOOLEAN     | boolean taking the “true” value for logged rollbacks          |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_session     | INT         | number of parallel sessions                                   |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_table       | INT         | number of tables contained in the processed tables groups     |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_nb_sequence    | INT         | number of sequences contained in the processed tables groups  |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_eff_nb_table   | INT         | number of tables having updates to cancel                     |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_status         | ENUM        | rollback operation state                                      |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_start_datetime | TIMESTAMPTZ | rollback operation start timestamp                            |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_elapse         | INTERVAL    | elapse time spent since the rollback operation start          |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_remaining      | INTERVAL    | estimated remaining duration                                  |
+---------------------+-------------+---------------------------------------------------------------+
| rlbk_completion_pct | SMALLINT    | estimated percentage of the completed work                    |
+---------------------+-------------+---------------------------------------------------------------+

An in progress rollback operation is in one of the following state:

* PLANNING : the operation is in its initial planning phase,
* LOCKING : the operation is setting locks,
* EXECUTING : the operation is currently executing one of the planned steps.

If the functions executing rollback operations cannot use *dblink* connections (extension not installed, missing or incorrect connection parameters,...), the *emaj_rollback_activity()* does not return any rows.

The remaining duration estimate is approximate. Its precision is similar to the precision of the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.

.. _emaj_cleanup_rollback_state:

Updating rollback operations state
----------------------------------

The *emaj_rlbk* technical table and its derived tables contain the history of E-Maj rollback operations.

When rollback functions cannot use *dblink* connections, all updates of these technical tables are all performed inside a single transaction. Therefore:

* any rollback operation that has not been completed is invisible in these technical tables,
* any rollback operation that has been validated is visible in these technical tables with a “*COMMITTED*” state.

When rollback functions can use *dblink* connections, all updates of *emaj_rlbk* and its related tables are performed in autonomous transactions. In this working mode, rollback functions leave the operation in a “*COMPLETED*” state when finished. A dedicated internal function is in charge of transforming the “*COMPLETED*” operations either into a “*COMMITTED*” state or into an “*ABORTED*” state, depending on how the main rollback transaction has ended. This function is automatically called when a new mark is set and when the rollback monitoring function is used.

If the E-Maj administrator wishes to check the status of recently executed rollback operations, he can use the *emaj_cleanup_rollback_state()* function at any time::

   SELECT emaj.emaj_cleanup_rollback_state();

The function returns the number of modified rollback operations.

.. _emaj_disable_protection_by_event_triggers:
.. _emaj_enable_protection_by_event_triggers:

Deactivating or reactivating event triggers
-------------------------------------------

The E-Maj extension installation procedure activates :ref:`event triggers <event_triggers>` to protect it. Normally, these triggers must remain in their state. But if the E-Maj administrator needs to deactivate and the reactivate them, he can use 2 dedicated functions.

To deactivate the existing event triggers::

   SELECT emaj.emaj_disable_protection_by_event_triggers();

The function returns the number of deactivated event triggers (this value depends on the installed PostgreSQL version).

To reactivate existing event triggers::

   SELECT emaj.emaj_enable_protection_by_event_triggers();

The function returns the number of reactivated event triggers.

