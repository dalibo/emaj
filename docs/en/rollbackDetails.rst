The E-Maj rollback under the Hood
=================================

Planning and execution
----------------------

E-Maj rollbacks are complex operations. They can be logged or not, concern one or several tables groups, with or without parallelism, and be lounched by a direct SQL function call or by a client. Thus E-Maj rollbacks are splitted into elementary steps.

An E-Maj rollback is executed in two phases: a planning phase and an execution phase.

The **planning** phase determines all the needed elementary steps and estimates the execution duration. The estimate is computed for each step by taking into account:

* duration statistics of similar steps for previous rolllback operations, stored into the *emaj_rlbk_stat* table
* and predefined :doc:`parameters<parameters>` of the cost model.

Then, for parallel rollbacks, elementary steps are assigned to the requested n sessions.

The :ref:`emaj_estimate_rollback_group()<emaj_estimate_rollback_group>` function executes the planning phase and just returns its result, without chaining the execution phase.

The plan produced by the planning phase is recorded into the *emaj_rlbk_plan* table.

The E-Maj rollback **execution** phase just chains the elementary steps of the built plan.

First, a lock of type *EXCLUSIVE* is set on all tables of the rolled back tables group or tables groups, so that any table’s content change attempt from another client be blocked.

Then, for each table having changes to revert, the elementary steps are chained. In ascending order:

* preparing application triggers;
* disabling E-Maj triggers;
* deleting or setting as *DEFERRED* foreign keys;
* rollbacking the table;
* deleting a content of the log table;
* recreating or resetting the state of foreign keys;
* reseting the state of application triggers;
* re-enabling E-Maj triggers.

The processing of all sequences concerned by the E-Maj rollback is performed by a single elementary step that is scheduled at the beginning of the operation.

For each elementary step, the function that drives the plan execution updates the *emaj_rlbk_plan* table. Reading this table’s content may bring interesting information about the way the E-Maj rollback operation has been processed.

If the *dblink_user_password* parameter is set, the *emaj_rlbk_plan* updates are processed into autonomous transactions, so that it is possible to look at the rollback operation in real time. That’s what the :doc:`emajRollbackMonitor<parallelRollbackClient>` and :doc:`Emaj_web<webUsage>` clients do.

Rollbacking a table
-------------------

Rollbacking a table consists in reseting its content in the state at the time of the E-Maj rollback target mark setting.

In order to optimize the operation and avoid the execution of one SQL statement for each elementary change, a table rollback just executes 4 global SQL statements:

* create and populate a temporary table containing all primary keys to process;
* delete from the table to process all rows corresponding to changes to revert of type *INSERT* and *UPDATE*;
* ANALYZE the log table if the rollback is logged and if the number of changes is greater than 1000 (to avoid a poor execution plan of the last statement);
* insert into the table to process the oldest rows content corresponding to the changes to revert of type *UPDATE* and *DELETE*.

Foreign keys management
-----------------------

If a table processed by the rollback operation has a foreign key or is referenced by a foreign key belonging to another table, then this foreign key needs to be taken into account for the rollback execution.

Depending on the context, several behaviours exist.

For a given table, if all other tables linked to it by foreign keys belong to the same tables group or tables groups processed by the E-Maj rollback operation, reverting the changes on all tables will safely preserve the referential integrity.

For this first case (which is the most frequent) the table rollback is executed with a *session_replication_role* parameter set to '*replica*'. In this mode, no check on foreign keys is performed while updating the table.

On the contrary, if tables are linked to other tables that do not belong to the tables groups processed by the rollback operation or that are not including into any tables groups, then it is essential that the referential integrity be checked.

In this second case, checking the referential integrity is performed:

* either by pushing the checks at the end of the transaction, with a *SET CONSTRAINTS … DEFERRED* statement, if needed;
* or by dropping the foreign key before rollbacking the table and recreating it after.

The first option is choosen if the foreign key is declared *DEFERRABLE* and does not hold an *ON DELETE* or *ON UPDATE* clause.

Application triggers management
-------------------------------

Triggers belonging to tables to rollback that are not E-Maj triggers are temporarily disabled during the operation. But this default behaviour can be adjusted when :ref:`assigning a table<assign_table_sequence>` to a tables group or :ref:`importing a tables group configuration<import_groups_conf>`, by defining a trigger as "not to be disabled at rollback time".

The technical way to disable or not the application triggers depends on the *session_replication_role* parameter value set for each table to rollback.

If *session_replication_role* equals *'replica'*, then the enabled triggers at the E-Maj rollback start are not called. If a trigger is declared as ‘not to be disabled', it is temporarily changed into an *ALWAYS* trigger during the operation.

If *session_replication_role* keeps its default value, enabled triggers to neutralize are just temporarily disabled during the operation.
