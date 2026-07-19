The E-Maj Rollback Under the Hood
=================================

Planning and Execution
----------------------

E-Maj rollbacks are complex operations. They can be logged or not, involve one or several table groups, with or without parallelism, and be launched by a direct SQL function call or by a client. Thus, E-Maj rollbacks are split into **elementary steps**.

An E-Maj rollback is executed in two phases: a **planning** phase and an **execution** phase.

The **planning** phase determines all the required elementary steps and estimates the execution duration. The estimate is computed for each step by taking into account:

* Duration statistics of similar steps from previous rollback operations, stored in the *emaj_rlbk_stat* table.
* Predefined :doc:`parameters<parameters>` of the cost model.

Then, for parallel rollbacks, elementary steps are assigned to the requested number of sessions.

The :ref:`emaj_estimate_rollback_group()<emaj_estimate_rollback_group>` function executes the planning phase and returns its result without proceeding to the execution phase.

The plan produced by the planning phase is recorded in the *emaj_rlbk_plan* table.

The E-Maj rollback **execution** phase simply chains the elementary steps of the built plan.

First, an *EXCLUSIVE* lock is set on all tables of the rolled-back table group or table groups, so that any attempt to change a table’s content from another client is blocked.

Then, for each table with changes to revert, the elementary steps are chained in ascending order:

* Preparing application triggers.
* Disabling E-Maj triggers.
* Deleting or setting as *DEFERRED* foreign keys.
* Rolling back the table.
* Deleting the content of the log table.
* Recreating or resetting the state of foreign keys.
* Resetting the state of application triggers.
* Re-enabling E-Maj triggers.

The processing of all sequences involved in the E-Maj rollback is performed by a single elementary step scheduled at the beginning of the operation.

For each elementary step, the function that drives the plan execution updates the *emaj_rlbk_plan* table. Reading this table’s content may provide interesting information about how the E-Maj rollback operation was processed.

If the *dblink_user_password* parameter is set, the *emaj_rlbk_plan* updates are processed in autonomous transactions, making it possible to monitor the rollback operation in real time. This is what the :ref:`emaj_rollback_activity()<emaj_rollback_activity>` function and the :doc:`emajRollbackMonitor<parallelRollbackClient>` and :doc:`Emaj_web<webUsage>` clients do. If the dblink connection is not operational, the :ref:`emaj_verify_all()<emaj_verify_all>` function explains why.

----

.. _single_table_rollback:

Rolling Back a Table
--------------------

Rolling back a table consists in resetting its content to the state at the time the E-Maj rollback target mark was set.

To optimize the operation and avoid executing one SQL statement for each elementary change, a table rollback executes just 4 global SQL statements:

* Create and populate a temporary table containing all primary keys to process.
* Delete from the table to process all rows corresponding to changes to revert of type *INSERT* and *UPDATE*.
* *ANALYZE* the log table if the rollback is logged and if the number of changes is greater than 1000 (to avoid a poor execution plan for the last statement).
* Insert into the table to process the oldest rows' content corresponding to the changes to revert of type *UPDATE* and *DELETE*.

----

Foreign Keys Management
-----------------------

If a table processed by the rollback operation has a foreign key or is referenced by a foreign key belonging to another table, then this foreign key must be taken into account for the rollback execution.

Depending on the context, several behaviors exist.

For a given table, if all other tables linked to it by foreign keys belong to the same table group or table groups processed by the E-Maj rollback operation, reverting the changes on all tables will safely preserve referential integrity.

For this first case (which is the most frequent), the table rollback is executed with a *session_replication_role* parameter set to '*replica*'. In this mode, no check on foreign keys is performed while updating the table.

On the other hand, if tables are linked to other tables that do not belong to the table groups processed by the rollback operation or that are not included in any table groups, then it is essential that referential integrity be checked.

In this second case, checking referential integrity is performed:

* Either by deferring the checks to the end of the transaction with a *SET CONSTRAINTS ... DEFERRED* statement executed if the key is declared *DEFERRABLE INITIALLY IMMEDIATE*.
* Or by dropping the foreign key before rolling back the table and recreating it afterward.

The first option is chosen if the foreign key is declared *DEFERRABLE* and does not have an *ON DELETE* or *ON UPDATE* clause.

A :ref:`foreign key defined on a partitioned table<fk_on_partitioned_tables>` is only supported by E-Maj rollback operations if:

    • All tables/partitions linked by the foreign key belong to the same table groups to process.
    • The foreign key is of type *DEFERRABLE*.
    • The foreign key does not have an *ON DELETE* or *ON UPDATE* clause.

----

Other Integrity Constraints
---------------------------

Tables may have other integrity constraints: *NOT NULL*, *CHECK*, *UNIQUE*, and *EXCLUDE*. However, these constraints only concern the content of the table that holds them, without any link to other tables.

During an E-Maj rollback, these constraints are verified by PostgreSQL immediately at data change or at the transaction end for *UNIQUE* or *EXCLUDE* constraints that are defined as *DEFERRED*. Considering the :ref:`way elementary tables are rolled back<single_table_rollback>`, no specific action is performed to support these constraints, and no integrity violation should occur if all these integrity constraints already existed when the rollback target mark was set.

----

Application Triggers Management
-------------------------------

Triggers belonging to tables to roll back that are not E-Maj triggers are temporarily disabled during the operation. However, this default behavior can be adjusted when :ref:`assigning a table<assign_table>` to a table group or :ref:`importing a table group configuration<import_groups_conf>`, by defining a trigger as "not to be disabled at rollback time".

The technical way to disable or not disable the application triggers depends on the *session_replication_role* parameter value set for each table to roll back.

If *session_replication_role* equals '*replica*', then the enabled triggers at the E-Maj rollback start are not called. If a trigger is declared as "not to be disabled", it is temporarily changed into an *ALWAYS* trigger during the operation.

If *session_replication_role* keeps its default value, enabled triggers to neutralize are just temporarily disabled during the operation.

In a declarative partitioning context, it is possible to create a :ref:`trigger on a partitioned table<trigger_on_partitioned_tables>`. As a result, each partition of the table inherits the trigger. There is no practical issue with this in E-Maj rollbacks.
