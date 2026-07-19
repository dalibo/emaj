Reliability
===========

Two additional elements help ensure E-Maj reliability: internal checks are performed at key moments in the life of table groups, and event triggers can block some risky operations.

.. _internal_checks:

Internal Checks
---------------

When a function is executed to start a table group, set a mark, or roll back a table group, E-Maj performs checks to verify the integrity of the table groups being processed.

These **table group integrity checks** verify that:

* The PostgreSQL version at table group creation time is compatible with the current version.
* Each application sequence or table of the group still exists.
* Each table of the group has its log table, log function, and triggers.
* The log tables' structure always reflects the related application tables' structure and contains all required technical columns.
* For each table, the generated columns list is unchanged.
* For *ROLLBACKABLE* table groups, no table has been altered as *UNLOGGED*.
* For *ROLLBACKABLE* table groups, application tables have their primary key, and their structure has not changed.

By using the :ref:`emaj_verify_all() <emaj_verify_all>` function, the administrator can perform the same checks on demand for all table groups.

----

.. _event_triggers:

Event Triggers
--------------

Installing E-Maj adds two event triggers of type *sql_drop*:

* *emaj_sql_drop_trg* blocks drop attempts of:

  * Any E-Maj object (log schema, log table, log sequence, log function, and log trigger).
  * Any application table or sequence belonging to a table group in *LOGGING* state.
  * Any primary key of a table belonging to a *ROLLBACKABLE* table group.
  * Any schema containing at least one table or sequence belonging to a table group in *LOGGING* state.

* *emaj_protection_trg* blocks drop attempts of the *emaj* extension itself and the main *emaj* schema.

Installing E-Maj also adds an event trigger of type *table_rewrite*:

* *emaj_table_rewrite_trg* blocks any structure change of application or log tables.

It is possible to deactivate and reactivate these event triggers using two functions: :ref:`emaj_disable_protection_by_event_triggers() <emaj_disable_protection_by_event_triggers>` and :ref:`emaj_enable_protection_by_event_triggers() <emaj_enable_protection_by_event_triggers>`.

However, the protections do not cover all risks. In particular, they do not prevent:

- Any table or sequence renaming.
- Any schema change.

Additionally, some other DDL statements that alter table structures do not fire any trigger.

Note that these event triggers may not exist when the E-Maj extension has been :ref:`installed by a non-SUPERUSER role<event_triggers_limits>`.
