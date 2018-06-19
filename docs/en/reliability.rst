Reliability
===========

Two additional elements help in ensuring the E-Maj reliability: internal checks are performed at some key moments of tables groups life and event trigers can block some risky operations.

.. _internal_checks:

Internal checks
---------------

When a function is executed to start a tables group, to set a mark or to rollback a tables group, E-Maj performs some checks in order to verify the integrity of the tables groups to process.

These **tables group integrity checks** verify that:

* the PostgreSQL version at tables group creation time is compatible with the current version,
* each application sequence or table of the group always exists, 
* each table of the group has its log table, its log function and its triggers,
* the log tables structure always reflects the related application tables structure,
* no table has been altered as *UNLOGGED* or *WITH OIDS* table,
* for *ROLLBACKABLE* tables groups, application tables have their primary key and their structure has not changed.

By using the :ref:`emaj_verify_all() <emaj_verify_all>` function, the administrator can perform the same checks on demand on all tables groups.

.. _event_triggers:

Event triggers
--------------

Installing E-Maj on instances using PostgreSQL version 9.3 or higher adds 2 event triggers of type “*sql_drop*“:

* *emaj_sql_drop_trg* blocks the drop attempts of:

  * any E-Maj object (log table, log sequence, log function, log trigger and secondary schema),
  * any application table or sequence belonging to a table group in *LOGGING* state,
  * any schema containing at least one table or sequence belonging to a table group in *LOGGING* state.

* *emaj_protection_trg* blocks the drop attempts of the *emaj* extension itself and the main *emaj* schema.

Installing E-Maj on instances using PostgreSQL version 9.5 or higher adds 1 event trigger of type “table_rewrite”:

* *emaj_table_rewrite_trg* blocks any structure change of application or log table.

It is possible to deactivate and reactivate these event triggers thanks to 2 functions: :ref:`emaj_disable_protection_by_event_triggers() <emaj_disable_protection_by_event_triggers>` and :ref:`emaj_enable_protection_by_event_triggers() <emaj_enable_protection_by_event_triggers>`.

However, the protections do not cover all risks. In particular, they do not prevent any tables or sequences renaming or any schema change. And some other DDL statements altering tables structure will not fire any trigger.

