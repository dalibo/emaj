Other Functions
===============

.. _emaj_get_version:

Getting the E-Maj Extension Version
-----------------------------------

The ``emaj_get_version()`` function returns the **current version** identifier of the *emaj* extension. ::

   SELECT emaj.emaj_get_version();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns a textual representation of the *emaj* current version.

----

.. _emaj_verify_all:

Checking the E-Maj Environment Consistency
------------------------------------------

The ``emaj_verify_all()`` function checks the consistency of the E-Maj environment. It verifies the integrity of all E-Maj schemas and created table groups. This function can be called with the following SQL statement::

   SELECT * FROM emaj.emaj_verify_all();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns a set of textual messages describing any detected discrepancies.

**Notes**

For each E-Maj schema (*emaj* and each log schema), the function verifies that:

* All tables, functions, sequences, and types contained in the schema are either objects of the extension or linked to created table groups.
* They do not contain any views, foreign tables, domains, conversions, operators, or operator classes.

For each created table group, the function performs the same checks as those executed when a group is started, a mark is set, or a rollback is performed (:ref:`more details <internal_checks>`).

If no errors are detected, the function returns a single row with the message::

   'No error detected'

The function also returns warnings when:

* A sequence linked to a column belongs to a table group, but the associated table does not belong to the same table group.
* A table in a table group is linked to another table by a foreign key, but the associated table does not belong to the same table group.
* A :ref:`foreign key is inherited from a partitioned table <fk_on_partitioned_tables>`, but it is either not *DEFERRABLE* or includes an *ON DELETE* or *ON UPDATE* clause, which would block its potential drop/recreation during an E-Maj rollback.
* The *dblink* connection is not operational.
* Event triggers protecting E-Maj are missing or disabled.

The *emaj_verify_all()* function can be executed by any role belonging to the *emaj_adm* or *emaj_viewer* roles (the *dblink* connection is not tested for the latter).

If errors are detected (e.g., after an application table referenced in a table group has been dropped), appropriate measures must be taken. Typically, orphaned log tables or functions must be manually dropped.

----

.. _emaj_get_current_log_table:

Identifying the Current Log Table for an Application Table
----------------------------------------------------------

The ``emaj_get_current_log_table()`` function retrieves the schema and table names of the log table linked to a given application table. ::

   SELECT log_schema, log_table FROM
       emaj_get_current_log_table(p_schema, p_table);

**Input Parameters**

- ``p_schema`` (*TEXT*): Application **schema name**.
- ``p_table`` (*TEXT*): Application **table name**.

**Returned data**

The function returns one row, with 2 columns:

- ``log_schema`` (*TEXT*): **Log schema**.
- ``log_table`` (*TEXT*): **Log table** that currently holds data change events (the application table may have several log tables if it has been removed from its group and then reassigned to a group).

**Notes**

If the application table does not currently belong to any table group, the *log_schema* and *log_table* columns are set to *NULL*.

The *emaj_get_current_log_table()* function can be used by roles with *emaj_adm* or *emaj_viewer* privileges.

It is possible to build a statement to access a log table. For example::

   SELECT 'SELECT count(*) FROM '
       || quote_ident(log_schema) || '.' || quote_ident(log_table)
       FROM emaj.emaj_get_current_log_table('myschema', 'mytable');

----

.. _emaj_purge_histories:

Purging History Data
--------------------

E-Maj retains historical data, such as traces of elementary operations, E-Maj rollback details, and table group structure changes (:ref:`more details <emaj_hist>`). Older traces are automatically purged by the extension. However, it is also possible to manually purge obsolete traces using::

   SELECT emaj.emaj_purge_histories(p_retentionDelay);

**Input Parameters**

- ``p_retentionDelay`` (*INTERVAL*, optional): **Histories retention delay**. If a not *NULL* value is provided, it overrides the *history_retention* E-Maj parameter.

**Returned data**

The function returns a summary message of the executed deletions.

----

.. _emaj_disable_protection_by_event_triggers:
.. _emaj_enable_protection_by_event_triggers:

Deactivating/Reactivating Event Triggers
----------------------------------------

The E-Maj extension installation procedure activates :ref:`event triggers <event_triggers>` to protect it. Normally, these triggers should remain active. However, if an E-Maj administrator needs to temporarily deactivate them, two dedicated functions are available.

To **deactivate** existing **event triggers**::

   SELECT emaj.emaj_disable_protection_by_event_triggers();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns the number of deactivated event triggers.

To **reactivate** existing **event triggers**::

   SELECT emaj.emaj_enable_protection_by_event_triggers();

**Input Parameters**

The function does not require any input parameter.

**Returned data**

The function returns the number of reactivated event triggers.
