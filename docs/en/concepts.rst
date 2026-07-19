Concepts
========

E-Maj is built on three core concepts.

.. _tables_group:

Tables Group
************

A **table group** represents a **set of application tables** that share the **same lifecycle**. This means their content can be restored as a whole if needed. Typically, a table group includes all tables in a database that are updated by one or more sets of programs.

Each table group is identified by a unique name within its database.

Additionally, a table group can also include **partitions** of partitioned tables and **sequences**.

Tables (including partitions) and sequences in a table group can belong to **different schemas** within the database.

At any given time, a table group is in one of two states:

- **LOGGING**: Updates applied to the tables in the group are recorded.
- **IDLE**: Updates are NOT recorded.

A table group can be either:

- **ROLLBACKABLE** (default): Allows reverting recorded changes.
- **AUDIT_ONLY**: Reverting recorded changes is NOT possible. This type is useful for tables without an explicitly created *PRIMARY KEY* or for *UNLOGGED* tables.

----

Mark
****

A **mark** is a **stable reference point** in the lifecycle of a table group. It captures the state of all tables and sequences in the group at a specific moment. A mark is explicitly created by a user action

The mark is identified by a **unique name within its table group**.

----

Rollback
********

An **E-Maj rollback** operation resets all tables and sequences in a **group** to the state they had when a **mark** was set.

There are two types of rollback:

* **Unlogged rollback**: Changes canceled by the rollback are permanently discarded, with no trace kept in the log tables.
* **Logged rollback**: Changes canceled by the rollback are recorded in the log tables, allowing the rollback itself to be reversed later. In other words, a logged rollback can be *rolled back*.

Note that the **E-Maj rollback** concept is distinct from PostgreSQL's **transaction rollback** mechanism.
