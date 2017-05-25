Concepts
========

E-Maj is built on three main concepts.

Tables Group
************

The **tables group** represents a set of **application tables** that live at the same rhythm, meaning that their content can be restored as a whole if needed. Typically, it deals with all tables of a database that are updated by one or more sets of programs.  Each tables group is defined by a name which must be unique inside its database. By extent, a tables group can also contain **partitions** of partitionned tables and **sequences**. Tables (including partitions) and sequences that constitute a tables group can belong to different schemas of the database.

At a given time, a tables group is either in a **LOGGING** state or in a **IDLE** state. The *LOGGING* state means that all updates applied on the tables of the group are recorded.

A tables group can be either **ROLLBACKABLE**, which is the standard case, or **AUDIT_ONLY**. In this latter case, it is not possible to rollback the group. But with this type of group, it is possible to record tables updates for auditing purposes, even with tables that do not have primary key known in PostgreSQL catalogue.

Mark
****

A **mark** is a particular point in the life of a tables group, corresponding to a stable point for all tables and sequences of the group. A mark is explicitly set by a user operation. It is defined by a name that must be unique for the tables group.

Rollback
********

The **rollback** operation consists of resetting all tables and sequences of a group in the state they had when a mark was set.

There are two rollback types:

* with a **unlogged rollback**, no trace of updates that are cancelled by the rollback operation are kept,
* with a **logged rollback**, update cancellations are recorded in log tables, so that they can be later cancelled: the rollback operation can be … rolled back.

Note that this concept of *E-Maj rollback* is different from the usual concept of *transactions rollback* managed by PostgreSQL.

