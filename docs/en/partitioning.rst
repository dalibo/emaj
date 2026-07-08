Management of Partitioned Tables
================================

E-Maj is able to manage partitioned tables.

Partitioning Based on Inheritance
---------------------------------

When using the older partitioning technique based on the inheritance mechanism, both parent and child tables contain data. These tables can be assigned to table groups. They are handled by E-Maj like any other table.

----

Declarative Partitioning
------------------------

PostgreSQL 10 introduced declarative partitioning, which uses DDL to handle distinct objects representing partitioned tables that describe data structures, and partitions containing data.

Assignment to Table Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^

E-Maj can process elementary partitions of partitioned tables created with declarative DDL. They are handled like any other table.

However, as there is no need to protect parent tables (which remain empty), E-Maj refuses to include them in table groups.

Partitions of a partitioned table can be assigned to different table groups. Some partitions may also be left unassigned to any group.

.. _fk_on_partitioned_tables:

Foreign Keys
^^^^^^^^^^^^

With declarative partitioning, a *FOREIGN KEY* can be defined either at the elementary partition level or at the partitioned table level to cover all partitions at once.

Foreign keys defined at the elementary partition level follow the same usage rules as any other foreign key.

On the other hand, a foreign key set on a partitioned table is **not** supported by E-Maj rollbacks if:

    • Tables/partitions linked by this key do not all belong to the same table groups to process.
    • The key is of type *IMMEDIATE*.
    • The key has *ON DELETE* or *ON UPDATE* clauses.

Indeed, it is impossible to drop and recreate a foreign key set on a partitioned table for just one partition.

As a workaround:

    • Foreign keys of type *IMMEDIATE* (the default state) can easily be declared as *DEFERRABLE INITIALLY IMMEDIATE*.
    • Foreign keys having *ON DELETE* or *ON UPDATE* clauses can be created on each elementary partition.

.. _trigger_on_partitioned_tables:

Application Triggers
^^^^^^^^^^^^^^^^^^^^

In a declarative partitioning context, it is possible to create a trigger on a partitioned table. As a result, each partition of the table inherits the trigger. There is no practical issue with this in E-Maj rollbacks.

If one wishes to leave the trigger enabled during the rollback, it must be declared as such for each partition.

Partition Attach / Detach
^^^^^^^^^^^^^^^^^^^^^^^^^

An ``ALTER TABLE ... ATTACH PARTITION ...`` SQL statement transforms an independent table into a partition. Symmetrically, an ``ALTER TABLE ... DETACH PARTITION ...`` statement transforms a partition into an independent table.

In both cases, the table/partition can already be assigned to a table group at the time of the ``ALTER TABLE`` execution. An E-Maj rollback targeting a mark set before the structure change will reset the table/partition content to its previous state.

Partitions Merge / Split
^^^^^^^^^^^^^^^^^^^^^^^^

An ``ALTER TABLE ... MERGE PARTITIONS ...`` SQL statement merges several partitions into a single one. Symmetrically, an ``ALTER TABLE ... SPLIT PARTITION ...`` statement splits a partition into several partitions.

In both cases, all partitions involved in the operation that are assigned to a table group must be removed from their group. As a result, their data content cannot be reset to a mark set before the partition split or merge.
