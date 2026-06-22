Management of partitioned tables
================================

E-Maj is able to manage partitioned tables.

Partitioning based on heritage
-------------------------------

When using the very old partitioning technic based on the heritage mechanism, both mother and child tables contain data. These tables can be assigned to tables groups. They are handled by E-Maj as any other table.

Declarative partitioning
-------------------------

PostgreSQL 10 has introduced the declarative partitioning, which DDL handles distinct objects representing partitioned tables that describe data structures, and partitions containing data.

Assignment to tables groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^

E-Maj can process elementary partitions of partitioned tables created with the declarative DDL. They are handled as any other tables.

However, as there is no need to protect mother tables, which remain empty, E-Maj refuses to include them in tables groups.

Partitions of a partitioned table can be assigned to different tables groups. And some partitions may be left not assigned to any group.

.. _fk_on_partitioned_tables:

Foreign keys
^^^^^^^^^^^^

With the declarative partitioning, a *FOREIGN KEY* can be defined either at elementary partitions level or at the partitioned table level, in order to cover all partitions at once.

Foreign keys defined at elementary partition level follow the same usage rules as any other foreign key.

On the contrary, a foreign key set on a partitioned table is NOT supported by E-Maj rollbacks:

    • if tables/partitions linked by this key do not all belong to the same tables groups to process, or
    • if the key is of type *IMMEDIATE*, or
    • if the key has *ON DELETE* or *ON UPDATE* clauses.

Indeed, it is impossible to drop and recreate a foreign key set on a partitioned table for just a partition.

As a workaround :

    • foreign keys of type *IMMEDIATE* (the default state) can easily be declared as *DEFERRABLE INITIALY IMMEDIATE*,
    • foreign keys having *ON DELETE* or *ON UPDATE* clauses can be created on each elementary partition.

.. _trigger_on_partitioned_tables:

Application triggers
^^^^^^^^^^^^^^^^^^^^

In a declarative partitioning context, it is possible to create a trigger on a partitioned table. As a result, each partition of the table inherits the trigger. There is no pratical issue with this on E-Maj rollbacks.

If one wishes to let the trigger enabled during the rollback, it must be declared as such for each partition.


Partition attach / detach
^^^^^^^^^^^^^^^^^^^^^^^^^

An "*ALTER TABLE ... ATTACH PARTITION ...*" SQL statement transforms an independent table into a partition. Symetrically, an "*ALTER TABLE ... DETACH PARTITION ...*" statement transforms a partition into an independent table.

In both cases, the table/partition can be already assigned to a tables group at ALTER TABLE time. An E-Maj rollback targeting a mark set before the structure change will reset the table/partition content to its previous content.


Partitions merge / split
^^^^^^^^^^^^^^^^^^^^^^^^

An "*ALTER TABLE ... MERGE PARTITIONS ...*" SQL statement merges several partitions into a single one. Symetrically, a "*ALTER TABLE ... SPLIT PARTITION ...*" statement splits a partition into several partitions.

In both cases, all partitions concerned by the operation that are assigned to a tables group must be removed from their group. As a result, their data content cannot be reset to a mark set before the partitions split or merge.
