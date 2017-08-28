Modifying tables groups
=======================

.. _emaj_alter_group:

Several types of events may lead to alter a tables group:

* the tables group definition may change, some tables or sequences may have been added or suppressed,
* one of the parameters linked to a table (priority, schema, tablespaces,...) may have been modified,
* the structure of one or several application tables of the tables group may have changed, such as an added or dropped column or a change in a column type.

Modifying a tables group in *IDLE* state
----------------------------------------

In all cases, the following steps can be performed:

* stop the group, if it is in *LOGGING* state, using the :ref:`emaj_stop_group() <emaj_stop_group>` function,
* update the :ref:`emaj_group_def <emaj_group_def>` table and/or modify the application schema,
* drop and recreate the tables group, using the :ref:`emaj_drop_group() <emaj_drop_group>` and :ref:`emaj_create_group() <emaj_create_group>` functions.

But this last step can be also performed by the *emaj_alter_group()* function, with a statement like::

   SELECT emaj.emaj_alter_group('<group.name>');

The function returns the number of tables and sequences that now belong to the tables group.

The *emaj_alter_group()* function also recreates E-Maj objects that may be missing (log tables, functions, …).

The function creates and drops the secondary schemas when needed.

Once altered, a tables group remains in *IDLE* state, but its log tables become empty.

The “*ROLLBACKABLE*” or “*AUDIT_ONLY*” characteristic of the tables group cannot be changed using the *emaj_alter_group()* function. To change it, the tables group must be dropped and re-created using the :ref:`emaj_drop_group() <emaj_drop_group>` and :ref:`emaj_create_group() <emaj_create_group>` functions.

All actions that are chained by the *emaj_alter_group()* function are executed on behalf of a unique transaction. As a consequence, if an error occurs during the operation, the tables group remains in its previous state.

In most cases, executing the *emaj_alter_group()* function is much more efficient than chaining both :ref:`emaj_drop_group() <emaj_drop_group>` and :ref:`emaj_create_group() <emaj_create_group>` functions.

It is possible to update the *emaj_group_def* table, when the tables group is in *LOGGING* state. However it will not have an effect until the group is altered (or dropped and re-created).

Using the *emaj_alter_groups()* function, several groups can be modified at once::

   SELECT emaj.emaj_alter_groups('<group.names.array>');

This function allows to move a table or a sequence from one tables group to another in a single operation.

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

Modifying a tables group in *LOGGING* state
-------------------------------------------

But the previous method has several drawbacks:

* logs recorded before the operation are lost,
* it is not possible to rollback a tables group to a previous state anymore.

However, some actions are possible while the tables groups are in *LOGGING* state. The following table lists the allowed actions.

+-------------------------------------+---------------+-----------------------+
| Action                              | LOGGING Group | Method                |
+=====================================+===============+=======================+
| Change the groupe ownership         | No            |                       | 
+-------------------------------------+---------------+-----------------------+
| Change the log schema suffix        | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Change the E-Maj names prefix       | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Change the log data tablespace      | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Change the log index tablespace     | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Change the E-Maj priority           | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Remove a table from a group         | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Remove a sequence from a group      | Yes           | emaj_group_def update |
+-------------------------------------+---------------+-----------------------+
| Add a table to a group              | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Add a sequence to a group           | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Repair a table or a sequence        | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Rename a table                      | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Rename a sequence                   | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Change the schema of a table        | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Change the schema of a sequence     | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Rename a table’s column             | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Change a table’s structure          | No            |                       |
+-------------------------------------+---------------+-----------------------+
| Other forms of ALTER TABLE          | Yes           | No E-Maj impact       |
+-------------------------------------+---------------+-----------------------+
| Other forms of ALTER SEQUENCE       | Yes           | No E-Maj impact       |
+-------------------------------------+---------------+-----------------------+

The "emaj_group_def update" method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Most attributes of the :ref:`emaj_group_def <emaj_group_def>` table describing the tables groups can be dynamicaly changed while groups have not been stopped.

To do this, the following steps can be performed:

* modify the :ref:`emaj_group_def <emaj_group_def>` table,
* call one of the *emaj_alter_group()* or *emaj_alter_groups()* functions.

For tables groups in *LOGGING* state, these functions set a *ROW EXCLUSIVE* lock on each application table of these groups.

On these same tables groups, they also set a mark whose name can be suppled as parameter. The syntax of these calls becomes::

   SELECT emaj.emaj_alter_group('<group.name>' [,’<mark>’]);

or ::

   SELECT emaj.emaj_alter_groups('<group.names.array>' [,’<mark>’]);

If the parameter representing the mark is not specified, or is empty or *NULL*, a name is automatically generated: “ALTER_%”, where the '%' character represents the current transaction start time with a “hh.mn.ss.mmm” pattern.

An E-Maj rollback operation targeting a mark set before such groups changes does **NOT** automaticaly cancel these changes.

However, the administrator can apply the same procedure to reset a tables group to a prior state.

.. caution::

	Once a table or a sequence is removed from a tables group, any rollback operation will leave this object unchanged. Once unlinked from its tables group, the application table or sequence can be altered or dropped. The historical data linked to the object (logs, marks traces,...) are kept as is so that they can be later examined. However, they remain linked to the tables group that owned the object and will only be deleted by a :ref:`group’s reset <emaj_reset_group>` operation or by the :ref:`deletion of the oldest marks <emaj_delete_before_mark_group>` of the group.

