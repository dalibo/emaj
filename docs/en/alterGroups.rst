Modifying tables groups
=======================

Several event types may lead to alter a tables group:

* the tables group definition may change, some tables or sequences may have been added or suppressed,
* one of the parameters linked to a table (priority, tablespaces,...) may have been modified,
* the structure of one or several application tables of the tables group may have changed, such as an added or dropped column or a column type change,
* a table or sequence may change its name or its schema.

When the modification concerns a tables group in *LOGGING* state, it may be necessary to temporarily remove the table or sequence from its tables group, with some impacts on potential future E-Maj rollback operations.

In most cases, depending whether the :ref:`emaj_group_def configuration table<emaj_group_def>` is used of not, the changes can be performed:

* either dynamically, with dedicated functions,
* or by adjusting the *emaj_group_def* table and by executing an *emaj_alter_group()* function.

Here are the possible actions, depending on the choosen method.

+------------------------------------------------------+-------------------------+---------------------------+
| Actions                                              | Dynamic Adjustment      | emaj_group_def adjustment |
+======================================================+=========================+===========================+
| Add a table/sequence to a group                      | Dedicated functions     | emaj_alter_group()        |
+------------------------------------------------------+-------------------------+---------------------------+
| Remove a table/sequence from a group                 | Dedicated functions     | emaj_alter_group()        |
+------------------------------------------------------+-------------------------+---------------------------+
| Move a table/sequence to another group               | Dedicated functions     | emaj_alter_group()        |
+------------------------------------------------------+-------------------------+---------------------------+
| Change the log data or index tablespace for a table  | Dedicated functions     | emaj_alter_group()        |
+------------------------------------------------------+-------------------------+---------------------------+
| Change the E-Maj priority for a table                | Dedicated functions     | emaj_alter_group()        |
+------------------------------------------------------+-------------------------+---------------------------+
| Repair a table                                       | Remove from the group + | emaj_alter_group()        |
|                                                      | add to the group        |                           |
+------------------------------------------------------+-------------------------+---------------------------+
| Rename a table                                       | Remove from the group + | Remove from the group +   |
|                                                      | ALTER TABLE + Add       | ALTER TABLE + Add         |
+------------------------------------------------------+-------------------------+---------------------------+
| Rename a sequence                                    | Remove from the group + | Remove from the group +   |
|                                                      | ALTER SEQUENCE + Add    | ALTER SEQUENCE + Add      |
+------------------------------------------------------+-------------------------+---------------------------+
| Change the schema of a table                         | Remove from the group + | Remove from the group +   |
|                                                      | ALTER TABLE + Add       | ALTER TABLE + Add         |
+------------------------------------------------------+-------------------------+---------------------------+
| Change the schema of a sequence                      | Remove from the group + | Remove from the group +   |
|                                                      | ALTER SEQUENCE + Add    | ALTER SEQUENCE + Add      |
+------------------------------------------------------+-------------------------+---------------------------+
| Rename a table’s column                              | Remove from the group + | Remove from the group +   |
|                                                      | ALTER TABLE + Add       | ALTER TABLE + Add         |
+------------------------------------------------------+-------------------------+---------------------------+
| Change a table’s structure                           | Remove from the group + | Remove from the group +   |
|                                                      | ALTER TABLE + Add       | ALTER TABLE + Add         |
+------------------------------------------------------+-------------------------+---------------------------+
| Other forms of ALTER TABLE                           | No E-Maj impact                                     |
+------------------------------------------------------+-------------------------+---------------------------+
| Other forms of ALTER SEQUENCE                        | No E-Maj impact                                     |
+------------------------------------------------------+-------------------------+---------------------------+

Adjusting the structure of in *LOGGING* state groups may have consequences on E-Maj rollback or SQL script generation (see below).

Even if the tables group is in *LOGGING* state, an E-Maj rollback operation targeting a mark set before a group’s change do NOT automatically revert this group’s change. However the E-Maj administrator can perform by himself the changes that would reset the group to its previous state.

.. _dynamic_ajustment:

The “Dynamic adjustment” method
-------------------------------

Some functions allow to dynamically adjust the tables groups content without modifying the *emaj_group_def* table.

Add tables or sequences to a tables group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The functions that :ref:`assign one or several tables or sequences<assign_table_sequence>` into a tables group that are used at group’s creation time are also usable during the whole group’s life.

When executing these functions, the tables group can be either in *IDLE* or in *LOGGING* state.

When the group is in *LOGGING* state, an exclusive lock is set on all tables of the group.

When the tables group is in *LOGGING* state, a mark is set. Its name is defined by the last parameter of the function. This parameter is optional. If not supplied, the mark name is generated, with a *ASSIGN* prefix.

.. _remove_table_sequence:

Remove tables from their tables group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The 3 following functions allow to remove one or several tables from their tables group::

	SELECT emaj.emaj_remove_table('<schema>', '<table>' [,'<mark>'] );

or ::

	SELECT emaj.emaj_remove_tables('<schema>', '<tables.array>' [,'<mark>'] );

or ::

	SELECT emaj.emaj_remove_tables('<schema>', '<tables.to.include.filter>', '<tables.to.exclude.filter>' [,'<mark>'] );

They are very similar to the tables assignment functions.

When several tables are removed, they do not necessarily belongs to the same group.

When the tables group or groups are in *LOGGING* state and no mark is supplied in parameters, the mark is generated with a *REMOVE* prefix.

Remove sequences from their tables group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The 3 following functions allow to remove one or several sequences from their tables group::

	SELECT emaj.emaj_remove_sequence('<schema>', '<sequence>' [,'<mark>'] );

or ::

	SELECT emaj.emaj_remove_sequences('<schema>', '<sequences.array>' [,'<mark>'] );

or ::

	SELECT emaj.emaj_remove_sequences('<schema>', '<sequences.to.include.filter>', '<sequences.to.exclude.filter>' [,'<mark>'] );

They are very similar to the sequences assignment functions.

When the tables group is in *LOGGING* state and no mark is supplied in parameters, the mark is generated with a *REMOVE* prefix,

.. _move_table_sequence:

Move tables to another tables group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

3 functions allow to move one or several tables to another tables group::

	SELECT emaj.emaj_move_table('<schema>', '<table>', '<new.group' [,'<mark>'] );

or ::

	SELECT emaj.emaj_move_tables('<schema>', '<tables.array>', '<new.group' [,'<mark>'] );

or ::

	SELECT emaj.emaj_move_tables('<schema>', '<tables.to.include.filter>', '<tables.to.exclude.filter>', '<new.group' [,'<mark>'] );

When serveral tables are moved to another tables group, they do not necessarily belong to the same source group.

When the tables group is in *LOGGING* state and no mark is supplied in parameters, the mark is generated with a *MOVE* prefix,

Move sequences to another tables group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

3 functions allow to move one or several sequences to another tables group::

	SELECT emaj.emaj_move_sequence('<schema>', '<sequence>', '<new.group' [,'<mark>'] );

or ::

	SELECT emaj.emaj_move_sequences('<schema>', '<sequences.array>', '<new.group' [,'<mark>'] );

or ::

	SELECT emaj.emaj_move_sequences('<schema>', '<sequences.to.include.filter>', '<sequences.to.exclude.filter>', '<new.group' [,'<mark>'] );

When serveral sequences are moved to another tables group, they do not necessarily belong to the same source group.

When the tables group is in *LOGGING* state and no mark is supplied in parameters, the mark is generated with a *MOVE* prefix,

.. _modify_table:

Modify tables properties
^^^^^^^^^^^^^^^^^^^^^^^^

3 functions allow to modify the properties of one or several tables from a single schema::

	SELECT emaj.emaj_modify_table('<schema>', '<table>', '<modified.properties>' [,'<mark>']]);

or ::

	SELECT emaj.emaj_modify_tables('<schema>', '<tables.array>', '<modified.properties>' [,'<mark>']]);

or ::

	SELECT emaj.emaj_modify_tables('<schema>', '<tables.to.include.filter>', '<tables.to.exclude.filter>', '<modified.properties>' [,'<mark>']]);

The <modified.properties> parameter is of type JSONB. Its elementary fields are the same as the <properties> parameter of the :ref:`tables assignment functions<assign_table_sequence>`. But this <modified.properties> parameter only contains ... the properties to modify. The not listed properties remain unchanged. It is possible to reset a property to its default value by setting a *NULL* value (the json null).

The functions return the number of tables that have effectively changed at least one property.

When the tables group is in *LOGGING* state and no mark is supplied in parameters, the mark is generated with a *MODIFY* prefix,

.. _emaj_alter_group:

The "emaj_group_def update" method
------------------------------------

To avoid to be obliged to drop and recreate the tables groups when the *emaj_group_def* table is modified, a function allows to only take into account the configuration changes::

   SELECT emaj.emaj_alter_group('<group.name>' [,’<mark>’]);

The function returns the number of tables and sequences that now belong to the tables group.

The *emaj_alter_group()* function also recreates E-Maj objects that may be missing (log tables, functions, ...). It also creates and drops the log schemas when needed.

If the group is in *IDLE* state, its log tables become empty.

If the tables group is in *LOGGING* state, the function:

* sets a *ROW EXCLUSIVE* lock on all application tables of the tables group,
* sets a mark, whose name can be supplied as parameter.

If the parameter representing the mark is not specified, or is empty or *NULL*, a name is automatically generated: *ALTER_%*, where the '%' character represents the current time with a "hh.mn.ss.mmmm" pattern.

All actions that are chained by the *emaj_alter_group()* function are executed on behalf of a unique transaction. As a consequence, if an error occurs during the operation, the tables group remains in its previous state.

It is possible to update the *emaj_group_def* table, when the tables group is in *LOGGING* state. However it will have no effect until the group is altered (or dropped and re-created).

Using the *emaj_alter_groups()* function, several groups can be modified at once::

   SELECT emaj.emaj_alter_groups('<group.names.array>' [,’<mark>’]);

This function especially allows to move a table or a sequence from one tables group to another in a single operation.

More detail about the way to describe a group names array :doc:`here<multiGroupsFunctions>`.

Incidence of tables or sequences addition or removal in a group in LOGGING state
--------------------------------------------------------------------------------

.. caution::

	Once a table or a sequence is removed from a tables group, any rollback operation will leave this object unchanged. Once unlinked from its tables group, the application table or sequence can be altered or dropped. 

The historical data linked to the object (logs, marks traces,...) are kept as is so that they can be later examined. However, they remain linked to the tables group that owned the object. To avoid any confusion, log tables are renamed, adding a numeric  suffix to its name. These logs and marks traces will only be deleted by a :ref:`group’s reset <emaj_reset_group>` operation or by the :ref:`deletion of the oldest marks <emaj_delete_before_mark_group>` of the group.

.. caution::

	When a table or a sequence is added into a tables group in *LOGGING* state, it is then processed by any further rollback operation. But if the rollback operation targets a mark set before the addition into the group, the table or the sequence is left in its state at the time of the addition into the group and a warning message is issued. Such a table or sequence will not be processed by a SQL script generation function call if the requested start mark has been set before the addition of the table or sequence into the group

Some graphs help to more easily visualize the consequences of the addition or the removal of a table or a sequence into/from a tables group in *LOGGING* state.

Let’s use a tables group containing 4 tables (t1 to t4) and 4 marks set over time (m1 to m4). At m2, t3 has been added to the group while t4 has been removed. At m3, t2 has been removed from the group while t4 has been re-added.

.. image:: images/logging_group_changes.png
   :align: center

A rollback to the mark m1:

* would process the table t1,
* would **NOT** process the table t2, for lack of log after m3,
* would process the table t3, but only up to m2,
* would process the table t4, but only up to m3, for lack of log between m2 and m3.

.. image:: images/logging_group_rollback.png
   :align: center

A log statistics report between the marks m1 and m4 would contain:

* 1 row for t1 (m1,m4),
* 1 row for t2 (m1,m3),
* 1 row for t3 (m2,m4),
* 2 rows for t4 (m1,m2) and (m3,m4).

.. image:: images/logging_group_stat.png
   :align: center

The SQL script generation for the marks interval m1 to m4:

* would process the table t1,
* would process the table t2, but only up the mark m3,
* would **NOT** process the table t3, for lack of log before m2,
* would process the table t4, but only up to the mark m2, for lack of log between m2 and m3.

.. image:: images/logging_group_gen_sql.png
   :align: center

If the structure of an application table has been inadvertently changed while it belonged to a tables group in *LOGGING* state, the mark set and rollback operations will be blocked by the E-Maj internal checks. To avoid stopping, altering and then restarting the tables group, it is possible to only remove the concerned table from its group and then to re-add it.

When a table changes its affected group, the impact on the ability to generate a SQL script or to rollback the source and destination tables groups is similar to removing the table from its source group and then adding the table to the destination group.

Reparing a tables group
-----------------------

Eventhough the event triggers created with E-Maj limit the risk, some E-Maj components that support an application table (log table, function or trigger) may have been dropped. In such a case, the associated tables group cannot work correctly anymore.

In order to solve the issue without stopping the tables group if it is in *LOGGING* state (and thus loose the benefits of the recorded logs), it is possible to remove the table from its group and then re-add it, by chaining both commands::

   SELECT emaj.emaj_remove_table('<schema>', '<table>' [,'<mark>']);

   SELECT emaj.emaj_assign_table('<schema>', '<table>', '<group>' [,'properties' [,'<mark>']] );

Of course, once the table is removed from its group, the content of the associated logs cannot be used for a potential rollback or script generation anymore.

However, if the log sequence is missing (which should never be the case) and the tables group is in *LOGGING* state, it is necessary to  :ref:`force the group’s stop<emaj_force_stop_group>` before removing and re-assigning the table.

It may also happen that an application table or sequence has been accidentaly dropped. In this case, the table of sequence can be simply a posteriori removed from its group, by:

* either execute the appropriate *emaj_remove_table()* or *emaj_remove_sequence()* function,
* or by chaining the deletion of the row corresponding to the application table or sequence from the *emaj_group_def* table, and the call of the *emaj_alter_group()* function for the related tables group.

.. _emaj_sync_def_group:

Combining dynamic tables groups management and configuration with emaj_group_def
--------------------------------------------------------------------------------

Normally, the tables group mangement method has to be choosen first: dynamic management or use of the *emaj_group_def* configuration table. However, it is possible to mix both methods.

The difficulty comes when, once dynamic changes performed on tables groups content, the *emaj_group_def* table does not reflect the configuration of the tables groups anymore.

To avoid to manually report the configuration changes into the *emaj_group_def* table, with errors risk, the E-Maj administrator can synchronize the *emaj_group_def* table with the actual groups configuration, for a given tables group. To achive this, he can execute::

   SELECT emaj.emaj_sync_def_group('<group>');

The function returns the number of tables and sequences contained in the tables group.

The schema below shows the possible change flows.

.. image:: images/alter_group_methods.png
   :align: center

