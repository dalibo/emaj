Creating and dropping tables groups
===================================

Tables groups configuration principles
--------------------------------------

Configuring a tables group consists in:

* defining the tables group characteristics,
* defining the tables and sequences to assign to the group,
* optionnaly, defining some specific properties for each table.

The tables group
^^^^^^^^^^^^^^^^

A tables group is identified by its **name**. Thus, the name must be unique withing the database. A tables group name contains at least 1 character. It may contain spaces and/or any punctuation characters. But it is advisable to avoid commas, single or double quotes.

At creation time, the :ref:`ROLLBACKABLE or AUDIT_ONLY <tables_group>` property of the group must be set. Note that this property cannot be modified once the tables group is created. If it needs to be changed, the tables group must be dropped and then recreated.

The tables and sequences to assign
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A tables group can contain tables and/or sequences belonging to one or several schemas.

All tables of a schema are not necessarily member of the same group. Some of them can belong to another group. Some others can belong to any group.

But **at a given time**, a table or a sequence cannot be assigned to more than **one tables group**.

.. caution::

   To guarantee the integrity of tables managed by E-Maj, it is essential to take a particular attention to the tables groups content definition. If a table were missing, its content would be out of synchronisation with other tables it is related to, after an E-Maj rollback operation. In particular, when application tables are created or suppressed, it is important to always maintain an up-to-date groups configuration.

All tables assigned to a *ROLLBACKABLE* group must have an explicit primary key (*PRIMARY KEY* clause in *CREATE TABLE* or *ALTER TABLE*).

E-Maj can process elementary partitions of partitionned tables created with the declarative DDL (with PostgreSQL 10+). They are processed as any other tables. However, as there is no need to protect mother tables, which remain empty, E-Maj refuses to include them in tables groups. All partitions of a partitionned table do not need to belong to a tables group. Partitions of a partitionned table can be assigned to different tables groups.

By their nature, *TEMPORARY TABLE* are not supported by E-Maj. *UNLOGGED* tables and tables created as *WITH OIDS* can only be members of *AUDIT_ONLY* tables groups.

If a sequence is associated to an application table, it is advisable to assign it into the same group as its table, so that, in case of E-maj rollback, the sequence can be reset to its state at the set mark time. If it were not the case, an E-Maj rollback would simply generate a hole in the sequence values.

E-Maj log tables and sequences should NOT be assigned in a tables group.

Specific tables properties
^^^^^^^^^^^^^^^^^^^^^^^^^^

Three properties are associated to tables assigned to tables group:

* priority level,
* tablespace for log data,
* tablespace for log index.

The **priority** level is of type *INTEGER*. It is *NULL* by default. It defines a priority order in E-Maj tables processing. This can be espacialy useful at table lock time. Indeed, by locking tables in the same order as what is typically done by applications, it may reduce the risk of deadlock. E-Maj functions process tables in priority ascending order, *NULL* being processed last. For a same priority level, tables are processed in alphabetic order of schema name and table name.

To optimize performances of E-Maj installations having a large number of tables, it may be useful to spread log tables and their index on several tablespaces. Two properties are available to specify:

* the name of the tablespace to use for the log table of an application table,
* the name of the tablespace to use for the index of the log table.

By default, these properties have a *NULL* value, meaning that the default tablespace of the current session at tables group creation is used.

Tables group creation
---------------------

There are two main ways to handle the structure of tables groups:

* dynamically manage the tables and sequences assignment into tables groups, using dedicated functions,
* describe the list of tables groups and the tables and sequences they contain into the *emaj_group_def* configuration table.

Eventhough these methods are very different, they can be :ref:`combined<emaj_sync_def_group>`.

The “dynamic configuration” method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creating a tables group requires several steps. The first one consists in creating an empty tables group. Then one populates it with tables and sequences.

.. _emaj_create_group:

**Creating the empty tables group**

To create en empty tables group, just execute the following SQL statement::

   SELECT emaj.emaj_create_group('<group.name>',<is_rollbackable>, <is_empty>);

The second parameter, of type boolean, indicates whether the group's type is *ROLLBACKABLE* (with value *TRUE*) or *AUDIT_ONLY* (with value *FALSE*). If this second parameter is not supplied, the group is considered *ROLLBACKABLE*.

Note that the third parameter must be explicitely set to *TRUE*.

If the group is also referenced into the *emaj_group_def* table, the *emaj_group_def* table’s content is simply ignored.

The function returns the number of tables and sequences contained by the group.

.. _assign_table_sequence:

**Assigning tables and sequences into the tables group**

Six functions allow to dynamically assign one or several tables or sequences to a group.

To add one or several tables into a tables group::

   SELECT emaj.emaj_assign_table('<schema>', '<table>', '<groupe.name>' [,'<properties>' [,'<mark>']]);

or::

   SELECT emaj.emaj_assign_tables('<schema>', '<tables.array>', '<group.name>' [,'<properties>' [,'<mark>']] );

or::

   SELECT emaj.emaj_assign_tables('<schema>', '<tables.to.include.filter>', '<tables.to.exclude.filter>', '<group.name>' [,'<properties>' [,'<mark>']] );

To add one or several sequences into a tables group::

   SELECT emaj.emaj_assign_sequence('<schema>', '<sequence>', '<group.name>' [,'<mark>'] );

or::

   SELECT emaj.emaj_assign_sequences('<schema>', '<sequences.array>', '<group.name>' [,'<mark>'] );

or::

   SELECT emaj.emaj_assign_sequences('<schema>', '<sequences.to.include.filter>', '<sequences.to.exclude.filter>', '<group.name>' [,'<mark>'] );

For functions processing several tables or sequences in a single operation, the list of tables or sequences to process is:

* either provided by a parameter of type TEXT array, 
* or built with two regular expressions provided as parameters.

A TEXT array is typically expressed with a syntax like::

   ARRAY[‘element_1’,’ element_2’, ...]

Both regular expressions follow the POSIX rules. Refer to the PostgreSQL documentation for more details. The first one defines a filter that selects the tables of the schema. The second one defines an exclusion filter applied on the selected tables. For instance:

To select all tables or sequences of the schema my_schema::

   'my_schema', '.*', ''

To select all tables of this schema and whose name start with 'tbl'::

   'my_schema', '^tbl.*', ''

To select all tables of this schema and whose name start with ‘tbl’, except those who end with ‘_sav’::

   'my_schema', '^tbl.*', '_sav$'

The functions assigning tables or sequences to tables groups that build their selection with regular expressions take into account the context of the tables or sequences. Are not selected for instance: tables or sequences already assigned, or tables without primary key for *rollbackable* groups, or tables declared *UNLOGGED*.

The *<properties>* parameter of functions that assign tables to a group allows to set values to some properties for the table or tables. Of type *JSONB*, its value can be set like this::

   '{ "priority" : <n> , "log_data_tablespace" : "<xxx>" , "log_index_tablespace" : "<yyy>" }'

where:

* <n> is the priority level for the table or tables
* <xxx> is the name of the tablespace to handle log tables
* <yyy> is the name of the tablespace to handle log indexes

If one of these properties is not set, its value is considered *NULL*.

If specific tablespaces are referenced for any log table or log index, these tablespaces must exist before the function's execution.

For all these functions, an exclusive lock is set on each table of the concerned table groups, so that the groups stability can be guaranted during these operations.

All these functions return the number of assigned tables or sequences.

The tables assignment functions create all the needed log tables, the log functions and triggers, as well as the triggers that process the execution of *TRUNCATE* SQL statements. They also create the log schemas if needed.

The "configuration table" method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the method that was initialy available with E-Maj.

Creating a tables group needs 2 steps:

* register the groups configuration into the *emaj_group_def* table,
* create the tables group by itself.

.. _emaj_group_def:

**Populating the emaj_group_def table**

The content of tables groups to create has to be defined by populating the *emaj.emaj_group_def* table. One row has to be inserted into this table for each application table or sequence to include into a tables group. This *emaj.emaj_group_def* table has the following structure:

+--------------------------+------+--------------------------------------------------------------------------+
| Column                   | Type | Description                                                              |
+==========================+======+==========================================================================+
| grpdef_group             | TEXT | tables group name                                                        |
+--------------------------+------+--------------------------------------------------------------------------+
| grpdef_schema            | TEXT | name of the schema containing the application table or sequence          |
+--------------------------+------+--------------------------------------------------------------------------+
| grpdef_tblseq            | TEXT | application table or sequence name                                       |
+--------------------------+------+--------------------------------------------------------------------------+
| grpdef_priority          | INT  | priority level for the table in E-Maj processing (optional)              |
+--------------------------+------+--------------------------------------------------------------------------+
| grpdef_log_dat_tsp       | TEXT | name of the tablespace containing the log table (optional)               |
+--------------------------+------+--------------------------------------------------------------------------+
| grpdef_log_idx_tsp       | TEXT | name of the tablespace containing the index of the log table (optional)  |
+--------------------------+------+--------------------------------------------------------------------------+

The administrator can populate this table by any usual mean: *INSERT* SQL verb, *COPY* SQL verb, *\\copy psql* command, graphic tool, etc.

The content of the *emaj_group_def* table is case sensitive. Schema names, table names and sequence names must reflect the way PostgreSQL registers them in its catalogue. These names are mostly in lower case. But if a name is encapsulated by double quotes in SQL statements because it contains any upper case characters or spaces, then it must be registered into the *emaj_group_def* table with the same upper case characters or spaces.

Caution: the *emaj_group_def* table content is modified by the *emaj_import_groups_configuration()* functions.

**Creating the tables group**

Once the content of the tables group is defined, E-Maj can create the group. To do this, there is only one SQL statement to execute::

   SELECT emaj.emaj_create_group('<group.name>',<is_rollbackable>);

or in an abbreviated form::

   SELECT emaj.emaj_create_group('<group.name>');

The second parameter, of type boolean, indicates whether the group is a *ROLLBACKABLE* (with value *TRUE*) or an *AUDIT_ONLY* (with value *FALSE*) group. If this second parameter is not supplied, the group is considered *ROLLBACKABLE*.

The function returns the number of tables and sequences contained by the group.

For each table of the group, this function creates the associated log table, the log function and trigger, as well as the trigger that processes the execution of *TRUNCATE* SQL statements.

The function also creates the log schemas if needed.

On the contrary, if specific tablespaces are referenced for any log table or log index, these tablespaces must exist before the function's execution.

The *emaj_create_group()* function also checks the existence of application triggers on any tables of the group. If a trigger exists on a table of the group, a message is returned, suggesting the user to check the impact of this trigger on E-Maj rollbacks.

If a sequence of the group is associated either to a *SERIAL* or *BIGSERIAL* column or to a column created with a *GENERATED AS IDENTITY* clause, and the table that owns this column does not belong to the same tables group, the function also issues a *WARNING* message.

A specific version of the function allows to create an empty tables group, i.e. without any table or sequence at creation time::

   SELECT emaj.emaj_create_group('<group.name>',<is_rollbackable>, <is_empty>);

The third parameter is *FALSE* by default. If it is set to *TRUE* and the group is referenced in the *emaj_group_def* table, the *emaj_group_def* table’s content is ignored. Once created, an empty group can be then populated using the :doc:`emaj_alter_group() <alterGroups>` function or :ref:`dynamic tables groups adjusment <dynamic_ajustment>` functions.

All actions that are chained by the *emaj_create_group()* function are executed on behalf of a unique transaction. As a consequence, if an error occurs during the operation, all tables, functions and triggers already created by the function are cancelled.

By registering the group composition in an internal table (*emaj_relation*), the *emaj_create_group()* function freezes its definition for the other E-Maj functions, even if the content of the *emaj_group_def* table is modified later.

A tables group can be altered by the :doc:`emaj_alter_group() <alterGroups>` function (see §4.4).

.. _emaj_drop_group:

Drop a tables group
-------------------

To drop a tables group previously created by the :ref:`emaj_create_group() <emaj_create_group>` function, this group must be already in *IDLE* state. If it is not the case, the :ref:`emaj_stop_group() <emaj_stop_group>` function has to be used first.

Then, just execute the SQL command::

   SELECT emaj.emaj_drop_group('<group.name>');

The function returns the number of tables and sequences contained in the group.

For this tables group, the *emaj_drop_group()* function drops all the objects that have been created by the assignment functions or by the :ref:`emaj_create_group() <emaj_create_group>` function: log tables, sequences, functions and triggers.

The function also drops all log schemas that are now useless.

The locks set by this operation can lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts.

