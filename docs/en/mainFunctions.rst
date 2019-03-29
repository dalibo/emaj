Main functions
==============

Before describing each main E-Maj function, it is interesting to have a global view on the typical operations chain. 

Operations chain
----------------

The possible chaining of operations for a tables group can be materialised by this schema. 

.. image:: images/group_flow.png
   :align: center

Define tables groups
--------------------

.. _emaj_group_def:

The emaj_group_def table
^^^^^^^^^^^^^^^^^^^^^^^^

The content of tables groups E-Maj will manage has to be defined by populating the **emaj.emaj_group_def** table. One row has to be inserted into this table for each application table or sequence to include into a tables group. This  *emaj.emaj_group_def* table has the following structure:

+--------------------------+------+---------------------------------------------------------------------------------------------------+
| Column                   | Type | Description                                                                                       |
+==========================+======+===================================================================================================+
| grpdef_group             | TEXT | tables group name                                                                                 |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_schema            | TEXT | name of the schema containing the application table or sequence                                   |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_tblseq            | TEXT | application table or sequence name                                                                |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_priority          | INT  | priority level for the table or sequence in E-Maj processing (optional)                           |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_log_schema_suffix | TEXT | suffix used to build the name of the schema containing the E-Maj objects for the table(deprecated)|
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_emaj_names_prefix | TEXT | prefix of E-Maj objects names generated for the table (optional)                                  |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_log_dat_tsp       | TEXT | name of the tablespace containing the log table (optional)                                        |
+--------------------------+------+---------------------------------------------------------------------------------------------------+
| grpdef_log_idx_tsp       | TEXT | name of the tablespace containing the index of the log table (optional)                           |
+--------------------------+------+---------------------------------------------------------------------------------------------------+

The administrator can populate this table by any usual mean: *INSERT* SQL verb, *COPY* SQL verb, *\\copy psql* command, graphic tool, etc.

The content of the *emaj_group_def* table is case sensitive. Schema names, table names and sequence names must reflect the way PostgreSQL registers them in its catalogue. These names are mostly in lower case. But if a name is encapsulated by double quotes in SQL statements because it contains any upper case characters or spaces, then it must be registered into the *emaj_group_def* table with the same upper case characters or spaces.

.. caution::

   To guarantee the integrity of tables managed by E-Maj, it is essential to take a particular attention to this tables groups content definition step. If a table were missing, its content would be out of synchronisation with other tables it is related to, after a *rollback* operation. In particular, when application tables are created or suppressed, it is important to always maintain an up-to-date content of this *emaj_group_def* table.

Main columns
^^^^^^^^^^^^

A tables group name (**grpdef_group** column) contains at least 1 character. It may contain spaces and/or any punctuation characters. But it is advisable to avoid commas, single or double quotes.

A table or a sequence of a given schema (**grpdef_schema** and **grpdef_tblseq** columns) cannot be assigned to more than one tables groups. All tables of a schema are not necessarily member of the same group. Some of them can belong to another group. Some others can belong to any group.

All tables assigned to a group not created in *AUDIT_ONLY* mode must have an explicit *primary key* (*PRIMARY KEY* clause in *CREATE TABLE* or *ALTER TABLE*).

E-Maj can process elementary partitions of partitionned tables created with the declarative DDL (with PostgreSQL 10+). They are processed as any other tables. However, as there is no need to protect mother tables, which remain empty, E-Maj refuses to include them in tables groups. All partitions of a partitionned table do not need to belong to a tables group. Partitions of a partitionned table can be assigned to different tables groups.

By their nature, *TEMPORARY TABLE* are not supported by E-Maj. *UNLOGGED* tables and tables created as *WITH OIDS* can only be members of “*audit_only*” tables groups.

If a sequence is associated to an application table, it must be explicitly declared as member of the same group as its table, so that, in case of rollback, the sequence can be reset to its state at the set mark time.

On the contrary, log tables and their sequences should NOT be referenced in a tables group!

Optional columns
^^^^^^^^^^^^^^^^

The type of the **grpdef_priority** column is *INTEGER* and may be *NULL*. It defines a priority order in E-Maj tables processing. This can be useful at table lock time. Indeed, by locking tables in the same order as what is typically done by applications, it may reduce the risk of deadlock. E-Maj functions process tables in *grpdef_priority* ascending order, *NULL* being processed last. For a same priority level, tables are processed in alphabetic order of schema name and table name.

For tables having long names, the default prefix for E-Maj objects names may be too long to fit the PostgreSQL limits. But another prefix may be defined for each table, by setting the **grpdef_emaj_names_prefix** column.

If this *grpdef_emaj_names_prefix* column contains a *NULL* value, the default prefix *<nom_schéma>_<nom_table>* is used.

Two different tables cannot have the same prefix, explicitely or implicitely.

For sequences, the *grpdef_emaj_names_prefix* column must be *NULL*.

To optimize performances of E-Maj installations having a large number of tables, it may be useful to spread log tables and their index on several tablespaces. The **grpdef_log_dat_tsp** column specifies the name of the tablespace to use for the log table of an application table. Similarly, the **grpdef_log_idx_tsp** column specifies the name of the tablespace to use for the index of the log table.

If a column *grpdef_log_dat_tsp* or *grpdef_log_idx_tsp* is *NULL* (default value), the default tablespace of the current session at tables group creation is used.

For sequences, both *grpdef_log_dat_tsp* and *grpdef_log_idx_tsp* columns must be *NULL*.


.. _emaj_create_group:

Create a tables group
---------------------

Once the content of a tables group is defined, E-Maj can create the group. To do this, there is only one SQL statement to execute::

   SELECT emaj.emaj_create_group('<group.name>', <is_rollbackable>);

or in an abbreviated form::

   SELECT emaj.emaj_create_group('<group.name>');

The second parameter, boolean, indicates whether the group is a *ROLLBACKABLE* (with value true) or an *AUDIT_ONLY* (with value false) group. If this second parameter is not supplied, the group is considered *ROLLBACKABLE*.

The function returns the number of tables and sequences contained by the group.

For each table of the group, this function creates the associated log table, the log function and trigger, as well as the trigger that blocks the execution of *TRUNCATE* SQL statements.

The function also creates the log schemas, if needed.

On the contrary, if specific tablespaces are referenced for any log table or log index, these tablespaces must exist before the function's execution.

The *emaj_create_group()* function also checks the existence of application triggers on any tables of the group. If a trigger exists on a table of the group, a message is returned, suggesting the user to verify that this trigger does not update any tables that would not belong to the group. 

If a sequence of the group is associated either to a *SERIAL* or *BIGSERIAL* column or to a column created with a *GENERATED AS IDENTITY* clause, and the table that owns this column does not belong to the same tables group, the function also issues a *WARNING* message.

A specific version of the function allows to create an empty tables group, i.e. without any table or sequence at creation time::

   SELECT emaj.emaj_create_group('<group.name>', <is_rollbackable>, <is_empty>);

The third parameter is *false* by default. If it is set to *true*, the group must not be referenced in the *emaj_group_def* table. Once created, an empty group can be then populated using the :doc:`emaj_alter_group() <alterGroups>` function.

All actions that are chained by the *emaj_create_group()* function are executed on behalf of a unique transaction. As a consequence, if an error occurs during the operation, all tables, functions and triggers already created by the function are cancelled.

By registering the group composition in the *emaj_relation* internal table, the *emaj_create_group()* function freezes its definition for the other E-Maj functions, even if the content of the *emaj_group_def* table is modified later.

A tables group can be altered by the :doc:`emaj_alter_group() <alterGroups>` function or suppressed by the :ref:`emaj_drop_group() <emaj_drop_group>` function.


.. _emaj_start_group:

Start a tables group
--------------------

Starting a tables group consists in activating the recording of updates for all tables of the group. To achieve this, the following command must be executed::

   SELECT emaj.emaj_start_group('<group.name>'[, '<mark.name>'[, <delete.old.logs?>]]);

The group must be first in *IDLE* state.

When a tables group is started, a first mark is created.
 
If specified, the initial mark name may contain a generic '%' character. Then this character is replaced by the current transaction start time, with the pattern "*hh.mn.ss.mmm*",

If the parameter representing the mark is not specified, or is empty or NULL, a name is automatically generated: "*START_%*", where the '%' character represents the current transaction start time with a "*hh.mn.ss.mmm*" pattern.

The *<are.old.logs.to.be.deleted?>* parameter is an optional boolean. By default, its value is true, meaning that all log tables of the tables group are purged before the trigger activation. If the value is explicitly set to false, all rows from log tables are kept as is. The old marks are also preserved, even-though they are not usable for a rollback any more, (unlogged updates may have occurred while the tables group was stopped).

The function returns the number of tables and sequences contained by the group.

To be sure that no transaction implying any table of the group is currently running, the *emaj_start_group()* function explicitly sets a *SHARE ROW EXCLUSIVE* lock on each table of the group. If transactions accessing these tables are running, this can lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts.

The function also performs a purge of the oldest events in the :ref:`emaj_hist <emaj_hist>` technical table.

When a group is started, its state becomes "*LOGGING*".

Using the *emaj_start_groups()* function, several groups can be started at once::

   SELECT emaj.emaj_start_groups('<group.names.array>'[, '<mark.name>'[,<delete.old.logs?>]]);

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.


.. _emaj_set_mark_group:

Set an intermediate mark
------------------------

When all tables and sequences of a group are considered as being in a stable state that can be used for a potential rollback, a mark can be set. This is done with the following SQL statement::

   SELECT emaj.emaj_set_mark_group('<group.name>', '<mark.name>');

The tables group must be in *LOGGING* state.

A mark having the same name can not already exist for this tables group.

The mark name may contain a generic '%' character. Then this character is replaced by the current transaction start time, with the pattern "*hh.mn.ss.mmm*",

If the parameter representing the mark is not specified or is empty or *NULL*, a name is automatically generated: "*MARK_%*", where the '%' character represents the current transaction start time with a “*hh.mn.ss.mmm*” pattern.

The function returns the number of tables and sequences contained in the group.

The *emaj_set_mark_group()* function records the identity of the new mark, with the state of the application sequences belonging to the group, as well as the state of the log sequences associated to each table of the group. The application sequences are processed first, to record their state as earlier as possible after the beginning of the transaction, these sequences not being protected against updates from concurrent transactions by any locking mechanism.

It is possible to set two consecutive marks without any update on any table between these marks.

The *emaj_set_mark_group()* function sets *ROW EXCLUSIVE* locks on each table of the group in order to be sure that no transaction having already performed updates on any table of the group is running. However, this does not guarantee that a transaction having already read one or several tables before the mark set, updates tables after the mark set. In such a case, these updates would be candidate for a potential rollback to this mark.

Using the *emaj_set_mark_groups()* function, a mark can be set on several groups at once::

   SELECT emaj.emaj_set_mark_groups('<group.names.array>', '<mark.name>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.


.. _emaj_rollback_group:

Rollback a tables group
-----------------------

If it is necessary to reset tables and sequences of a group in the state they were when a mark was set, a rollback must be performed. To perform a simple (“*unlogged*”) rollback, the following SQL statement can be executed::

   SELECT * FROM emaj.emaj_rollback_group('<group.name>', '<mark.name>', <is_alter_group_allowed>);

The tables group must be in *LOGGING* state and the supplied mark must be usable for a rollback, i.e. it cannot be logically deleted.

The '*EMAJ_LAST_MARK*' keyword can be used as mark name, meaning the last set mark.

The third parameter is a boolean that indicates whether the rollback operation may target a mark set before an :doc:`alter group <alterGroups>` operation. Depending on their nature, changes performed on tables groups in *LOGGING* state can be automatically cancelled or not. In some cases, this cancellation can be partial. By default, this parameter is set to *FALSE*.

The function returns a set of rows with a severity level set to either “*Notice*” or “*Warning*” values, and a textual message. The function returns a “*Notice*” row indicating the number of tables and sequences that have been effectively modified by the rollback operation. Other messages of type “*Warning*” may also be reported when the rollback operation has processed tables group changes.

To be sure that no concurrent transaction updates any table of the group during the rollback operation, the *emaj_rollback_group()* function explicitly sets an *EXCLUSIVE* lock on each table of the group. If transactions updating these tables are running, this can lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts. But tables of the group remain accessible for read only transactions during the operation.

If tables belonging to the group to rollback have triggers, it may be necessary to de-activate them before the rollback and re-activate them after (more details :ref:`here <application_triggers>`).

If a table impacted by the rollback owns a foreign key or is referenced by a foreign key from another table, then this foreign key is taken into account by the rollback operation. If the check of the keys created or modified by the rollback cannot be deferred at the end of the operation (constraint not declared as *DEFERRABLE*), then this foreign key is dropped at the beginning of the rollback and recreated at the end.

When the volume of updates to cancel is high and the rollback operation is therefore long, it is possible to monitor the operation using the :ref:`emaj_rollback_activity() <emaj_rollback_activity>` function or the :doc:`emajRollbackMonitor.php <rollbackMonitorClient>` client.

When the rollback operation is completed, the following are deleted:

* all log tables rows corresponding to the rolled back updates,
* all marks later than the mark referenced in the rollback operation.

The history of executed rollback operations is maintained into the *emaj_rlbk* table. The final state of the operation is accessible from the *rlbk_status* and *rlbk_msg* columns of this *emaj_rlbk* table.

Then, it is possible to continue updating processes, to set other marks, and if needed, to perform another rollback at any mark.

.. caution::

   By their nature, the reset of sequences is not “cancellable” in case of abort and rollback of the transaction that executes the *emaj_rollback_group()* function. That is the reason why the processing of application sequences is always performed after the processing of application tables. However, even-though the time needed to rollback a sequence is very short, a problem may occur during this last phase. Rerunning immediately the *emaj_rollback_group()* function would not break database integrity. But any other database access before the second execution may lead to wrong values for some sequences.

Using the *emaj_rollback_groups()* function, several groups can be rolled back at once::

   SELECT * FROM emaj.emaj_rollback_groups('<group.names.array>', '<mark.name>', <is_alter_group_allowed>);

The supplied mark must correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_group() <emaj_set_mark_group>` function call.

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

An old version of these functions had only 2 input parameters and just returned an integer representing the number of effectively processed tables and sequences::

   SELECT emaj.emaj_rollback_group('<group.name>', '<mark.name>');

   SELECT emaj.emaj_rollback_groups('<group.names.array>', '<mark.name>');

Both functions are deprecated and are subject to be deleted in a future E-Maj version.


.. _emaj_logged_rollback_group:

Perform a logged rollback of a tables group
-------------------------------------------

Another function executes a “*logged*” rollback. In this case, log triggers on application tables are not disabled during the rollback operation. As a consequence, the updates on application tables are also recorded into log tables, so that it is possible to cancel a rollback. In other words, it is possible to rollback … a rollback.

To execute a “*logged*” rollback, the following SQL statement can be executed::

   SELECT * FROM emaj.emaj_logged_rollback_group('<group.name>', '<mark.name>', <is_alter_group_allowed>);

The usage rules are the same as with *emaj_rollback_group()* function.

The tables group must be in *LOGGING* state and the supplied mark must be usable for a rollback, i.e. it cannot be logically deleted.

The '*EMAJ_LAST_MARK*' keyword can be used as mark name, meaning the last set mark.

The third parameter is a boolean that indicates whether the rollback operation may target a mark set before an :doc:`alter group <alterGroups>` operation. Depending on their nature, changes performed on tables groups in *LOGGING* state can be automatically cancelled or not. In some cases, this cancellation can be partial. By default, this parameter is set to *FALSE*.

The function returns a set of rows with a severity level set to either “*Notice*” or “*Warning*” values, and a textual message. The function returns a “*Notice*” row indicating the number of tables and sequences that have been effectively modified by the rollback operation. Other messages of type “*Warning*” may also be reported when the rollback operation has processed tables group changes.

To be sure that no concurrent transaction updates any table of the group during the rollback operation, the *emaj_rollback_group()* function explicitly sets an *EXCLUSIVE* lock on each table of the group. If transactions updating these tables are running, this can lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts. But tables of the group remain accessible for read only transactions during the operation.

If tables belonging to the group to rollback have triggers, it may be necessary to de-activate them before the rollback and re-activate them after (more details :ref:`here <application_triggers>`).

If a table impacted the rollback owns a foreign key or is referenced by a foreign key from another table, then this foreign key is taken into account by the rollback operation. If the check of the keys created or modified by the rollback cannot be deferred at the end of the operation (constraint not declared as *DEFERRABLE*), then this foreign key is dropped at the beginning of the rollback and recreated at the end.

Unlike with :ref:`emaj_rollback_group() <emaj_rollback_group>` function, at the end of the operation, the log tables content as well as the marks following the rollback mark remain.
At the beginning and at the end of the operation, the function automatically sets on the group two marks named:

* '*RLBK_<rollback.mark>_<rollback.time>_START*'
* '*RLBK_<rollback.mark>_<rollback.time>_DONE*'

where rollback.time represents the start time of the transaction performing the rollback, expressed as “hours.minutes.seconds.milliseconds”.

When the volume of updates to cancel is high and the rollback operation is therefore long, it is possible to monitor the operation using the :ref:`emaj_rollback_activity() <emaj_rollback_activity>` function or the :doc:`emajRollbackMonitor.php <rollbackMonitorClient>` client.

The history of executed rollback operations is maintained into the *emaj_rlbk* table. The final state of the operation is accessible from the *rlbk_status* and *rlbk_msg* columns of this *emaj_rlbk* table.

Following the rollback operation, it is possible to resume updating the database, to set other marks, and if needed to perform another rollback at any mark, including the mark set at the beginning of the rollback, to cancel it, or even delete an old mark that was set after the mark used for the rollback.

Rollback from different types (logged/unlogged) may be executed in sequence. For instance, it is possible to chain the following steps:

* Set Mark M1
* …
* Set Mark M2
* …
* Logged Rollback to M1 (generating RLBK_M1_<time>_STRT, and RLBK_M1_<time>_DONE)
* …
* Rollback to RLBK_M1_<time>_DONE (to cancel the updates performed after the first rollback)
* …
* Rollback to  RLBK_M1_<time>_STRT (to finally cancel the first rollback)

A :ref:`"consolidation" function <emaj_consolidate_rollback_group>` for “logged rollback“ allows to transform a logged rollback into a simple unlogged rollback.

Using the *emaj_rollback_groups()* function, several groups can be rolled back at once::

   SELECT * FROM emaj.emaj_logged_rollback_groups('<group.names.array>', '<mark.name>', <is_alter_group_allowed>);

The supplied mark must correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_group() <emaj_set_mark_group>` function call.

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

An old version of these functions had only 2 input parameters and just returned an integer representing the number of effectively processed tables and sequences::

   SELECT emaj.emaj_logged_rollback_group('<group.name>', '<mark.name>');

   SELECT emaj.emaj_logged_rollback_groups('<group.names.array>', '<mark.name>');

Both functions are deprecated and are subject to be deleted in a future E-Maj version.

.. _emaj_stop_group:

Stop a tables group
-------------------

When one wishes to stop the updates recording for tables of a group, it is possible to deactivate the logging mechanism, using the command::

   SELECT emaj.emaj_stop_group('<group.name>'[, '<mark.name>')];

The function returns the number of tables and sequences contained in the group.

If the mark parameter is not specified or is empty or *NULL*, a mark name is generated: "*STOP_%*" where '%' represents the current transaction start time expressed as “*hh.mn.ss.mmm*”.

Stopping a tables group simply deactivates log triggers of application tables of the group. The setting of *SHARE ROW EXCLUSIVE* locks may lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts.

Additionally, the *emaj_stop_group()* function changes the status of all marks set for the group into a *DELETED* state. Then, it is not possible to execute a rollback command any more, even though no updates have been applied on tables between the execution of both *emaj_stop_group()* and :ref:`emaj_rollback_group() <emaj_rollback_group>` functions.

But the content of log tables and E-Maj technical tables can be examined. 

When a group is stopped, its state becomes "*IDLE*" again.

Executing the *emaj_stop_group()* function for a tables group already stopped does not generate an error. Only a warning message is returned.

Using the *emaj_stop_groups()* function, several groups can be stopped at once::

   SELECT emaj.emaj_stop_groups('<group.names.array>'[, '<mark.name>')];

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.


.. _emaj_drop_group:

Drop a tables group
-------------------

To drop a tables group previously created by the :ref:`emaj_create_group() <emaj_create_group>` function, this group must be already in *IDLE* state. If it is not the case, the :ref:`emaj_stop_group() <emaj_stop_group>` function has to be used first.

Then, just execute the SQL command::

   SELECT emaj.emaj_drop_group('<group.name>');

The function returns the number of tables and sequences contained in the group.

For this tables group, the *emaj_drop_group()* function drops all the objects that have been created by the :ref:`emaj_create_group() <emaj_create_group>` function: log tables, log and rollback functions, log triggers.

The function also drops all log schemas that are now useless.

The locks set by this operation can lead to deadlock. If the deadlock processing impacts the execution of the E-Maj function, the error is trapped and the lock operation is repeated, with a maximum of 5 attempts.

