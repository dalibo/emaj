Upgrade an Existing E-Maj Version
=================================

General Approach
----------------

The first step is to :doc:`install the new version of the E-Maj software <install>`.

You should also check whether any :ref:`preliminary operations <preliminary_operations>` need to be performed.

The upgrade process for an E-Maj extension in a database depends on the currently installed E-Maj version and the method used for the initial installation.

Any E-Maj environment can be upgraded using a simple :ref:`uninstall and re-install <uninstall_reinstall>` process.

For E-Maj versions **installed as an EXTENSION** with a version ID greater than or equal to 2.3.1, you can :ref:`upgrade the version <extension_upgrade>` without uninstalling the existing version. This approach **preserves all logs**, allowing you to examine prior changes and perform E-Maj rollbacks targeting marks set before the upgrade.

For E-Maj versions installed via the **psql script** (not as an *EXTENSION*), no specific upgrade procedure exists. In such cases, you must **delete and reinstall** E-Maj.

.. caution::
   Verify the :doc:`PostgreSQL and E-Maj versions compatibility matrix <versionsMatrix>` to ensure the upgrade is supported. If your PostgreSQL version is too old, **upgrade PostgreSQL first** before migrating E-Maj to a newer version.

----

.. _uninstall_reinstall:

Upgrade by Deletion and Reinstallation
---------------------------------------

For this upgrade method, you do not need to follow the full :doc:`uninstall <uninstall>` procedure. The tablespace and roles can remain unchanged. However, it is advisable to back up useful information. Below is a recommended procedure.

Stop Tables Groups
^^^^^^^^^^^^^^^^^^

If any table groups are in *LOGGING* state, stop them using the :ref:`emaj_stop_group() <emaj_stop_group>` function (or the :ref:`emaj_force_stop_group() <emaj_force_stop_group>` function if emaj_stop_group() returns an error).

Save User Data
^^^^^^^^^^^^^^

The procedure depends on the installed E-Maj version.

**Installed Version ≥ 3.3**

You can export the full table groups configuration and E-Maj parameters to flat files::

   SELECT emaj.emaj_export_groups_configuration('<file_path_1>');
   SELECT emaj.emaj_export_parameters_configuration('<file_path_2>');

**Installed Version < 3.3**

If your E-Maj version is prior to 3.3.0, the export functions are unavailable.

To rebuild **table groups** after upgrading, you must either:

- Edit a JSON configuration file for import, **or**
- Execute a set of table/sequence assignment functions.

If the *emaj_param* table contains custom **parameters**, back it up outside the *emaj* schema::

   CREATE TABLE public.sav_param AS
       SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';

If the installed E-Maj version is 3.1.0 or higher and **application triggers** were registered as "not to be automatically disabled at E-Maj rollback time," back up the *emaj_ignored_app_trigger* table::

   CREATE TABLE public.sav_ignored_app_trigger AS
       SELECT * FROM emaj.emaj_ignored_app_trigger;

E-Maj Deletion and Reinstallation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a superuser, execute the following commands in sequence::

   \i <old_emaj_directory>/sql/emaj_uninstall.sql
   CREATE EXTENSION emaj CASCADE;

Restore User Data
^^^^^^^^^^^^^^^^^

**Previous Version ≥ 3.3**
Reload the exported table groups and parameters configurations::

   SELECT emaj.emaj_import_parameters_configuration('<file_path_2>', TRUE);
   SELECT emaj.emaj_import_groups_configuration('<file_path_1>');

**Previous Version < 3.3**
Reload the saved parameters and application triggers configurations::

   SELECT emaj.emaj_set_param(
            param_key,
            COALESCE(
                param_value_text,
                param_value_numeric::TEXT,
                param_value_boolean::TEXT,
                param_value_interval::TEXT
            ))
     FROM public.sav_param;

   INSERT INTO emaj.emaj_ignored_app_trigger
       SELECT * FROM public.sav_ignored_app_trigger;

Rebuild the table groups using the :doc:`standard methods <groupsCreationFunctions>` of the new version.

Finally, delete temporary tables or files.

----

.. _extension_upgrade:

Upgrade an E-Maj Version Installed as an EXTENSION
--------------------------------------------------

To upgrade an existing E-Maj version installed as an *EXTENSION*, use the following SQL command::

   ALTER EXTENSION emaj UPDATE;

PostgreSQL’s extension manager automatically determines the scripts to execute based on the currently installed E-Maj version and the version specified in the *emaj.control* file.

This operation is **fast** and **does not disrupt table groups**. They can remain in the *LOGGING* state during the upgrade. This means:

- Changes on application tables continue to be recorded during and after the version change.
- E-Maj Rollbacks to marks set before the version change can still be performed after the upgrade.

Version-Specific Details
^^^^^^^^^^^^^^^^^^^^^^^^

- **Upgrading from 2.3.1 to 3.0.0**:

  The **log tables structure** changes: the *emaj_client_ip* and *emaj_client_port* columns are no longer created. Existing log tables remain unchanged; only new log tables are affected. Administrators can :ref:`add these columns <addLogColumns>` using the ``alter_log_tables`` parameter.

- **Upgrading from 3.0.0 to 3.1.0**:

  Existing **log objects are renamed**, which locks application tables and may cause conflicts with concurrent operations. A warning message is displayed, indicating that changes to E-Maj rollback functions regarding application triggers may require updates to user procedures.

- **Upgrading from 3.4.0 to 4.0.0**:

  The log tables content for **TRUNCATE statements** is updated. The upgrade duration depends on the total size of the log tables.

- **Upgrading from 4.1.0 to 4.2.0**:

  The upgrade checks for missing **event triggers**. If any are missing, the E-Maj environment must be recreated. Alternatively, you can execute the *sql/emaj_upgrade_after_postgres_upgrade.sql* script (provided with E-Maj 4.1.0) to create the missing triggers.

- **Upgrading from 4.3.1 to 4.4.0**:

  The *emaj_hist* table content is read to populate three new internal history tables. Although brief, the upgrade duration depends on the *emaj_hist* table size.

----

Compatibility Breaks
--------------------

As a general rule, upgrading E-Maj does **not** change how the extension is used. There is **ascending compatibility** between versions. Exceptions to this rule are detailed below.

Upgrading to Version 4.0.0
^^^^^^^^^^^^^^^^^^^^^^^^^^

The compatibility breaks in E-Maj 4.0.0 primarily concern **table groups configuration management**. Version 3.2.0 introduced dynamic management of table and sequence assignments to table groups and version 3.3.0 allowed describing table groups configurations using JSON structures. Starting with E-Maj 4.0.0, the old method of managing table groups configurations via the **emaj_group_def** table is **removed**.

Concretely:

1. The *emaj_group_def* table no longer exists.
2. The :ref:`emaj_create_group() <emaj_create_group>` function now only creates empty table groups, which must then be populated using either functions from the :ref:`emaj_assign_table() / emaj_assign_sequence() <assign_table_sequence>` family, or the :ref:`emaj_import_groups_configuration() <import_groups_conf>` function. Thus, the third parameter of the :ref:`emaj_create_group() <emaj_create_group>` function (previously used to create empty table groups) is removed.
3. The now useless *emaj_alter_group()*, *emaj_alter_groups()* and *emaj_sync_def_group()* functions are removed too.

Additionally:

4. The *emaj_ignore_app_trigger()* function is deleted. Triggers to ignore during E-Maj rollback can now be registered using functions from the :ref:`emaj_assign_table() <assign_table>` family.
5. In JSON structures managed by :ref:`emaj_export_groups_configuration() <export_groups_conf>` and :ref:`emaj_import_groups_configuration() <import_groups_conf>`, the ``ignored_triggers`` property format is simplified to a text array.
6. The old E-Maj rollback functions that returned only an integer are deleted. Only functions returning a set of messages remain.
7. Function parameter names have changed: the "*v_*" prefix is replaced with "*p_*". This only affects function calls using named parameters, which is uncommon.

Upgrading to Version 4.3.0
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Prior to E-Maj 4.3.0, the *emaj_log_stat_group()*, *emaj_gen_sql_group()*, and *emaj_snap_log_group()* function families accepted *NULL* or an empty string as the first mark name of the requested time range, representing the oldest known mark for the table group(s). This feature was ambiguous, especially for multi-group functions, and has been removed in 4.3.0.

2. The *emaj_snap_log_group()* function is replaced by:

   - :ref:`emaj_dump_changes_group() <emaj_dump_changes_group>`
   - :ref:`emaj_gen_sql_dump_changes_group() <emaj_gen_sql_dump_changes_group>`

   These new functions offer enhanced features. To migrate from *emaj_snap_log_group()*, update your statement as follows:

   **Before (E-Maj < 4.3.0):**::

     SELECT emaj.emaj_snap_log_group(<group>, <start_mark>, <end_mark>, <directory>, <copy_options>);

   **After (E-Maj ≥ 4.3.0):**::

     SELECT emaj.emaj_dump_changes_group(<group>, <start_mark>, <end_mark>, 'COPY_OPTIONS=(<copy_options>)', NULL, <directory>);

   Note that:

   - None of the start and end marks can now be *NULL*.
   - Data format about sequences has changed: while two files grouped the initial and final sequences states respectively, there is now one file per sequence with the same elementary information.
