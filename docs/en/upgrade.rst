Upgrade an existing E-Maj version
=================================

General approach
----------------

The first step consists in :doc:`installing the new version of the E-Maj software <install>`. Keep the old E-Maj version directory at least until the end of the upgrade. Some files may be needed.

It is also necessary to check whether some :ref:`preliminary operations <preliminary_operations>` must be executed.

Then the process to upgrade an E-Maj extension in a database depends on the already installed E-Maj version.

For E-Maj versions prior 0.11.0, there is no particular update procedure. A simple  E-Maj deletion and then re-installation has to be done. This approach can also be used for any E-Maj version, even though it has a drawback: all log contents are deleted, resulting in no further way to rollback or look at the recorded updates.  

For installed E-Maj version 0.11.0 and later, it is possible to perform an upgrade without E-Maj deletion. But the upgrade procedure depends on the installed E-Maj version.

.. caution::

   Starting from version 2.2.0, E-Maj no longer supports PostgreSQL versions prior 9.2. Starting from version 3.0.0, E-Maj no longer supports PostgreSQL versions prior 9.5. If an older PostgreSQL version is used, it must be updated before migrating E-Maj to a higher version.

.. _uninstall_reinstall:

Upgrade by deletion and re-installation
---------------------------------------

For this upgrade path, it is not necessary to use the full procedure described in :doc:`uninstall`. In particular, the tablespace and both roles can remain as is. However, it may be judicious to save some useful pieces of information. Here is a suggested procedure.

Stop tables groups
^^^^^^^^^^^^^^^^^^

If some tables groups are in *LOGGING* state, they must be stopped, using the :ref:`emaj_stop_group() <emaj_stop_group>` function (or the :ref:`emaj_force_stop_group() <emaj_force_stop_group>` function if :ref:`emaj_stop_group() <emaj_stop_group>` returns an error).

Save user data
^^^^^^^^^^^^^^

The procedure depends on the installed E-Maj version.

**Installed version >= 3.3**

The full existing tables groups configuration, as well as E-Maj parameters, can be saved on flat files, using::

   SELECT emaj.emaj_export_groups_configuration('<file.path.1>');

   SELECT emaj.emaj_export_parameters_configuration('<file.path.2>');

**Installed version < 3.3**

If the installed E-Maj version is prior 3.3.0, these export functions are not available.

As starting from E-Maj 4.0 the tables groups configuration doesn’t use the *emaj_group_def* table anymore, rebuilding the tables groups after the E-Maj version upgrade will need either to edit a JSON configuration file to import or to execute a set of tables/sequences assignment functions.

If the emaj_param tables contains specific parameters, it can be saved on file with a *copy* command, or duplicated ouside the *emaj* schema.

If the installed E-Maj version is 3.1.0 or higher, and if the E-Maj administrator has registered application triggers as "not to be automatically disabled at E-Maj rollback time", the *emaj_ignored_app_trigger* table can also be saved::

  CREATE TABLE public.sav_ignored_app_trigger AS SELECT * FROM emaj.emaj_ignored_app_trigger;

  CREATE TABLE public.sav_param AS SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';

E-Maj deletion and re-installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once connected as super-user, just chain the execution of the *uninstall.sql* script, of the current version and then the extension creation. ::

   \i <old_emaj_directory>/sql/emaj_uninstall.sql

   CREATE EXTENSION emaj;

NB: before E-Maj 2.0.0, the uninstall script was named *uninstall.sql*.

Restore user data
^^^^^^^^^^^^^^^^^

**Previous version >= 3.3**

The exported tables groups and parameters configurations can be reloaded with::

   SELECT emaj.emaj_import_parameters_configuration('<file.path.2>', TRUE);

   SELECT emaj.emaj_import_groups_configuration('<file.path.1>');

**Previous version < 3.3**

The saved parameters and application triggers configurations can be reloaded for instance with *INSERT SELECT* statements::

   INSERT INTO emaj.emaj_ignored_app_trigger SELECT * FROM public.sav_ignored_app_trigger;

   INSERT INTO emaj.emaj_param SELECT * FROM public.sav_param;

The tables groups need to be rebuilt using the :doc:`standard methods<groupsCreationFunctions>` of the new version.

Then, temporary tables or files can be deleted.

Upgrade from an E-Maj version between 0.11.0 to 1.3.1
-----------------------------------------------------

For installed version between 0.11.0 and 1.3.1, **psql upgrade scripts** are supplied. They allow to upgrade from one version to the next one.

Each step can be performed without impact on existing tables groups. They may even remain in *LOGGING* state during the upgrade operations. This means in particular that:

* updates on application tables can continue to be recorded during and after this version change,
* a *rollback* on a mark set before the version change can also be performed after the migration.

+---------------+----------------+---------------------------+------------+------------------------+
|Source version | Target version | psql script               | Duration   | Concurrent updates (1) |
+===============+================+===========================+============+========================+
| 0.11.0        | 0.11.1         | emaj-0.11.0-to-0.11.1.sql | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+
| 0.11.1        | 1.0.0          | emaj-0.11.1-to-1.0.0.sql  | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.0.0         | 1.0.1          | emaj-1.0.0-to-1.0.1.sql   | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.0.1         | 1.0.2          | emaj-1.0.1-to-1.0.2.sql   | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.0.2         | 1.1.0          | emaj-1.0.2-to-1.1.0.sql   | Variable   | No (2)                 |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.1.0         | 1.2.0          | emaj-1.1.0-to-1.2.0.sql   | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.2.0         | 1.3.0          | emaj-1.2.0-to-1.3.0.sql   | Quick      | Yes (3)                |
+---------------+----------------+---------------------------+------------+------------------------+
| 1.3.0         | 1.3.1          | emaj-1.3.0-to-1.3.1.sql   | Very quick | Yes                    |
+---------------+----------------+---------------------------+------------+------------------------+

(1) The last column indicates whether the E-Maj upgrade can be executed while application tables handled by E-Maj are accessed in update mode. Note that any other E-Maj operation executed during the upgrade operation would wait until the end of the upgrade.

(2) When upgrading into 1.1.0, log tables structure changes. As a consequence:

* eventhough tables groups may remain in *LOGGING* state, the upgrade can only be executed during a time period when application tables are not updated by any application processing,
* the operation duration will mostly depends on the volume of data stored into the log tables.

Note also that E-Maj statistics collected during previous rollback operations are not kept (due to large differences in the way rollbacks are performed, the old statistics are not pertinent any more).

(3) It is advisable to perform the upgrade into 1.3.0 in a period of low database activity. This is due to *Access Exclusive* locks that are set on application tables while the E-Maj triggers are renamed.

At the end of each upgrade step, the script displays the following message:

>>> E-Maj successfully migrated to <new_version>


E-Maj upgrade from 1.3.1 to a higher version
--------------------------------------------

The upgrade from the 1.3.1 version is specific as it must handle the installation mode change, moving from a *psql* script to an *extension*.

Concretely, the operation is performed with a single SQL statement::

   CREATE EXTENSION emaj FROM unpackaged;

The PostgreSQL extension manager determines the scripts to execute depending on the E-Maj version identifier found in the *emaj.control* file.

But this upgrade is not able to process cases when at least one tables group has been created with a PostgreSQL version prior 8.4. In such a case, these old tables groups must be dropped before the upgrade and recreated after.

This upgrade is also not possible with PostgreSQL version 13 and higher. For these PostgreSQL versions, E-Maj must be uninstalled and re-installed in its latest version.

.. _extension_upgrade:

Upgrade an E-Maj version already installed as an extension
----------------------------------------------------------

An existing version already installed as an extension can be upgraded using the SQL statement::

   ALTER EXTENSION emaj UPDATE;

The PostgreSQL extension manager determines the scripts to execute depending on the current installed E-Maj version and the version found in the *emaj.control* file.

The operation is very quick et does not alter tables groups. They may remain in *LOGGING* state during the upgrade. As for previous upgrades, this means that:

* updates on application tables can continue to be recorded during and after this version change,
* a *rollback* on a mark set before the version change can also be performed after the migration.

Version specific details:

* The  procedure that upgrades a version 2.2.2 into 2.2.3 checks the recorded log sequences values. In some cases, it may ask for a preliminary reset of some tables groups.

* The  procedure that upgrades a version 2.3.1 into 3.0.0 changes the structure of log tables: both *emaj_client_ip* and *emaj_client_port* columns are not created anymore. Existing log tables are not modified. Only the new log tables are impacted. But the administrator can :ref:`add these columns<addLogColumns>`, by using the *'alter_log_tables'* parameter.

* The procedure that upgrades a version 3.0.0 into 3.1.0 renames existing log objects. This leads to locking the application tables, which may generate conflicts with the parallel use of these tables. This procedure also issues a warning message indicating that the changes in E-Maj rollback functions regarding the application triggers processing may require changes in user’s procedures.

* The procedure that upgrades a version 3.4.0 into 4.0.0 updates the log tables content for TRUNCATE recorded statements. The upgrade duration depends on the global log tables size.
