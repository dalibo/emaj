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


Upgrade by deletion and re-installation
---------------------------------------

For this upgrade path, it is not necessary to use the full procedure described in :doc:`uninstall`. In particular, the tablespace and both roles can remain as is. However, it may be judicious to save some useful pieces of information. Here is a suggested procedure.

Stop tables groups
^^^^^^^^^^^^^^^^^^

If some tables groups are in *LOGGING* state, they must be stopped, using the :ref:`emaj_stop_group() <emaj_stop_group>` function (or the :ref:`emaj_force_stop_group() <emaj_force_stop_group>` function if :ref:`emaj_stop_group() <emaj_stop_group>` returns an error).

Save user data
^^^^^^^^^^^^^^

It may be useful to save the content of the *emaj_group_def* table in order to be able to easily reload it after the version update, by copying it outside the cluster with a *\copy* command, or by duplicating the table outside the *emaj* schema with a SQL statement like::

   CREATE TABLE public.sav_group_def AS SELECT * FROM emaj.emaj_group_def;

The same way, if the E-Maj administrator had changed parameters value into the *emaj_param* table, it may also be useful to keep a trace of these changes, for instance with::

   CREATE TABLE public.sav_param AS SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';

E-Maj deletion and re-installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once connected as super-user, just chain the execution of the *uninstall.sql* script, of the current version and then the extension creation. ::

   \i <old_emaj_directory>/sql/uninstall.sql

   CREATE EXTENSION emaj;

NB: starting from E-Maj 2.0.0, the uninstall script is named *emaj_uninstall.sql*.

Restore user data
^^^^^^^^^^^^^^^^^
Data previously saved can now be restored into E-Maj both technical tables, for instance with *INSERT … SELECT* statements. ::

   INSERT INTO emaj.emaj_group_def SELECT * FROM public.sav_group_def;

   INSERT INTO emaj.emaj_param SELECT * FROM public.sav_param;

Once data are copied, temporary tables or files can be deleted.

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

* The procedure that upgrades a version 2.0.1 into 2.1.0, may modify the :ref:`emaj_group_def <emaj_group_def>` table in order to reflect the fact that the *tspemaj* tablespace is not automaticaly considered as a default tablespace anymore. If *tspemaj* was effectively used as default tablespace for created tables groups, the related *grpdef_log_dat_tsp* and *grpdef_log_idx_tsp* columns content of the *emaj_group_def* table is automatically adjusted so that a future drop and recreate operation would store the log tables and indexes in the same tablespace. The administrator may review these changes to be sure they correspond to his expectations.

* The  procedure that upgrades a version 2.2.2 into 2.2.3 checks the recorded log sequences values. In some cases, it may ask for a preliminary reset of some tables groups.

* The  procedure that upgrades a version 2.3.1 into 3.0.0 changes the structure of log tables: both *emaj_client_ip* and *emaj_client_port* columns are not created anymore. Existing log tables are not modified. Only the new log tables are impacted. But the administrator can :ref:`add these columns<addLogColumns>`, by using the *'alter_log_tables'* parameter.
