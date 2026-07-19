Usage Limits
============

General Limits
--------------

The E-Maj extension has the following usage limits:

* The minimum required **PostgreSQL version** is 14.
* All tables belonging to a *ROLLBACKABLE* table group must have an **explicit PRIMARY KEY**. If a table has no explicit *PRIMARY KEY* but has a *UNIQUE* index referencing *NOT NULL* columns, this index should be transformed into a *PRIMARY KEY*.
* **UNLOGGED** tables can only be members of *AUDIT_ONLY* table groups.
* **TEMPORARY** tables are not supported by E-Maj.
* In some configurations, :ref:`foreign keys defined on partitioned tables<fk_on_partitioned_tables>` are not supported by E-Maj rollback operations.
* If a **DDL operation** is executed on an application table belonging to a table group, E-Maj cannot reset the table to its previous state (:doc:`more details<alterGroups>`).

----

.. _non_superuser_install_limits:

Limits for Installations Without SUPERUSER Privileges
-----------------------------------------------------

A non-SUPERUSER role can :ref:`install the E-Maj extension with the psql script<create_emaj_extension_by_script>` (e.g., *emaj-<version>.sql*). However, in this case, there are some limits on the extension's usage or behavior. These limits depend on the effective rights owned by the installer role, either at installation time or at runtime.

Tables and Sequences Ownership
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The main constraint concerns the ownership of tables and sequences assigned to table groups. Only **tables and sequences owned by the E-Maj installer** role can be assigned to a table group.

.. _roles_limits:

emaj_adm and emaj_viewer Roles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If, at installation time, the *emaj_adm* role does not exist or the installer role does not have *ADMIN* rights on *emaj_adm*, E-Maj will work without this role, and the installer role will be the only E-Maj administrator.

If, at installation time, the *emaj_viewer* role does not exist and the installer role does not have the *CREATEROLE* privilege, *emaj_viewer* is not created, and E-Maj will work without a read-only role.

Before installing *emaj*, a role with sufficient privileges can execute the following statements::

   CREATE ROLE emaj_adm;
   GRANT emaj_adm TO <installer_role> WITH ADMIN TRUE;

and::

   CREATE ROLE emaj_viewer;
   -- or --
   ALTER ROLE <installer_role> CREATEROLE;

.. _event_triggers_limits:

Event Triggers
^^^^^^^^^^^^^^

Some :ref:`event triggers<event_triggers>` protect the E-Maj environment. If the role installing emaj does not have the required rights, these event triggers are not created, reducing the extension's protection level.

Files Import and Export
^^^^^^^^^^^^^^^^^^^^^^^

Functions that import table groups or parameter configurations read data from external files. If the installer role does not have the *pg_read_server_files* privilege, the operation is forbidden.

Similarly, functions that export table groups or parameter configurations, snap tables, or logged changes, and generate SQL scripts, write to external files. If the installer role does not have the *pg_write_server_files* privilege, the operation is forbidden. Some of these functions also require the *pg_execute_server_program* privilege.

To allow the installer role to read or write external files, a role with sufficient privileges can execute the following statement::

   GRANT pg_read_server_files, pg_write_server_files, pg_execute_server_program
         TO <installer_role>;

Executing such functions becomes possible as soon as these privileges are granted, without needing to reinstall the extension.

.. _rollbacks_limits:

E-Maj Rollbacks Management
^^^^^^^^^^^^^^^^^^^^^^^^^^

When the role installing the extension is not a *SUPERUSER*, :ref:`monitoring E-Maj rollbacks<emaj_rollback_activity>` and :doc:`submitting parallel rollbacks<parallelRollbackClient>` require granting the following rights to this role (and only this role):

* The right to execute the *dblink_connect_u()* function (this right is not granted by default for security reasons).
* The right to read the *unix_socket_directories* PostgreSQL parameter when the instance is only accessible through a socket (i.e., when the *listen_addresses parameter* is empty).

A role with sufficient privileges can execute::

   GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO <installer_role>;

   GRANT pg_read_all_settings TO <installer_role>;

It is possible to monitor E-Maj rollbacks and submit parallel rollbacks as soon as these rights are granted, without needing to reinstall the extension.

Furthermore, in this installation mode without *SUPERUSER* rights, all optimizations related to E-Maj rollbacks are not available, leading to decreased performance for these operations.
