Usage limits
============

General limits
--------------

The E-Maj extension usage has some limits:

* The minimum required **PostgreSQL version** is 12.
* All tables belonging to a “*ROLLBACKABLE*” tables group must have an explicit **PRIMARY KEY**. If a table has no explicit *PRIMARY KEY* but has a *UNIQUE* index referencing *NOT NULL* columns, this index should rather be transformed into *PRIMARY KEY*.
* *UNLOGGED* tables can only be members of “*audit_only*” tables groups.
* *TEMPORARY* tables are not supported by E-Maj.
* In some configurations, *FOREIGN KEYs* defined on partitionned tables are not supported by E-Maj rollback operations (:ref:`more details<fk_on_partitionned_tables>`).
* If a **DDL operation** is executed on an application table belonging to a tables group, E-Maj is not able to reset the table in its previous state (:doc:`more details<alterGroups>`).

.. _non_superuser_install_limits:

Limits for installations without SUPERUSER privileges
-----------------------------------------------------

A non SUPERUSER role can :ref:`install the emaj extension with the psql<create_emaj_extension_by_script>` emaj-<version> script. But in this case, there are some limits in the extension use or behaviour. These limits depend on effective rights owned by the installer role either at installation time or at use time.

Tables and sequences ownership
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The main constraint deals with the ownership of tables and sequences assigned to tables groups. **Nothing but tables and sequences owned by the emaj installer role can be assigned to a tables group**.

.. _roles_limits:

emaj_adm and emaj_viewer roles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If at install time the *emaj_adm* role doesn’t exist or the installer role has not the *ADMIN* rights on *emaj_adm*, E-Maj will work without this role and the installer role will be the only E-Maj administrator.

If at install time the *emaj_viewer* role doesn’t exist and the installer role has not the *CREATEROLE* privilege, *emaj_viewer* is not created and E-Maj will work without read-only role.

Before the emaj installation, a role with enough privileges can execute the following statements::

   CREATE ROLE emaj_adm;
   GRANT emaj_adm TO <installer.role> WITH ADMIN TRUE;

and::

   CREATE ROLE emaj_viewer;
   or
   ALTER ROLE <installer.role> CREATEROLE;

.. _event_triggers_limits:

Event triggers
^^^^^^^^^^^^^^

Some :ref:`event triggers<event_triggers>` protect the E-Maj environment. If the role who installs *emaj* has not the needed rights, these event triggers are simply not created, decreasing the extension protection level.

Files import and export
^^^^^^^^^^^^^^^^^^^^^^^

Functions that import tables groups or parameters configurations read data from external files. If the installer role has not the *pg_read_server_files* privileges, the operation is forbidden.

Similarly, functions that export tables groups or parameters configurations, snap tables or logged changes and generate sql scripts write into external files. If the installer role has not the *pg_write_server_files* privileges, the operation is forbidden. Some of these functions also require the *pg_execute_server_program* privilege.

To allow the installer role to read or write external files, a role having enough privileges can execute the following statement::

   GRANT pg_read_server_files, pg_write_server_files, pg_execute_server_program
         TO <installer.role>;

Executing such functions becomes possible as soon as these privileges have been granted, without been obliged to reinstall the extension.

.. _rollbacks_limits:

E-Maj rollbacks management
^^^^^^^^^^^^^^^^^^^^^^^^^^

When the role who installs the extension is not a *SUPERUSER*, :ref:`monitoring E-Maj rollbacks<emaj_rollback_activity>` and :doc:`submitting parallel rollbacks<parallelRollbackClient>` need to give this role (and only him):

* the right to execute the *dblink_connect_u()* function, this right not being given by default for security reasons ;
* the right to read the *unix_socket_directories* postgres parameter when the instance is only accessible through socket (i.e. when the *listen_addresses* parameter is empty).

A role having enough privileges can execute::

   GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO <installer.role>;
   
   GRANT pg_read_all_settings TO <installer.role>;

It is possible to monitor E-Maj rollbacks and submit parallel rollbacks as soon as these rights are granted, without been obliged to reinstall the extension.

Furthermore, in this installation mode without *SUPERUSER* rights, all optimizations regarding E-Maj rollbacks are not available, leading to a decreased performance level of these operations.
