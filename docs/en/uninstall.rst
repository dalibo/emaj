Uninstall
=========

Uninstalling an E-Maj extension from a database
***********************************************

To uninstall E-Maj from a database, the user must log on this database with *psql*, as a superuser.

If the drop of the *emaj_adm* and *emaj_viewer* **roles** is desirable, rights on them given to other roles must be previously deleted, using *REVOKE SQL* verbs. ::

   REVOKE emaj_adm FROM <role.or.roles.list>;
   REVOKE emaj_viewer FROM <role.or.roles.list>;

If these *emaj_adm* and *emaj_viewer* roles own access rights on other application objects, these rights must be suppressed too, before starting the uninstall operation.

Allthough E-Maj is usualy installed with a *CREATE EXTENSION* statement, it cannot be uninstalled with a simple *DROP EXTENSION* statement. An event trigger blocks such a statement.

To uninstall E-Maj, just execute the *emaj_uninstall.sql* **delivered script**. ::

   \i <emaj_directory>/emaj_uninstall.sql

This script performs the following steps:

* it executes the cleaning functions created by demo or test scripts, if they exist,
* it stops the tables groups in *LOGGING* state, if any,
* it drops the tables groups, removing in particular the triggers on application tables,
* it drops the extension and the main *emaj* schema,
* it drops roles *emaj_adm* and *emaj_viewer* if they are not linked to any objects in the current database or in other databases of the instance.

The uninstallation script execution displays::

   $ psql ... -f sql/emaj_uninstall.sql 
   >>> Starting E-Maj uninstallation procedure...
   SET
   psql:sql/emaj_uninstall.sql:203: WARNING:  emaj_uninstall: emaj_adm and emaj_viewer roles have been dropped.
   DO
   SET
   >>> E-maj successfully uninstalled from this database

Removing the E-Maj software
***************************

The way to remove the E-Maj software depends on the way it has been installed.

For a standart install with the *pgxn* client, a single command is required::

  pgxn uninstall E-Maj --sudo

For a standart install without the *pgxn* client, reach the initial directory of the E-Maj distribution and type::

  sudo make uninstall

For a manual install, the installed components must be removed by reverting the initial installation steps.
