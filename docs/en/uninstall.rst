Uninstall
=========

Remove an E-Maj extension from a database
*****************************************

To remove E-Maj from a database, the user must log on this database with *psql*, as a superuser.

If the drop of the *emaj_adm* and *emaj_viewer* **roles** is desirable, rights on them given to other roles must be previously deleted, using *REVOKE SQL* verbs. ::

   REVOKE emaj_adm FROM <role.or.roles.list>;
   REVOKE emaj_viewer FROM <role.or.roles.list>;

If these *emaj_adm* and *emaj_viewer* roles own access rights on other application objects, these rights must be suppressed too, before starting the removal operation.

Allthough the *emaj* extension is usualy installed with a *CREATE EXTENSION* statement, it cannot be removed with a simple *DROP EXTENSION* statement. An event trigger blocks such a statement.

Whatever the manner the *emaj* extension has been installed (using the standart *CREATE EXTENSION* statement or a *psql* script when adding an extension is forbidden), its removal just needs to execute the *emaj_drop_extension()* function::

   SELECT emaj.emaj_drop_extension();

This function performs the following steps:

* it executes the cleaning functions created by demo or test scripts, if they exist,
* it stops the tables groups in *LOGGING* state, if any,
* it drops the tables groups, removing in particular the triggers on application tables,
* it drops the extension and the main *emaj* schema,
* it drops roles *emaj_adm* and *emaj_viewer* if they are not linked to any objects in the current database or in other databases of the instance.

In E-Maj versions 4.4.0 and previous, the *emaj* extension removal was done by the execution of a *sql/emaj_uninstall.sql* script. Although deprecated, the removal can always be done the same manner.

Uninstall the E-Maj software
****************************

The way to uninstall the E-Maj software depends on the way it has been installed.

For a standart install with the *pgxn* client, a single command is required::

  pgxn uninstall E-Maj --sudo

For a standart install without the *pgxn* client, reach the initial directory of the E-Maj distribution and type::

  sudo make uninstall

For a manual install, the installed components must be removed by reverting the initial installation steps.
