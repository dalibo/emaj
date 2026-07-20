Uninstall
=========

Remove E-Maj from a Database
----------------------------

To remove E-Maj from a database, log in as a **SUPERUSER** using *psql*.

If you want to **drop** the *emaj_adm* and *emaj_viewer* **roles**, you must first revoke any rights granted to other roles using the *REVOKE* SQL command::

     REVOKE emaj_adm FROM <role_or_roles_list>;
     REVOKE emaj_viewer FROM <role_or_roles_list>;

If these roles own access rights on other application objects, revoke those rights as well **before** starting the removal process.

Although the *emaj* extension is typically installed using ``CREATE EXTENSION``, it **cannot** be removed with a simple ``DROP EXTENSION`` statement. An event trigger blocks this operation.

Regardless of how the *emaj* extension was installed (using the standard *CREATE EXTENSION* statement or a *psql* script in restricted environments), you can remove it by executing the **emaj_drop_extension()** function::

   SELECT emaj.emaj_drop_extension();

This function performs the following steps:

- Executes cleanup functions created by demo or test scripts (if they exist).
- Stops any table groups in *LOGGING* state.
- Drops all table groups, including triggers on application tables.
- Drops the extension and the main *emaj* schema.
- Drops the *emaj_adm* and *emaj_viewer* roles if they are not linked to any objects in the current database or other databases in the instance.

----

Uninstall the E-Maj Software
----------------------------

The method to uninstall the E-Maj software depends on how it was initially installed.

- **Standard installation with the pgxn client**

  Run the following command::

     pgxn uninstall emaj --sudo

- **Standard installation without the pgxn client**

  Navigate to the initial E-Maj distribution directory and run::

     sudo make uninstall

- **Manual installation**

  Remove the installed components by reversing the initial installation steps.
