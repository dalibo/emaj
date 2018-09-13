PostgreSQL version upgrade
==========================

Changing PostgreSQL minor versions
----------------------------------

As changing the minor PostgreSQL version only consists in replacing the binary files of the software, there is no particular constraint regarding E-Maj.

Changing the major PostgreSQL version and the E-Maj version simultaneously
--------------------------------------------------------------------------

A PostgreSQL major version change may be the opportunity to also change the E-Maj version. But in this case, the E-Maj environment has to be recreated from scratch and old objects (tables groups, logs, marks,…) cannot be reused.

Changing the PostgreSQL major version and keeping the existing E-Maj environment
--------------------------------------------------------------------------------

Nevertheless, it is possible to keep the existing E-Maj components (tables groups, logs, marks,…) while changing the PostgreSQL major version. And the tables groups may event stay in logging state during the operation.

But 2 conditions must be met:

* the old and new instances must share the same E-Maj version,
* a post migration script must be executed, before any E-Maj use.

Of course, it is possible to upgrade the E-Maj version before or after the PostgreSQL version change.

If the PostgreSQL version upgrade is performed using a database dump and restore, and if the tables groups may be stopped, a log tables purge, using the :ref:`emaj_reset_group()<emaj_reset_group>` function, may reduce the volume of data to manipulate, thus reducing the time needed for the operation.

Post migration adaptation script
--------------------------------

It may happen that a PostgreSQL major version change has an impact on the E-Maj extension content. Thus, a script is supplied to handle such cases.

After each PostgreSQL major version upgrade, a *psql* script must be executed as superuser::

   \i <emaj_directory>/sql/emaj_upgrade_after_postgres_upgrade.sql

For E-Maj versions 2.0.0 and later, the script only creates the event triggers that may be missing:

* those that appear in version 9.3 and protect against the drop of the extension itself and the drop of E-Maj objects (log tables, functions,...),
* those that appear in version 9.5 and protect against log table structure changes.

The script may be executed several times on the same version, only the first execution modifying the environment.
