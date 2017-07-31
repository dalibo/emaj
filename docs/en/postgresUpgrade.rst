PostgreSQL version upgrade
==========================

It may happen that a PostgreSQL version change has an impact on the E-Maj extension content. But the main principles apply:

* it is possible to upgrade PostgreSQL version, without reinstalling E-Maj,
* tables groups may even remain in *LOGGING* state at PostgreSQL upgrade,
* if the E-Maj extension content needs to be adapted, this must be performed using a script.

So a supplied *psql* script must be executed after each PostgreSQL version upgrade in order to process the potential impact. It must be executed as superuser::

   \i <emaj_directory>/sql/emaj_upgrade_after_postgres_upgrade.sql

For E-Maj versions 2.0.0 and later, the script only creates the event triggers that may be missing:

* those that appear in version 9.3 and protect against the drop of the extension itself and the drop of E-Maj objects (log tables, functions,...),
* those that appear in version 9.5 and protect against log table structure changes.

The script may be executed several times on the same version, only the first execution modifying the environment.

If the PostgreSQL version upgrade is performed using a database dump and restore, and if the tables groups may be stopped, the execution of an :ref:`emaj_reset_group() <emaj_reset_group>` function may reduce the volume of data to manipulate, thus reducing the time needed for the operation.

