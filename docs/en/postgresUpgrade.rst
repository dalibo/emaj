Upgrade the PostgreSQL Version
==============================

Changing PostgreSQL Minor Versions
----------------------------------

Changing a PostgreSQL **minor version** involves only replacing the binary files of the software. This process **does not impose any specific constraints** on E-Maj.

----

Changing the PostgreSQL Major Version and the E-Maj Version Simultaneously
--------------------------------------------------------------------------

A PostgreSQL **major version upgrade** can be an opportunity to also upgrade the E-Maj version. However, in this case, the **E-Maj environment must be recreated from scratch**. Thus, existing objects (table groups, logs, marks, etc.) cannot be reused.

----

Changing the PostgreSQL Major Version While Keeping the Existing E-Maj Environment
----------------------------------------------------------------------------------

It is possible to **preserve the existing E-Maj components** (table groups, logs, marks, etc.) while upgrading the PostgreSQL major version. Table groups can even remain in LOGGING state during the operation. However, one condition must be met: the old and new PostgreSQL instances must use **the same E-Maj version**.

You can **upgrade the E-Maj version before or after** the PostgreSQL version change.

If the PostgreSQL upgrade is performed using a database dump and restore, and if the table groups can be stopped, you can reduce the volume of data to be processed by purging the log tables using the :ref:`emaj_reset_group() <emaj_reset_group>` function. This reduces the time required for the operation.
