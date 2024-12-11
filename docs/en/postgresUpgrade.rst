Upgrade the PostgreSQL version
==============================

Changing PostgreSQL minor versions
----------------------------------

As changing the minor PostgreSQL version only consists in replacing the binary files of the software, there is no particular constraint regarding E-Maj.

Changing the major PostgreSQL version and the E-Maj version simultaneously
--------------------------------------------------------------------------

A PostgreSQL major version change may be the opportunity to also change the E-Maj version. But in this case, the E-Maj environment has to be recreated from scratch and old objects (tables groups, logs, marks,…) cannot be reused.

Changing the PostgreSQL major version and keeping the existing E-Maj environment
--------------------------------------------------------------------------------

Nevertheless, it is possible to keep the existing E-Maj components (tables groups, logs, marks,…) while changing the PostgreSQL major version. And the tables groups may event stay in logging state during the operation. But one condition must be met: the old and new instances must share the **same E-Maj version**.

Of course, it is possible to upgrade the E-Maj version before or after the PostgreSQL version change.

If the PostgreSQL version upgrade is performed using a database dump and restore, and if the tables groups may be stopped, a log tables purge, using the :ref:`emaj_reset_group()<emaj_reset_group>` function, may reduce the volume of data to manipulate, thus reducing the time needed for the operation.
