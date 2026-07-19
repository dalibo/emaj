Rollback Monitoring Client
==========================

E-Maj provides an external client to run as a command that monitors the progress of rollback operations in execution.

Prerequisites
-------------

The provided tool is coded in *Perl*. It requires that the **Perl software** with the *DBI* and *DBD::Pg* modules be installed on the server that executes the command (which is not necessarily the same as the one hosting the PostgreSQL instance).

To obtain detailed information about in-progress rollback operations, it is necessary to set the :doc:`dblink_user_password<parameters>` E-Maj parameter.

If the extension has been installed by a *non-SUPERUSER* role, the role must have been granted the right to execute the :ref:`dblink_connect_u(text,text)<rollbacks_limits>` function.

----

Syntax
------

The syntax is::

   emajRollbackMonitor.pl [OPTIONS]...

General Options
^^^^^^^^^^^^^^^

* ``-i <time interval>``: Time interval between displays (in seconds, default = 5s).
* ``-n <iteration>``: Number of displays (default = 1, 0 for infinite loop).
* ``-a <oldest completed rollback>``: Maximum time interval for rollback operations to display (in hours, default = 24h).
* ``-l <maximum completed rollbacks>``: Maximum number of completed rollback operations to display (default = 3).
* ``--help``: Displays only the command help.
* ``--version``: Displays only the software version.

Connection Options
^^^^^^^^^^^^^^^^^^

* ``-d <database>``: Database to connect to.
* ``-h <host>``: Host to connect to.
* ``-p <IP>``: IP port to connect to.
* ``-U <role>``: Connection role to use.
* ``-W <password>``: Password associated with the role, if needed.

To replace some or all of these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST*, and/or *PGUSER* environment variables can be used.

The supplied connection role must be allowed to :doc:`view E-Maj data<accessPolicy>`.

For security reasons, it is not recommended to use the ``-W`` option to supply a password. Instead, it is advisable to use the *.pgpass* file (see PostgreSQL documentation).

----

Command Examples
----------------

The command::

   emajRollbackMonitor.pl -i 3 -n 10

displays 10 times, every 3 seconds, the list of in-progress rollback operations and the list of at most 3 latest rollback operations completed in the last 24 hours.

The command::

   emajRollbackMonitor.pl -a 12 -l 10

displays only once the list of in-progress rollback operations and the list of at most 10 operations completed in the last 12 hours.

----

Display Example
---------------
::

    E-Maj (version 4.2.0) - Monitoring rollbacks activity
    ---------------------------------------------------------------
    21/03/2023 - 08:31:23
    ** rollback 34 started at 2023-03-21 08:31:16.777887+01 for groups {myGroup1,myGroup2}
       status: COMMITTED ; ended at 2023-03-21 08:31:16.9553+01
    ** rollback 35 started at 2023-03-21 08:31:17.180421+01 for groups {myGroup1}
       status: COMMITTED ; ended at 2023-03-21 08:31:17.480194+01
    -> rollback 36 started at 2023-03-21 08:29:26.003502+01 for groups {group20101}
       status: EXECUTING ; completion 85 %; 00:00:20 remaining
    -> rollback 37 started at 2023-03-21 08:29:16.123386+01 for groups {group20102}
       status: LOCKING ; completion 0 %; 00:22:20 remaining
    -> rollback 38 started at 2023-03-21 08:30:16.130833+01 for groups {group20103}
       status: PLANNING ; completion 0 %
