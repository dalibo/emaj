Rollback monitoring client
==========================

E-Maj delivers an external client to run as a command that monitors the progress of rollback operations in execution.

Prerequisite
------------

Two equivalent tools are actually provided, one coded in *php* and the other in *perl*. Both need that some software components be installed on the server that executes the command (which is not necessarily the same as the one that hosts the PostgreSQL instance) :

* for the *php* client, the **php** software and its PostgreSQL interface,
* for the *perl* client, the **perl** software with the *DBI* and *DBD::Pg* modules.

In order to get detailed information about the in-progress rollback operations, it is necessary to set the :doc:`dblink_user_password<parameters>` parameter and give right to execute the *dblink_connect_u* function. :ref:`More details... <emaj_rollback_activity_prerequisites>`

Syntax
------

Both php and perl commands share the same syntax::

   emajRollbackMonitor.php [OPTIONS]...

and::

   emajRollbackMonitor.pl [OPTIONS]...

The general options are:

* -i <time interval between 2 displays> (in seconds, default = 5s)
* -n <number of displays> (default = 1, 0 for infinite loop)
* -a <maximum time interval for rollback operations to display> (in hours, default = 24h)
* -l <maximum number of completed rollback operations to display> (default = 3)
* --help only displays a command help
* --version only displays the software version

The connection options are:

* -d <database to connect to>
* -h <host to connect to>
* -p <ip-port to connect to>
* -U <connection role to use>
* -W <password associated to the role>

To replace some or all these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST* and/or *PGUSER* environment variables can be used.

The supplied connection role must either be a *superuser* or have *emaj_adm* or *emaj_viewer* rights.

For security reasons, it is not recommended to use the -W option to supply a password. Rather, it is advisable to use the *.pgpass* file (see PostgreSQL documentation).

Examples
--------

The command::

   emajRollbackMonitor.php -i 3 -n 10

displays 10 times and every 3 seconds, the list of in progress rollback operations and the list of the at most 3 latest rollback operations completed in the latest 24 hours.

The command::

   emajRollbackMonitor.pl -a 12 -l 10

displays only once the list of in progress rollback operations and the list of at most 10 operations completed in the latest 12 hours.

Example of display::

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
