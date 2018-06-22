Rollback monitoring client
==========================

E-Maj delivers an external client to run as a command that monitors the progress of rollback operations in execution.

Prerequisite
------------

The command to monitor rollback operations is written in *php*. As a consequence, **php** software and its PostgreSQL interface has to be installed on the server that executes the command (which is not necessarily the same as the one that hosts the PostgreSQL instance).

Syntax
------

The command that monitors rollback operations has the following syntax::

   emajRollbackMonitor.php [OPTIONS]... 

The general options are:

* -i <time interval between 2 displays> (in seconds, default = 5s)
* -n <number of displays> (default = 1)
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

   ./client/emajRollbackMonitor.php -i 3 -n 10

displays 10 times and every 3 seconds, the list of in progress rollback operations and the list of the at most 3 latest rollback operations completed in the latest 24 hours.

The command::

   ./client/emajRollbackMonitor.php -a 12 -l 10

displays only once the list of in progress rollback operations and the list of at most 10 operations completed in the latest 12 hours.

Example of display::

    E-Maj (version 1.1.0) - Monitoring rollbacks activity
   ---------------------------------------------------------------
   04/07/2013 - 12:07:17
   ** rollback 34 started at 2013-07-04 12:06:20.350962+02 for groups {myGroup1,myGroup2}
      status: COMMITTED ; ended at 2013-07-04 12:06:21.149111+02 
   ** rollback 35 started at 2013-07-04 12:06:21.474217+02 for groups {myGroup1}
      status: COMMITTED ; ended at 2013-07-04 12:06:21.787615+02 
   -> rollback 36 started at 2013-07-04 12:04:31.769992+02 for groups {group1232}
      status: EXECUTING ; completion 89 % ; 00:00:20 remaining
   -> rollback 37 started at 2013-07-04 12:04:21.894546+02 for groups {group1233}
      status: LOCKING ; completion 0 % ; 00:22:20 remaining
   -> rollback 38 started at 2013-07-04 12:05:21.900311+02 for groups {group1234}
      status: PLANNING ; completion 0 %

