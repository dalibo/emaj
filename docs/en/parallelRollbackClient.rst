Parallel Rollback client
========================

On servers having several processors or processor cores, it may be possible to reduce rollback elapse time by paralleling the operation on multiple threads of execution. For this purpose, E-Maj delivers a specific client to run as a command. It activates E-Maj rollback functions though several parallel connections to the database.

Sessions
--------

To run a rollback in parallel, E-Maj spreads tables and sequences to process for one or several tables groups into "**sessions**". Each *session* is then processed in its own thread.

However, in order to guarantee the integrity of the global operation, the rollback of all sessions is executed inside a single transaction.

Tables are assigned to sessions so that the estimated session durations be the most balanced as possible.

Prerequisites
-------------

Two equivalent tools are actually provided, one coded in *php* and the other in *perl*. Both need that some software components be installed on the server that executes the command (which is not necessarily the same as the one that hosts the PostgreSQL instance) :

* for the *php* client, the **php** software and its PostgreSQL interface,
* for the *perl* client, the **perl** software with the *DBI* and *DBD::Pg* modules.

Rolling back each session on behalf of a unique transaction implies the use of two phase commit. As a consequence, the **max_prepared_transaction** parameter of the *postgresql.conf* file must be adjusted. As the default value of this parameter equals 0, it must be modified by specifying a value at least equal to the maximum number of *sessions* that will be used.

Syntax
------

Both php and perl commands share the same syntax::

   emajParallelRollback.php -g <group(s).name> -m <mark> -s <number.of.sessions> [OPTIONS]...

and::

   emajParallelRollback.pl -g <group(s).name> -m <mark> -s <number.of.sessions> [OPTIONS]...

The general options are:

* -l specifies that the requested rollback is a :ref:`logged rollback <emaj_logged_rollback_group>`
* -a specifies that the requested rollback is :ref:`allowed to reach a mark set before an alter group operation<emaj_rollback_group>`
* -v displays more information about the execution of the processing
* --help only displays a command help
* --version only displays the software version

And the connection options are:

* -d <database to connect to>
* -h <host to connect to>
* -p <ip-port to connect to>
* -U <connection role to use>
* -W <password associated to the role>, if needed

To replace some or all these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST* and/or *PGUSER* environment variables can be used.

To specify a list of tables groups in the -g parameter, separate the name of each group by a comma.

The supplied connection role must be either a superuser or a role having *emaj_adm* rights.

For safety reasons, it is not recommended to use the -W option to supply a password. It is rather advisable to use the *.pgpass* file (see PostgreSQL documentation).

To allow the rollback operation to work, the tables group or groups must be in *LOGGING* state. The supplied mark must also correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_group() <emaj_set_mark_group>` function call.

The *'EMAJ_LAST_MARK'* keyword can be used as mark name, meaning the last set mark.

It is possible to monitor the multi-session rollback operations with the same tools as for mono-session rollbacks: :ref:`emaj_rollback_activity()<emaj_rollback_activity>` function, the :doc:`emajRollbackMonitor<rollbackMonitorClient>` command or the Emaj_web rollback monitor page. As for mono-session rollbacks, the :doc:`dblink_user_password<parameters>` parameter must be set in order to get detailed status of the operations progress.

In order to test both *emajParallelRollback* commands, the E-Maj extension supplies a test script, *emaj_prepare_parallel_rollback_test.sql*. It prepares an environment with two tables groups containing some tables and sequences, on which some updates have been performed, with intermediate marks. Once this script has been executed under *psql*, the command displayed at the end of the script can be simply run.

Examples
--------

The command::

   ./client/emajParallelRollback.php -d mydb -g myGroup1 -m Mark1 -s 3

logs on database mydb and executes a rollback of group myGroup1 to mark Mark1, using 3 parallel sessions.

The command::

   ./client/emajParallelRollback.pl -d mydb -g "myGroup1,myGroup2" -m Mark1 -s 3 -l

logs on database mydb and executes a logged rollback of both groups myGroup1 and myGroup2 to mark Mark1, using 3 parallel sessions.

