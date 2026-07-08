Parallel Rollback Client
========================

On servers with multiple processors or processor cores, it may be possible to reduce rollback elapsed time by parallelizing the operation across multiple execution threads. For this purpose, E-Maj provides a specific client to run as a command. It activates E-Maj rollback functions through several parallel connections to the database.

Sessions
--------

To run a rollback in parallel, E-Maj distributes tables and sequences to process for one or more table groups into **sessions**. Each *session* is then processed in its own thread.

However, to guarantee the integrity of the overall operation, the rollback of all sessions is executed within a single transaction.

Tables are assigned to sessions so that the estimated session durations are as balanced as possible.

----

.. _parallel_rollback_prerequisite:

Prerequisites
-------------

The provided tool is coded in *Perl*. It requires that the **Perl software** with the *DBI* and *DBD::Pg* modules be installed on the server that executes the command (which is not necessarily the same as the one hosting the PostgreSQL instance).

Rolling back each session within a single transaction implies the use of two-phase commit. As a consequence, the **max_prepared_transactions** parameter in the *postgresql.conf* file must be adjusted. As the default value of this parameter is 0, it must be modified by specifying a value at least equal to the maximum number of *sessions* that will be used.

It is also necessary to set the E-Maj :doc:`dblink_user_password parameter<parameters>`.

If the extension has been installed by a *non-SUPERUSER* role, the role must have been granted the right to execute the :ref:`dblink_connect_u(text,text)<rollbacks_limits>` function.

----

Syntax
------

The syntax is::

   emajParallelRollback.pl -g <group(s).name> -m <mark> -s <number_of_sessions> [OPTIONS]...

General Options
^^^^^^^^^^^^^^^

* ``-l``: Specifies that the requested rollback is a :ref:`logged rollback<emaj_logged_rollback_group>`.
* ``-a``: Specifies that the requested rollback is :ref:`allowed to reach a mark set before an alter group operation<emaj_rollback_group>`.
* ``-c <comment>``: Sets a comment on the rollback operation.
* ``-v``: Displays more information about the execution of the processing.
* ``--help``: Displays only the command help.
* ``--version``: Displays only the software version.

Connection Options
^^^^^^^^^^^^^^^^^^

* ``-d <database to connect to>``: Database to connect to.
* ``-h <host to connect to>``: Host to connect to.
* ``-p <IP port to connect to>``: IP port to connect to.
* ``-U <connection role to use>``: Connection role to use.
* ``-W <password associated with the role>``: Password associated with the role, if needed.

To replace some or all of these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST*, and/or *PGUSER* environment variables can be used.

To specify a list of table groups in the ``-g`` parameter, separate the names of the groups by a comma.

The supplied connection role must have the :doc:`E-Maj administration rights<accessPolicy>`.

For security reasons, it is not recommended to use the ``-W`` option to supply a password. Instead, it is advisable to use the ``.pgpass`` file (see PostgreSQL documentation).

To allow the rollback operation to work, the table group or groups must be in *LOGGING* state. The supplied mark must also correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_group() <emaj_set_mark_group>` function call.

The ``'EMAJ_LAST_MARK'`` keyword can be used as a mark name, meaning the last set mark.

It is possible to monitor multi-session rollback operations with the same tools as for single-session rollbacks: the :ref:`emaj_rollback_activity()<emaj_rollback_activity>` function, the :doc:`emajRollbackMonitor<rollbackMonitorClient>` command, or the Emaj_web rollback monitor page.

To test the ``emajParallelRollback`` client, the E-Maj extension provides a test script, ``emaj_prepare_parallel_rollback_test.sql``. It prepares an environment with two table groups containing some tables and sequences on which some updates have been performed, with intermediate marks. Once this script has been executed under *psql*, the command displayed at the end of the script can be run.

----

Examples
--------

The command::

   emajParallelRollback.pl -d mydb -g myGroup1 -m Mark1 -s 3

logs into database ``mydb`` and executes a rollback of group ``myGroup1`` to mark ``Mark1``, using 3 parallel sessions.

The command::

   emajParallelRollback.pl -d mydb -g "myGroup1,myGroup2" -m Mark1 -s 3 -l

logs into database ``mydb`` and executes a logged rollback of both groups ``myGroup1`` and ``myGroup2`` to mark ``Mark1``, using 3 parallel sessions.
