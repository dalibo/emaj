Impacts on cluster and database administration
==============================================

Stopping and restarting the cluster
-----------------------------------

Using E-Maj doesn't bring any particular constraint regarding stopping and restarting a PostgreSQL cluster.

General rule
^^^^^^^^^^^^

At cluster restart, all E-Maj objects are in the same state as at cluster stop: log triggers of tables groups in *LOGGING* state remains enabled and log tables contain cancel-able updates already recorded.

If a transaction with table updates were not committed at cluster stop, it would be rolled back during the recovery phase of the cluster start, the application tables updates and the log tables updates being cancelled at the same time. 

This rule also applies of course to transactions that execute E-Maj functions, like a tables group start or stop, a rollback, a mark deletion,...

Sequences rollback
^^^^^^^^^^^^^^^^^^

Due to a PostgreSQL constraint, the rollback of application sequences assigned to a tables group is the only operation that is not protected by transactions. That is the reason why application sequences are processed at the very end of the :ref:`rollback operations <emaj_rollback_group>`. (For the same reason, at set mark time, application sequences are processed at the beginning of the operation.) 

In case of a cluster stop during an E-Maj rollback execution, it is recommended to rerun this rollback just after the cluster restart, to ensure that application sequences and tables remain properly synchronised.

Saving and restoring the database
---------------------------------

.. caution::
   Using E-Maj allows a reduction in the database saves frequency. But E-Maj cannot be considered as a substitute to regular database saves that remain indispensable to keep a full image of databases on an external support.

File level saves and restores
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When saving or restoring clusters at file level, it is essential to save or restore **ALL** cluster files. This includes of course all files from the *tspemaj* tablespace, if it exists.

After a file level restore, tables groups are in the very same state as at the save time, and the database activity can be restarted without any particular E-Maj operation.

Logical saves and restores of entire database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Regarding stopped tables groups (in *IDLE* state), as log triggers are disabled and the content of related log tables is meaningless, there is no action required to find them in the same state as at save time.

Concerning tables groups in *LOGGING* state at save time, it is important to be sure that log triggers will only be activated after the application tables rebuild. Otherwise, during the tables rebuild, tables updates would also be recorded in log tables!

When using *pg_dump* command for saves and *psql* or *pg_restore* commands for restores, and processing full databases (schema + data), these tools recreate triggers, E-Maj log triggers among them, after tables have been rebuilt. So there is no specific precaution to take.

On the other hand, in case of data only save or restore (i.e. without schema, using -a or --data-only options), the --disable-triggers must be supplied:

* with *pg_dump* (or *pg_dumpall*) with save in *plain* format (and *psql* is used to restore),
* with *pg_restore* command with save in *tar* or *custom* format.

Logical save and restore of partial database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With *pg_dump* and *pg_restore* tools, database administrators can perform on a subset of database schemas or tables.

Restoring a subset of application tables and/or log tables generates a heavy risk of data corruption in case of later E-Maj rollback of concerned tables. Indeed, it is impossible to guarantee in this case that application tables, log tables and internal E-Maj tables that contain essential data for rollback, remain coherent. 

If it is necessary to perform partial application tables restores, a drop and recreation of all tables groups concerned by the operation must be performed just after. 

The same way, it is strongly recommended to NOT restore a partial *emaj* schema content.

The only case of safe partial restore concerns a full restore of the *emaj* schema content as well as all tables belonging to all groups that are created in the database.

Data load
---------

Beside using *pg_restore* or *psql* with files produced by *pg_dump*, it is possible to efficiently load large amounts of data with the *COPY* SQL verb or the *\copy* *psql* meta-command. In both cases, this data loading fires *INSERT* triggers, among them the E-Maj log trigger. Therefore, there is no constraint to use *COPY* or *\copy* in E-Maj environment.

With other loading tools, it is important to check that triggers are effectively fired for each row insertion.


Tables reorganisation
---------------------

Reorganisation of application tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Application tables protected by E-Maj can be reorganised using the SQL *CLUSTER* command. Whether or not log triggers are enabled, the organisation process has no impact on log tables content.

Reorganisation of E-Maj tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The index corresponding to the primary key of each table from E-Maj schemas (neither log tables nor technical tables) is declared “*cluster*”.

.. caution::
   So using E-Maj may have an operational impact regarding the execution of *CLUSTER* SQL commands at database level.

When E-Maj is used in continuous mode (with deletion of oldest marks instead of regular tables groups stop and restart), it is recommended to regularly reorganize E-Maj log tables. This reclaims unused disk space following mark deletions.


Using E-Maj with replication
----------------------------

Integrated replication
^^^^^^^^^^^^^^^^^^^^^^

E-Maj is totally compatible with the use of the different PostgreSQL integrated replication modes (*WAL* archiving and *PITR*, asynchronous and synchronous *Streaming Replication*). Indeed, all E-Maj objects hosted in the cluster are replicated like all other objects of the cluster.

However, because of the way PostgreSQL manages sequences, the sequences' current values may be a little forward on slave clusters than on the master cluster. For E-Maj, this may lightly overestimate the number of log rows in general statistics. But there is no consequence on the data integrity.

Other replication solutions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using E-Maj with external replication solutions based on triggers like *Slony* or *Londiste*, requires some attention... It is probably advisable to avoid replicating log tables and E-Maj technical tables.

