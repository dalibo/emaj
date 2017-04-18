Data extraction functions
=========================

Three functions extract data from E-Maj infrastructure and store them into external files.

.. _emaj_snap_group:

Snap tables of a group
----------------------

It may be useful to take images of all tables and sequences belonging to a group to be able to analyse their content or compare them. It is possible to dump to files all tables and sequences of a group with::

   SELECT emaj.emaj_snap_group('<group.name>', '<storage.directory>', '<COPY.options>');
 
The directory/folder name must be supplied as an absolute pathname and must have been previously created. This directory/folder must have the appropriate permission so that the PostgreSQL cluster can write in it.

The third parameter defines the output files format. It is a character string that matches the precise syntax available for the *COPY TO* SQL statement. 

The function returns the number of tables and sequences contained by the group.

This *emaj_snap_group()* function generates one file per table and sequence belonging to the supplied tables group. These files are stored in the directory or folder corresponding to the second parameter.

New files will overwrite existing files of the same name.

Created files are named with the following pattern: *<schema.name>_<table/sequence.name>.snap*

Each file corresponding to a sequence has only one row, containing all characteristics of the sequence.

Files corresponding to tables contain one record per row, in the format corresponding to the supplied parameter. These records are sorted in ascending order of the primary key.

At the end of the operation, a file named *_INFO* is created in this same directory/folder. It contains a message including the tables group name and the date and time of the snap operation.

It is not necessary that the tables group be in *IDLE* state to snap tables.

As this function may generate large or very large files (of course depending on tables sizes), it is user's responsibility to provide a sufficient disk space.

Thanks to this function, a simple test of the E-Maj behaviour could chain:

* :ref:`emaj_create_group() <emaj_create_group>`,
* :ref:`emaj_start_group() <emaj_start_group>`,
* emaj_snap_group(<directory_1>),
* updates of application tables,
* :ref:`emaj_rollback_group() <emaj_rollback_group>`,
* emaj_snap_group(<directory_2>),
* comparison of both directories content, using a diff command for instance.

.. _emaj_snap_log_group:

Snap log tables of a group
--------------------------

It is also possible to record a full or a partial image of all log tables related to a group. This provides a way to archive updates performed by one or more previous operations. It is possible to dump on files all tables and sequences of a group with::

   SELECT emaj.emaj_snap_log_group('<group.name>', '<start.mark>', '<end.mark>', '<storage.directory>', '<COPY.options>');

A *NULL* value or an empty string may be used as start mark, representing the first known mark.
A *NULL* value or an empty string may be used as end mark, representing the current situation.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name, representing the last set mark.

The directory/folder name must be supplied as an absolute pathname and must have been previously created. This directory/folder must have the appropriate permission so that the PostgreSQL cluster can write in it.

The fifth parameter defines the output files format. It is a character string that matches the precise syntax available for the *COPY TO* SQL statement.

The function returns the number of tables and sequences contained by the group.

This *emaj_snap_log_group()* function generates one file per log table, containing the part of this table that correspond to the updates performed between both supplied marks. Created files name has the following pattern: *<schema.name>_<table/sequence.name>_log.snap*

The function also generates two files, containing the application sequences state at the time of the respective supplied marks, and named: 
*<group.name>_sequences_at_<mark.name>*

These files are stored in the directory or folder corresponding to the fourth parameter. New files will overwrite existing files of the same name.

At the end of the operation, a file named *_INFO* is created in this same directory/folder. It contains a message including the table's group name, the mark's name that defined the mark range and the date and time of the snap operation.

It is not necessary that the tables group be in *IDLE* state to snap log tables.

As this function may generate large or very large files (of course depending on tables sizes), it is user's responsibility to provide a sufficient disk space.

The structure of log tables is directly derived from the structure of the related application  table. The log tables contain the same columns with the same type. But they also have some additional technical columns:

* emaj_verb : type of the SQL verb that generated the update (*INS*, *UPD*, *DEL*) 
* emaj_tuple : row version (*OLD* for *DEL* and *UPD*, *NEW* for *INS* and *UPD*)
* emaj_gid : log row identifier
* emaj_changed : log row insertion timestamp 
* emaj_txid : transaction id that performed the update
* emaj_user : connection role that performed the update
* emaj_user_ip : ip address of the client that performed the update (if the client was connected with ip protocol)

.. _emaj_gen_sql_group:

SQL script generation to replay logged updates
----------------------------------------------

Log tables contain all needed information to replay updates. Therefore, it is possible to generate SQL statements corresponding to all updates that occurred between two marks or between a mark and the current situation, and record them into a file. This is the purpose of the *emaj_gen_sql_group()* function.

So these updates can be replayed after the corresponding tables have been restored in their state at the initial mark, without being obliged to rerun application programs.

To generate this SQL script, just execute the following statement::

   SELECT emaj.emaj_gen_sql_group('<group.name>', '<start.mark>', '<end.mark>', '<file>' [, <tables/sequences.array>);

A *NULL* value or an empty string may be used as start mark, representing the first known mark.
A *NULL* value or an empty string may be used as end mark, representing the current situation.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name, representing the last set mark.

The output file name must be supplied as an absolute pathname. It must have the appropriate permission so that the PostgreSQL cluster can write to it. If the file already exists, its content is overwritten.

The last parameter is optional. It allows filtering of the tables and sequences to process. If the parameter is omitted or has a *NULL* value, all tables and sequences of the tables group are processed. If specified, the parameter must be expressed as a non empty array of text elements, each of them representing a schema qualified table or sequence name. Both syntaxes can be used::

   ARRAY['sch1.tbl1','sch1.tbl2']

or::

   '{ "sch1.tbl1" , "sch1.tbl2" }'

The function returns the number of generated statements (not including comments and transaction management statements).

The tables group may be in *IDLE* state while the function is called.

In order to generate the script, all tables must have an explicit *PRIMARY KEY*.

.. caution::

   If a tables and sequences list is specified to limit the *emaj_gen_sql_group()* function's work, it is the user's responsibility to take into account the possible presence of foreign keys, in order to let the function produce a viable SQL script.

All statements, *INSERT*, *UPDATE*, *DELETE* and *TRUNCATE* (for *AUDIT_ONLY* tables groups), are generated in the order of their initial execution.

The statements are inserted into a single transaction. They are surrounded by a *BEGIN TRANSACTION;* statement and a *COMMIT;* statement. An initial comment specifies the characteristics of the script generation: generation date and time, related tables group and used marks. 

*TRUNCATE* statements recorded for *AUDIT_ONLY* tables groups are also included into the script.

At the end of the script, sequences belonging to the tables group are set to their final state.

Then, the generated file may be executed as is by psql tool, using a connection role that has enough rights on accessed tables and sequences.

The used technology may result to doubled backslashes in the output file. These doubled characters must be suppressed before executing the script, for instance, in Unix/Linux environment, using a command like::

   sed 's/\\\\/\\/g' <file.name> | psql ...

As the function can generate a large or even very large file (depending on the log volume), it is the user's responsibility to provide a sufficient disk space.

It is also the user's responsibility to deactivate triggers, if any exist, before executing the generated script.

Using the *emaj_gen_sql_groups()* function, it is possible to generate a sql script related to several groups::

   SELECT emaj.emaj_gen_sql_groups('<group.names.array>', '<start.mark>', '<end.mark>', '<file>' [, <tables/sequences.array>);

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

