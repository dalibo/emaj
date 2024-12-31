Generate SQL scripts to replay logged changes
=============================================

.. _emaj_gen_sql_group:

Log tables contain all needed information to replay changes. Therefore, it is possible to generate SQL statements corresponding to all changes that occurred between two marks or between a mark and the current state. This is the purpose of the *emaj_gen_sql_group()* function.

So these changes can be replayed after the corresponding tables have been restored in their state at the initial mark, without being obliged to rerun application programs.

To generate this SQL script, just execute the following statement::

   SELECT emaj.emaj_gen_sql_group('<group.name>', '<start.mark>', '<end.mark>', '<file>' [, <tables/sequences.array>);

A *NULL* value or an empty string may be used as end mark, representing the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name, representing the last set mark.

If the marks range is not contained by a single *log session*, i.e. if group stops/restarts occured between these marks, a warning message is raised, indicating that data changes may have been not recorded.

If supplied, the output file name must be an absolute pathname. It must have the appropriate permission so that the PostgreSQL instance can write to it. If the file already exists, its content is overwritten.

The output file name may be set to NULL. In this case, the SQL script is prepared in a temporary table that can then be accessed through a temporary view, *emaj_sql_script*. Using *psql*, the script can be exported with both commands::

   SELECT emaj.emaj_gen_sql_group('<group.name>', '<start.mark>', '<end.mark>', NULL [, <tables/sequences.array>);
   \copy (SELECT * FROM emaj_sql_script) TO ‘file’

This method allows to generate a script in a file located outside the file systems accessible by the PostgreSQL instance.

The last parameter of the *emaj_gen_sql_group()* function is optional. It allows filtering of the tables and sequences to process. If the parameter is omitted or has a *NULL* value, all tables and sequences of the tables group are processed. If specified, the parameter must be expressed as a non empty array of text elements, each of them representing a schema qualified table or sequence name. Both syntaxes can be used::

   ARRAY['sch1.tbl1','sch1.tbl2']

or::

   '{ "sch1.tbl1" , "sch1.tbl2" }'

The function returns the number of generated statements (not including comments and transaction management statements).

The tables group may be in *IDLE* or in *LOGGING* state while the function is called.

In order to generate the script, all tables must have an explicit *PRIMARY KEY*.

.. caution::

   If a tables and sequences list is specified to limit the *emaj_gen_sql_group()* function's work, it is the user's responsibility to take into account the possible presence of foreign keys, in order to let the function produce a viable SQL script.

Statements are generated in the order of their initial execution.

The statements are inserted into a single transaction. They are surrounded by a *BEGIN TRANSACTION;* statement and a *COMMIT;* statement. An initial comment specifies the characteristics of the script generation: generation date and time, related tables group and used marks. 

At the end of the script, sequences belonging to the tables group are set to their final state.

Then, the generated file may be executed as is by *psql*, using a connection role that has enough rights on accessed tables and sequences.

The used technology may result to doubled backslashes in the output file. These doubled characters must be suppressed before executing the script, for instance, in Unix/Linux environment, using a command like::

   sed 's/\\\\/\\/g' <file.name> | psql ...

As the function can generate a large, or even very large, file (depending on the log volume), it is the user's responsibility to provide a sufficient disk space.

It is also the user's responsibility to deactivate application triggers, if any exist, before executing the generated script.

Using the *emaj_gen_sql_groups()* function, it is possible to generate a sql script related to several groups::

   SELECT emaj.emaj_gen_sql_groups('<group.names.array>', '<start.mark>', '<end.mark>', '<file>' [, <tables/sequences.array>);

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.
