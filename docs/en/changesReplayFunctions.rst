Generating SQL Scripts to Replay Logged Changes
===============================================

.. _emaj_gen_sql_group:

Log tables contain all the information needed to replay changes. Therefore, it is possible to generate SQL statements corresponding to all changes that occurred between two marks or between a mark and the current state. This is the purpose of the ``emaj_gen_sql_group()`` function.

These changes can then be replayed after the corresponding tables have been restored to their state at the initial mark, without needing to re-run application programs.

To generate this SQL script, execute the following statement::

   SELECT emaj.emaj_gen_sql_group(p_groupName, p_firstMark, p_lastMark, p_location, p_tblseqs);


**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_firstMark`` (*TEXT*): **First mark name**. The keyword *EMAJ_LAST_MARK* represents the last set mark.
- ``p_lastMark`` (*TEXT*): **Last mark name**. The keyword *EMAJ_LAST_MARK* represents the last set mark. A *NULL* value or an empty string represents the current state.
- ``p_location`` (*TEXT*): Output **script location**. If *NULL*, SQL statements are generated into a temporary table.
- ``p_tblseqs`` (*TEXT[]*, optional): Array of **tables or sequences** to process. *NULL* means all tables and sequences are processed.

**Returned data**

The function returns the number of generated statements (excluding comments and transaction management statements).

**Notes**

The table group can be in either *IDLE* or *LOGGING* **state**.

If the marks range is not contained within a single *log session* (i.e., if the group was stopped or restarted between these marks), a warning message is raised, indicating that some data changes may not have been recorded.

If provided, the output **script location** (``p_location`` parameter) must be an absolute pathname. The PostgreSQL instance must have write permissions for this file. If the file already exists, its content will be overwritten.

If the ``p_location`` parameter is set to *NULL*, the SQL script is prepared in a **temporary table**, which can then be accessed through the ``emaj_sql_script`` temporary view. Using *psql*, the script can be exported with the following commands::

   SELECT emaj.emaj_gen_sql_group('my_group', 'M1', 'M2', NULL);
   \copy (SELECT * FROM emaj_sql_script) TO 'my_file'

This method allows generating a script in a file located outside the file systems accessible by the PostgreSQL instance.

The ``p_tblseqs`` parameter **selects the tables and sequences** of the requested table group to process. If the parameter is omitted or set to *NULL*, all tables and sequences in the table group are processed. If specified, it must be a non-empty array of text elements, each representing a schema-qualified table or sequence name. Both syntaxes are supported::

   ARRAY['sch1.tbl1','sch1.tbl2']

or::

   '{ "sch1.tbl1", "sch1.tbl2" }'

All tables must have an explicit **PRIMARY KEY** to generate the script.

.. caution::

   If a list of tables and sequences is specified to limit the scope of the *emaj_gen_sql_group()* function, it is the user's responsibility to account for any foreign key constraints to ensure the function produces a viable SQL script.

**Statements** are generated in the order of their original execution.

They are inserted into a **single transaction**, being enclosed between a *BEGIN TRANSACTION;* statement and a *COMMIT;* statement. An initial comment specifies the script generation details, including the generation date and time, the related table group, and the marks used.

At the end of the script, **sequences** belonging to the table group are set to their final state.

The generated file can then be executed as-is by *psql* using a connection role with sufficient privileges on the accessed tables and sequences.

The technics used may result in doubled backslashes in the output file. These doubled characters must be removed before executing the script. For example, in a Unix/Linux environment, use a command like::

   sed 's/\\\\/\\/g' <file_name> | psql ...

As the function can generate a large or very large file (depending on the log volume), users must ensure sufficient disk space is available.

Users are also responsible for deactivating any application triggers before executing the generated script.

**Multi-groups operation**

Using the ``emaj_gen_sql_groups()`` function, it is possible to generate a SQL script for **multiple groups**::

   SELECT emaj.emaj_gen_sql_groups(p_groupName, p_firstMark, p_lastMark, p_location, p_tblseqs);

The differences with the *emaj_gen_sql_group()* function are:

- The first parameter is a *TEXT array* representing all table groups to process. For more information, see :doc:`multi-groups functions <multiGroupsFunctions>`.
