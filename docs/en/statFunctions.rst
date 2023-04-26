Statistics functions
====================

There are two functions that return statistics on log tables content:

* :ref:`emaj_log_stat_group() <emaj_log_stat_group>` quickly delivers, for each table of a group, the number of updates that have been recorded in the related log tables, either between 2 marks or since a particular mark, 
* :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` provides more detailed information than *emaj_log_stat_group()*, the number of updates been reported per table, SQL type (*INSERT/UPDATE/DELETE*) and connection role.

Two other E-Maj functions, :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` and :doc:`emaj_estimate_rollback_groups() <multiGroupsFunctions>` , provide an estimate of how long a rollback for one or several groups to a given mark may last.

These functions can be used by *emaj_adm* and *emaj_viewer* E-Maj roles.

.. _emaj_log_stat_group:

Global statistics about logs
----------------------------

Full global statistics about logs content are available with this SQL statement::

   SELECT * FROM emaj.emaj_log_stat_group('<group.name>', '<start.mark>', '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_log_stat_type*, and contains the following columns:

+--------------------------+-------------+-------------------------------------------------------+
| Column                   | Type        | Description                                           |
+==========================+=============+=======================================================+ 
| stat_group               | TEXT        | tables group name                                     |
+--------------------------+-------------+-------------------------------------------------------+
| stat_schema              | TEXT        | schema name                                           |
+--------------------------+-------------+-------------------------------------------------------+
| stat_table               | TEXT        | table name                                            |
+--------------------------+-------------+-------------------------------------------------------+
| stat_first_mark          | TEXT        | mark name of the period start                         |
+--------------------------+-------------+-------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | mark timestamp of the period start                    |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark           | TEXT        | mark name of the period end                           |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | mark timestamp of the period end                      |
+--------------------------+-------------+-------------------------------------------------------+
| stat_rows                | BIGINT      | number of updates recorded into the related log table |
+--------------------------+-------------+-------------------------------------------------------+

A *NULL* value or an empty string ('') supplied as start mark represents the oldest mark.

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function returns one row per table, even if there is no logged update for this table. In this case, stat_rows columns value is 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a table has been added or removed from the tables group during the requested time interval.

It is possible to easily execute more precise requests on these statistics. For instance, it is possible to get the number of database updates by application schema, with a statement like:

.. code-block:: sql

   postgres=# SELECT stat_schema, sum(stat_rows) 
   FROM emaj.emaj_log_stat_group('myAppl1', NULL, NULL) 
   GROUP BY stat_schema;
    stat_schema | sum 
   -------------+-----
    myschema    |  41
   (1 row)

There is no need for log table scans to get these statistics. For this reason, they are delivered quickly.

But returned values may be approximative (in fact over-estimated). This occurs in particular when transactions executed between both requested marks have performed table updates before being cancelled.

Using the *emaj_log_stat_groups()* function, log statistics can be obtained for several groups at once::

   SELECT emaj.emaj_log_stat_groups('<group.names.array>', '<start.mark>', '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

.. _emaj_detailed_log_stat_group:

Detailed statistics about logs
------------------------------

Scanning log tables brings a more detailed information, at a higher response time cost. So can we get fully detailed statistics with the following SQL statement::

   SELECT * FROM emaj.emaj_detailed_log_stat_group('<group.name>', '<start.mark>', '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_detailed_log_stat_type*, and contains the following columns:

+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| Column                   | Type        | Description                                                                                      |
+==========================+=============+==================================================================================================+
| stat_group               | TEXT        | tables group name                                                                                |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_schema              | TEXT        | schema name                                                                                      |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_table               | TEXT        | table name                                                                                       |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_first_mark          | TEXT        | mark name of the period start                                                                    |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | mark timestamp of the period start                                                               |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | mark name of the period end                                                                      |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | mark timestamp of the period end                                                                 |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_role                | VARCHAR(32) | connection role                                                                                  |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_verb                | VARCHAR(6)  | type of the SQL verb that has performed the update, with values: *INSERT* / *UPDATE* / *DELETE*) |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | number of updates recorded into the related log table                                            |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+

A *NULL* value or an empty string ('') supplied as start mark represents the oldest mark.

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

Unlike :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, the *emaj_detailed_log_stat_group()* function doesn't return any rows for tables having no logged updates inside the requested marks range. So *stat_rows* column never contains 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a table has been added or removed from the tables group during the requested time interval.

Using the *emaj_detailed_log_stat_groups()* function, detailed log statistics can be obtained for several groups at once::

   SELECT emaj.emaj_detailed_log_stat_groups('<group.names.array>', '<start.mark>', '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.
