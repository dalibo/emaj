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

.. _emaj_estimate_rollback_group:

Estimate the rollback duration
------------------------------

The *emaj_estimate_rollback_group()* function returns an idea of the time needed to rollback a tables group to a given mark. It can be called with a statement like::

   SELECT emaj.emaj_estimate_rollback_group('<group.name>', '<mark.name>', <is.logged>);

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The third parameter indicates whether the E-Maj rollback to simulate is a *logged rollback* or not.

The function returns an *INTERVAL* value.

The tables group must be in *LOGGING* state and the supplied mark must be usable for a rollback, i.e. it cannot be logically deleted.

This duration estimate is approximative. It takes into account:

* the number of updates in log tables to process, as returned by the :ref:`emaj_log_stat_group() <emaj_log_stat_group>` function,
* recorded duration of already performed rollbacks for the same tables,  
* 6 generic :doc:`parameters <parameters>` that are used as default values when no statistics have been already recorded for the tables to process.

The precision of the result cannot be high. The first reason is that, *INSERT*, *UPDATE* and *DELETE* having not the same cost, the part of each SQL type may vary. The second reason is that the load of the server at rollback time can be very different from one run to another. However, if there is a time constraint, the order of magnitude delivered by the function can be helpful to determine of the rollback operation can be performed in the available time interval.

If no statistics on previous rollbacks are available and if the results quality is poor, it is possible to adjust the generic :doc:`parameters <parameters>`. It is also possible to manually change the *emaj.emaj_rlbk_stat* table's content that keep a trace of the previous rollback durations, for instance by deleting rows corresponding to rollback operations performed in unusual load conditions.

Using the *emaj_estimate_rollback_groups()* function, it is possible to estimate the duration of a rollback operation on several groups::

   SELECT emaj.emaj_estimate_rollback_groups('<group.names.array>', '<mark.name>', <is.logged>);

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

