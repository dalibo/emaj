Count data content changes
==========================

There are six functions that return statistics about recorded data content changes:

* :ref:`emaj_log_stat_group() <emaj_log_stat_group>` and :ref:`emaj_log_stat_groups() <emaj_log_stat_group>` quickly deliver, for each table from one or several tables groups, the number of changes that have been recorded in the related log tables, either between 2 marks or since a given mark,
* :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` and :ref:`emaj_detailed_log_stat_groups()<emaj_detailed_log_stat_group>` provide more detailed information than *emaj_log_stat_group()*, the number of updates been reported per table, SQL verb type and connection role,
* :ref:`emaj_sequence_stat_group() <emaj_sequence_stat_group>` and :ref:`emaj_sequence_stat_groups() <emaj_sequence_stat_group>` return statistics about how sequences from one or several tables groups change between two marks or since a given mark.

These functions can be used by *emaj_adm* and *emaj_viewer* E-Maj roles.

.. _emaj_log_stat_group:

Global statistics about log tables contents
-------------------------------------------

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

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

If the marks range is not contained by a single *log session*, i.e. if group stops/restarts occured between these marks, a warning message is raised, indicating that data changes may have been not recorded.

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

   SELECT * FROM emaj.emaj_log_stat_groups('<group.names.array>', '<start.mark>', '<end.mark>');

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
| stat_role                | TEXT        | connection role                                                                                  |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_verb                | TEXT        | type of the SQL verb that has performed the update, with values: *INSERT* / *UPDATE* / *DELETE*) |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | number of updates recorded into the related log table                                            |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

If the marks range is not contained by a single *log session*, i.e. if group stops/restarts occured between these marks, a warning message is raised, indicating that data changes may have been not recorded.

Unlike :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, the *emaj_detailed_log_stat_group()* function doesn't return any rows for tables having no logged updates inside the requested marks range. So *stat_rows* column never contains 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a table has been added or removed from the tables group during the requested time interval.

Using the *emaj_detailed_log_stat_groups()* function, detailed log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_detailed_log_stat_groups('<group.names.array>', '<start.mark>', '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

.. _emaj_sequence_stat_group:

Statistics about sequence changes
---------------------------------

Global statistics about how sequences change are available with this SQL statement::

   SELECT * FROM emaj.emaj_sequence_stat_group('<group.name>', '<start.mark>', '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_sequence_stat_type*, and contains the following columns:

+----------------------------+-------------+--------------------------------------------------------------------------------------+
| Column                     | Type        | Description                                                                          |
+============================+=============+======================================================================================+
| stat_group                 | TEXT        | tables group name                                                                    |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_schema                | TEXT        | schema name                                                                          |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_sequence              | TEXT        | sequence name                                                                        |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_first_mark            | TEXT        | mark name of the period start                                                        |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | mark timestamp of the period start                                                   |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark             | TEXT        | mark name of the period end                                                          |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | mark timestamp of the period end                                                     |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_increments            | BIGINT      | number of increments separating both sequence value at the period beginning and end  |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | flag indicating whether any property of this sequence has changed during the period  |
+----------------------------+-------------+--------------------------------------------------------------------------------------+

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function returns one row per sequence, even if no change has been detected during the period.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a sequence has been added or removed from the tables group during the requested time interval.

Sequence statistics are delivered quickly. Needed data are only stored into the small internal table that records the sequences state when marks are set.

But returned values may be approximative. Indeed, there is no way to detect temporary property changes during the period. Similarly, regarding the number of increments, there is no way to detect:

* *setval()* function calls (used by E-Maj rollbacks for instance),
* a return to the sequence minimum value (*MINVALUE*) if the sequence is cyclic (*CYCLE*) and the maximum value (*MAXVALUE*) has been reached,
* an increment change during the period.

For a given sequence, the number of increments is computed as the difference between the *LAST_VALUE* at the period end and the *LAST_VALUE* at the period beginning, divided by the *INCREMENT* value at the period beginning. As a consequence, it is possible to get negative numbers of increments.

Using the *emaj_sequence_stat_groups()* function, log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_sequence_stat_groups('<group.names.array>', '<start.mark>', '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.
