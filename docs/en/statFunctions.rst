Count data content changes
==========================

Data stored into E-Maj technical tables or log tables allow to build statistics about recorded changes.

For this purpose, two functions sets are available for users. They produce statistics either at tables group level, or at individual table or sequence level.

These functions can be used by *emaj_adm* and *emaj_viewer* E-Maj roles.

Tables group level statistics
-----------------------------

Six functions return statistics about recorded data content changes for **all tables or sequences** belonging to one or several **tables groups** on a **single marks interval** or since a mark:

* :ref:`emaj_log_stat_group() <emaj_log_stat_group>` and :ref:`emaj_log_stat_groups() <emaj_log_stat_group>` quickly deliver, for each table from one or several tables groups, the number of changes that have been recorded in their related log tables,
* :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` and :ref:`emaj_detailed_log_stat_groups()<emaj_detailed_log_stat_group>` provide more detailed information than *emaj_log_stat_group()*, the number of updates been reported per table, SQL verb type and connection role,
* :ref:`emaj_sequence_stat_group() <emaj_sequence_stat_group>` and :ref:`emaj_sequence_stat_groups() <emaj_sequence_stat_group>` return statistics about how sequences from one or several tables groups change.

.. _emaj_log_stat_group:

Global statistics about log tables contents for one or several tables groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Full global statistics about logs content for a tables group are available with this SQL statement::

   SELECT * FROM emaj.emaj_log_stat_group('<group.name>', '<start.mark>', '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_log_stat_type*, and that contains the following columns:

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
| stat_first_time_id       | BIGINT      | internal time id of the period start                  |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark           | TEXT        | mark name of the period end                           |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | mark timestamp of the period end                      |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_time_id        | BIGINT      | internal time id of the period end                    |
+--------------------------+-------------+-------------------------------------------------------+
| stat_rows                | BIGINT      | number of recorded row changes                        |
+--------------------------+-------------+-------------------------------------------------------+

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

If the marks range is not contained by a single *log session*, i.e. if group stops/restarts occured between these marks, a warning message is raised, indicating that data changes may have been not recorded.

The function returns one row per table, even if there is no logged update for this table. In this case, stat_rows columns value is 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a table has been added or removed from the tables group during the requested time interval.

If a table is removed from its group and later re-assigned to it during the resquested time frame, several rows are returned in the statistics. In this case, *stat_first_time_id* and *stat_last_time_id* columns can used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* not always in ascending order).

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

Detailed statistics about logs for one or several tables groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scanning log tables brings a more detailed information, at a higher response time cost. So can we get fully detailed statistics with the following SQL statement::

   SELECT * FROM emaj.emaj_detailed_log_stat_group('<group.name>', '<start.mark>',
               '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_detailed_log_stat_type*, and that contains the following columns:

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
| stat_first_time_id       | BIGINT      | internal time id of the period start                                                             |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | mark name of the period end                                                                      |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | mark timestamp of the period end                                                                 |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_last_time_id        | BIGINT      | internal time id of the period end                                                               |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_role                | TEXT        | connection role                                                                                  |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_verb                | TEXT        | type of the SQL verb that has performed the update, with values: *INSERT* / *UPDATE* / *DELETE*) |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | number of recorded row changes                                                                   |
+--------------------------+-------------+--------------------------------------------------------------------------------------------------+

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

If the marks range is not contained by a single *log session*, i.e. if group stops/restarts occured between these marks, a warning message is raised, indicating that data changes may have been not recorded.

Unlike :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, the *emaj_detailed_log_stat_group()* function doesn't return any rows for tables having no logged updates inside the requested marks range. So *stat_rows* column never contains 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a table has been added or removed from the tables group during the requested time interval.

If a table is removed from its group and later re-assigned to it during the resquested time frame, several rows are returned in the statistics. In this case, *stat_first_time_id* and *stat_last_time_id* columns can used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* not always in ascending order).

Using the *emaj_detailed_log_stat_groups()* function, detailed log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_detailed_log_stat_groups('<group.names.array>', '<start.mark>',
               '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

.. _emaj_sequence_stat_group:

Statistics about sequence changes for one or several tables groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Global statistics about how sequences of a tables group change are available with this SQL statement::

   SELECT * FROM emaj.emaj_sequence_stat_group('<group.name>', '<start.mark>', '<end.mark>');

The function returns a set of rows, whose type is named *emaj.emaj_sequence_stat_type*, and that contains the following columns:

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
| stat_first_time_id         | BIGINT      | internal time id of the period start                                                 |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark             | TEXT        | mark name of the period end                                                          |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | mark timestamp of the period end                                                     |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_time_id          | BIGINT      | internal time id of the period end                                                   |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_increments            | BIGINT      | number of increments separating both sequence value at the period beginning and end  |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | flag indicating whether any property of this sequence has changed during the period  |
+----------------------------+-------------+--------------------------------------------------------------------------------------+

A *NULL* value supplied as end mark represents the current state.

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function returns one row per sequence, even if no change has been detected during the period.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. But they can contain other values when a sequence has been added or removed from the tables group during the requested time interval.

If a sequence is removed from its group and later re-assigned to it during the resquested time frame, several rows are returned in the statistics. In this case, *stat_first_time_id* and *stat_last_time_id* columns can used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* not always in ascending order).

Sequence statistics are delivered quickly. Needed data are only stored into the small internal table that records the sequences state when marks are set.

But returned values may be approximative. Indeed, there is no way to detect temporary property changes during the period. Similarly, regarding the number of increments, there is no way to detect:

* *setval()* function calls (used by E-Maj rollbacks for instance),
* a return to the sequence minimum value (*MINVALUE*) if the sequence is cyclic (*CYCLE*) and the maximum value (*MAXVALUE*) has been reached,
* an increment change during the period.

For a given sequence, the number of increments is computed as the difference between the *LAST_VALUE* at the period end and the *LAST_VALUE* at the period beginning, divided by the *INCREMENT* value at the period beginning. As a consequence, it is possible to get negative numbers of increments.

Using the *emaj_sequence_stat_groups()* function, log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_sequence_stat_groups('<group.names.array>', '<start.mark>',
               '<end.mark>');

More information about :doc:`multi-groups functions <multiGroupsFunctions>`.

Table or sequence level statistics
----------------------------------

Two functions return statistics about recorded data changes for a **single table or sequence** on **each elementary marks interval** of a given time frame:

• :ref:`emaj_log_stat_table() <emaj_log_stat_table>` quickly returns the number of changes that have been recorded for a table on each elementary marks interval,
• :ref:`emaj_log_stat_sequence() <emaj_log_stat_sequence>` returns the number of increments for a sequence on each elementary marks interval.

.. _emaj_log_stat_table:

Statistics about changes recorded for a table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Statistics about changes recorded for a single table on a given time frame are available through one of these statements::

   SELECT * FROM emaj.emaj_log_stat_table('<schema.name>', '<table.name>' [, '<start.date-time>'
               [, '<end.date-time>']] );

   or

   SELECT * FROM emaj.emaj_log_stat_table('<schema.name>', '<table.name>',
               '<start.tables.group>', '<start.mark>' [, '<end.tables.group>', '<end.mark>'] );

Both functions return a set of rows of type *emaj.emaj_log_stat_table_type* and containing the following columns:

+----------------------------+-------------+-------------------------------------------------------+
| Column                     | Type        | Description                                           |
+============================+=============+=======================================================+
| stat_group                 | TEXT        | tables group name                                     |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_mark            | TEXT        | mark of the time slice lower bound                    |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | timestamp of the time slice lower bound               |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_time_id         | BIGINT      | internal time id of the time slice lower bound        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | indicator of log start for the table                  |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_mark             | TEXT        | mark of the time slice upper bound                    |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | timestamp of the time slice upper bound               |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_time_id          | BIGINT      | internal time id of the time slice upper bound        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | indicator of log stop for the table                   |
+----------------------------+-------------+-------------------------------------------------------+
| stat_changes               | BIGINT      | number of recorded row changes                        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_rollbacks             | INT         | number of E-Maj rollbacks executed on the time slice  |
+----------------------------+-------------+-------------------------------------------------------+

In the first function variant, the observation is framed by two start date-time and end date-time parameters of type *TIMESTAMPTZ*. The first returned interval surrounds the start date-time. The last returned interval surrounds the end date-time.

In the second function variant, the observation is framed by two marks defined by their tables group and mark names. These marks are just points in time: they not necessarily belong to the tables group owning the examined table. If the lower bound mark doesn’t match a known state of the table (i.e. if the start tables group didn’t owned the table at this start mark time), the first returned interval surrounds this first mark. Similarly, If the upper bound mark doesn’t match a known state of the table (i.e. if the end tables group didn’t owned the table at this end mark time), the last returned interval surrounds this end mark.

If parameters defining the observation start are not set or are set to *NULL*, the observation starts at the oldest available data for the table.

If parameters defining the observation end are not set or are set to *NULL*, the observation ends at the table current state.

These functions don’t return any rows for marks intervals when data changes were not recorded for the table. The *stat_is_log_start* and *stat_is_log_stop* columns help to detect gaps in the changes recording.

These statistics are quickly delivered because they do not need to scan log tables.

But returned values may be approximative (in fact over-estimated). This occurs in particular when transactions executed between both requested marks have performed table updates before being cancelled.

.. _emaj_log_stat_sequence:

Statistics about changes recorded for a sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Statistics about changes recorded for a single sequence on a given time frame are available through one of these statements::

   SELECT * FROM emaj.emaj_log_stat_sequence('<schema.name>', '<sequence.name>' [, '<start.date-time>'
               [, '<end.date-time>']] );

   or

   SELECT * FROM emaj.emaj_log_stat_sequence('<schema.name>', '<sequence.name>',
               '<start.tables.group>', '<start.mark>' [, '<end.tables.group>', '<end.mark>'] );

Both functions return a set of rows of type *emaj.emaj_log_stat_sequence_type* and containing the following columns:

+----------------------------+-------------+--------------------------------------------------------+
| Column                     | Type        | Description                                            |
+============================+=============+========================================================+
| stat_group                 | TEXT        | tables group name                                      |
+----------------------------+-------------+--------------------------------------------------------+
| stat_first_mark            | TEXT        | mark of the time slice lower bound                     |
+----------------------------+-------------+--------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | timestamp of the time slice lower bound                |
+----------------------------+-------------+--------------------------------------------------------+
| stat_first_time_id         | BIGINT      | internal time id of the time slice lower bound         |
+----------------------------+-------------+--------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | indicator of log start for the sequence                |
+----------------------------+-------------+--------------------------------------------------------+
| stat_last_mark             | TEXT        | mark of the time slice upper bound                     |
+----------------------------+-------------+--------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | timestamp of the time slice upper bound                |
+----------------------------+-------------+--------------------------------------------------------+
| stat_last_time_id          | BIGINT      | internal time id of the time slice upper bound         |
+----------------------------+-------------+--------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | indicator of log stop for the sequence                 |
+----------------------------+-------------+--------------------------------------------------------+
| stat_increments            | BIGINT      | number of sequence increments                          |
+----------------------------+-------------+--------------------------------------------------------+
| stat_has_structure_changed | BIGINT      | TRUE if any property other than last_value has changed |
+----------------------------+-------------+--------------------------------------------------------+
| stat_rollbacks             | INT         | number of E-Maj rollbacks executed on the time slice   |
+----------------------------+-------------+--------------------------------------------------------+

In the first function variant, the observation is framed by two start date-time and end date-time parameters of type TIMESTAMPTZ. The first returned interval surrounds the start date-time. The last returned interval surrounds the end date-time.

In the second function variant, the observation is framed by two marks defined by their tables group and mark names. These marks are just points in time: they not necessarily belong to the tables group owning the examined sequence. If the lower bound mark doesn’t match a known state of the sequence (i.e. if the start tables group didn’t owned the sequence at this start mark time), the first returned interval surrounds this first mark. Similarly, If the upper bound mark doesn’t match a known state of the sequence (i.e. if the end tables group didn’t owned the sequence at this end mark time), the last returned interval surrounds this end mark.

If parameters defining the observation start are not set or are set to *NULL*, the observation starts at the oldest available data for the sequence.

If parameters defining the observation end are not set or are set to *NULL*, the observation ends at the sequence current state.

These functions don’t return any rows for marks intervals when data changes were not recorded for the sequence. The *stat_is_log_start* and *stat_is_log_stop* columns help to detect recording gaps.
