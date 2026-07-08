Counting Data Content Changes
=============================

Data stored in E-Maj technical tables or log tables can be used to generate statistics about recorded changes.

For this purpose, two sets of functions are available for users. They produce statistics either at the table group level or at the individual table or sequence level.

These functions can be used by ``emaj_adm`` and ``emaj_viewer`` E-Maj roles.

Table Group Level Statistics
----------------------------

Six functions return statistics about recorded data content changes for **all tables or sequences** belonging to one or more **table groups** over a **single marks interval** or since a mark:

* :ref:`emaj_log_stat_group() <emaj_log_stat_group>` and :ref:`emaj_log_stat_groups() <emaj_log_stat_group>` quickly deliver, for each table from one or more table groups, the number of changes recorded in their related log tables.
* :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` and :ref:`emaj_detailed_log_stat_groups()<emaj_detailed_log_stat_group>` provide more detailed information than *emaj_log_stat_group()* and *emaj_log_stat_groups()*. The number of changes is reported per table, SQL verb type, and connection role.
* :ref:`emaj_sequence_stat_group() <emaj_sequence_stat_group>` and :ref:`emaj_sequence_stat_groups() <emaj_sequence_stat_group>` return statistics about how sequences from one or more table groups change.

----

.. _emaj_log_stat_group:

Global Statistics About Log Tables Contents for One or More Table Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Full global statistics about log content for a table group are available with this SQL statement::

   SELECT * FROM emaj.emaj_log_stat_group(p_groupName, p_firstMark, p_lastMark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_firstMark`` (*TEXT*): **Mark name** representing the period lower bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_lastMark`` (*TEXT*): **Mark name** representing the period upper bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark. A *NULL* value represents the current state.

**Returned data**

The function returns a set of rows of type *emaj.emaj_log_stat_type* containing the following columns:

+--------------------------+-------------+-------------------------------------------------------+
| Column                   | Type        | Description                                           |
+==========================+=============+=======================================================+
| stat_group               | TEXT        | Table group name                                      |
+--------------------------+-------------+-------------------------------------------------------+
| stat_schema              | TEXT        | Schema name                                           |
+--------------------------+-------------+-------------------------------------------------------+
| stat_table               | TEXT        | Table name                                            |
+--------------------------+-------------+-------------------------------------------------------+
| stat_first_mark          | TEXT        | Mark name of the period start                         |
+--------------------------+-------------+-------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | Mark timestamp of the period start                    |
+--------------------------+-------------+-------------------------------------------------------+
| stat_first_time_id       | BIGINT      | Internal time ID of the period start                  |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark           | TEXT        | Mark name of the period end                           |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | Mark timestamp of the period end                      |
+--------------------------+-------------+-------------------------------------------------------+
| stat_last_time_id        | BIGINT      | Internal time ID of the period end                    |
+--------------------------+-------------+-------------------------------------------------------+
| stat_rows                | BIGINT      | Number of recorded row changes                        |
+--------------------------+-------------+-------------------------------------------------------+

The function returns one row per table, even if there are no logged changes for this table. In this case, the *stat_rows* column value is 0.

**Notes**

If the marks range is not contained within a single *log session* (i.e., if group stops/restarts occurred between these marks), a warning message is raised, indicating that data changes may not have been recorded.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark*, and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. However, they can contain other values when a table has been added or removed from the table group during the requested mark interval.

If a table is removed from its group and later reassigned to it during the requested time frame, multiple rows are returned in the statistics. In this case, the *stat_first_time_id* and *stat_last_time_id* columns can be used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* values that are not always in ascending order).

It is possible to execute more precise queries on these statistics. For example, to get the number of database updates by application schema, use a statement like:

.. code-block:: sql

   SELECT stat_schema, sum(stat_rows)
   FROM emaj.emaj_log_stat_group('my_group', 'mark_1', NULL)
   GROUP BY stat_schema;

There is no need to scan log tables to get these statistics. For this reason, they are delivered **quickly**.

However, returned values may be **approximate** (in fact, overestimated). This occurs when transactions executed between the requested marks have performed table updates before being canceled.

**Multi-groups operation**

Using the ``emaj_log_stat_groups()`` function, log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_log_stat_groups(p_groupNames, p_firstMark, p_lastMark);

The differences with the *emaj_log_stat_group()* function are:

- The first parameter is a *TEXT array* representing all table groups to process. For more information, see :doc:`multi-groups functions <multiGroupsFunctions>`.

----

.. _emaj_detailed_log_stat_group:

Detailed Statistics About Logs for One or More Table Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scanning log tables provides **more detailed information** at a **higher response time** cost. Full detailed statistics can be obtained with the following SQL statement::

   SELECT * FROM emaj.emaj_detailed_log_stat_group(p_groupName, p_firstMark, p_lastMark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_firstMark`` (*TEXT*): **Mark name** representing the period lower bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_lastMark`` (*TEXT*): **Mark name** representing the period upper bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark. A *NULL* value represents the current state.

**Returned data**

The function returns a set of rows of type *emaj.emaj_detailed_log_stat_type* containing the following columns:

+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| Column                   | Type        | Description                                                                                  |
+==========================+=============+==============================================================================================+
| stat_group               | TEXT        | Table group name                                                                             |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_schema              | TEXT        | Schema name                                                                                  |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_table               | TEXT        | Table name                                                                                   |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_mark          | TEXT        | Mark name of the period start                                                                |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | Mark timestamp of the period start                                                           |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_time_id       | BIGINT      | Internal time ID of the period start                                                         |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | Mark name of the period end                                                                  |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | Mark timestamp of the period end                                                             |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_time_id        | BIGINT      | Internal time ID of the period end                                                           |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_role                | TEXT        | Connection role                                                                              |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_verb                | TEXT        | Type of the SQL verb that performed the change, with values: *INSERT* / *UPDATE* / *DELETE*  |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | Number of recorded row changes                                                               |
+--------------------------+-------------+----------------------------------------------------------------------------------------------+

**Notes**

If the marks range is not contained within a single *log session* (i.e., if group stops/restarts occurred between these marks), a warning message is raised, indicating that data changes may not have been recorded.

Unlike :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, the *emaj_detailed_log_stat_group()`* function does not return any rows for tables with no logged updates within the requested marks range. Therefore, the *stat_rows* column never contains 0.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark*, and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. However, they can contain other values when a table has been added or removed from the table group during the requested time interval.

If a table is removed from its group and later reassigned to it during the requested time frame, multiple rows are returned in the statistics. In this case, the *stat_first_time_id* and *stat_last_time_id* columns can be used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* values that are not always in ascending order).

**Multi-groups operation**

Using the ``emaj_detailed_log_stat_groups()`` function, detailed log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_detailed_log_stat_groups(p_groupNames, p_firstMark, p_lastMark);

The differences with the *emaj_detailed_log_stat_group()* function are:

- The first parameter is a *TEXT array* representing all table groups to process. For more information, see :doc:`multi-groups functions <multiGroupsFunctions>`.

----

.. _emaj_sequence_stat_group:

Statistics About Sequence Changes for One or More Table Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Global statistics about how sequences of a table group change are available with this SQL statement::

   SELECT * FROM emaj.emaj_sequence_stat_group(p_groupName, p_firstMark, p_lastMark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_firstMark`` (*TEXT*): **Mark name** representing the period lower bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_lastMark`` (*TEXT*): **Mark name** representing the period upper bound. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark. A *NULL* value represents the current state.

**Returned data**

The function returns a set of rows of type *emaj.emaj_sequence_stat_type* containing the following columns:

+----------------------------+-------------+--------------------------------------------------------------------------------------+
| Column                     | Type        | Description                                                                          |
+============================+=============+======================================================================================+
| stat_group                 | TEXT        | Table group name                                                                     |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_schema                | TEXT        | Schema name                                                                          |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_sequence              | TEXT        | Sequence name                                                                        |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_first_mark            | TEXT        | Mark name of the period start                                                        |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | Mark timestamp of the period start                                                   |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_first_time_id         | BIGINT      | Internal time ID of the period start                                                 |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark             | TEXT        | Mark name of the period end                                                          |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | Mark timestamp of the period end                                                     |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_last_time_id          | BIGINT      | Internal time ID of the period end                                                   |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_increments            | BIGINT      | Number of increments separating both sequence values at the period start and end     |
+----------------------------+-------------+--------------------------------------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | Flag indicating whether any property of this sequence has changed during the period  |
+----------------------------+-------------+--------------------------------------------------------------------------------------+

**Notes**

The function returns one row per sequence, even if no change has been detected during the period.

Most of the time, the *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark*, and *stat_last_mark_datetime* columns reference the start and end marks of the requested period. However, they can contain other values when a sequence has been added or removed from the table group during the requested time interval.

If a sequence is removed from its group and later reassigned to it during the requested time frame, multiple rows are returned in the statistics. In this case, the *stat_first_time_id* and *stat_last_time_id* columns can be used to reliably sort these multiple time slices (internal server clock fluctuations may produce consecutive *stat_first_datetime* or *stat_last_datetime* values that are not always in ascending order).

Sequence statistics are delivered **quickly**. The required data are only stored in the small internal table that records the sequence states when marks are set.

However, returned values may be **approximate**. There is no way to detect temporary property changes during the period. Similarly, regarding the number of increments, there is no way to detect:

* ``setval()`` function calls (used by E-Maj rollbacks, for instance).
* A return to the sequence minimum value (*MINVALUE*) if the sequence is cyclic (*CYCLE*) and the maximum value (*MAXVALUE*) has been reached.
* An increment change during the period.

For a given sequence, the number of increments is computed as the difference between the *LAST_VALUE* at the period end and the *LAST_VALUE* at the period beginning, divided by the *INCREMENT* value at the period beginning. As a consequence, it is possible to get negative numbers of increments.

**Multi-groups operation**

Using the ``emaj_sequence_stat_groups()`` function, log statistics can be obtained for several groups at once::

   SELECT * FROM emaj.emaj_sequence_stat_groups(p_groupName, p_firstMark, p_lastMark);

The differences with the *emaj_sequence_stat_group()* function are:

- The first parameter is a *TEXT array* representing all table groups to process. For more information, see :doc:`multi-groups functions <multiGroupsFunctions>`.

----

Table or Sequence Level Statistics
----------------------------------

Two functions return statistics about recorded data changes for a **single table or sequence** over **each elementary marks interval** of a given time frame:

* :ref:`emaj_log_stat_table() <emaj_log_stat_table>` quickly returns the number of changes recorded for a table over each elementary marks interval.
* :ref:`emaj_log_stat_sequence() <emaj_log_stat_sequence>` returns the number of increments for a sequence over each elementary marks interval.

.. _emaj_log_stat_table:

Statistics About Changes Recorded for a Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Statistics about changes recorded for a single table over a given time frame are available through one of these statements::

   SELECT * FROM emaj.emaj_log_stat_table(p_schema, p_table, p_startTs, p_endTs);

or::

   SELECT * FROM emaj.emaj_log_stat_table(p_schema, p_table, p_startGroup, p_startMark, p_endGroup, p_endMark);

**Input Parameters**

- ``p_schema`` (*TEXT*): Schema holding the table to examine.
- ``p_table`` (*TEXT*): Name of the table to examine.
- ``p_startTs`` (*TIMESTAMPTZ*, optional, default *NULL*): Timestamp of the **time slice lower bound**. *NULL* means the observation starts at the oldest available data for the table.
- ``p_endTs`` (*TIMESTAMPTZ*, optional, default *NULL*): Timestamp of the **time slice upper bound**. *NULL* means the observation ends at the table current state.
- ``p_startGroup`` (*TEXT*): **Start mark table group**.
- ``p_startMark`` (*TEXT*): **Start mark**. *NULL* means the observation starts at the oldest available data for the table.
- ``p_endGroup`` (*TEXT*, optional, default *NULL*): **End mark table group**.
- ``p_endMark`` (*TEXT*, optional, default *NULL*): **End mark**. *NULL* means the observation ends at the table current state.

**Returned data**

Both functions return a set of rows of type *emaj.emaj_log_stat_table_type* containing the following columns:

+----------------------------+-------------+-------------------------------------------------------+
| Column                     | Type        | Description                                           |
+============================+=============+=======================================================+
| stat_group                 | TEXT        | Table group name                                      |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_mark            | TEXT        | Mark of the time slice lower bound                    |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | Timestamp of the time slice lower bound               |
+----------------------------+-------------+-------------------------------------------------------+
| stat_first_time_id         | BIGINT      | Internal time ID of the time slice lower bound        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | Indicator of log start for the table                  |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_mark             | TEXT        | Mark of the time slice upper bound                    |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | Timestamp of the time slice upper bound               |
+----------------------------+-------------+-------------------------------------------------------+
| stat_last_time_id          | BIGINT      | Internal time ID of the time slice upper bound        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | Indicator of log stop for the table                   |
+----------------------------+-------------+-------------------------------------------------------+
| stat_changes               | BIGINT      | Number of recorded row changes                        |
+----------------------------+-------------+-------------------------------------------------------+
| stat_rollbacks             | INT         | Number of E-Maj rollbacks executed on the time slice  |
+----------------------------+-------------+-------------------------------------------------------+

**Notes**

In the first function variant, the observation is **framed by two start and end date-time** parameters of type *TIMESTAMPTZ*. The first returned interval **surrounds** the start date-time. The last returned interval surrounds the end date-time.

In the second function variant, the observation is framed by **two marks** defined by their table group and mark names. These marks are just points in time: they do not necessarily belong to the table group owning the examined table. If the lower bound mark does not match a known state of the table (i.e., if the start table group did not own the table at this start mark time), the first returned interval surrounds this first mark. Similarly, if the upper bound mark does not match a known state of the table (i.e., if the end table group did not own the table at this end mark time), the last returned interval surrounds this end mark.

These functions do not return any rows for mark intervals when data changes were not recorded for the table. The *stat_is_log_start* and *stat_is_log_stop* columns help detect gaps in the change recording.

These statistics are delivered **quickly** because they do not require scanning log tables.

However, returned values may be **approximate** (in fact, overestimated). This occurs when transactions executed between the requested marks have performed table updates before being canceled.

----

.. _emaj_log_stat_sequence:

Statistics About Changes Recorded for a Sequence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Statistics about changes recorded for a single sequence over a given time frame are available through one of these statements::

   SELECT * FROM emaj.emaj_log_stat_sequence(p_schema, p_sequence, p_startTs, p_endTs);

or::

   SELECT * FROM emaj.emaj_log_stat_sequence(p_schema, p_sequence, p_startGroup, p_startMark, p_endGroup, p_endMark);

**Input Parameters**

- ``p_schema`` (*TEXT*): Schema holding the sequence to examine.
- ``p_sequence`` (*TEXT*): Name of the sequence to examine.
- ``p_startTs`` (*TIMESTAMPTZ*, optional, default *NULL*): Timestamp of the **time slice lower bound**. *NULL* means the observation starts at the oldest available data for the sequence.
- ``p_endTs`` (*TIMESTAMPTZ*, optional, default *NULL*): Timestamp of the **time slice upper bound**. *NULL* means the observation ends at the sequence current state.
- ``p_startGroup`` (*TEXT*): **Start mark table group**.
- ``p_startMark`` (*TEXT*): **Start mark**. *NULL* means the observation starts at the oldest available data for the sequence.
- ``p_endGroup`` (*TEXT*, optional, default *NULL*): **End mark table group**.
- ``p_endMark`` (*TEXT*, optional, default *NULL*): **End mark**. *NULL* means the observation ends at the sequence current state.

**Returned data**

Both functions return a set of rows of type *emaj.emaj_log_stat_sequence_type* containing the following columns:

+----------------------------+-------------+----------------------------------------------------------+
| Column                     | Type        | Description                                              |
+============================+=============+==========================================================+
| stat_group                 | TEXT        | Table group name                                         |
+----------------------------+-------------+----------------------------------------------------------+
| stat_first_mark            | TEXT        | Mark of the time slice lower bound                       |
+----------------------------+-------------+----------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | Timestamp of the time slice lower bound                  |
+----------------------------+-------------+----------------------------------------------------------+
| stat_first_time_id         | BIGINT      | Internal time ID of the time slice lower bound           |
+----------------------------+-------------+----------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | Indicator of log start for the sequence                  |
+----------------------------+-------------+----------------------------------------------------------+
| stat_last_mark             | TEXT        | Mark of the time slice upper bound                       |
+----------------------------+-------------+----------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | Timestamp of the time slice upper bound                  |
+----------------------------+-------------+----------------------------------------------------------+
| stat_last_time_id          | BIGINT      | Internal time ID of the time slice upper bound           |
+----------------------------+-------------+----------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | Indicator of log stop for the sequence                   |
+----------------------------+-------------+----------------------------------------------------------+
| stat_increments            | BIGINT      | Number of sequence increments                            |
+----------------------------+-------------+----------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | *TRUE* if any property other than last_value has changed |
+----------------------------+-------------+----------------------------------------------------------+
| stat_rollbacks             | INT         | Number of E-Maj rollbacks executed on the time slice     |
+----------------------------+-------------+----------------------------------------------------------+

In the first function variant, the observation is framed by **two start and end date-time** parameters of type *TIMESTAMPTZ*. The first returned interval **surrounds** the start date-time. The last returned interval surrounds the end date-time.

In the second function variant, the observation is framed by **two marks** defined by their table group and mark names. These marks are just points in time: they do not necessarily belong to the table group owning the examined sequence. If the lower bound mark does not match a known state of the sequence (i.e., if the start table group did not own the sequence at this start mark time), the first returned interval surrounds this first mark. Similarly, if the upper bound mark does not match a known state of the sequence (i.e., if the end table group did not own the sequence at this end mark time), the last returned interval surrounds this end mark.

These functions do not return any rows for mark intervals when data changes were not recorded for the sequence. The *stat_is_log_start* and *stat_is_log_stop* columns help detect recording gaps.
