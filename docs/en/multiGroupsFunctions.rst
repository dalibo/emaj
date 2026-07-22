Multi-Groups Functions
======================

General Information
-------------------

To synchronize current operations such as group start or stop, mark setting, or rollback, the usual functions dedicated to these tasks have twin functions that process multiple table groups in a single call.

The resulting advantages are:

* The ability to easily process all table groups in a single transaction.
* Locking tables belonging to all groups at the beginning of the operation to minimize the risk of deadlocks.

----

.. _multi_groups_functions_list:

Functions List
--------------

The following table lists the multi-group functions along with their corresponding single-group functions. Some of these are discussed later.

+------------------------------------------+---------------------------------------------------------------------------+
| Multi-Group Function                     | Corresponding Single-Group Function                                       |
+==========================================+===========================================================================+
| **emaj.emaj_start_groups()**             | :ref:`emaj.emaj_start_group() <emaj_start_group>`                         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_stop_groups()**              | :ref:`emaj.emaj_stop_group() <emaj_stop_group>`                           |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_set_mark_groups()**          | :ref:`emaj.emaj_set_mark_group() <emaj_set_mark_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_rollback_groups()**          | :ref:`emaj.emaj_rollback_group() <emaj_rollback_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_logged_rollback_groups()**   | :ref:`emaj.emaj_logged_rollback_group() <emaj_logged_rollback_group>`     |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_estimate_rollback_groups()** | :ref:`emaj.emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_log_stat_groups()**          | :ref:`emaj.emaj_log_stat_group() <emaj_log_stat_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_detailed_log_stat_groups()** | :ref:`emaj.emaj_detailed_log_stat_group() <emaj_detailed_log_stat_group>` |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_sequence_stat_groups()**     | :ref:`emaj.emaj_sequence_stat_group() <emaj_sequence_stat_group>`         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_gen_sql_groups()**           | :ref:`emaj.emaj_gen_sql_group() <emaj_gen_sql_group>`                     |
+------------------------------------------+---------------------------------------------------------------------------+

The parameters of multi-group functions are the same as those of their corresponding single-group functions, except for the first parameter. The *TEXT* table group parameter is replaced by a *TEXT ARRAY* parameter representing a list of table groups.

----

.. _multi_groups_syntax:

Syntax for Groups Array
-----------------------

The ``p_groups`` parameter passed to the multi-group functions is of SQL type *TEXT[]*, i.e., an array of text data.

According to the SQL standard, there are two possible syntaxes to specify a groups array: using braces ``{ }`` or the *ARRAY* function.

When using braces ``{ }``, the full list is written between single quotes, and braces frame the comma-separated list of elements. Each element is placed between double quotes. For example, in this case, you can write::

   '{ "group 1", "group 2", "group 3" }'

The SQL *ARRAY* function builds an array of data. The list of values is placed between brackets ``[ ]``, and values are separated by commas. For example, in this case, you can write::

   ARRAY['group 1', 'group 2', 'group 3']

Both syntaxes are equivalent.

----

Other Considerations
--------------------

A table groups list may contain **duplicate values**, *NULL* values, or empty strings. These *NULL* values or empty strings are simply ignored. If a table group name is listed multiple times, only one occurrence is retained.

The **order** of the groups in the groups list is not meaningful. During the E-Maj operation, the processing order of tables depends only on the priority level defined for each table and, for tables with the same priority level, on the alphabetical order of their schema and table names.

The format and usage of these functions are strictly equivalent to those of their corresponding single-group functions.

However, an additional condition exists for rollback functions: the supplied mark must correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_groups() <emaj_set_mark_group>` function call.

----

.. _groups_array_building_functions:

Functions to Ease Table Groups Array Building
----------------------------------------------

Three functions help build table groups arrays and simplify :ref:`writing idempotent administration scripts<idempotent_groups_state>`.

The ``emaj_get_groups()`` function returns the array of existing table groups::

   SELECT emaj.emaj_get_groups(p_includeFilter, p_excludeFilter);

The ``emaj_get_logging_groups()`` function returns the array of table groups in *LOGGING* state::

   SELECT emaj.emaj_get_logging_groups(p_includeFilter, p_excludeFilter);

The ``emaj_get_idle_groups()`` function returns the array of table groups in *IDLE* state::

   SELECT emaj.emaj_get_idle_groups(p_includeFilter, p_excludeFilter);

**Input Parameters**

- ``p_includeFilter`` (*TEXT*, optional): Regular expression that selects table group names. If the parameter is omitted or set to *NULL*, no include filtering is performed.
- ``p_excludeFilter`` (*TEXT*, optional): Regular expression that excludes table group names. If the parameter is omitted or set to *NULL*, no exclude filtering is performed.

**Notes**

Examples:

* ``emaj_get_groups('^APP1')`` selects table groups whose names start with *'APP1'*.
* ``emaj_get_logging_groups(NULL, 'excluded')`` returns all table groups already started, except those with a name containing the string *'excluded'*.
